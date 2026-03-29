package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dsn := fmt.Sprintf("root:root@tcp(%s:%s)/demo",
		envOr("WRITER_HOST", "127.0.0.1"),
		envOr("WRITER_PORT", "13306"),
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)

	ctx := context.Background()

	fmt.Println("================================================================")
	fmt.Println("  レースコンディション検証: commit前のdispatchは安全か？")
	fmt.Println("================================================================")
	fmt.Println()

	// --- Test 1: コミット後に読む（正常パターン） ---
	fmt.Println("--- Test 1: COMMIT後に別接続で読む（正常） ---")
	runTest(ctx, db, true)

	fmt.Println()

	// --- Test 2: コミット前に読む（レースコンディション） ---
	fmt.Println("--- Test 2: COMMIT前に別接続で読む（レースコンディション） ---")
	runTest(ctx, db, false)

	fmt.Println()

	// --- Test 3: 並行ジョブdispatchシミュレーション ---
	fmt.Println("--- Test 3: 実際のdispatch→commit順序をシミュレーション ---")
	runDispatchSimulation(ctx, db)

	fmt.Println()
	fmt.Println("================================================================")
	fmt.Println("  結論: commitの前にdispatchすると、ワーカーはデータを")
	fmt.Println("  見つけられない。afterCommitでdispatch順序を修正すべき。")
	fmt.Println("================================================================")
}

func runTest(ctx context.Context, db *sql.DB, commitFirst bool) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	// INSERT
	result, err := tx.ExecContext(ctx,
		"INSERT INTO traffic_log (tenant_id, payload) VALUES (?, ?)",
		"race-test", fmt.Sprintf("test-%d", time.Now().UnixNano()))
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	newID, _ := result.LastInsertId()
	fmt.Printf("  INSERT id=%d (未コミット)\n", newID)

	if commitFirst {
		// 正常: 先にcommit
		tx.Commit()
		fmt.Println("  COMMIT完了")

		// 別接続で読む
		exists := checkExists(ctx, db, newID)
		fmt.Printf("  別接続で読む → %s\n", visibilityStr(exists))
	} else {
		// レースコンディション: commit前に別接続で読む
		exists := checkExists(ctx, db, newID)
		fmt.Printf("  別接続で読む（未コミット）→ %s\n", visibilityStr(exists))

		tx.Commit()
		fmt.Println("  COMMIT完了")

		exists = checkExists(ctx, db, newID)
		fmt.Printf("  別接続で読む（コミット後）→ %s\n", visibilityStr(exists))
	}
}

func runDispatchSimulation(ctx context.Context, db *sql.DB) {
	const numJobs = 10
	var notFoundCount int
	var mu sync.Mutex

	for i := 0; i < numJobs; i++ {
		tx, _ := db.BeginTx(ctx, nil)

		// INSERT (未コミット)
		result, err := tx.ExecContext(ctx,
			"INSERT INTO traffic_log (tenant_id, payload) VALUES (?, ?)",
			"dispatch-test", fmt.Sprintf("job-%d", i))
		if err != nil {
			tx.Rollback()
			continue
		}
		newID, _ := result.LastInsertId()

		// "ジョブをdispatch" = 別goroutineで即座にSELECT（ワーカーの模擬）
		var wg sync.WaitGroup
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			exists := checkExists(ctx, db, id)
			if !exists {
				mu.Lock()
				notFoundCount++
				mu.Unlock()
			}
		}(newID)

		// ワーカーが先に動く隙間を作る
		time.Sleep(1 * time.Millisecond)

		// commit
		tx.Commit()
		wg.Wait()
	}

	fmt.Printf("  %d回のdispatch中、%d回がNOT FOUND（%.0f%%）\n",
		numJobs, notFoundCount, float64(notFoundCount)/float64(numJobs)*100)

	if notFoundCount > 0 {
		fmt.Println("  → commit前にワーカーがデータを読むと見つからない！")
	}

	// 比較: afterCommitパターン
	fmt.Println()
	fmt.Println("  [afterCommit版] commit後にdispatch:")
	notFoundCount = 0
	for i := 0; i < numJobs; i++ {
		tx, _ := db.BeginTx(ctx, nil)
		result, err := tx.ExecContext(ctx,
			"INSERT INTO traffic_log (tenant_id, payload) VALUES (?, ?)",
			"aftercommit-test", fmt.Sprintf("job-%d", i))
		if err != nil {
			tx.Rollback()
			continue
		}
		newID, _ := result.LastInsertId()

		// 先にcommit
		tx.Commit()

		// commit後にdispatch
		exists := checkExists(ctx, db, newID)
		if !exists {
			notFoundCount++
		}
	}
	fmt.Printf("  %d回のdispatch中、%d回がNOT FOUND（%.0f%%）\n",
		numJobs, notFoundCount, float64(notFoundCount)/float64(numJobs)*100)
	fmt.Println("  → commit後なら必ず見つかる")
}

func checkExists(ctx context.Context, db *sql.DB, id int64) bool {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM traffic_log WHERE id = ?", id).Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
}

func visibilityStr(exists bool) string {
	if exists {
		return "✅ 見つかった"
	}
	return "❌ NOT FOUND"
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
