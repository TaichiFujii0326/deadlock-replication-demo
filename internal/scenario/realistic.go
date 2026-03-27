package scenario

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	internalmysql "deadlock-replication-demo/internal/mysql"
	"deadlock-replication-demo/internal/process"
)

type RealisticResult struct {
	DeadlockOccurred bool
	SurvivorID       int64
	StaleReadCount   int
	MaxLagSeconds    int64
	StaleDurationMs  int64
}

// RunRealistic reproduces the production scenario:
//  1. Large SEED data exists (10,000 rows)
//  2. Two Process1 run concurrently → one deadlock
//  3. The survivor commits a huge transaction (INSERT + UPDATE 10,000 rows)
//  4. Replica takes a long time to apply the large binlog event
//  5. During that time, reads from the replica return stale data
func RunRealistic(ctx context.Context, writer, reader *sql.DB, monitor *internalmysql.ReplicationMonitor) (*RealisticResult, error) {
	const tenantID = "tenant-A"

	slog.Info("phase_start", "phase", "realistic",
		"description", "Production scenario: 2 concurrent transactions, 1 deadlock, large binlog → long stale read window")

	start := time.Now()
	result := &RealisticResult{}

	// --- Step 0: Re-seed 10,000 active rows ---
	slog.Info("realistic_step", "phase", "realistic", "step", 0,
		"description", "Re-seeding 10,000 active rows for tenant-A")

	if _, err := writer.ExecContext(ctx, "DELETE FROM configs WHERE tenant_id = ?", tenantID); err != nil {
		return result, fmt.Errorf("truncate failed: %w", err)
	}
	// 1KB padding per row — makes binlog events large, slowing replica applier
	padding := ""
	for len(padding) < 1024 {
		padding += "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	}
	padding = padding[:1024]

	const seedRows = 50000
	const batchSize = 500
	for batch := 0; batch < seedRows/batchSize; batch++ {
		query := "INSERT INTO configs (tenant_id, config_value) VALUES "
		for i := 0; i < batchSize; i++ {
			if i > 0 {
				query += ","
			}
			query += fmt.Sprintf("('%s','%s-reseed-%d')", tenantID, padding, batch*batchSize+i+1)
		}
		if _, err := writer.ExecContext(ctx, query); err != nil {
			return result, fmt.Errorf("seed failed at batch %d: %w", batch, err)
		}
	}
	slog.Info("realistic_seeded", "phase", "realistic", "rows", seedRows,
		"seconds_behind_source", monitor.SecondsBehindSource())

	// NO wait — run immediately while replica is still processing seed data.
	// The survivor's large binlog event (10,000 row UPDATE) enters the relay log
	// BEHIND the seed events, maximizing the stale read window.

	// --- Step 1: Run two Process1 concurrently → expect one deadlock ---
	slog.Info("realistic_step", "phase", "realistic", "step", 1,
		"description", "Two concurrent Process1 executions — replica still processing seed data")

	type txResult struct {
		r1  *process.Process1Result
		idx int
	}

	var wg sync.WaitGroup
	results := make(chan txResult, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r := process.RunProcess1(ctx, writer, tenantID,
				fmt.Sprintf("realistic-tx%d-%d", idx, time.Now().UnixNano()), 1*time.Second)
			results <- txResult{r1: r, idx: idx}
		}(i)
	}

	wg.Wait()
	close(results)

	var survivor *process.Process1Result
	for tr := range results {
		if tr.r1.Err != nil {
			if tr.r1.Deadlocked {
				result.DeadlockOccurred = true
				slog.Warn("process1_deadlock",
					"phase", "realistic",
					"tx", tr.idx,
					"error", tr.r1.Err.Error(),
				)
			} else {
				slog.Error("process1_error",
					"phase", "realistic",
					"tx", tr.idx,
					"error", tr.r1.Err.Error(),
				)
			}
			continue
		}
		survivor = tr.r1
		result.SurvivorID = tr.r1.NewID
		slog.Info("process1_committed",
			"phase", "realistic",
			"tx", tr.idx,
			"new_id", tr.r1.NewID,
			"deleted", tr.r1.DeletedCount,
			"commit_ms", tr.r1.CommitMs,
		)
	}

	if survivor == nil {
		return result, fmt.Errorf("both transactions failed, no survivor")
	}

	// --- Step 2: Continuously read from Replica until the survivor's ID is visible ---
	slog.Info("realistic_step", "phase", "realistic", "step", 2,
		"description", "Polling replica for survivor's ID — measuring stale read window")

	pollStart := time.Now()
	timeout := 60 * time.Second
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-ticker.C:
		}

		elapsed := time.Since(pollStart)
		if elapsed > timeout {
			slog.Warn("realistic_timeout", "phase", "realistic",
				"stale_duration_ms", elapsed.Milliseconds())
			break
		}

		lag := monitor.SecondsBehindSource()
		if lag > result.MaxLagSeconds {
			result.MaxLagSeconds = lag
		}

		exists, err := process.CheckIDExists(ctx, reader, result.SurvivorID)
		if err != nil {
			slog.Error("check_exists_error", "phase", "realistic", "error", err)
			continue
		}

		if !exists {
			result.StaleReadCount++
			if result.StaleReadCount%10 == 1 {
				slog.Warn("stale_read",
					"phase", "realistic",
					"expected_id", result.SurvivorID,
					"type", "not_replicated",
					"elapsed_ms", elapsed.Milliseconds(),
					"seconds_behind_source", lag,
				)
			}
			continue
		}

		// ID found — stale read window is over
		result.StaleDurationMs = elapsed.Milliseconds()
		slog.Info("realistic_replicated",
			"phase", "realistic",
			"survivor_id", result.SurvivorID,
			"stale_read_count", result.StaleReadCount,
			"stale_duration_ms", result.StaleDurationMs,
			"seconds_behind_source", lag,
		)
		break
	}

	// --- Step 3: Verify Process2 now returns the correct active config ---
	r2 := process.RunProcess2(ctx, reader, tenantID, result.SurvivorID)
	if r2.Err == nil {
		slog.Info("process2_final_check",
			"phase", "realistic",
			"expected_id", result.SurvivorID,
			"actual_id", r2.ActualID,
			"match", r2.Match,
			"seconds_behind_source", monitor.SecondsBehindSource(),
		)
	}

	slog.Info("phase_end",
		"phase", "realistic",
		"duration_ms", time.Since(start).Milliseconds(),
		"deadlock_occurred", result.DeadlockOccurred,
		"survivor_id", result.SurvivorID,
		"stale_reads", result.StaleReadCount,
		"stale_duration_ms", result.StaleDurationMs,
		"max_lag_seconds", result.MaxLagSeconds,
	)

	return result, nil
}
