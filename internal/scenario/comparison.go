package scenario

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	internalmysql "deadlock-replication-demo/internal/mysql"
	"deadlock-replication-demo/internal/process"
)

type PatternResult struct {
	Name           string
	DeadlockCount  int
	CommitCount    int
	AvgCommitMs    int64
	StaleReadCount int
	StaleDurationMs int64
	MaxLagMs       int64
}

type ComparisonResult struct {
	Patterns []PatternResult
}

// RunComparison runs the same test (2 concurrent txs, 50k rows, 1s API delay)
// against 4 patterns and compares the results.
func RunComparison(ctx context.Context, writer, reader *sql.DB, monitor *internalmysql.ReplicationMonitor) (*ComparisonResult, error) {
	slog.Info("phase_start", "phase", "comparison",
		"description", "Before/After: 4 patterns with same conditions")

	const tenantID = "tenant-A"
	const apiDelay = 2 * time.Second

	type runFunc func(ctx context.Context, db *sql.DB, tenantID, value string, apiDelay time.Duration) *process.Process1Result

	patterns := []struct {
		name string
		fn   runFunc
	}{
		{"Before: API in Tx", process.RunProcess1},
		{"A: API Outside Tx", process.RunProcess1_APIOutside},
		{"B: API First", process.RunProcess1_APIFirst},
		{"C: SELECT FOR UPDATE", process.RunProcess1_SelectForUpdate},
		{"B+C: API First + SFU", process.RunProcess1_APIFirst_SFU},
		{"A+C: API Outside + SFU", process.RunProcess1_APIOutside_SFU},
	}

	result := &ComparisonResult{}

	for _, p := range patterns {
		slog.Info("comparison_pattern_start", "phase", "comparison", "pattern", p.name)

		pr, err := runPattern(ctx, writer, reader, monitor, tenantID, apiDelay, p.name, p.fn)
		if err != nil {
			slog.Error("comparison_pattern_failed", "pattern", p.name, "error", err)
			// Record what we have and continue
			result.Patterns = append(result.Patterns, PatternResult{Name: p.name})
			continue
		}
		result.Patterns = append(result.Patterns, *pr)

		slog.Info("comparison_pattern_result",
			"phase", "comparison",
			"pattern", p.name,
			"deadlocks", pr.DeadlockCount,
			"commits", pr.CommitCount,
			"avg_commit_ms", pr.AvgCommitMs,
			"stale_reads", pr.StaleReadCount,
			"stale_duration_ms", pr.StaleDurationMs,
			"max_lag_ms", pr.MaxLagMs,
		)

		// Wait for replica to catch up between patterns
		time.Sleep(3 * time.Second)
	}

	// Log final comparison table
	slog.Info("comparison_summary", "phase", "comparison")
	for _, pr := range result.Patterns {
		slog.Info("comparison_row",
			"pattern", pr.Name,
			"deadlocks", pr.DeadlockCount,
			"commits", pr.CommitCount,
			"avg_commit_ms", pr.AvgCommitMs,
			"stale_reads", pr.StaleReadCount,
			"stale_duration_ms", pr.StaleDurationMs,
			"max_lag_ms", pr.MaxLagMs,
		)
	}

	slog.Info("phase_end", "phase", "comparison")
	return result, nil
}

func runPattern(
	ctx context.Context,
	writer, reader *sql.DB,
	monitor *internalmysql.ReplicationMonitor,
	tenantID string,
	apiDelay time.Duration,
	patternName string,
	fn func(context.Context, *sql.DB, string, string, time.Duration) *process.Process1Result,
) (*PatternResult, error) {
	pr := &PatternResult{Name: patternName}

	// Re-seed 50,000 rows (matching all test conditions)
	if _, err := writer.ExecContext(ctx, "DELETE FROM configs WHERE tenant_id = ?", tenantID); err != nil {
		return nil, fmt.Errorf("truncate failed: %w", err)
	}

	padding := strings.Repeat("X", 1024)
	for batch := 0; batch < 100; batch++ {
		query := "INSERT INTO configs (tenant_id, config_value) VALUES "
		for i := 0; i < 500; i++ {
			if i > 0 {
				query += ","
			}
			query += fmt.Sprintf("('%s','%s-seed-%d')", tenantID, padding, batch*500+i+1)
		}
		if _, err := writer.ExecContext(ctx, query); err != nil {
			return nil, fmt.Errorf("seed failed at batch %d: %w", batch, err)
		}
	}
	slog.Info("comparison_seeded", "pattern", patternName, "rows", 50000)

	// Wait for seed replication
	time.Sleep(3 * time.Second)

	// Run 2 concurrent txs
	var wg sync.WaitGroup
	type txResult struct {
		r   *process.Process1Result
		idx int
	}
	results := make(chan txResult, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r := fn(ctx, writer, tenantID,
				fmt.Sprintf("%s-tx%d-%d", patternName, idx, time.Now().UnixNano()),
				apiDelay)
			results <- txResult{r: r, idx: idx}
		}(i)
	}
	wg.Wait()
	close(results)

	var survivorID int64
	var totalCommitMs int64
	for tr := range results {
		if tr.r.Err != nil {
			if tr.r.Deadlocked {
				pr.DeadlockCount++
				slog.Warn("comparison_deadlock", "pattern", patternName, "tx", tr.idx)
			}
			continue
		}
		pr.CommitCount++
		totalCommitMs += tr.r.CommitMs
		survivorID = tr.r.NewID
		slog.Info("comparison_committed", "pattern", patternName,
			"tx", tr.idx, "new_id", tr.r.NewID,
			"deleted", tr.r.DeletedCount, "commit_ms", tr.r.CommitMs)
	}

	if pr.CommitCount > 0 {
		pr.AvgCommitMs = totalCommitMs / int64(pr.CommitCount)
	}

	if survivorID == 0 {
		slog.Warn("comparison_no_survivor", "pattern", patternName)
		return pr, nil
	}

	// Measure stale read window
	pollStart := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return pr, ctx.Err()
		case <-ticker.C:
		}

		if time.Since(pollStart) > 30*time.Second {
			break
		}

		lagMs := monitor.LagMs()
		if lagMs > pr.MaxLagMs {
			pr.MaxLagMs = lagMs
		}

		exists, err := process.CheckIDExists(ctx, reader, survivorID)
		if err != nil {
			continue
		}

		if !exists {
			pr.StaleReadCount++
			continue
		}

		pr.StaleDurationMs = time.Since(pollStart).Milliseconds()
		break
	}

	return pr, nil
}
