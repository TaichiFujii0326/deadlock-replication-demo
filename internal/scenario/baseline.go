package scenario

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	internalmysql "deadlock-replication-demo/internal/mysql"
	"deadlock-replication-demo/internal/process"
)

func RunBaseline(ctx context.Context, writer, reader *sql.DB, monitor *internalmysql.ReplicationMonitor) error {
	slog.Info("phase_start", "phase", "baseline", "description", "Normal read-after-write consistency check")

	start := time.Now()
	const tenantID = "tenant-A"

	for i := 1; i <= 3; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Process 1: INSERT + logical delete on Writer
		r1 := process.RunProcess1(ctx, writer, tenantID, fmt.Sprintf("baseline-config-%d", i), 0)
		if r1.Err != nil {
			return fmt.Errorf("baseline process1 failed: %w", r1.Err)
		}
		slog.Info("process1_committed",
			"phase", "baseline",
			"tenant", tenantID,
			"new_id", r1.NewID,
			"deleted", r1.DeletedCount,
			"commit_ms", r1.CommitMs,
		)

		// Wait for replication
		time.Sleep(500 * time.Millisecond)

		// Process 2: Read from Replica
		lag := monitor.SecondsBehindSource()
		r2 := process.RunProcess2(ctx, reader, tenantID, r1.NewID)
		if r2.Err != nil {
			return fmt.Errorf("baseline process2 failed: %w", r2.Err)
		}
		slog.Info("process2_read",
			"phase", "baseline",
			"tenant", tenantID,
			"expected_id", r1.NewID,
			"actual_id", r2.ActualID,
			"match", r2.Match,
			"found", r2.Found,
			"query_ms", r2.QueryMs,
			"seconds_behind_source", lag,
		)

		if !r2.Match {
			slog.Warn("stale_read",
				"phase", "baseline",
				"expected_id", r1.NewID,
				"actual_id", r2.ActualID,
				"seconds_behind_source", lag,
			)
		}

		if i < 3 {
			time.Sleep(2 * time.Second)
		}
	}

	slog.Info("phase_end", "phase", "baseline", "duration_ms", time.Since(start).Milliseconds())
	return nil
}
