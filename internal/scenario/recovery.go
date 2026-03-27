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

func RunRecovery(ctx context.Context, writer, reader *sql.DB, monitor *internalmysql.ReplicationMonitor) error {
	slog.Info("phase_start", "phase", "recovery", "description", "Monitoring replication catch-up after storm")

	start := time.Now()
	const (
		tenantID = "tenant-A"
		timeout  = 30 * time.Second
	)
	deadline := time.Now().Add(timeout)

	cycle := 1
	consecutiveOK := 0
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Single Process 1 execution
		r1 := process.RunProcess1(ctx, writer, tenantID, fmt.Sprintf("recovery-config-%d", cycle), 0)
		if r1.Err != nil {
			slog.Error("recovery_process1_failed", "error", r1.Err)
			time.Sleep(2 * time.Second)
			cycle++
			consecutiveOK = 0
			continue
		}
		slog.Info("process1_committed",
			"phase", "recovery",
			"tenant", tenantID,
			"new_id", r1.NewID,
			"deleted", r1.DeletedCount,
			"commit_ms", r1.CommitMs,
		)

		time.Sleep(500 * time.Millisecond)

		// Process 2: Read from Replica
		lag := monitor.SecondsBehindSource()
		r2 := process.RunProcess2(ctx, reader, tenantID, r1.NewID)
		if r2.Err != nil {
			slog.Error("recovery_process2_failed", "error", r2.Err)
			time.Sleep(2 * time.Second)
			cycle++
			consecutiveOK = 0
			continue
		}

		slog.Info("process2_read",
			"phase", "recovery",
			"tenant", tenantID,
			"expected_id", r1.NewID,
			"actual_id", r2.ActualID,
			"match", r2.Match,
			"found", r2.Found,
			"seconds_behind_source", lag,
		)

		if !r2.Match {
			consecutiveOK = 0
			slog.Warn("stale_read",
				"phase", "recovery",
				"expected_id", r1.NewID,
				"actual_id", r2.ActualID,
				"seconds_behind_source", lag,
			)
		} else {
			consecutiveOK++
		}

		// Recovered: 2 consecutive successful reads
		if consecutiveOK >= 2 {
			slog.Info("phase_end",
				"phase", "recovery",
				"duration_ms", time.Since(start).Milliseconds(),
				"cycles_until_recovered", cycle,
				"seconds_behind_source", lag,
			)
			return nil
		}

		cycle++
		time.Sleep(2 * time.Second)
	}

	slog.Warn("recovery_timeout",
		"phase", "recovery",
		"duration_ms", time.Since(start).Milliseconds(),
		"final_lag", monitor.SecondsBehindSource(),
	)
	return fmt.Errorf("recovery did not complete within %v", timeout)
}
