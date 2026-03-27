package scenario

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	internalmysql "deadlock-replication-demo/internal/mysql"
	"deadlock-replication-demo/internal/process"
)

type StormResult struct {
	StaleReads int64
	MaxLag     int64
	Deadlocks  int64
	Commits    int64
}

func RunStorm(ctx context.Context, writer, reader *sql.DB, monitor *internalmysql.ReplicationMonitor) (*StormResult, error) {
	const (
		tenantID = "tenant-A"
		workers  = 30
		duration = 30 * time.Second
	)

	slog.Info("phase_start", "phase", "concurrent-storm",
		"description", "Concurrent process1 + immediate read-after-write check on replica",
		"workers", workers)

	start := time.Now()
	result := &StormResult{}

	stormCtx, stormCancel := context.WithCancel(ctx)
	defer stormCancel()

	var (
		deadlocks  atomic.Int64
		commits    atomic.Int64
		staleReads atomic.Int64
		maxLag     atomic.Int64
		wg         sync.WaitGroup
	)

	// Launch concurrent workers.
	// Each worker: Process1 (write) → Process2 (immediate read from replica) → check match
	// This mirrors production: write to writer, then read from replica in the next API call.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-stormCtx.Done():
					return
				default:
				}

				// Step 1: Process 1 on Writer
				r1 := process.RunProcess1(stormCtx, writer, tenantID,
					fmt.Sprintf("storm-w%d-%d", workerID, time.Now().UnixNano()), 0)

				if r1.Err != nil {
					if r1.Deadlocked {
						deadlocks.Add(1)
						slog.Warn("process1_deadlock",
							"phase", "concurrent-storm",
							"tenant", tenantID,
							"worker", workerID,
							"error", r1.Err.Error(),
						)
					} else if stormCtx.Err() == nil {
						slog.Error("process1_error",
							"phase", "concurrent-storm",
							"worker", workerID,
							"error", r1.Err.Error(),
						)
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}

				commits.Add(1)
				slog.Info("process1_committed",
					"phase", "concurrent-storm",
					"tenant", tenantID,
					"worker", workerID,
					"new_id", r1.NewID,
					"deleted", r1.DeletedCount,
					"commit_ms", r1.CommitMs,
				)

				// Step 2: Immediately read from Replica (same goroutine, no delay)
				// In production: user updates config, next request reads from replica
				lagMs := monitor.LagMs()
				lagSec := monitor.SecondsBehindSource()
				if lagMs > maxLag.Load() {
					maxLag.Store(lagMs)
				}

				// Check 1: Does my specific ID exist on the replica yet?
				exists, err := process.CheckIDExists(stormCtx, reader, r1.NewID)
				if err != nil {
					if stormCtx.Err() == nil {
						slog.Error("check_exists_error", "worker", workerID, "error", err.Error())
					}
					continue
				}
				if !exists {
					staleReads.Add(1)
					slog.Warn("stale_read",
						"phase", "concurrent-storm",
						"tenant", tenantID,
						"worker", workerID,
						"expected_id", r1.NewID,
						"type", "not_replicated",
						"lag_ms", lagMs,
						"seconds_behind_source", lagSec,
					)
					continue
				}

				// Check 2: Is the latest active row correct?
				r2 := process.RunProcess2(stormCtx, reader, tenantID, r1.NewID)
				if r2.Err != nil {
					if stormCtx.Err() == nil {
						slog.Error("process2_error", "worker", workerID, "error", r2.Err.Error())
					}
					continue
				}

				if !r2.Found {
					staleReads.Add(1)
					slog.Warn("stale_read",
						"phase", "concurrent-storm",
						"tenant", tenantID,
						"worker", workerID,
						"expected_id", r1.NewID,
						"actual_id", 0,
						"type", "no_active_row",
						"lag_ms", lagMs,
					)
				} else if r2.ActualID < r1.NewID {
					staleReads.Add(1)
					slog.Warn("stale_read",
						"phase", "concurrent-storm",
						"tenant", tenantID,
						"worker", workerID,
						"expected_id", r1.NewID,
						"actual_id", r2.ActualID,
						"type", "outdated",
						"lag_ms", lagMs,
					)
				}
			}
		}(w)
	}

	// Stats reporter
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stormCtx.Done():
				return
			case <-ticker.C:
				slog.Info("storm_stats",
					"phase", "concurrent-storm",
					"deadlocks", deadlocks.Load(),
					"commits", commits.Load(),
					"stale_reads", staleReads.Load(),
					"seconds_behind_source", monitor.SecondsBehindSource(),
				)
			}
		}
	}()

	// Run for the specified duration
	select {
	case <-ctx.Done():
		stormCancel()
		wg.Wait()
		return result, ctx.Err()
	case <-time.After(duration):
	}

	stormCancel()
	wg.Wait()

	result.Deadlocks = deadlocks.Load()
	result.Commits = commits.Load()
	result.StaleReads = staleReads.Load()
	result.MaxLag = maxLag.Load()

	slog.Info("phase_end",
		"phase", "concurrent-storm",
		"duration_ms", time.Since(start).Milliseconds(),
		"deadlocks", result.Deadlocks,
		"commits", result.Commits,
		"stale_reads", result.StaleReads,
		"max_lag", result.MaxLag,
	)

	return result, nil
}
