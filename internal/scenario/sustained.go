package scenario

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	internalmysql "deadlock-replication-demo/internal/mysql"
	"deadlock-replication-demo/internal/process"
)

type SustainedResult struct {
	BackgroundWrites   int64
	InjectionDeadlocks int
	InjectionCommits   int
	MaxLagMs           int64
	LagTimeline        []LagSample
	RecoveryMs         int64
}

type LagSample struct {
	ElapsedMs int64
	LagMs     int64
	Phase     string // "warmup", "injection", "recovery"
}

// RunSustained reproduces the production scenario:
//  1. Background traffic: 10 workers continuously writing to traffic_log (simulating normal app load)
//  2. After warmup, inject the problematic pattern (INSERT → API → DELETE)
//  3. Observe how the "dam burst" from lock wait resolution overwhelms Replica
//  4. Measure how long it takes for Replica to recover (or if it can't)
func RunSustained(ctx context.Context, writer, reader *sql.DB, monitor *internalmysql.ReplicationMonitor) (*SustainedResult, error) {
	const (
		tenantID         = "tenant-A"
		bgWorkers        = 5
		writeInterval    = 3 * time.Millisecond // ~330 ops/sec per worker × 5 = ~1,600 ops/sec (INSERT+UPDATE)
		warmupDuration   = 10 * time.Second
		observeDuration  = 60 * time.Second
		apiDelay         = 2 * time.Second
	)

	slog.Info("phase_start", "phase", "sustained",
		"description", "Background traffic + problematic Tx injection → observe prolonged lag")

	start := time.Now()
	result := &SustainedResult{}

	// Re-seed configs for the injection
	slog.Info("sustained_seeding", "phase", "sustained")
	if _, err := writer.ExecContext(ctx, "DELETE FROM configs WHERE tenant_id = ?", tenantID); err != nil {
		return nil, err
	}
	if _, err := writer.ExecContext(ctx, "TRUNCATE TABLE traffic_log"); err != nil {
		return nil, err
	}

	// Seed 100 rows (small, like production)
	for i := 1; i <= 100; i++ {
		if _, err := writer.ExecContext(ctx,
			"INSERT INTO configs (tenant_id, config_value) VALUES (?, ?)",
			tenantID, fmt.Sprintf("seed-%d", i)); err != nil {
			return nil, err
		}
	}
	slog.Info("sustained_seeded", "phase", "sustained", "config_rows", 100)

	// Wait for seed to replicate
	time.Sleep(3 * time.Second)

	// --- Start background traffic ---
	bgCtx, bgCancel := context.WithCancel(ctx)
	defer bgCancel()

	var bgWrites atomic.Int64
	var bgWg sync.WaitGroup

	// Realistic payload size (~4KB per write, simulating JSON configs/logs)
	padding := strings.Repeat("X", 4096)

	for w := 0; w < bgWorkers; w++ {
		bgWg.Add(1)
		go func(workerID int) {
			defer bgWg.Done()
			tid := fmt.Sprintf("bg-tenant-%d", workerID)
			for {
				select {
				case <-bgCtx.Done():
					return
				default:
				}
				// INSERT a new row
				res, err := writer.ExecContext(bgCtx,
					"INSERT INTO traffic_log (tenant_id, payload) VALUES (?, ?)",
					tid, padding)
				if err != nil {
					if bgCtx.Err() != nil {
						return
					}
					time.Sleep(writeInterval)
					continue
				}
				bgWrites.Add(1)

				// Also UPDATE the row (simulating typical read-modify-write)
				if newID, err := res.LastInsertId(); err == nil && newID > 1 {
					writer.ExecContext(bgCtx,
						"UPDATE traffic_log SET payload = ? WHERE id = ?",
						padding, newID-1)
					bgWrites.Add(1)
				}
				time.Sleep(writeInterval)
			}
		}(w)
	}

	slog.Info("sustained_bg_started", "phase", "sustained", "workers", bgWorkers)

	// --- Lag sampling goroutine ---
	samplerCtx, samplerCancel := context.WithCancel(ctx)
	defer samplerCancel()

	var lagMu sync.Mutex

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-samplerCtx.Done():
				return
			case <-ticker.C:
				lagMs := monitor.LagMs()
				elapsed := time.Since(start).Milliseconds()

				lagMu.Lock()
				if lagMs > result.MaxLagMs {
					result.MaxLagMs = lagMs
				}

				phase := "warmup"
				if elapsed > warmupDuration.Milliseconds() && elapsed < (warmupDuration+10*time.Second).Milliseconds() {
					phase = "injection"
				} else if elapsed > (warmupDuration + 10*time.Second).Milliseconds() {
					phase = "recovery"
				}

				result.LagTimeline = append(result.LagTimeline, LagSample{
					ElapsedMs: elapsed,
					LagMs:     lagMs,
					Phase:     phase,
				})
				lagMu.Unlock()
			}
		}
	}()

	// --- Phase 1: Warmup — let background traffic establish a baseline ---
	slog.Info("sustained_warmup", "phase", "sustained", "duration_seconds", warmupDuration.Seconds())
	select {
	case <-ctx.Done():
		bgCancel()
		bgWg.Wait()
		return result, ctx.Err()
	case <-time.After(warmupDuration):
	}

	warmupWrites := bgWrites.Load()
	slog.Info("sustained_warmup_done", "phase", "sustained",
		"bg_writes", warmupWrites,
		"lag_ms", monitor.LagMs())

	// --- Phase 2: Inject the problematic pattern ---
	slog.Info("sustained_injection", "phase", "sustained",
		"description", "Injecting INSERT → API(2s) → DELETE with background traffic running")

	// Run 3 injections with short gaps (simulating periodic batch job)
	for i := 0; i < 3; i++ {
		var injWg sync.WaitGroup
		type injResult struct {
			r   *process.Process1Result
			idx int
		}
		injResults := make(chan injResult, 2)

		// 2 concurrent problematic Txs
		for j := 0; j < 2; j++ {
			injWg.Add(1)
			go func(idx int) {
				defer injWg.Done()
				r := process.RunProcess1(ctx, writer, tenantID,
					fmt.Sprintf("inject-%d-%d", i, idx), apiDelay)
				injResults <- injResult{r: r, idx: idx}
			}(j)
		}
		injWg.Wait()
		close(injResults)

		for ir := range injResults {
			if ir.r.Err != nil {
				if ir.r.Deadlocked {
					result.InjectionDeadlocks++
					slog.Warn("sustained_deadlock", "phase", "sustained",
						"injection", i, "tx", ir.idx,
						"lag_ms", monitor.LagMs(),
						"bg_writes", bgWrites.Load())
				}
				continue
			}
			result.InjectionCommits++
			slog.Info("sustained_committed", "phase", "sustained",
				"injection", i, "tx", ir.idx,
				"new_id", ir.r.NewID,
				"deleted", ir.r.DeletedCount,
				"commit_ms", ir.r.CommitMs,
				"lag_ms", monitor.LagMs(),
				"bg_writes", bgWrites.Load())
		}

		// Short gap between injections
		time.Sleep(2 * time.Second)
	}

	slog.Info("sustained_injection_done", "phase", "sustained",
		"deadlocks", result.InjectionDeadlocks,
		"commits", result.InjectionCommits,
		"lag_ms", monitor.LagMs())

	// --- Phase 3: Observe recovery (or lack thereof) ---
	slog.Info("sustained_recovery_observe", "phase", "sustained",
		"description", "Background traffic continues. Watching if Replica can recover.")

	recoveryStart := time.Now()
	recovered := false

	for time.Since(recoveryStart) < observeDuration {
		select {
		case <-ctx.Done():
			bgCancel()
			bgWg.Wait()
			return result, ctx.Err()
		case <-time.After(2 * time.Second):
		}

		lagMs := monitor.LagMs()
		slog.Info("sustained_lag_check", "phase", "sustained",
			"elapsed_since_injection_ms", time.Since(recoveryStart).Milliseconds(),
			"lag_ms", lagMs,
			"bg_writes_total", bgWrites.Load())

		// Check if a recent write is visible on Replica
		var latestBg int64
		writer.QueryRowContext(ctx, "SELECT MAX(id) FROM traffic_log").Scan(&latestBg)
		if latestBg > 0 {
			exists, _ := process.CheckIDExists(ctx, reader, latestBg)
			if !exists {
				slog.Warn("sustained_stale_read", "phase", "sustained",
					"expected_traffic_log_id", latestBg,
					"lag_ms", lagMs)
			}
		}

		if lagMs >= 0 && lagMs < 100 {
			if !recovered {
				recovered = true
				result.RecoveryMs = time.Since(recoveryStart).Milliseconds()
				slog.Info("sustained_recovered", "phase", "sustained",
					"recovery_ms", result.RecoveryMs,
					"lag_ms", lagMs)
				// Keep observing for a bit to confirm it stays recovered
			}
		} else {
			recovered = false
			result.RecoveryMs = 0
		}
	}

	// Stop background traffic
	bgCancel()
	bgWg.Wait()

	result.BackgroundWrites = bgWrites.Load()

	// Log timeline summary (every 5th sample)
	slog.Info("sustained_timeline_summary", "phase", "sustained")
	lagMu.Lock()
	for i, s := range result.LagTimeline {
		if i%5 == 0 || s.Phase == "injection" {
			slog.Info("sustained_timeline",
				"elapsed_ms", s.ElapsedMs,
				"lag_ms", s.LagMs,
				"phase", s.Phase)
		}
	}
	lagMu.Unlock()

	slog.Info("phase_end", "phase", "sustained",
		"duration_ms", time.Since(start).Milliseconds(),
		"bg_writes_total", result.BackgroundWrites,
		"injection_deadlocks", result.InjectionDeadlocks,
		"injection_commits", result.InjectionCommits,
		"max_lag_ms", result.MaxLagMs,
		"recovery_ms", result.RecoveryMs,
	)

	return result, nil
}
