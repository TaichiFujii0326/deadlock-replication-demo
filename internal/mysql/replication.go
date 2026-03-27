package mysql

import (
	"context"
	"database/sql"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"
)

type ReplicationMonitor struct {
	db                  *sql.DB
	secondsBehindSource atomic.Int64
	lagMs               atomic.Int64 // millisecond precision lag
	running             atomic.Bool
}

func NewReplicationMonitor(db *sql.DB) *ReplicationMonitor {
	m := &ReplicationMonitor{db: db}
	m.secondsBehindSource.Store(-1)
	m.lagMs.Store(-1)
	return m
}

func (m *ReplicationMonitor) SecondsBehindSource() int64 {
	return m.secondsBehindSource.Load()
}

func (m *ReplicationMonitor) LagMs() int64 {
	return m.lagMs.Load()
}

func (m *ReplicationMonitor) Start(ctx context.Context) {
	m.running.Store(true)
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				m.running.Store(false)
				return
			case <-ticker.C:
				m.poll(ctx)
			}
		}
	}()
}

func (m *ReplicationMonitor) poll(ctx context.Context) {
	// 1. Millisecond-precision lag from performance_schema
	var lagMs sql.NullFloat64
	err := m.db.QueryRowContext(ctx, `
		SELECT TIMESTAMPDIFF(MICROSECOND,
			LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP,
			NOW(6)) / 1000
		FROM performance_schema.replication_applier_status_by_worker
		WHERE CHANNEL_NAME = '' AND SERVICE_STATE = 'ON'
		LIMIT 1
	`).Scan(&lagMs)

	if err == nil && lagMs.Valid {
		ms := int64(lagMs.Float64)
		m.lagMs.Store(ms)
		m.secondsBehindSource.Store(ms / 1000)
	}

	// 2. Relay log queue depth from SHOW REPLICA STATUS
	m.pollReplicaStatus(ctx)
}

func (m *ReplicationMonitor) pollReplicaStatus(ctx context.Context) {
	rows, err := m.db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		return
	}

	cols, err := rows.Columns()
	if err != nil {
		return
	}

	rawValues := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range rawValues {
		scanArgs[i] = &rawValues[i]
	}
	if err := rows.Scan(scanArgs...); err != nil {
		return
	}

	data := make(map[string]string, len(cols))
	for i, col := range cols {
		if rawValues[i] != nil {
			data[col] = string(rawValues[i])
		}
	}

	// Parse key fields
	readPos, _ := strconv.ParseInt(data["Read_Source_Log_Pos"], 10, 64)
	execPos, _ := strconv.ParseInt(data["Exec_Source_Log_Pos"], 10, 64)
	relayLogSpace, _ := strconv.ParseInt(data["Relay_Log_Space"], 10, 64)

	// Queue gap: how many bytes of binlog have been received but not yet applied
	queueGap := readPos - execPos

	// Fallback lag if performance_schema didn't work
	if m.lagMs.Load() < 0 {
		if v, ok := data["Seconds_Behind_Source"]; ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				m.secondsBehindSource.Store(n)
				m.lagMs.Store(n * 1000)
			}
		}
	}

	slog.Info("replication_status",
		"lag_ms", m.lagMs.Load(),
		"seconds_behind_source", m.secondsBehindSource.Load(),
		"relay_log_queue_bytes", queueGap,
		"relay_log_space", relayLogSpace,
		"read_pos", readPos,
		"exec_pos", execPos,
	)
}
