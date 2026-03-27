package process

import (
	"context"
	"database/sql"
	"time"
)

// --- Pattern B+C: API First + SELECT FOR UPDATE ---
// API call → Tx: SELECT FOR UPDATE → INSERT → UPDATE → COMMIT
// API is outside lock window + deterministic lock order = no deadlock + short lock.
func RunProcess1_APIFirst_SFU(ctx context.Context, db *sql.DB, tenantID, value string, apiDelay time.Duration) *Process1Result {
	start := time.Now()
	result := &Process1Result{}

	if apiDelay > 0 {
		time.Sleep(apiDelay)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.Err = err
		return result
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM configs WHERE tenant_id = ? AND is_deleted = 0 FOR UPDATE`, tenantID)
	if err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	rows.Close()

	insResult, err := tx.ExecContext(ctx,
		"INSERT INTO configs (tenant_id, config_value) VALUES (?, ?)", tenantID, value)
	if err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	newID, _ := insResult.LastInsertId()
	result.NewID = newID

	delResult, err := tx.ExecContext(ctx,
		`UPDATE configs SET is_deleted = 1, deleted_at = NOW(6)
		 WHERE tenant_id = ? AND id != ? AND is_deleted = 0`, tenantID, newID)
	if err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	result.DeletedCount, _ = delResult.RowsAffected()

	if err := tx.Commit(); err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	result.CommitMs = time.Since(start).Milliseconds()
	return result
}

// --- Pattern A+C: API Outside Tx + SELECT FOR UPDATE ---
// Tx1: INSERT → COMMIT → API → Tx2: SELECT FOR UPDATE → UPDATE → COMMIT
// Maximum safety: short locks + deterministic order + API completely outside.
func RunProcess1_APIOutside_SFU(ctx context.Context, db *sql.DB, tenantID, value string, apiDelay time.Duration) *Process1Result {
	start := time.Now()
	result := &Process1Result{}

	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.Err = err
		return result
	}
	insResult, err := tx1.ExecContext(ctx,
		"INSERT INTO configs (tenant_id, config_value) VALUES (?, ?)", tenantID, value)
	if err != nil {
		tx1.Rollback()
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	newID, _ := insResult.LastInsertId()
	result.NewID = newID
	if err := tx1.Commit(); err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}

	if apiDelay > 0 {
		time.Sleep(apiDelay)
	}

	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.Err = err
		return result
	}
	defer tx2.Rollback()

	rows, err := tx2.QueryContext(ctx,
		`SELECT id FROM configs WHERE tenant_id = ? AND is_deleted = 0 FOR UPDATE`, tenantID)
	if err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	rows.Close()

	delResult, err := tx2.ExecContext(ctx,
		`UPDATE configs SET is_deleted = 1, deleted_at = NOW(6)
		 WHERE tenant_id = ? AND id != ? AND is_deleted = 0`, tenantID, newID)
	if err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	result.DeletedCount, _ = delResult.RowsAffected()

	if err := tx2.Commit(); err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	result.CommitMs = time.Since(start).Milliseconds()
	return result
}
