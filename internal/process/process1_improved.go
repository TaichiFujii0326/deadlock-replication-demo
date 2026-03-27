package process

import (
	"context"
	"database/sql"
	"time"
)

// --- Pattern A: API Outside Transaction (Transaction Split) ---
// Tx1: INSERT (pending) → COMMIT → API call → Tx2: UPDATE (logical delete) → COMMIT
// Lock hold time: ~ms per tx. Deadlock: nearly zero. Replication lag: minimal.
func RunProcess1_APIOutside(ctx context.Context, db *sql.DB, tenantID, value string, apiDelay time.Duration) *Process1Result {
	start := time.Now()
	result := &Process1Result{}

	// Tx1: INSERT only
	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.Err = err
		return result
	}
	insResult, err := tx1.ExecContext(ctx,
		"INSERT INTO configs (tenant_id, config_value) VALUES (?, ?)",
		tenantID, value)
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

	// API call OUTSIDE transaction — no locks held
	if apiDelay > 0 {
		time.Sleep(apiDelay)
	}

	// Tx2: Logical delete
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.Err = err
		return result
	}
	delResult, err := tx2.ExecContext(ctx,
		`UPDATE configs SET is_deleted = 1, deleted_at = NOW(6)
		 WHERE tenant_id = ? AND id != ? AND is_deleted = 0`,
		tenantID, newID)
	if err != nil {
		tx2.Rollback()
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

// --- Pattern B: API First (Reorder) ---
// API call → Tx: INSERT + UPDATE → COMMIT
// Lock hold time: ~ms. Deadlock: greatly reduced. Simple implementation.
func RunProcess1_APIFirst(ctx context.Context, db *sql.DB, tenantID, value string, apiDelay time.Duration) *Process1Result {
	start := time.Now()
	result := &Process1Result{}

	// API call BEFORE transaction — no locks held
	if apiDelay > 0 {
		time.Sleep(apiDelay)
	}

	// Single transaction: INSERT + DELETE (fast, no API wait inside)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.Err = err
		return result
	}
	defer tx.Rollback()

	insResult, err := tx.ExecContext(ctx,
		"INSERT INTO configs (tenant_id, config_value) VALUES (?, ?)",
		tenantID, value)
	if err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	newID, _ := insResult.LastInsertId()
	result.NewID = newID

	delResult, err := tx.ExecContext(ctx,
		`UPDATE configs SET is_deleted = 1, deleted_at = NOW(6)
		 WHERE tenant_id = ? AND id != ? AND is_deleted = 0`,
		tenantID, newID)
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

// --- Pattern C: SELECT FOR UPDATE (Pessimistic Lock) ---
// Tx: SELECT FOR UPDATE → INSERT → API → UPDATE → COMMIT
// Deadlock: avoided (deterministic lock order). BUT lock wait remains (API wait inside tx).
// This pattern shows that "fixing deadlock" != "fixing the real problem".
//
func RunProcess1_SelectForUpdate(ctx context.Context, db *sql.DB, tenantID, value string, apiDelay time.Duration) *Process1Result {
	start := time.Now()
	result := &Process1Result{}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.Err = err
		return result
	}
	defer tx.Rollback()

	// Step 1: Lock all active rows first with SELECT FOR UPDATE
	// This establishes a deterministic lock order, preventing deadlock.
	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM configs
		 WHERE tenant_id = ? AND is_deleted = 0
		 FOR UPDATE`,
		tenantID)
	if err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	rows.Close()

	// Step 2: INSERT
	insResult, err := tx.ExecContext(ctx,
		"INSERT INTO configs (tenant_id, config_value) VALUES (?, ?)",
		tenantID, value)
	if err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	newID, _ := insResult.LastInsertId()
	result.NewID = newID

	// Step 3: API call — still inside transaction, still holding all locks
	if apiDelay > 0 {
		time.Sleep(apiDelay)
	}

	// Step 4: Logical delete
	delResult, err := tx.ExecContext(ctx,
		`UPDATE configs SET is_deleted = 1, deleted_at = NOW(6)
		 WHERE tenant_id = ? AND id != ? AND is_deleted = 0`,
		tenantID, newID)
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
