package process

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

type Process1Result struct {
	NewID        int64
	DeletedCount int64
	CommitMs     int64
	Deadlocked   bool
	Err          error
}

// RunProcess1 executes the business logic on the Writer:
//  1. INSERT a new config row
//  2. Logical-delete all other active rows for the same tenant
//
// When run concurrently, step 2 causes deadlocks because each transaction
// holds an X-lock on its INSERT row and then tries to lock all other rows.
// RunProcess1 executes INSERT + optional API wait + logical delete in one transaction.
// apiDelay simulates external API call time (0 for no delay).
func RunProcess1(ctx context.Context, db *sql.DB, tenantID, value string, apiDelay time.Duration) *Process1Result {
	start := time.Now()
	result := &Process1Result{}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.Err = err
		return result
	}
	defer tx.Rollback()

	// Step 1: INSERT new config
	insResult, err := tx.ExecContext(ctx,
		"INSERT INTO configs (tenant_id, config_value) VALUES (?, ?)",
		tenantID, value,
	)
	if err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	newID, _ := insResult.LastInsertId()
	result.NewID = newID

	// Step 2: Simulate external API call (e.g., payment, notification)
	// Holds the INSERT lock during the entire wait, increasing deadlock probability.
	if apiDelay > 0 {
		time.Sleep(apiDelay)
	}

	// Step 3: Logical delete all other active rows for this tenant
	delResult, err := tx.ExecContext(ctx,
		`UPDATE configs SET is_deleted = 1, deleted_at = NOW(6)
		 WHERE tenant_id = ? AND id != ? AND is_deleted = 0`,
		tenantID, newID,
	)
	if err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}
	result.DeletedCount, _ = delResult.RowsAffected()

	// Commit
	if err := tx.Commit(); err != nil {
		result.Deadlocked = isDeadlock(err)
		result.Err = err
		return result
	}

	result.CommitMs = time.Since(start).Milliseconds()
	return result
}

func isDeadlock(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "1213") || strings.Contains(s, "Deadlock")
}
