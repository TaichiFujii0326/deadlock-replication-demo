package process

import (
	"context"
	"database/sql"
	"time"
)

type Process2Result struct {
	Found    bool
	ActualID int64
	Match    bool
	QueryMs  int64
	Err      error
}

// RunProcess2 reads the latest active config from the Replica.
// It compares the result against expectedID to detect stale reads.
func RunProcess2(ctx context.Context, db *sql.DB, tenantID string, expectedID int64) *Process2Result {
	start := time.Now()
	result := &Process2Result{}

	var actualID int64
	err := db.QueryRowContext(ctx,
		`SELECT id FROM configs
		 WHERE tenant_id = ? AND is_deleted = 0
		 ORDER BY id DESC LIMIT 1`,
		tenantID,
	).Scan(&actualID)

	result.QueryMs = time.Since(start).Milliseconds()

	if err == sql.ErrNoRows {
		result.Found = false
		return result
	}
	if err != nil {
		result.Err = err
		return result
	}

	result.Found = true
	result.ActualID = actualID
	result.Match = (actualID == expectedID)
	return result
}

// CheckIDExists checks whether a specific ID exists on the Replica at all.
// This is a direct replication lag check: if the row doesn't exist,
// the INSERT hasn't been replicated yet.
func CheckIDExists(ctx context.Context, db *sql.DB, id int64) (bool, error) {
	var exists int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM configs WHERE id = ? LIMIT 1`, id,
	).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
