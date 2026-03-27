package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func ConnectWriter(ctx context.Context) (*sql.DB, error) {
	host := env("WRITER_HOST", "127.0.0.1")
	port := env("WRITER_PORT", "13306")
	return connect(ctx, host, port, "writer", 50)
}

func ConnectReader(ctx context.Context) (*sql.DB, error) {
	host := env("READER_HOST", "127.0.0.1")
	port := env("READER_PORT", "13307")
	return connect(ctx, host, port, "reader", 10)
}

func connect(ctx context.Context, host, port, label string, maxConn int) (*sql.DB, error) {
	dsn := fmt.Sprintf("root:root@tcp(%s:%s)/demo?parseTime=true&interpolateParams=true&timeout=5s&readTimeout=10s&writeTimeout=10s", host, port)

	var db *sql.DB
	var err error
	for i := 0; i < 30; i++ {
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			slog.Warn("db_open_failed", "label", label, "attempt", i+1, "error", err)
			time.Sleep(2 * time.Second)
			continue
		}

		err = db.PingContext(ctx)
		if err == nil {
			db.SetMaxOpenConns(maxConn)
			db.SetMaxIdleConns(maxConn)
			db.SetConnMaxLifetime(5 * time.Minute)
			slog.Info("db_connected", "label", label, "host", host, "port", port)
			return db, nil
		}

		db.Close()
		slog.Warn("db_ping_failed", "label", label, "attempt", i+1, "error", err)
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("failed to connect to %s after 30 attempts: %w", label, err)
}
