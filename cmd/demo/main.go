package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"deadlock-replication-demo/internal/logger"
	internalmysql "deadlock-replication-demo/internal/mysql"
	"deadlock-replication-demo/internal/scenario"
)

func main() {
	mode := flag.String("mode", "all", "Run mode: all, comparison, sustained")
	flag.Parse()

	logger.Init()
	logger.Banner("MySQL Deadlock & Replication Lag Demo",
		"検証①: 改善パターン比較  検証②: 持続的負荷による遅延再現")

	slog.Info("demo_start", "mode", *mode)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	writer, err := internalmysql.ConnectWriter(ctx)
	if err != nil {
		slog.Error("writer_connect_failed", "error", err)
		os.Exit(1)
	}
	defer writer.Close()

	reader, err := internalmysql.ConnectReader(ctx)
	if err != nil {
		slog.Error("reader_connect_failed", "error", err)
		os.Exit(1)
	}
	defer reader.Close()

	monitor := internalmysql.NewReplicationMonitor(reader)
	monCtx, monCancel := context.WithCancel(ctx)
	defer monCancel()
	monitor.Start(monCtx)

	switch *mode {
	case "comparison":
		if _, err := scenario.RunComparison(ctx, writer, reader, monitor); err != nil {
			slog.Error("comparison_failed", "error", err)
			os.Exit(1)
		}
	case "sustained":
		if _, err := scenario.RunSustained(ctx, writer, reader, monitor); err != nil {
			slog.Error("sustained_failed", "error", err)
			os.Exit(1)
		}
	default: // "all"
		slog.Info("phase", "name", "comparison")
		if _, err := scenario.RunComparison(ctx, writer, reader, monitor); err != nil {
			slog.Error("comparison_failed", "error", err)
			os.Exit(1)
		}
		slog.Info("phase", "name", "sustained")
		if _, err := scenario.RunSustained(ctx, writer, reader, monitor); err != nil {
			slog.Error("sustained_failed", "error", err)
			os.Exit(1)
		}
	}

	slog.Info("demo_complete")
	logger.Banner("Demo Complete", "全シナリオ完了")
}
