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
	"deadlock-replication-demo/internal/process"
	"deadlock-replication-demo/internal/scenario"
)

func main() {
	mode := flag.String("mode", "all", "Run mode: all, comparison, sustained, sustained-before, sustained-bc")
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

	sustainedPatterns := []scenario.InjectionPattern{
		{"Before: API in Tx", process.RunProcess1},
		{"B+C: API First + SFU", process.RunProcess1_APIFirst_SFU},
	}

	runSustainedPattern := func(p scenario.InjectionPattern) {
		slog.Info("phase", "name", "sustained", "pattern", p.Name)
		if _, err := scenario.RunSustained(ctx, writer, reader, monitor, p); err != nil {
			slog.Error("sustained_failed", "pattern", p.Name, "error", err)
			os.Exit(1)
		}
	}

	switch *mode {
	case "comparison":
		if _, err := scenario.RunComparison(ctx, writer, reader, monitor); err != nil {
			slog.Error("comparison_failed", "error", err)
			os.Exit(1)
		}
	case "sustained":
		for _, p := range sustainedPatterns {
			runSustainedPattern(p)
		}
	case "sustained-before":
		runSustainedPattern(sustainedPatterns[0])
	case "sustained-bc":
		runSustainedPattern(sustainedPatterns[1])
	default: // "all"
		slog.Info("phase", "name", "comparison")
		if _, err := scenario.RunComparison(ctx, writer, reader, monitor); err != nil {
			slog.Error("comparison_failed", "error", err)
			os.Exit(1)
		}
		for _, p := range sustainedPatterns {
			runSustainedPattern(p)
		}
	}

	slog.Info("demo_complete")
	logger.Banner("Demo Complete", "全シナリオ完了")
}
