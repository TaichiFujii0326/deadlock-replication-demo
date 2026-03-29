package logger

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	startTime time.Time
	once      sync.Once
)

func Init() {
	once.Do(func() {
		startTime = time.Now()
		handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey {
					return slog.Attr{}
				}
				return a
			},
		})
		slog.SetDefault(slog.New(&elapsedHandler{inner: handler}))
	})
}

type elapsedHandler struct {
	inner slog.Handler
}

func (h *elapsedHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *elapsedHandler) Handle(ctx context.Context, r slog.Record) error {
	elapsed := time.Since(startTime).Milliseconds()
	r.AddAttrs(slog.Int64("T+ms", elapsed))
	return h.inner.Handle(ctx, r)
}

func (h *elapsedHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &elapsedHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *elapsedHandler) WithGroup(name string) slog.Handler {
	return &elapsedHandler{inner: h.inner.WithGroup(name)}
}

// Banner prints a human-readable phase header to stderr.
// stdout (JSON logs) is unaffected.
func Banner(title, description string) {
	line := strings.Repeat("=", 64)
	fmt.Fprintf(os.Stderr, "\n%s\n  %s\n  %s\n%s\n", line, title, description, line)
}

// BannerWithExpect prints a phase header with expected outcome to stderr.
func BannerWithExpect(title, description, expect string) {
	line := strings.Repeat("=", 64)
	fmt.Fprintf(os.Stderr, "\n%s\n  %s\n  %s\n  EXPECT: %s\n%s\n", line, title, description, expect, line)
}

// Result prints a one-line result summary to stderr.
func Result(msg string) {
	fmt.Fprintf(os.Stderr, "  >> %s\n", msg)
}

// Table prints an ASCII table to stderr. header is the column names,
// rows is a slice of row values (same length as header).
func Table(header []string, rows [][]string) {
	widths := make([]int, len(header))
	for i, h := range header {
		widths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	sep := "+"
	for _, w := range widths {
		sep += strings.Repeat("-", w+2) + "+"
	}

	printRow := func(cells []string) {
		line := "|"
		for i, cell := range cells {
			if i < len(widths) {
				line += fmt.Sprintf(" %-*s |", widths[i], cell)
			}
		}
		fmt.Fprintln(os.Stderr, line)
	}

	fmt.Fprintln(os.Stderr, sep)
	printRow(header)
	fmt.Fprintln(os.Stderr, sep)
	for _, row := range rows {
		printRow(row)
	}
	fmt.Fprintln(os.Stderr, sep)
}
