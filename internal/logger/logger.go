package logger

import (
	"context"
	"log/slog"
	"os"
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
