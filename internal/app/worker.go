package app

import (
	"context"
	"log/slog"
)

// Run owns the worker process lifecycle. Job intake and execution are added by
// later plans; until then, the process remains alive until shutdown is signaled.
func Run(ctx context.Context, logger *slog.Logger) {
	logger.Info("worker started")
	<-ctx.Done()
	logger.Info("worker stopped", "cause", ctx.Err())
}
