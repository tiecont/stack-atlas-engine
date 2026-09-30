package app

import (
	"context"
	"fmt"
	"log/slog"
)

// Worker owns the worker process lifecycle. Job intake and execution are added
// by later plans; until then, the process remains alive until shutdown is
// signaled.
type Worker struct {
	logger *slog.Logger
}

// NewWorker composes a worker from the dependencies available in this phase.
func NewWorker(logger *slog.Logger) (*Worker, error) {
	if logger == nil {
		return nil, fmt.Errorf("worker logger must not be nil")
	}
	return &Worker{logger: logger}, nil
}

// Run blocks until the worker context is canceled.
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("worker started")
	<-ctx.Done()
	w.logger.Info("worker stopped", "cause", ctx.Err())
}
