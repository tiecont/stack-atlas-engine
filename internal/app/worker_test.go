package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type startupWriter struct {
	bytes.Buffer
	started chan struct{}
	once    sync.Once
}

func (w *startupWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	w.once.Do(func() { close(w.started) })
	return n, err
}

func TestRunWaitsForCancellationAndLogsLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	output := &startupWriter{started: make(chan struct{})}
	logger := slog.New(slog.NewJSONHandler(output, nil))
	done := make(chan struct{})
	go func() {
		Run(ctx, logger)
		close(done)
	}()

	select {
	case <-output.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not log startup")
	}

	select {
	case <-done:
		t.Fatal("worker returned before its context was canceled")
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after its context was canceled")
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected startup and shutdown log records, got %d: %q", len(lines), output.String())
	}

	var records []map[string]any
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log record is not valid JSON: %v", err)
		}
		records = append(records, record)
	}
	if got := records[0]["msg"]; got != "worker started" {
		t.Errorf("startup message = %v, want %q", got, "worker started")
	}
	if got := records[1]["msg"]; got != "worker stopped" {
		t.Errorf("shutdown message = %v, want %q", got, "worker stopped")
	}
	if got := records[1]["cause"]; got != context.Canceled.Error() {
		t.Errorf("shutdown cause = %v, want %q", got, context.Canceled)
	}
}

var _ io.Writer = (*startupWriter)(nil)
