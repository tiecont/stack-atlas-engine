package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestNewWritesStructuredJSONAtConfiguredLevel(t *testing.T) {
	var output bytes.Buffer
	logger := New(slog.LevelWarn, &output)
	logger.Info("filtered")
	logger.Warn("worker stopping", "signal", "SIGTERM")

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}
	if got := record["msg"]; got != "worker stopping" {
		t.Errorf("message = %v, want %q", got, "worker stopping")
	}
	if got := record["signal"]; got != "SIGTERM" {
		t.Errorf("signal = %v, want %q", got, "SIGTERM")
	}
	if got := record["level"]; got != "WARN" {
		t.Errorf("level = %v, want %q", got, "WARN")
	}
}
