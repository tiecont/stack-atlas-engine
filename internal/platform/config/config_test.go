package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadFromLogLevel(t *testing.T) {
	tests := []struct {
		name  string
		value string
		set   bool
		want  slog.Level
	}{
		{name: "default", want: slog.LevelInfo},
		{name: "debug", value: "debug", set: true, want: slog.LevelDebug},
		{name: "case and whitespace", value: " WARN ", set: true, want: slog.LevelWarn},
		{name: "warning alias", value: "warning", set: true, want: slog.LevelWarn},
		{name: "error", value: "error", set: true, want: slog.LevelError},
		{name: "blank uses default", value: "  ", set: true, want: slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadFrom(func(key string) (string, bool) {
				if key == LogLevelEnv {
					return tt.value, tt.set
				}
				return "", false
			})
			if err != nil {
				t.Fatalf("LoadFrom() error = %v", err)
			}
			if cfg.LogLevel != tt.want {
				t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, tt.want)
			}
		})
	}
}

func TestLoadFromKafkaConfig(t *testing.T) {
	values := map[string]string{
		KafkaBrokersEnv:         " kafka-1:9092, kafka-2:9092 ",
		KafkaGroupIDEnv:         "engine-workers",
		KafkaRequestTopicEnv:    "execution-requests",
		KafkaResultTopicEnv:     "execution-results",
		KafkaQuarantineTopicEnv: "execution-quarantine",
		KafkaRetryBackoffEnv:    "750ms",
	}
	cfg, err := LoadFrom(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatalf("LoadFrom() error = %v", err)
	}
	want := KafkaConfig{
		Brokers:         []string{"kafka-1:9092", "kafka-2:9092"},
		GroupID:         "engine-workers",
		RequestTopic:    "execution-requests",
		ResultTopic:     "execution-results",
		QuarantineTopic: "execution-quarantine",
		RetryBackoff:    750 * time.Millisecond,
	}
	if cfg.Kafka.Brokers[0] != want.Brokers[0] || cfg.Kafka.Brokers[1] != want.Brokers[1] {
		t.Errorf("Brokers = %v, want %v", cfg.Kafka.Brokers, want.Brokers)
	}
	if cfg.Kafka.GroupID != want.GroupID || cfg.Kafka.RequestTopic != want.RequestTopic || cfg.Kafka.ResultTopic != want.ResultTopic || cfg.Kafka.QuarantineTopic != want.QuarantineTopic || cfg.Kafka.RetryBackoff != want.RetryBackoff {
		t.Errorf("Kafka config = %+v, want %+v", cfg.Kafka, want)
	}
	if err := cfg.Kafka.Validate(); err != nil {
		t.Errorf("Kafka.Validate() error = %v", err)
	}
}

func TestLoadFromRejectsInvalidKafkaConfig(t *testing.T) {
	tests := []struct {
		name  string
		value string
		key   string
	}{
		{name: "blank broker entry", key: KafkaBrokersEnv, value: "broker:9092, "},
		{name: "invalid retry backoff", key: KafkaRetryBackoffEnv, value: "0s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadFrom(func(key string) (string, bool) {
				return tt.value, key == tt.key
			})
			if err == nil || !strings.Contains(err.Error(), tt.key) {
				t.Fatalf("LoadFrom() error = %v, want error naming %s", err, tt.key)
			}
		})
	}
}

func TestKafkaConfigValidateRequiresConnectionAndTopics(t *testing.T) {
	if err := (KafkaConfig{}).Validate(); err == nil {
		t.Fatal("Validate() accepted an empty Kafka config")
	}
}

func TestLoadFromRejectsInvalidLogLevel(t *testing.T) {
	_, err := LoadFrom(func(string) (string, bool) { return "verbose", true })
	if err == nil || !strings.Contains(err.Error(), LogLevelEnv) {
		t.Fatalf("LoadFrom() error = %v, want an error naming %s", err, LogLevelEnv)
	}
}

func TestLoadFromRejectsNilLookup(t *testing.T) {
	if _, err := LoadFrom(nil); err == nil {
		t.Fatal("LoadFrom(nil) succeeded, want an error")
	}
}
