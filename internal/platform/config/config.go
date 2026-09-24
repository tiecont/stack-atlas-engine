package config

import (
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"
)

const LogLevelEnv = "ENGINE_LOG_LEVEL"

const (
	KafkaBrokersEnv         = "ENGINE_KAFKA_BROKERS"
	KafkaGroupIDEnv         = "ENGINE_KAFKA_GROUP_ID"
	KafkaRequestTopicEnv    = "ENGINE_KAFKA_REQUEST_TOPIC"
	KafkaResultTopicEnv     = "ENGINE_KAFKA_RESULT_TOPIC"
	KafkaQuarantineTopicEnv = "ENGINE_KAFKA_QUARANTINE_TOPIC"
	KafkaRetryBackoffEnv    = "ENGINE_KAFKA_RETRY_BACKOFF"
)

type Config struct {
	LogLevel slog.Level
	Kafka    KafkaConfig
}

type KafkaConfig struct {
	Brokers         []string
	GroupID         string
	RequestTopic    string
	ResultTopic     string
	QuarantineTopic string
	RetryBackoff    time.Duration
}

func (c KafkaConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("Kafka brokers are required")
	}
	for _, broker := range c.Brokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("Kafka broker address must not be blank")
		}
	}
	if strings.TrimSpace(c.GroupID) == "" {
		return fmt.Errorf("Kafka consumer group ID is required")
	}
	if strings.TrimSpace(c.RequestTopic) == "" || strings.TrimSpace(c.ResultTopic) == "" || strings.TrimSpace(c.QuarantineTopic) == "" {
		return fmt.Errorf("Kafka request, result, and quarantine topics are required")
	}
	if c.RetryBackoff <= 0 {
		return fmt.Errorf("Kafka retry backoff must be positive")
	}
	return nil
}

func Load() (Config, error) {
	return LoadFrom(os.LookupEnv)
}

// LoadFrom reads configuration through lookup so parsing can be tested without
// mutating the process environment.
func LoadFrom(lookup func(string) (string, bool)) (Config, error) {
	if lookup == nil {
		return Config{}, fmt.Errorf("environment lookup must not be nil")
	}

	value, ok := lookup(LogLevelEnv)
	if !ok || strings.TrimSpace(value) == "" {
		value = "info"
	}

	level, err := parseLogLevel(value)
	if err != nil {
		return Config{}, err
	}
	kafkaConfig, err := loadKafkaConfig(lookup)
	if err != nil {
		return Config{}, err
	}
	return Config{LogLevel: level, Kafka: kafkaConfig}, nil
}

func loadKafkaConfig(lookup func(string) (string, bool)) (KafkaConfig, error) {
	brokersValue, _ := lookup(KafkaBrokersEnv)
	var brokers []string
	if strings.TrimSpace(brokersValue) != "" {
		for _, broker := range strings.Split(brokersValue, ",") {
			broker = strings.TrimSpace(broker)
			if broker == "" {
				return KafkaConfig{}, fmt.Errorf("invalid %s: broker list contains a blank address", KafkaBrokersEnv)
			}
			brokers = append(brokers, broker)
		}
	}

	backoff := 250 * time.Millisecond
	if value, ok := lookup(KafkaRetryBackoffEnv); ok && strings.TrimSpace(value) != "" {
		parsed, err := time.ParseDuration(strings.TrimSpace(value))
		if err != nil || parsed <= 0 {
			return KafkaConfig{}, fmt.Errorf("invalid %s %q: want a positive duration", KafkaRetryBackoffEnv, value)
		}
		backoff = parsed
	}

	return KafkaConfig{
		Brokers:         slices.Clone(brokers),
		GroupID:         envValue(lookup, KafkaGroupIDEnv),
		RequestTopic:    envValue(lookup, KafkaRequestTopicEnv),
		ResultTopic:     envValue(lookup, KafkaResultTopicEnv),
		QuarantineTopic: envValue(lookup, KafkaQuarantineTopicEnv),
		RetryBackoff:    backoff,
	}, nil
}

func envValue(lookup func(string) (string, bool), key string) string {
	value, _ := lookup(key)
	return strings.TrimSpace(value)
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid %s %q: want debug, info, warn, or error", LogLevelEnv, value)
	}
}
