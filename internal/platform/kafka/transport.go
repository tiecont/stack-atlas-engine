package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"

	"stack-atlas-engine/internal/execution"
	"stack-atlas-engine/internal/platform/config"
)

type QuarantineEncoder interface {
	// EncodeInvalid must retain the original request bytes and failure reason.
	EncodeInvalid(ctx context.Context, request Record, cause error) (Record, error)
}

type messageReader interface {
	FetchMessage(context.Context) (kafkago.Message, error)
	CommitMessages(context.Context, ...kafkago.Message) error
	Close() error
}

type messageWriter interface {
	WriteMessages(context.Context, ...kafkago.Message) error
	Close() error
}

type idleConnectionCloser interface {
	CloseIdleConnections()
}

type Transport struct {
	reader           messageReader
	resultWriter     messageWriter
	quarantineWriter messageWriter
	quarantineCodec  QuarantineEncoder
	backoff          time.Duration
	connections      idleConnectionCloser
}

func NewTransport(
	cfg config.KafkaConfig,
	quarantineCodec QuarantineEncoder,
	tlsConfig *tls.Config,
	saslMechanism sasl.Mechanism,
) (*Transport, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if quarantineCodec == nil {
		return nil, errors.New("Kafka quarantine codec must not be nil")
	}
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) != broker {
			return nil, errors.New("Kafka broker addresses must not contain surrounding whitespace")
		}
	}

	const dialTimeout = 10 * time.Second
	transport := &kafkago.Transport{
		DialTimeout: dialTimeout,
		TLS:         tlsConfig,
		SASL:        saslMechanism,
	}
	dialer := &kafkago.Dialer{
		Timeout:       dialTimeout,
		TLS:           tlsConfig,
		SASLMechanism: saslMechanism,
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:               cfg.Brokers,
		GroupID:               cfg.GroupID,
		Topic:                 cfg.RequestTopic,
		Dialer:                dialer,
		QueueCapacity:         1,
		CommitInterval:        0,
		MaxAttempts:           1,
		WatchPartitionChanges: true,
	})
	resultWriter := newWriter(cfg.Brokers, cfg.ResultTopic, transport)
	quarantineWriter := newWriter(cfg.Brokers, cfg.QuarantineTopic, transport)

	return &Transport{
		reader:           reader,
		resultWriter:     resultWriter,
		quarantineWriter: quarantineWriter,
		quarantineCodec:  quarantineCodec,
		backoff:          cfg.RetryBackoff,
		connections:      transport,
	}, nil
}

func newWriter(brokers []string, topic string, transport *kafkago.Transport) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.LeastBytes{},
		MaxAttempts:            1,
		RequiredAcks:           kafkago.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
		Transport:              transport,
	}
}

func (t *Transport) Publish(ctx context.Context, record Record) error {
	if err := t.resultWriter.WriteMessages(ctx, messageForWrite(record)); err != nil {
		return classifyBrokerError(err)
	}
	return nil
}

func (t *Transport) PublishInvalid(ctx context.Context, request Record, cause error) error {
	encoded, err := t.quarantineCodec.EncodeInvalid(ctx, request, cause)
	if err != nil {
		if _, classified := execution.FailureClassOf(err); classified {
			return err
		}
		return failure(execution.FailurePermanentPlatform, err)
	}
	if err := t.quarantineWriter.WriteMessages(ctx, messageForWrite(encoded)); err != nil {
		return classifyBrokerError(err)
	}
	return nil
}

func (t *Transport) Commit(ctx context.Context, record Record) error {
	if err := t.reader.CommitMessages(ctx, messageFromRecord(record)); err != nil {
		return classifyBrokerError(err)
	}
	return nil
}

// Run processes records sequentially and retries the same uncommitted record
// before fetching another one. This is required because a later committed Kafka
// offset would also acknowledge earlier records from that partition.
func (t *Transport) Run(ctx context.Context, consumer *Consumer) error {
	if consumer == nil {
		return errors.New("Kafka request consumer must not be nil")
	}
	for {
		message, err := t.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return nil
			}
			classified := classifyBrokerError(err)
			if execution.PolicyFor(classified) != execution.PolicyRetry {
				return classified
			}
			if !waitForRetry(ctx, t.backoff) {
				return nil
			}
			continue
		}

		record := recordFromMessage(message)
		for {
			err := consumer.Handle(ctx, record)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			if execution.PolicyFor(err) != execution.PolicyRetry {
				return err
			}
			if !waitForRetry(ctx, t.backoff) {
				return nil
			}
		}
	}
}

func (t *Transport) Close() error {
	return errors.Join(
		t.reader.Close(),
		t.resultWriter.Close(),
		t.quarantineWriter.Close(),
		closeConnections(t.connections),
	)
}

func closeConnections(closer idleConnectionCloser) error {
	closer.CloseIdleConnections()
	return nil
}

func waitForRetry(ctx context.Context, backoff time.Duration) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func recordFromMessage(message kafkago.Message) Record {
	headers := make([]Header, len(message.Headers))
	for i, header := range message.Headers {
		headers[i] = Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return Record{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
		Key:       append([]byte(nil), message.Key...),
		Value:     append([]byte(nil), message.Value...),
		Headers:   headers,
	}
}

func messageFromRecord(record Record) kafkago.Message {
	headers := make([]kafkago.Header, len(record.Headers))
	for i, header := range record.Headers {
		headers[i] = kafkago.Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return kafkago.Message{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Key:       append([]byte(nil), record.Key...),
		Value:     append([]byte(nil), record.Value...),
		Headers:   headers,
	}
}

func messageForWrite(record Record) kafkago.Message {
	message := messageFromRecord(record)
	message.Topic = "" // Writers are pinned to their configured destination topic.
	message.Partition = 0
	message.Offset = 0
	return message
}

func classifyBrokerError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return failure(execution.FailureTransientPlatform, err)
	}
	class := execution.FailurePermanentPlatform
	if brokerErrorIsTransient(err) {
		class = execution.FailureTransientPlatform
	}
	return failure(class, err)
}

func brokerErrorIsTransient(err error) bool {
	if writeErrors, ok := err.(kafkago.WriteErrors); ok {
		for _, writeErr := range writeErrors {
			if writeErr != nil && brokerErrorIsTransient(writeErr) {
				return true
			}
		}
		return false
	}
	var kafkaErr kafkago.Error
	if errors.As(err, &kafkaErr) {
		return kafkaErr.Temporary() || kafkaErr.Timeout()
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) && (networkErr.Temporary() || networkErr.Timeout())
}
