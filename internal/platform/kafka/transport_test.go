package kafka

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"stack-atlas-engine/internal/execution"
	"stack-atlas-engine/internal/platform/config"
)

type fakeReader struct {
	message    kafkago.Message
	fetchCount int
	commits    []kafkago.Message
	cancel     context.CancelFunc
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.fetchCount++
	if r.fetchCount == 1 {
		return r.message, nil
	}
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, messages ...kafkago.Message) error {
	r.commits = append(r.commits, messages...)
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

func (*fakeReader) Close() error { return nil }

type fakeWriter struct {
	messages []kafkago.Message
	results  []error
}

func (w *fakeWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.messages = append(w.messages, messages...)
	if len(w.results) == 0 {
		return nil
	}
	err := w.results[0]
	w.results = w.results[1:]
	return err
}

func (*fakeWriter) Close() error { return nil }

type fakeIdleCloser struct{ calls int }

func (c *fakeIdleCloser) CloseIdleConnections() { c.calls++ }

type temporaryNetworkError struct{}

func (temporaryNetworkError) Error() string   { return "temporary network failure" }
func (temporaryNetworkError) Timeout() bool   { return true }
func (temporaryNetworkError) Temporary() bool { return true }

var _ net.Error = temporaryNetworkError{}

func TestMessageConversionsPreserveRecordData(t *testing.T) {
	message := kafkago.Message{
		Topic:     "requests",
		Partition: 3,
		Offset:    22,
		Key:       []byte("key"),
		Value:     []byte("value"),
		Headers:   []kafkago.Header{{Key: "traceparent", Value: []byte("trace")}},
	}
	record := recordFromMessage(message)
	if record.Topic != message.Topic || record.Partition != message.Partition || record.Offset != message.Offset {
		t.Fatalf("record metadata = %+v, want topic/partition/offset from message", record)
	}
	if !reflect.DeepEqual(record.Key, message.Key) || !reflect.DeepEqual(record.Value, message.Value) || record.Headers[0].Key != message.Headers[0].Key || !reflect.DeepEqual(record.Headers[0].Value, message.Headers[0].Value) {
		t.Fatalf("record data = %+v, want message data preserved", record)
	}
	commitMessage := messageFromRecord(record)
	if commitMessage.Topic != message.Topic || commitMessage.Partition != message.Partition || commitMessage.Offset != message.Offset {
		t.Errorf("commit message metadata = %+v, want %+v", commitMessage, message)
	}
	writeMessage := messageForWrite(record)
	if writeMessage.Topic != "" || writeMessage.Partition != 0 || writeMessage.Offset != 0 {
		t.Errorf("writer message includes read-only metadata: %+v", writeMessage)
	}
	if !reflect.DeepEqual(writeMessage.Key, message.Key) || !reflect.DeepEqual(writeMessage.Value, message.Value) || !reflect.DeepEqual(writeMessage.Headers, message.Headers) {
		t.Errorf("writer message lost key/value/headers: %+v", writeMessage)
	}
}

func TestClassifyBrokerError(t *testing.T) {
	if got := execution.PolicyFor(classifyBrokerError(temporaryNetworkError{})); got != execution.PolicyRetry {
		t.Errorf("temporary error policy = %q, want %q", got, execution.PolicyRetry)
	}
	if got := execution.PolicyFor(classifyBrokerError(errors.New("invalid broker configuration"))); got != execution.PolicyNoRetry {
		t.Errorf("permanent error policy = %q, want %q", got, execution.PolicyNoRetry)
	}
}

func TestRunRetriesCurrentRecordBeforeFetchingAnother(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := &fakeReader{
		message: kafkago.Message{
			Topic:     "requests",
			Partition: 1,
			Offset:    7,
			Key:       []byte("exec-1"),
			Value:     []byte("request"),
		},
		cancel: cancel,
	}
	resultWriter := &fakeWriter{results: []error{temporaryNetworkError{}, nil}}
	quarantineWriter := &fakeWriter{}
	closer := &fakeIdleCloser{}
	transport := &Transport{
		reader:           reader,
		resultWriter:     resultWriter,
		quarantineWriter: quarantineWriter,
		quarantineCodec:  testQuarantineEncoder{},
		backoff:          time.Millisecond,
		connections:      closer,
	}
	services := validServices()
	consumer, err := NewConsumer(
		testDecoder{services},
		testRunner{services},
		testEncoder{services},
		transport,
		transport,
		transport,
	)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	if err := transport.Run(ctx, consumer); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if reader.fetchCount != 2 {
		t.Errorf("FetchMessage calls = %d, want current message plus canceled next fetch", reader.fetchCount)
	}
	if len(reader.commits) != 1 || reader.commits[0].Offset != 7 {
		t.Errorf("commits = %+v, want only request offset 7", reader.commits)
	}
	if len(resultWriter.messages) != 2 {
		t.Errorf("result publish attempts = %d, want 2", len(resultWriter.messages))
	}
	if got, want := services.events, []string{"decode", "run", "encode", "decode", "run", "encode"}; !reflect.DeepEqual(got, want) {
		t.Errorf("processor events = %v, want %v", got, want)
	}
}

type testQuarantineEncoder struct{}

func (testQuarantineEncoder) EncodeInvalid(_ context.Context, request Record, _ error) (Record, error) {
	return Record{Key: request.Key, Value: request.Value, Headers: request.Headers}, nil
}

func TestNewTransportRequiresTopicsAndCodec(t *testing.T) {
	valid := config.KafkaConfig{
		Brokers:         []string{"localhost:9092"},
		GroupID:         "engine",
		RequestTopic:    "requests",
		ResultTopic:     "results",
		QuarantineTopic: "quarantine",
		RetryBackoff:    time.Millisecond,
	}
	if _, err := NewTransport(valid, nil, nil, nil); err == nil {
		t.Fatal("NewTransport() accepted a nil quarantine codec")
	}
	valid.ResultTopic = ""
	if _, err := NewTransport(valid, testQuarantineEncoder{}, nil, nil); err == nil {
		t.Fatal("NewTransport() accepted a missing result topic")
	}
}

func TestTransportCloseClosesAllResources(t *testing.T) {
	closer := &fakeIdleCloser{}
	transport := &Transport{
		reader:           &fakeReader{},
		resultWriter:     &fakeWriter{},
		quarantineWriter: &fakeWriter{},
		connections:      closer,
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if closer.calls != 1 {
		t.Errorf("CloseIdleConnections calls = %d, want 1", closer.calls)
	}
}
