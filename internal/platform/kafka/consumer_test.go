package kafka

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"stack-atlas-engine/internal/execution"
)

type testServices struct {
	events          []string
	job             execution.Job
	decodeErr       error
	runResult       execution.Result
	runErr          error
	encodeErr       error
	resultErr       error
	quarantineErr   error
	commitErr       error
	encodedResult   execution.Result
	encodedRequest  Record
	quarantined     Record
	quarantineCause error
	committed       Record
}

type testDecoder struct{ services *testServices }

func (d testDecoder) DecodeRequest(context.Context, Record) (execution.Job, error) {
	d.services.events = append(d.services.events, "decode")
	return d.services.job, d.services.decodeErr
}

type testRunner struct{ services *testServices }

func (r testRunner) Run(context.Context, execution.Job) (execution.Result, error) {
	r.services.events = append(r.services.events, "run")
	return r.services.runResult, r.services.runErr
}

type testEncoder struct{ services *testServices }

func (e testEncoder) EncodeResult(_ context.Context, request Record, result execution.Result) (Record, error) {
	e.services.events = append(e.services.events, "encode")
	e.services.encodedRequest = request
	e.services.encodedResult = result
	return Record{Topic: "encoded-result", Key: []byte(result.ExecutionID), Value: []byte("result")}, e.services.encodeErr
}

type testResultPublisher struct{ services *testServices }

func (p testResultPublisher) Publish(context.Context, Record) error {
	p.services.events = append(p.services.events, "publish-result")
	return p.services.resultErr
}

type testQuarantinePublisher struct{ services *testServices }

func (p testQuarantinePublisher) PublishInvalid(_ context.Context, request Record, cause error) error {
	p.services.events = append(p.services.events, "quarantine")
	p.services.quarantined = request
	p.services.quarantineCause = cause
	return p.services.quarantineErr
}

type testCommitter struct{ services *testServices }

func (c testCommitter) Commit(_ context.Context, request Record) error {
	c.services.events = append(c.services.events, "commit")
	c.services.committed = request
	return c.services.commitErr
}

func newTestConsumer(t *testing.T, services *testServices) *Consumer {
	t.Helper()
	consumer, err := NewConsumer(
		testDecoder{services},
		testRunner{services},
		testEncoder{services},
		testResultPublisher{services},
		testQuarantinePublisher{services},
		testCommitter{services},
	)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	return consumer
}

func validServices() *testServices {
	return &testServices{
		job: execution.Job{ExecutionID: "exec-1", SubmissionID: "sub-1", Runtime: "go"},
		runResult: execution.Result{
			ExecutionID:  "runner-exec",
			SubmissionID: "runner-sub",
			Runner:       "golang",
			Runtime:      "runner-runtime",
			Outcome:      execution.OutcomeSucceeded,
		},
	}
}

func testRecord() Record {
	return Record{
		Topic:     "requests",
		Partition: 2,
		Offset:    41,
		Key:       []byte("request-key"),
		Value:     []byte("opaque-request"),
		Headers:   []Header{{Key: "traceparent", Value: []byte("trace-context")}},
	}
}

func TestHandlePublishesNormalizedResultBeforeCommit(t *testing.T) {
	services := validServices()
	consumer := newTestConsumer(t, services)
	request := testRecord()
	if err := consumer.Handle(context.Background(), request); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	wantEvents := []string{"decode", "run", "encode", "publish-result", "commit"}
	if !reflect.DeepEqual(services.events, wantEvents) {
		t.Fatalf("events = %v, want %v", services.events, wantEvents)
	}
	if services.encodedResult.ExecutionID != services.job.ExecutionID || services.encodedResult.SubmissionID != services.job.SubmissionID {
		t.Errorf("result IDs were not normalized from request: %+v", services.encodedResult)
	}
	if services.encodedResult.Runtime != services.job.Runtime {
		t.Errorf("result runtime = %q, want %q", services.encodedResult.Runtime, services.job.Runtime)
	}
	if !reflect.DeepEqual(services.encodedRequest.Headers, request.Headers) {
		t.Errorf("request headers were not passed to result encoder: %v", services.encodedRequest.Headers)
	}
	if services.committed.Offset != request.Offset || services.committed.Topic != request.Topic {
		t.Errorf("committed record = %+v, want original request metadata", services.committed)
	}
}

func TestHandleQuarantinesInvalidRequestBeforeCommit(t *testing.T) {
	services := validServices()
	services.decodeErr = errors.New("unsupported schema version")
	consumer := newTestConsumer(t, services)
	request := testRecord()
	if err := consumer.Handle(context.Background(), request); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if got, want := services.events, []string{"decode", "quarantine", "commit"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(services.quarantined, request) {
		t.Errorf("quarantine did not receive original record: %+v", services.quarantined)
	}
	if got := execution.PolicyFor(services.quarantineCause); got != execution.PolicyQuarantine {
		t.Errorf("quarantine cause policy = %q, want %q", got, execution.PolicyQuarantine)
	}
}

func TestHandleTransientDecodeFailureLeavesRequestUncommitted(t *testing.T) {
	services := validServices()
	services.decodeErr, _ = execution.NewFailureError(execution.FailureTransientPlatform, errors.New("decoder unavailable"))
	consumer := newTestConsumer(t, services)
	err := consumer.Handle(context.Background(), testRecord())
	if execution.PolicyFor(err) != execution.PolicyRetry {
		t.Fatalf("PolicyFor(error) = %q, want %q", execution.PolicyFor(err), execution.PolicyRetry)
	}
	if got, want := services.events, []string{"decode"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestHandlePermanentDecodeFailureIsQuarantined(t *testing.T) {
	services := validServices()
	services.decodeErr, _ = execution.NewFailureError(execution.FailurePermanentPlatform, errors.New("codec configuration invalid"))
	consumer := newTestConsumer(t, services)
	if err := consumer.Handle(context.Background(), testRecord()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got, want := services.events, []string{"decode", "quarantine", "commit"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestHandleQuarantinesInvalidJob(t *testing.T) {
	services := validServices()
	services.job.Runtime = " "
	consumer := newTestConsumer(t, services)
	if err := consumer.Handle(context.Background(), testRecord()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got, want := services.events, []string{"decode", "quarantine", "commit"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestHandleTurnsLearnerFailureIntoResult(t *testing.T) {
	services := validServices()
	services.runErr, _ = execution.NewFailureError(execution.FailureLearner, errors.New("compile failed"))
	consumer := newTestConsumer(t, services)
	if err := consumer.Handle(context.Background(), testRecord()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if services.encodedResult.Outcome != execution.OutcomeFailed {
		t.Errorf("outcome = %q, want %q", services.encodedResult.Outcome, execution.OutcomeFailed)
	}
	if got, want := services.events, []string{"decode", "run", "encode", "publish-result", "commit"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestHandleLeavesTransientRunnerFailureUncommitted(t *testing.T) {
	services := validServices()
	services.runErr, _ = execution.NewFailureError(execution.FailureTransientPlatform, errors.New("runner unavailable"))
	consumer := newTestConsumer(t, services)
	err := consumer.Handle(context.Background(), testRecord())
	if execution.PolicyFor(err) != execution.PolicyRetry {
		t.Fatalf("PolicyFor(error) = %q, want %q", execution.PolicyFor(err), execution.PolicyRetry)
	}
	if got, want := services.events, []string{"decode", "run"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestHandleQuarantinesPermanentRunnerFailure(t *testing.T) {
	services := validServices()
	services.runErr, _ = execution.NewFailureError(execution.FailurePermanentPlatform, errors.New("runtime image missing"))
	consumer := newTestConsumer(t, services)
	if err := consumer.Handle(context.Background(), testRecord()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got, want := services.events, []string{"decode", "run", "quarantine", "commit"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestHandlePublishFailureLeavesRequestUncommitted(t *testing.T) {
	services := validServices()
	services.resultErr = errors.New("broker unavailable")
	consumer := newTestConsumer(t, services)
	err := consumer.Handle(context.Background(), testRecord())
	if execution.PolicyFor(err) != execution.PolicyRetry {
		t.Fatalf("PolicyFor(error) = %q, want %q", execution.PolicyFor(err), execution.PolicyRetry)
	}
	if got, want := services.events, []string{"decode", "run", "encode", "publish-result"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestHandleTransientEncodeFailureLeavesRequestUncommitted(t *testing.T) {
	services := validServices()
	services.encodeErr, _ = execution.NewFailureError(execution.FailureTransientPlatform, errors.New("encoder unavailable"))
	consumer := newTestConsumer(t, services)
	err := consumer.Handle(context.Background(), testRecord())
	if execution.PolicyFor(err) != execution.PolicyRetry {
		t.Fatalf("PolicyFor(error) = %q, want %q", execution.PolicyFor(err), execution.PolicyRetry)
	}
	if got, want := services.events, []string{"decode", "run", "encode"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestHandleQuarantineFailureLeavesRequestUncommitted(t *testing.T) {
	services := validServices()
	services.decodeErr = errors.New("invalid contract")
	services.quarantineErr = errors.New("DLQ unavailable")
	consumer := newTestConsumer(t, services)
	err := consumer.Handle(context.Background(), testRecord())
	if execution.PolicyFor(err) != execution.PolicyRetry {
		t.Fatalf("PolicyFor(error) = %q, want %q", execution.PolicyFor(err), execution.PolicyRetry)
	}
	if got, want := services.events, []string{"decode", "quarantine"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestHandleQuarantineFailurePreservesOriginalBytes(t *testing.T) {
	services := validServices()
	services.decodeErr = errors.New("invalid contract")
	services.quarantineErr = errors.New("DLQ unavailable")
	request := testRecord()
	consumer := newTestConsumer(t, services)
	_ = consumer.Handle(context.Background(), request)
	if !bytes.Equal(services.quarantined.Value, request.Value) {
		t.Errorf("quarantined value = %q, want original %q", services.quarantined.Value, request.Value)
	}
}

func TestHandleCommitFailureCanRedeliverPublishedResult(t *testing.T) {
	services := validServices()
	services.commitErr = errors.New("commit unavailable")
	consumer := newTestConsumer(t, services)
	err := consumer.Handle(context.Background(), testRecord())
	if execution.PolicyFor(err) != execution.PolicyRetry {
		t.Fatalf("PolicyFor(error) = %q, want %q", execution.PolicyFor(err), execution.PolicyRetry)
	}
	if got, want := services.events, []string{"decode", "run", "encode", "publish-result", "commit"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestNewConsumerRejectsMissingDependency(t *testing.T) {
	services := validServices()
	_, err := NewConsumer(nil, testRunner{services}, testEncoder{services}, testResultPublisher{services}, testQuarantinePublisher{services}, testCommitter{services})
	if err == nil {
		t.Fatal("NewConsumer() accepted a nil decoder")
	}
}
