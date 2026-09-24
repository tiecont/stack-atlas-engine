// Package kafka contains the request processing boundary for Kafka records.
// Wire encoding and concrete broker clients are injected so the adapter does
// not invent an event schema or broker configuration.
package kafka

import (
	"context"
	"fmt"

	"stack-atlas-engine/internal/execution"
)

type Header struct {
	Key   string
	Value []byte
}

// Record carries Kafka metadata and raw bytes without prescribing an event
// envelope. The codec owns version and trace-header validation.
type Record struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

type RequestDecoder interface {
	// DecodeRequest must validate the versioned request contract before returning
	// a domain job. The original record remains available to preserve trace data.
	DecodeRequest(ctx context.Context, record Record) (execution.Job, error)
}

type ResultEncoder interface {
	// EncodeResult must serialize the versioned result contract. It receives the
	// request record so trace headers can be propagated without decoding guesses.
	EncodeResult(ctx context.Context, request Record, result execution.Result) (Record, error)
}

type ResultPublisher interface {
	Publish(ctx context.Context, record Record) error
}

type QuarantinePublisher interface {
	// PublishInvalid must durably publish the original record and failure reason.
	PublishInvalid(ctx context.Context, request Record, cause error) error
}

type Committer interface {
	Commit(ctx context.Context, request Record) error
}

type Consumer struct {
	decoder    RequestDecoder
	runner     execution.Runner
	encoder    ResultEncoder
	results    ResultPublisher
	quarantine QuarantinePublisher
	committer  Committer
}

func NewConsumer(
	decoder RequestDecoder,
	runner execution.Runner,
	encoder ResultEncoder,
	results ResultPublisher,
	quarantine QuarantinePublisher,
	committer Committer,
) (*Consumer, error) {
	if decoder == nil || runner == nil || encoder == nil || results == nil || quarantine == nil || committer == nil {
		return nil, fmt.Errorf("Kafka consumer dependencies must not be nil")
	}
	return &Consumer{
		decoder:    decoder,
		runner:     runner,
		encoder:    encoder,
		results:    results,
		quarantine: quarantine,
		committer:  committer,
	}, nil
}

// Handle processes one fetched request. It commits only after a result or
// quarantine record has been published successfully. A publish followed by a
// commit failure can be delivered again; result consumers must tolerate that
// at-least-once behavior.
func (c *Consumer) Handle(ctx context.Context, request Record) error {
	job, err := c.decoder.DecodeRequest(ctx, request)
	if err != nil {
		if ctx.Err() != nil {
			return failure(execution.FailureTransientPlatform, ctx.Err())
		}
		if class, ok := execution.FailureClassOf(err); ok {
			switch class {
			case execution.FailureTransientPlatform:
				return err
			case execution.FailurePermanentPlatform:
				return c.quarantineAndCommit(ctx, request, err)
			}
		}
		return c.quarantineAndCommit(ctx, request, failure(execution.FailureInvalidContract, err))
	}
	if err := job.Validate(); err != nil {
		return c.quarantineAndCommit(ctx, request, failure(execution.FailureInvalidContract, err))
	}

	result, runErr := c.runner.Run(ctx, job)
	if runErr != nil {
		if ctx.Err() != nil {
			return failure(execution.FailureTransientPlatform, ctx.Err())
		}
		class, classified := execution.FailureClassOf(runErr)
		if !classified {
			class = execution.FailurePermanentPlatform
			runErr = failure(class, runErr)
		}
		switch class {
		case execution.FailureTransientPlatform:
			return runErr
		case execution.FailureLearner:
			result.Outcome = execution.OutcomeFailed
		case execution.FailurePermanentPlatform, execution.FailureInvalidContract:
			return c.quarantineAndCommit(ctx, request, runErr)
		default:
			return c.quarantineAndCommit(ctx, request, failure(execution.FailurePermanentPlatform, runErr))
		}
	}

	// Execution orchestration owns correlation and normalization, so runner
	// output cannot replace the request's identity or selected runtime.
	result.ExecutionID = job.ExecutionID
	result.SubmissionID = job.SubmissionID
	result.Runtime = job.Runtime
	if err := result.Validate(); err != nil {
		return c.quarantineAndCommit(ctx, request, failure(execution.FailurePermanentPlatform, err))
	}

	encoded, err := c.encoder.EncodeResult(ctx, request, result)
	if err != nil {
		if ctx.Err() != nil {
			return failure(execution.FailureTransientPlatform, ctx.Err())
		}
		if class, ok := execution.FailureClassOf(err); ok && class == execution.FailureTransientPlatform {
			return err
		}
		return c.quarantineAndCommit(ctx, request, failure(execution.FailurePermanentPlatform, err))
	}
	if err := c.results.Publish(ctx, encoded); err != nil {
		return transientIfUnclassified(err)
	}
	if err := c.committer.Commit(ctx, request); err != nil {
		return transientIfUnclassified(err)
	}
	return nil
}

func (c *Consumer) quarantineAndCommit(ctx context.Context, request Record, cause error) error {
	if err := c.quarantine.PublishInvalid(ctx, request, cause); err != nil {
		return transientIfUnclassified(err)
	}
	if err := c.committer.Commit(ctx, request); err != nil {
		return transientIfUnclassified(err)
	}
	return nil
}

func transientIfUnclassified(err error) error {
	if _, classified := execution.FailureClassOf(err); classified {
		return err
	}
	return failure(execution.FailureTransientPlatform, err)
}

func failure(class execution.FailureClass, cause error) error {
	classified, err := execution.NewFailureError(class, cause)
	if err != nil {
		return err
	}
	return classified
}
