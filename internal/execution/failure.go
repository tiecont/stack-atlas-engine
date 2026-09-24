package execution

import (
	"errors"
	"fmt"
)

// FailureClass identifies whether an execution error is safe to retry.
type FailureClass string

const (
	// FailureLearner covers deterministic compile, test, and runtime failures.
	FailureLearner FailureClass = "learner"
	// FailureTransientPlatform covers temporary service or infrastructure faults.
	FailureTransientPlatform FailureClass = "transient_platform"
	// FailurePermanentPlatform covers non-transient engine or infrastructure faults.
	FailurePermanentPlatform FailureClass = "permanent_platform"
	// FailureInvalidContract covers requests that fail schema or job validation.
	FailureInvalidContract FailureClass = "invalid_contract"
)

// FailurePolicy is the disposition for a failed execution attempt.
type FailurePolicy string

const (
	// PolicyNoRetry marks the attempt terminal without retrying it as infrastructure failure.
	PolicyNoRetry FailurePolicy = "no_retry"
	// PolicyRetry allows retrying a transient platform failure.
	PolicyRetry FailurePolicy = "retry"
	// PolicyQuarantine sends an invalid request to the quarantine/DLQ path.
	PolicyQuarantine FailurePolicy = "quarantine"
)

// FailureError wraps a cause with its execution failure class.
type FailureError struct {
	class FailureClass
	cause error
}

// NewFailureError constructs a classified failure around a non-nil cause.
func NewFailureError(class FailureClass, cause error) (*FailureError, error) {
	if cause == nil {
		return nil, fmt.Errorf("failure cause is required")
	}
	switch class {
	case FailureLearner, FailureTransientPlatform, FailurePermanentPlatform, FailureInvalidContract:
		return &FailureError{class: class, cause: cause}, nil
	default:
		return nil, fmt.Errorf("unknown failure class %q", class)
	}
}

func (e *FailureError) Error() string {
	if e == nil || e.cause == nil {
		return "<nil>"
	}
	return e.cause.Error()
}

func (e *FailureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *FailureError) Class() FailureClass {
	if e == nil {
		return ""
	}
	return e.class
}

func FailureClassOf(err error) (FailureClass, bool) {
	var failure *FailureError
	if !errors.As(err, &failure) || failure == nil {
		return "", false
	}
	return failure.Class(), true
}

// PolicyFor retries only explicitly classified transient platform failures.
// Unclassified errors fail closed to no-retry until their owner classifies them.
func PolicyFor(err error) FailurePolicy {
	class, ok := FailureClassOf(err)
	if !ok {
		return PolicyNoRetry
	}

	switch class {
	case FailureTransientPlatform:
		return PolicyRetry
	case FailureInvalidContract:
		return PolicyQuarantine
	case FailureLearner, FailurePermanentPlatform:
		return PolicyNoRetry
	default:
		return PolicyNoRetry
	}
}
