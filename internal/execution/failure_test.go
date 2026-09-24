package execution

import (
	"errors"
	"fmt"
	"testing"
)

func TestPolicyFor(t *testing.T) {
	tests := []struct {
		name   string
		class  FailureClass
		policy FailurePolicy
	}{
		{name: "learner failure is terminal", class: FailureLearner, policy: PolicyNoRetry},
		{name: "transient platform failure retries", class: FailureTransientPlatform, policy: PolicyRetry},
		{name: "permanent platform failure does not retry", class: FailurePermanentPlatform, policy: PolicyNoRetry},
		{name: "invalid contract is quarantined", class: FailureInvalidContract, policy: PolicyQuarantine},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failure, err := NewFailureError(tt.class, errors.New("failure"))
			if err != nil {
				t.Fatalf("NewFailureError() error = %v", err)
			}
			if got := PolicyFor(failure); got != tt.policy {
				t.Errorf("PolicyFor() = %q, want %q", got, tt.policy)
			}
			if got := failure.Class(); got != tt.class {
				t.Errorf("Class() = %q, want %q", got, tt.class)
			}
		})
	}
}

func TestPolicyForWrappedAndUnclassifiedErrors(t *testing.T) {
	cause := errors.New("broker unavailable")
	failure, err := NewFailureError(FailureTransientPlatform, cause)
	if err != nil {
		t.Fatalf("NewFailureError() error = %v", err)
	}
	wrapped := fmt.Errorf("consume request: %w", failure)
	if got := PolicyFor(wrapped); got != PolicyRetry {
		t.Errorf("PolicyFor(wrapped) = %q, want %q", got, PolicyRetry)
	}
	if !errors.Is(wrapped, cause) {
		t.Fatal("wrapped failure does not preserve its cause")
	}
	if got := PolicyFor(errors.New("unclassified")); got != PolicyNoRetry {
		t.Errorf("PolicyFor(unclassified) = %q, want %q", got, PolicyNoRetry)
	}
}

func TestNewFailureErrorRejectsInvalidInput(t *testing.T) {
	if _, err := NewFailureError(FailureTransientPlatform, nil); err == nil {
		t.Fatal("NewFailureError() accepted a nil cause")
	}
	if _, err := NewFailureError("future", errors.New("failure")); err == nil {
		t.Fatal("NewFailureError() accepted an unknown class")
	}
}
