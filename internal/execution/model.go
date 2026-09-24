package execution

import (
	"context"
	"fmt"
	"strings"
)

// Job contains the stable orchestration fields needed to select a runtime.
// Contract-specific source and grader payloads are mapped into the domain by
// the event adapter once shared contract fixtures are available.
type Job struct {
	ExecutionID  string
	SubmissionID string
	Runtime      string
}

func (j Job) Validate() error {
	if strings.TrimSpace(j.ExecutionID) == "" {
		return fmt.Errorf("execution ID is required")
	}
	if strings.TrimSpace(j.SubmissionID) == "" {
		return fmt.Errorf("submission ID is required")
	}
	if strings.TrimSpace(j.Runtime) == "" {
		return fmt.Errorf("runtime is required")
	}
	return nil
}

type Outcome string

const (
	// OutcomeSucceeded means execution and learner checks passed.
	OutcomeSucceeded Outcome = "succeeded"
	// OutcomeFailed is a deterministic learner failure such as a compile or test failure.
	OutcomeFailed Outcome = "failed"
)

// Result describes the normalized learner outcome. Platform failures are
// returned as errors and classified separately.
type Result struct {
	ExecutionID  string
	SubmissionID string
	Runner       string
	Runtime      string
	Outcome      Outcome
}

func (r Result) Validate() error {
	if strings.TrimSpace(r.ExecutionID) == "" {
		return fmt.Errorf("execution ID is required")
	}
	if strings.TrimSpace(r.SubmissionID) == "" {
		return fmt.Errorf("submission ID is required")
	}
	if strings.TrimSpace(r.Runner) == "" {
		return fmt.Errorf("runner is required")
	}
	if strings.TrimSpace(r.Runtime) == "" {
		return fmt.Errorf("runtime is required")
	}
	switch r.Outcome {
	case OutcomeSucceeded, OutcomeFailed:
		return nil
	default:
		return fmt.Errorf("unsupported outcome %q", r.Outcome)
	}
}

// Runner executes a validated job for its selected runtime.
type Runner interface {
	Run(ctx context.Context, job Job) (Result, error)
}
