package execution

import (
	"context"
	"testing"
)

func TestJobValidate(t *testing.T) {
	tests := []struct {
		name    string
		job     Job
		wantErr bool
	}{
		{
			name: "valid",
			job: Job{
				ExecutionID:  "exec-1",
				SubmissionID: "sub-1",
				Runtime:      "go",
			},
		},
		{name: "missing execution ID", job: Job{SubmissionID: "sub-1", Runtime: "go"}, wantErr: true},
		{name: "blank submission ID", job: Job{ExecutionID: "exec-1", SubmissionID: "  ", Runtime: "go"}, wantErr: true},
		{name: "missing runtime", job: Job{ExecutionID: "exec-1", SubmissionID: "sub-1"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestResultValidate(t *testing.T) {
	base := Result{
		ExecutionID:  "exec-1",
		SubmissionID: "sub-1",
		Runner:       "go",
		Runtime:      "go1.23",
		Outcome:      OutcomeSucceeded,
	}
	tests := []struct {
		name    string
		result  Result
		wantErr bool
	}{
		{name: "valid success", result: base},
		{name: "valid learner failure", result: func() Result { r := base; r.Outcome = OutcomeFailed; return r }()},
		{name: "missing execution ID", result: func() Result { r := base; r.ExecutionID = ""; return r }(), wantErr: true},
		{name: "missing runner", result: func() Result { r := base; r.Runner = ""; return r }(), wantErr: true},
		{name: "unknown outcome", result: func() Result { r := base; r.Outcome = "retry"; return r }(), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.result.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type contractRunner struct{}

func (contractRunner) Run(_ context.Context, job Job) (Result, error) {
	return Result{
		ExecutionID:  job.ExecutionID,
		SubmissionID: job.SubmissionID,
		Runner:       "test",
		Runtime:      job.Runtime,
		Outcome:      OutcomeSucceeded,
	}, nil
}

var _ Runner = contractRunner{}

func TestRunnerContract(t *testing.T) {
	job := Job{ExecutionID: "exec-1", SubmissionID: "sub-1", Runtime: "go"}
	got, err := Runner(contractRunner{}).Run(context.Background(), job)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got.ExecutionID != job.ExecutionID || got.SubmissionID != job.SubmissionID {
		t.Fatalf("Run() did not preserve job IDs: %+v", got)
	}
}
