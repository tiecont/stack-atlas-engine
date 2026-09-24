# Stack Atlas Engine — Agent Instructions

Canonical agent contract for `stack-atlas-engine`.

The Engine is the Go execution plane for untrusted learner workloads.

It is not a business backend.

## 1. Mandatory pre-work

Before editing:
1. identify execution capability or runner;
2. inspect neighboring code/tests;
3. classify Kafka, lifecycle, sandbox, runner, concurrency, resource or
   observability change;
4. identify security impact;
5. define focused tests.

## 2. Canonical architecture

One Go module.

```text
cmd/
  worker/
internal/
  app/
  execution/
  runners/
    golang/
    sql/           # only when implemented
    kubernetes/    # only when implemented
  platform/
    config/
    logging/
    telemetry/
    kafka/
    sandbox/
test/
  integration/
```

Do not create empty future runner packages.

## 3. Ownership

Engine owns:
- execution job consumption;
- job validation;
- sandbox lifecycle;
- runtime selection;
- compile/run/test;
- grader adapters;
- normalized execution result;
- result publication;
- execution metrics.

Engine does NOT own:
- users;
- auth;
- progress;
- courses;
- organizations;
- billing;
- entitlements;
- mastery.

## 4. Security merge blockers

Treat learner code as hostile.

MUST:
- run non-root;
- disable network by default;
- use ephemeral workspace;
- prevent path traversal;
- enforce CPU/memory/PID/wall-clock/output limits;
- avoid host filesystem mounts;
- avoid host Docker socket;
- avoid platform secret injection;
- pin runtime images in production;
- clean resources on every exit path.

MUST NOT:
- run privileged sandboxes by default;
- interpolate learner content into shell commands;
- expose cloud metadata;
- reuse writable workspace across learners;
- execute on worker host without isolation.

## 5. Runner contract

Prefer one common runner interface:

```go
type Runner interface {
    Run(ctx context.Context, job Job) (Result, error)
}
```

Execution orchestration owns:
- lifecycle;
- timeout;
- result normalization;
- correlation;
- publication.

Runtime-specific runners own runtime behavior only.

## 6. Kafka semantics

Consume `execution.requested.v1`.

Publish `execution.result.v1`.

Assume at-least-once.

Consumer:
- validates schema/version;
- preserves IDs/trace context;
- handles redelivery safely;
- distinguishes learner vs platform failure.

Do not claim exactly-once end-to-end.

## 7. Retry rules

Retry only platform/transient failures.

Do not retry as infra failure:
- compile error;
- failed tests;
- deterministic learner runtime failure.

Invalid contracts go to quarantine/DLQ strategy rather than infinite retry.

## 8. Go runner

Must support:
- source materialization;
- pinned runtime;
- compile/test;
- deterministic environment;
- hidden/public grader separation;
- bounded test result;
- resource/time controls.

## 9. SQL runner

Only add at SQL phase.

Requirements:
- isolated ephemeral DB;
- deterministic fixture;
- no application DB access;
- no learner superuser;
- statement timeout;
- cleanup;
- safe EXPLAIN/plan support.

## 10. Kubernetes runner

Only add at K8s phase.

Requirements:
- isolated session/namespace;
- restricted identity;
- quotas;
- network policy;
- TTL cleanup;
- orphan detection;
- no cluster-admin.

## 11. Context/concurrency

Propagate context.

Every goroutine has:
- owner;
- stop condition;
- synchronization strategy.

No global mutable runtime state.

Concurrency changes require race tests where supported.

## 12. Shutdown

```text
signal
-> stop new job intake
-> bounded drain
-> cancel/terminate remaining jobs safely
-> cleanup
-> close clients
-> exit
```

No indefinite shutdown.

## 13. Observability

Per execution:
- execution ID;
- submission ID;
- runner;
- runtime;
- correlation;
- trace context.

Metrics:
- queue-to-start;
- execution duration;
- sandbox startup;
- failure classes;
- timeout;
- cleanup failure;
- result publish failure.

Do not log full learner source by default.

## 14. Tests

Unit:
- job validation;
- lifecycle;
- retry classification;
- runner selection;
- normalization.

Security integration:
- timeout;
- process abuse;
- memory abuse;
- output flooding;
- path traversal;
- network isolation;
- cleanup.

Every bug fix gets regression coverage.

## 15. Independent development rule

Engine must be developable without API or Web checkout.

Use:
- shared contract fixtures;
- local Kafka fixture/integration harness;
- deterministic execution request samples;
- result golden files.

Do not import sibling-repo code.

## 16. Verification

Repository-equivalent:

```bash
gofmt
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Run security/integration tests when dependencies are available.

## 17. Completion report

```text
Summary
Runner/execution area
Security impact
Event contract impact
Tests/commands
Integration dependency
Remaining risks
```
