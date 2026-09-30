# Stack Atlas Engine — Agent Instructions

Canonical execution rules for `stack-atlas-engine`, the independent Go
execution plane. The Engine is not a business backend. Read before planning,
reviewing, or changing code. A closer `AGENTS.md` may add stricter rules but
must not weaken these invariants.

**MUST / MUST NOT** are merge blockers. **SHOULD / SHOULD NOT** are defaults;
a deviation needs a concrete repository-specific reason. Preserve unrelated
user changes.

## 1. Mandatory pre-work

Before editing:

1. Identify the execution behavior and owning package.
2. Inspect neighboring implementation/tests and
   `docs/architecture/execution-readiness.md`.
3. Classify contract, Kafka, runner, sandbox, retry, lifecycle, concurrency,
   resource-limit, or observability effects.
4. Identify hostile-input/security consequences and failure class.
5. Define focused tests and prove Engine-only builds still work without API or
   Web checkouts.

For non-trivial work, record current behavior, target behavior, invariant,
owning package, and objectively verifiable acceptance criteria before
implementation. Do not infer the API contract from Engine types or neighboring
repositories.

## 2. Package ownership and boundaries

### ENG-001 — Package responsibility

- `cmd/worker` owns process startup and signal handling.
- `internal/app` owns worker composition and lifecycle.
- `internal/execution` owns validated domain job/result types, runner contract,
  and failure classification. These are not Kafka wire models.
- `internal/platform/config`, `logging`, and `kafka` own infrastructure
  adapters, not learner or API business policy.
- Runtime-specific code belongs in a runner package only when that runner's
  roadmap phase starts. Do not create empty future runner packages.

Do not import API/Web code or add sibling-repository modules. Engine must build
and test from its own checkout.

### ENG-002 — Current implementation boundary

The current `worker.Run` starts, waits for context cancellation, and stops. The
Kafka consumer/transport exists behind raw records and injected codecs, but is
not wired into the worker. There is no event codec, runner composition, or
sandbox in this foundation.

Do not describe or implement this foundation as consuming jobs or executing
learner code end to end. Before adding wiring, confirm the API-owned contract
and fixtures described below.

### ENG-003 — Complex repeated execution behavior

When non-trivial execution policy, job resolution, or execution-context
construction is required by two or more runners/services, extract it from the
worker/orchestrator's main flow into one focused component such as
`ResolverService`, `ContextService`, or an idiomatic Go type in the owning
`internal/` package.

That component MUST own the decision rules, define explicit inputs/outputs,
and have focused tests. Callers delegate rather than copy policy. Keep one-off
logic local; do not add pass-through wrappers or move API business policy into
Engine.

## 3. Contract and fixture rules

### CONTRACT-001 — API owns the wire schema

The API owns future versioned `execution.requested.v1` and
`execution.result.v1` contracts. Do not invent fields, JSON codecs, or a second
wire model in Engine. `internal/execution.Job` and `Result` remain internal
domain types; codecs map the agreed contract into those types.

Do not implement the codec until the API contract and canonical fixtures exist.
When they do, check in an identical fixture copy in Engine's test tree; never
load a fixture from a sibling checkout at build or test time.

### CONTRACT-002 — Codec tests

For each codec, test the API-owned canonical fixture, unsupported schema
version, malformed payload, required IDs, ID preservation, and trace/header
propagation. Invalid input must be classified for quarantine, not retried
forever. Keep wire validation at the codec boundary and domain validation in
`internal/execution`.

Cross-repository execution tests are a later integration gate after API
outbox publishing, shared fixtures, Engine codecs, worker composition, and
Kafka infrastructure are implemented. Do not require them for ordinary Engine
unit tests.

## 4. Kafka processing and retry rules

### KAFKA-001 — Record lifecycle

The Kafka adapter accepts raw topic/partition/offset/key/value/headers and
injected decoder, runner, encoder, result publisher, quarantine publisher, and
committer. Do not let the transport invent the event schema.

Process one record in this order:

1. Decode and validate the request.
2. Quarantine an invalid/permanent contract record, then commit it only after
   quarantine publication succeeds.
3. Run a valid job and classify the result/error.
4. Encode and publish the result.
5. Commit the request offset only after result publication succeeds.

A publish followed by a commit failure can redeliver the same request. This is
at-least-once delivery; API result application must be idempotent. Never claim
exactly-once end to end.

### KAFKA-002 — Retry classification

- Transient platform error: return an error and do not commit; allow retry.
- Learner compile/test/runtime failure: publish a normal failed outcome; do not
  retry it as an infrastructure failure.
- Invalid contract: quarantine the original record and commit only after the
  quarantine publish succeeds.
- Permanent platform failure: follow the existing failure/quarantine policy;
  do not retry forever.
- Publish or commit failure: preserve at-least-once behavior and classify
  untyped transport errors as transient only at the transport boundary.

Preserve execution/submission IDs and available correlation/trace headers.
Never place secrets or full learner source in Kafka records or logs by default.

## 5. Untrusted-code security and runner rules

### SEC-001 — Sandbox merge blockers

Treat source and job data as hostile. A runner/sandbox MUST:

- run non-root and disable network by default;
- use a fresh ephemeral workspace and prevent path traversal;
- enforce CPU, memory, PID, wall-clock, and output limits;
- avoid host filesystem and Docker socket mounts;
- avoid platform/cloud credential exposure;
- pin production runtime images and clean up on every exit path.

MUST NOT run privileged sandboxes by default, interpolate learner content into
shell commands, reuse writable workspaces across learners, or execute learner
code directly on the worker host.

### RUNNER-001 — Go runner phase

Only implement the Go runner when its roadmap phase starts. It must materialize
source safely, use a pinned runtime and deterministic environment, separate
public and hidden grader data, bound test output, and enforce resource/time
limits. Runner code owns runtime behavior; orchestration owns timeout,
correlation, result normalization, and publication.

### RUNNER-002 — Deferred SQL and Kubernetes runners

Do not add these runners before their roadmap phase.

- SQL runner: ephemeral isolated database, deterministic fixtures, no
  application DB access or learner superuser, statement timeout, cleanup.
- Kubernetes runner: isolated session/namespace, restricted identity, quotas,
  network policy, TTL cleanup and orphan detection, never cluster-admin.

## 6. Context, concurrency, and shutdown

Propagate `context.Context` through record handling, runner calls, and
infrastructure adapters. Every goroutine MUST have an owner, stop condition,
and synchronization strategy. Do not use global mutable runtime state. Any
concurrency change needs race-test coverage.

Worker shutdown MUST stop intake, drain within a configured bound, cancel or
terminate remaining jobs safely, clean sandbox resources, close clients, and
exit. Do not introduce an indefinite wait.

## 7. Tests and verification

### TEST-001 — Test observable behavior

Keep unit tests beside the owning Go package. Current examples are
`internal/execution/*_test.go`, `internal/platform/kafka/*_test.go`, and
`internal/app/worker_test.go`. Do not add import-only tests. Every bug fix gets
a regression test.

### TEST-002 — Kafka consumer coverage

For consumer changes, cover at least the affected cases:

- result publish succeeds before offset commit;
- result publish failure leaves the record uncommitted;
- commit failure permits safe redelivery;
- malformed request is quarantined before commit;
- learner failure becomes a result and is not retried as platform failure;
- transient runner/transport failure is returned without commit.

### TEST-003 — Sandbox security coverage

Once sandbox support exists, add integration tests for timeout, process/memory
abuse, output flooding, path traversal, network isolation, and cleanup. Do not
claim a security property from a unit mock alone.

### TEST-004 — Unit and full-flow placement

Keep Go unit tests beside the package they exercise as `*_test.go`. Keep
feature-level acceptance and cross-repository execution tests in the shared
`integration/<feature>/` tree, grouped by the API-owned execution contract.
Do not put acceptance or e2e tests inside an `internal/` implementation
package. Engine is Go-only; the TypeScript service-sidecar rule applies to the
API or any other repository that introduces production `*.service.ts` files.

The cross-repository integration gate becomes a required release check only
after API outbox publishing, shared fixtures, Engine codecs, worker composition,
and Kafka infrastructure exist. Until then, ordinary Engine unit/CI checks
MUST remain independent of API, Web, PostgreSQL, and live Kafka.

Use the repository checks relevant to the change:

```sh
make fmt-check
make test
make test-race
go vet ./...
make build
```

CI's aggregate is `make check-ci` (format check, race tests, vet, build). Report
checks that could not run with the exact command and concrete environmental
reason.

## 8. Command execution and ownership

### CMD-001 — Makefile owns developer commands

`Makefile` is the canonical command registry. Use `make run`, `make fmt-check`,
`make test`, `make test-race`, `make vet`, `make build`, and `make check-ci`;
do not copy their command lines into scripts or bypass repository flags.
`make fmt` mutates Go files; use `make fmt-check` for read-only verification.
`cmd/worker` is the executable entry point; keep composition and lifecycle in
`internal/app`, and business/execution policy in its owning `internal/`
package.

Every new executable or Make target MUST name its owner, validated arguments,
side effects, idempotency/retry behavior, and verification command. A command
that writes external or durable state needs a separate preflight and apply
path. Unit tests and `make check-ci` MUST remain independent of API/Web
checkouts, live Kafka, and sandbox infrastructure unless the change explicitly
adds an integration test for that dependency.

## 9. Observability

Carry execution ID, submission ID, runner/runtime, correlation ID, and trace
context through processing. Measure queue-to-start time, execution duration,
sandbox startup, failure class, timeout, cleanup failure, and result publish
failure. Do not log learner source or secrets by default.

## 10. Change checklists

### Contract/Kafka change

Before completion, verify API-owned schema/fixture, version handling, malformed
record behavior, required ID/trace propagation, publish-before-commit ordering,
quarantine behavior, and redelivery safety.

### Runner/sandbox change

Before completion, verify runtime/image pinning, non-root execution, network
policy, workspace isolation, CPU/memory/PID/time/output limits, cleanup on
success/failure/cancel, and a regression test for the failure that motivated
the change.

### Concurrency/lifecycle change

Before completion, verify context cancellation, goroutine ownership and stop
conditions, race coverage, bounded shutdown, and cleanup of active resources.

## 11. Completion report

GitHub branch protection MUST require the Engine CI workflow and code-owner
review for pull requests. `.github/CODEOWNERS` names the repository owner;
required-review and status-check settings are repository-host controls and
must be checked in GitHub. A local hook or this file alone cannot enforce them.

Report summary, owning package/runner, security and contract impact, tests and
commands run, API/Kafka integration dependency, and remaining risks.
