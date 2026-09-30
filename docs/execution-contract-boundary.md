# Execution contract boundary

## Ownership and data flow

`internal/execution` is the Engine's internal domain model. It contains the job
and normalized result fields needed by orchestration, the `Runner` interface,
and failure classification. These types are not Kafka wire models, and the
domain package does not import Kafka or a transport library.

The intended boundary is:

```text
Kafka record -> request decoder -> execution.Job -> runner
runner result -> execution.Result -> result encoder -> Kafka record
```

`internal/platform/kafka` currently owns the raw record adapter and codec
seams. `RequestDecoder` receives context and the complete record, including
headers, and returns an `execution.Job`. `ResultEncoder` receives the original
request record and the normalized `execution.Result`. `QuarantineEncoder`
receives the original record and failure. This leaves version validation,
wire fields, and trace-header mapping to a contract-specific codec.

No V1 payload fields or JSON codecs are defined in this foundation. Add
contract packages under `internal/contracts/` only when a released contract
has real interfaces, documentation, and tests; do not add empty future
packages.

## Source of truth and delivery

Kafka carries execution events. It is not the business source of truth. The
Engine owns execution processing and normalized outcomes; it does not own
learner identity, course or organization state, progress, billing, or other
business records. API remains the source of truth for those records.

Delivery is at-least-once. The request processor publishes a result before it
commits the request offset. If publication succeeds and commit fails, the
request can run again and produce a duplicate result event. Consumers applying
results in the API must therefore be idempotent using an agreed stable
contract identity. The system does not claim end-to-end exactly-once delivery.

## Failure and offset policy

The failure classes and current processing dispositions are:

| Failure class | Processing disposition | Request offset |
| --- | --- | --- |
| `invalid_contract` | Publish the original request and failure to quarantine | Commit after quarantine publish succeeds |
| `transient_platform` | Retry the current record | Leave uncommitted while retrying |
| `learner` | Publish a normalized failed result | Commit after result publish succeeds |
| `permanent_platform` | Quarantine; do not retry it as an infrastructure failure | Commit after quarantine publish succeeds |

Transient result publication and commit failures are retried without committing
the request. A permanent result publication failure is quarantined, then
committed only after quarantine publication succeeds. A permanent commit
failure stops processing with the request uncommitted because no successful
offset acknowledgement occurred. A result that was published before a commit
failure can be delivered again, which is why downstream idempotency is
required.

## Contract fixtures

When Execution Contract V1 is agreed, put JSON fixtures under
`test/fixtures/contracts/`. Fixtures are versioned and immutable after release.
API and Engine each keep an independently copied fixture so either repository
can build and test without a sibling checkout or symlink. Codec tests should
cover accepted and unsupported versions, malformed input, identity
preservation, and trace headers. No fixture contents are proposed before the
contract is agreed.

## Worker and runner readiness

`cmd/worker` composes `app.NewWorker(...)` and calls `worker.Run(ctx)`. The
worker currently handles process lifecycle and cancellation only. Kafka
settings remain optional until real Kafka consumption is wired. Future worker
composition may inject a consumer, decoder, runner registry, encoder, and
publisher; production codecs are not guessed here. `execution.Runner` remains
the minimal `Run(ctx, Job) (Result, error)` contract.

No learner code is compiled or executed in this phase. Before a runner can
merge, it must meet these security requirements:

- Run workloads as a non-root user with network access off by default.
- Use an ephemeral workspace and prevent path traversal.
- Enforce CPU, memory, PID, wall-clock, and output limits.
- Never execute learner code on the worker host without isolation, and do not
  run privileged sandboxes by default.
- Never interpolate learner content into shell commands or reuse writable
  workspaces across learners.
- Avoid host filesystem mounts and the host Docker socket.
- Do not inject platform secrets or expose cloud metadata.
- Pin runtime images in production and clean resources on every exit path.
- Never log full learner source by default.

Sandboxing, runtime selection, Go/SQL/Kubernetes runners, production Kafka
composition, and end-to-end integration remain later work.
