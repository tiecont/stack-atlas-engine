# Execution readiness

For the domain/wire distinction, processing policy, fixture strategy, and
security requirements, see [the execution contract boundary](../execution-contract-boundary.md).

Engine remains an independent Go module and execution plane. CI checks Go
formatting, race-enabled unit tests, `go vet`, worker compilation, and that its
module graph contains no API or Web module dependency. It does not require
sibling checkouts, PostgreSQL, Kafka, a sandbox, or a runner.

## Domain and transport boundary

`internal/execution` contains internal job/result semantics and failure
classification. `internal/platform/kafka` accepts raw records and injected
request/result codecs. These are boundaries, not event wire models. The worker
is intentionally not wired to consume messages or execute learner code.

## Fixture strategy

The API owns future versioned execution request/result contracts. Engine will
not add contract fields or JSON codecs until the API has agreed the contract
and checked in representative fixtures. At that point:

1. API records the contract and canonical fixture in its own repository.
2. Engine checks in an identical fixture copy under its own test tree; builds
   and tests continue to work from an Engine-only checkout.
3. Codec tests cover valid fixtures, unsupported versions, malformed payloads,
   ID preservation, and trace headers.
4. Cross-repository execution tests are a later integration gate after API
   submission/outbox, Engine wiring, and Kafka infrastructure are implemented.

No fixture is checked in now because there is no API-owned wire contract yet.
The Kafka adapter and internal execution model remain the only execution
readiness boundary in this phase. Runner, sandbox, and Kafka end-to-end work
remain later phases.
