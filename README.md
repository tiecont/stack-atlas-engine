# Stack Atlas Engine

The Engine is the Go execution plane for learner workloads. This repository is
independent of the API and Web repositories.

## Foundation worker

`go run ./cmd/worker` starts the worker process. It writes JSON logs to stdout
and stops cleanly on SIGINT or SIGTERM. Set `ENGINE_LOG_LEVEL` to `debug`,
`info`, `warn`, or `error`; the default is `info`.

The repo has no runtime dependency on Web or API checkouts. From the repository
root, `make run` starts the worker; `make fmt`, `make test`, `make vet`, and
`make build` are the corresponding focused Go workflows.

This foundation does not consume Kafka messages or execute learner code.

## Execution domain

`internal/execution` defines the internal runner interface, execution identity,
normalized learner outcome, and failure policies. These Go types are not Kafka
wire models. The target event flow is for the Engine to consume
`execution.requested.v1` and publish `execution.result.v1`. Event codecs and
application wiring are not implemented yet. The next plan must map the
contracts through codecs backed by shared fixtures, then provide source and
grader data to runners without requiring sibling repository checkouts.

The Kafka package includes a `kafka-go` transport and a per-record processor.
It publishes before committing and retries the same transiently failing record
before fetching another. This preserves per-partition commit order and remains
at-least-once, so result consumers must tolerate duplicates.

Configure `ENGINE_KAFKA_BROKERS`, `ENGINE_KAFKA_GROUP_ID`,
`ENGINE_KAFKA_REQUEST_TOPIC`, `ENGINE_KAFKA_RESULT_TOPIC`, and
`ENGINE_KAFKA_QUARANTINE_TOPIC`; retry backoff defaults to 250ms and can be set
with `ENGINE_KAFKA_RETRY_BACKOFF`. TLS and SASL are injected when constructing
the transport. The worker binary is not wired to this transport yet because
request/result codecs and runner composition require the shared contract fixture.

Engine work can proceed independently on runner behavior and transport
adapters. Do not claim end-to-end API execution until API event publishing, the
shared fixtures, and worker composition are implemented together.
