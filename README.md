# streamledger

An event-sourced, CQRS-projected ledger service (Go, gRPC) for balances and inventory, designed so concurrent writes cannot produce a double-spend or a lost update.

[![CI](https://github.com/SlateGitOrg/streamledger/actions/workflows/ci.yml/badge.svg)](https://github.com/SlateGitOrg/streamledger/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Why

A ledger has to stay correct when many requests hit the same account at once. The naive `SELECT balance ...; UPDATE balance = balance - amount` is a race: two concurrent withdrawals can both read the same balance, both pass the check, and both commit, which is a double-spend. Retrying a timed-out payment can double-charge. A plain `UPDATE` also leaves no record of what happened to the account.

StreamLedger addresses this with three techniques:

- **Event sourcing**: every state change is an immutable, append-only event. The balance is never stored directly; it is derived by replaying events.
- **Optimistic concurrency**: every append is a compare-and-swap against the stream version. Of two writers racing on one account, one wins and the other retries against fresh state (or fails fast, caller's choice).
- **Idempotent consumers via dedupe keys**: a command may carry a `dedupe_key`. A retried command (client timeout, at-least-once bus, a script run twice) replays the original result instead of re-applying the effect.

## Delivery guarantee: effectively-once

Delivery is at-least-once: the bus can redeliver. Consumers are idempotent, so a redelivery is a no-op. Two mechanisms provide this:

1. **Command-side dedupe.** `dedupe_key` is checked against the event log before any business rule runs. A retried `Deposit` with the same key appends no second event and returns the cached result of the first.
2. **Projection-side checkpoints.** Each projection tracks the highest event sequence number it has applied and skips anything at or below it, whether the duplicate came from the bus or an overlapping catch-up poll. `internal/projections/projector_test.go` exercises concurrent redelivery.

The event log is the source of truth; the bus is a best-effort, low-latency notification on top of it. If every bus message were lost, a projection's periodic catch-up poll against the log would still converge it.

## Features

- gRPC command API: `OpenAccount`, `Deposit`, `Withdraw`, `Transfer`, `ReserveStock` and more, each with an optional `dedupe_key` and `expected_version`.
- Append-only event store with optimistic concurrency (in-memory and Postgres implementations).
- CQRS projections (balances, inventory) with checkpoints, idempotent apply and a `GetProjectionLag` query.
- Pluggable event bus: in-memory, NATS JetStream, and a redelivery simulator for testing.
- Prometheus metrics for commands, conflicts, projection lag and dedupe hits.
- Runs fully in-process with no database or broker; Docker image included.

## Architecture

```mermaid
flowchart LR
    subgraph Write["Write path (commands)"]
        Client[gRPC client] -->|OpenAccount / Deposit /<br/>Withdraw / Transfer<br/>+ dedupe_key| API[gRPC Command API]
        API --> Svc[Ledger Service]
        Svc -->|1: dedupe check| Store[(Event Store<br/>append-only log)]
        Svc -->|2: decide event<br/>from folded state| Decide[Decide* pure functions]
        Svc -->|3: append with<br/>expected_version| Store
    end

    Store -->|best-effort publish| Bus[[Event Bus<br/>in-memory / NATS JetStream]]
    Store -->|catch-up poll,<br/>the durability backstop| Proj

    subgraph Read["Read path (CQRS projections)"]
        Bus -->|at-least-once delivery| Proj[Projector<br/>checkpoint + idempotent apply]
        Proj --> Views[(Read Models<br/>balances / inventory)]
        Proj -.->|lag, dedupe, throughput| Metrics[[Prometheus]]
    end

    ClientQ[gRPC client] -->|GetBalance / GetInventory /<br/>GetProjectionLag| Views
```

1. A command (`Deposit`, `Withdraw`, `Transfer`, `ReserveStock`, ...) arrives over gRPC with an optional `dedupe_key` and, optionally, an `expected_version` for explicit optimistic locking.
2. `Service` checks the dedupe key, loads the aggregate's current state by folding its event stream (`FoldAccount` / `FoldInventory`), and calls the matching pure `Decide*` function — the only place business rules live, and the reason they're trivial to table-drive-test with no mocks.
3. The resulting event is appended with an `expected_version` check. On a real conflict (someone else wrote first), the handler reloads state and retries automatically — up to a bounded retry budget — so callers see a conflict only when it's genuinely irreconcilable (e.g. insufficient funds on every retry).
4. The event is durably in the log before anything else happens. It's then published, best-effort, to the bus for low-latency projection updates.
5. Projections apply events behind a monotonically increasing checkpoint. Both the checkpoint check and the view mutation happen inside one `WithTx` call, so a duplicate delivery — whether from the bus or an overlapping catch-up poll — is a guaranteed no-op, not a race.
6. Queries (`GetBalance`, `GetInventory`) read directly from the materialized view — no replay on the read path — and `GetProjectionLag` reports exactly how far behind the write side each projection currently is, in events.

## Quick start

Prerequisites: Go 1.25+. No Docker, no database, no message broker needed to build, test, or run the demo.

```bash
git clone https://github.com/SlateGitOrg/streamledger.git
cd streamledger
go build ./...
go test -race -count=1 ./...   # everything in-memory, a few seconds

# Run the service entirely in-process (in-memory log + read models + bus):
go run ./cmd/streamledger
# gRPC command/query API on :9090, Prometheus metrics on :9091/metrics
```

Point it at real infrastructure for a production deployment:

```bash
go run ./cmd/streamledger \
  -storage=postgres -database-url="postgres://user:pass@host/db" \
  -bus=nats -nats-url="nats://host:4222"
```

### Docker

The image runs the in-memory configuration by default:

```bash
docker build -t streamledger .
docker run --rm -p 9090:9090 -p 9091:9091 streamledger
```

Pass the same flags as above (for example `-storage=postgres -database-url=...`) after the image name to use real infrastructure.

### Usage

With [grpcurl](https://github.com/fullstorydev/grpcurl) (server reflection is enabled):

```bash
grpcurl -plaintext -d '{"account_id":"acct-1","initial_balance_cents":10000}' \
  localhost:9090 ledger.v1.LedgerService/OpenAccount

grpcurl -plaintext -d '{"account_id":"acct-1","amount_cents":2500,"dedupe_key":"req-42","expected_version":-1}' \
  localhost:9090 ledger.v1.LedgerService/Deposit

grpcurl -plaintext -d '{"account_id":"acct-1"}' \
  localhost:9090 ledger.v1.LedgerService/GetBalance
```

Output captured from a local run (trimmed). Repeating the same `Deposit` with `req-42` replays the first result instead of applying it again:

```text
# OpenAccount        {"applied": true, "newVersion": "1", "message": "applied"}
# Deposit            {"applied": true, "newVersion": "2", "message": "applied"}
# Deposit (retry)    {"duplicate": true, "newVersion": "2", "message": "duplicate: replaying cached result"}
# GetBalance         {"accountId": "acct-1", "found": true, "balanceCents": "12500", "version": "2"}
```

## Testing

Every test in the default suite (`go test ./...`) runs against in-memory fakes (`internal/eventstore/memory`, `internal/projections/memory`, `internal/bus/inmemory`) — no Docker daemon, no live Postgres, no live NATS, no network. That's a deliberate constraint, not a shortcut: it means the properties under test are properties of the *code* (retry logic, checkpoint math, decide functions), not of infrastructure that happened to be up when the test ran. The table-driven suite specifically proves:

- **Idempotent apply**: injecting the same event multiple times (including concurrently, using plain goroutines and a `sync.WaitGroup`) never double-applies a projection (`internal/projections/projector_test.go`).
- **Optimistic-concurrency conflicts**: a stale `expected_version` is rejected (`ErrVersionConflict`), and 30 goroutines racing withdrawals against a single account converge to *exactly* the correct number of successes with a final balance of zero — never negative (`internal/ledger/service_test.go`).
- **Projection correctness**: the CQRS read model, rebuilt purely from the event log, matches a direct fold of that same log bit-for-bit.
- **Command-level dedupe**: a retried command (including concurrently, simulating a racing at-least-once bus) appends exactly one event, not `N`.

The Postgres and NATS JetStream implementations are real, complete production code (`internal/eventstore/postgres`, `internal/projections/postgres`, `internal/bus/natsbus`) — they're just not exercised by the default suite. `internal/eventstore/postgres/postgres_integration_test.go` and `internal/projections/postgres/postgres_integration_test.go` are gated behind a `//go:build integration` tag and a `DATABASE_URL` environment variable, so `go test -tags=integration ./...` against a real Postgres instance (local, CI service container or Testcontainers) exercises the exact same behavior against the real thing. Neither file compiles into, nor is required by, the default build.

## Project layout

```
cmd/streamledger/        entrypoint: wires storage + bus + gRPC server + metrics
internal/ledger/         pure domain logic: Decide* functions, event types, fold/state
internal/eventstore/      the append-only log interface + memory & postgres implementations
internal/projections/    CQRS read side: Projector, checkpoints, account/inventory appliers
internal/bus/            pub/sub abstraction + in-memory, NATS JetStream, and a redelivery
                          simulator (DuplicatingPublisher) used to test idempotency honestly
internal/grpcserver/     gRPC service implementation + domain-error-to-gRPC-code mapping
internal/metrics/        Prometheus collectors (commands, conflicts, projection lag/dedupe)
proto/, gen/             the LedgerService API definition and its generated Go bindings
```

## Limitations and next steps

- **Snapshots**: long-lived streams currently replay from the beginning on every command. A snapshot-every-N-events strategy would bound replay cost; the `Store` interface already has the seam for it (`LoadStream` could take an `afterVersion` cursor).
- **Outbox pattern for the bus publish**: today's "durable log write, then best-effort bus publish" is correct (catch-up polling is the backstop) but means a lost publish costs up to `catchUpPollInterval` of added latency, not correctness. A transactional outbox row updated in the same Postgres transaction as the event insert would tighten that further.
- **Multiple projector replicas**: checkpoint check and view update run in one `WithTx` call, and concurrent `ApplyEvent` calls on one projector are tested. Running several projector replicas for the same projection has not been tested, so treat it as unverified.

## License

MIT — see [LICENSE](LICENSE).
