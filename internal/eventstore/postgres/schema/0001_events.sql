-- StreamLedger event log schema.
--
-- events is the append-only source of truth. stream_versions caches each
-- stream's current version so Append can take a single row lock per stream
-- (SELECT ... FOR UPDATE) instead of scanning/COUNT(*)-ing the events table
-- on every write. idempotency_keys + idempotency_key_events make command
-- dedupe (the "duplicate=true" path of Append) durable across restarts.

CREATE TABLE IF NOT EXISTS events (
    global_seq     BIGSERIAL PRIMARY KEY,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    version        BIGINT      NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    dedupe_key     TEXT,
    correlation_id TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_type, aggregate_id, version)
);

CREATE INDEX IF NOT EXISTS events_aggregate_idx
    ON events (aggregate_type, aggregate_id, version);

CREATE TABLE IF NOT EXISTS stream_versions (
    aggregate_type TEXT   NOT NULL,
    aggregate_id   TEXT   NOT NULL,
    version        BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (aggregate_type, aggregate_id)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    dedupe_key TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS idempotency_key_events (
    dedupe_key TEXT   NOT NULL REFERENCES idempotency_keys (dedupe_key) ON DELETE CASCADE,
    global_seq BIGINT NOT NULL REFERENCES events (global_seq),
    PRIMARY KEY (dedupe_key, global_seq)
);
