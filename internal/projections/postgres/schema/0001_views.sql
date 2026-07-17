-- StreamLedger CQRS read-model schema.
--
-- accounts_view / inventory_view are the materialized projections served
-- directly by the query API. projection_checkpoints tracks, per named
-- projection, the last GlobalSeq applied — the durable dedupe mechanism
-- that makes re-application of an already-seen event a no-op regardless of
-- how many times the message bus redelivers it.

CREATE TABLE IF NOT EXISTS accounts_view (
    account_id    TEXT PRIMARY KEY,
    balance_cents BIGINT      NOT NULL,
    version       BIGINT      NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_view (
    item_id            TEXT PRIMARY KEY,
    available_quantity BIGINT      NOT NULL,
    reserved_quantity  BIGINT      NOT NULL,
    version            BIGINT      NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    global_seq      BIGINT NOT NULL DEFAULT 0
);
