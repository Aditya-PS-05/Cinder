-- Base lookup tables. Every other relation joins against these.
-- Writes are idempotent so seeds and fixtures can re-run safely.

CREATE TABLE IF NOT EXISTS venues (
    venue_id   TEXT PRIMARY KEY,
    kind       TEXT        NOT NULL CHECK (kind IN ('cex','dex')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS instruments (
    instrument_id BIGSERIAL PRIMARY KEY,
    venue_id      TEXT        NOT NULL REFERENCES venues(venue_id),
    symbol        TEXT        NOT NULL,
    base_asset    TEXT        NOT NULL,
    quote_asset   TEXT        NOT NULL,
    -- Fixed-point scales matching the in-memory Price/Qty representation.
    price_scale   SMALLINT    NOT NULL CHECK (price_scale BETWEEN 0 AND 18),
    qty_scale     SMALLINT    NOT NULL CHECK (qty_scale   BETWEEN 0 AND 18),
    min_qty       BIGINT      NOT NULL DEFAULT 0,
    min_notional  BIGINT      NOT NULL DEFAULT 0,
    active        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (venue_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_instruments_active
    ON instruments (venue_id, symbol) WHERE active;

CREATE TABLE IF NOT EXISTS accounts (
    account_id  BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    venue_id    TEXT        NOT NULL REFERENCES venues(venue_id),
    environment TEXT        NOT NULL CHECK (environment IN ('paper','testnet','live')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
