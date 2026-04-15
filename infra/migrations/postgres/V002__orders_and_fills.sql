-- Orders + fills. `client_order_id` is our idempotency key and is
-- generated before we hit the wire. `venue_order_id` fills in once the
-- venue acks. Prices and quantities are fixed-point mantissas at the
-- instrument's scale so arithmetic stays exact.

DO $$ BEGIN
    CREATE TYPE order_side AS ENUM ('buy','sell');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE order_kind AS ENUM ('market','limit','stop','stop_limit');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE time_in_force AS ENUM ('gtc','ioc','fok','day');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE order_status AS ENUM (
        'new','pending_submit','submitted','partially_filled','filled',
        'pending_cancel','canceled','rejected','expired'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS orders (
    order_id        BIGSERIAL     PRIMARY KEY,
    client_order_id TEXT          NOT NULL UNIQUE,
    venue_order_id  TEXT,
    account_id      BIGINT        NOT NULL REFERENCES accounts(account_id),
    instrument_id   BIGINT        NOT NULL REFERENCES instruments(instrument_id),
    strategy_id     TEXT          NOT NULL,
    side            order_side    NOT NULL,
    kind            order_kind    NOT NULL,
    tif             time_in_force NOT NULL DEFAULT 'gtc',
    limit_price     BIGINT,
    stop_price      BIGINT,
    qty             BIGINT        NOT NULL CHECK (qty > 0),
    filled_qty      BIGINT        NOT NULL DEFAULT 0 CHECK (filled_qty >= 0),
    avg_fill_price  BIGINT,
    status          order_status  NOT NULL,
    reject_reason   TEXT,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    submitted_at    TIMESTAMPTZ,
    terminal_at     TIMESTAMPTZ,
    CHECK (filled_qty <= qty)
);

CREATE INDEX IF NOT EXISTS idx_orders_strategy_created
    ON orders (strategy_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_open_by_account
    ON orders (account_id, status)
    WHERE status IN ('new','pending_submit','submitted','partially_filled','pending_cancel');

CREATE INDEX IF NOT EXISTS idx_orders_venue_order_id
    ON orders (venue_order_id) WHERE venue_order_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS fills (
    fill_id       BIGSERIAL   PRIMARY KEY,
    order_id      BIGINT      NOT NULL REFERENCES orders(order_id),
    venue_fill_id TEXT        NOT NULL,
    price         BIGINT      NOT NULL,
    qty           BIGINT      NOT NULL CHECK (qty > 0),
    fee           BIGINT      NOT NULL DEFAULT 0,
    fee_asset     TEXT,
    is_maker      BOOLEAN,
    exchange_ts   TIMESTAMPTZ NOT NULL,
    local_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (order_id, venue_fill_id)
);

CREATE INDEX IF NOT EXISTS idx_fills_order       ON fills (order_id);
CREATE INDEX IF NOT EXISTS idx_fills_exchange_ts ON fills (exchange_ts DESC);
