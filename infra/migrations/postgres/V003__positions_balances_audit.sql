-- Positions and balances are derived state but we persist a materialized
-- view for fast risk checks. `audit_log` is the compliance append-only
-- trail: every order mutation and risk override writes a row here.

CREATE TABLE IF NOT EXISTS positions (
    account_id      BIGINT      NOT NULL REFERENCES accounts(account_id),
    instrument_id   BIGINT      NOT NULL REFERENCES instruments(instrument_id),
    qty             BIGINT      NOT NULL DEFAULT 0,   -- signed
    avg_entry_price BIGINT      NOT NULL DEFAULT 0,
    realized_pnl    BIGINT      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, instrument_id)
);

CREATE TABLE IF NOT EXISTS balances (
    account_id BIGINT      NOT NULL REFERENCES accounts(account_id),
    asset      TEXT        NOT NULL,
    free       BIGINT      NOT NULL DEFAULT 0,
    locked     BIGINT      NOT NULL DEFAULT 0,
    scale      SMALLINT    NOT NULL CHECK (scale BETWEEN 0 AND 18),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, asset)
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id     BIGSERIAL   PRIMARY KEY,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor        TEXT        NOT NULL,
    action       TEXT        NOT NULL,
    subject_type TEXT        NOT NULL,
    subject_id   TEXT        NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_audit_subject
    ON audit_log (subject_type, subject_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_actor
    ON audit_log (actor, occurred_at DESC);
