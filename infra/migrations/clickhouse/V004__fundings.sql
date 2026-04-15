-- Perpetual funding rate events. `rate` is stored as a signed fraction
-- (0.0001 = 1bp) because funding intervals and basis models prefer
-- double-precision math.

CREATE TABLE IF NOT EXISTS fundings
(
    venue             LowCardinality(String),
    symbol            LowCardinality(String),
    exchange_ts       DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
    local_ts          DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
    rate              Float64              CODEC(ZSTD(3)),
    next_funding_time DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toDate(exchange_ts)
ORDER BY (venue, symbol, exchange_ts)
SETTINGS index_granularity = 8192
