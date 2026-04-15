-- Raw trade prints. One row per venue execution. `price` and `qty` are
-- fixed-point mantissas at the instrument's scale (same representation
-- as in-memory Price/Qty) so tick math stays exact.

CREATE TABLE IF NOT EXISTS trades
(
    venue       LowCardinality(String),
    symbol      LowCardinality(String),
    exchange_ts DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
    local_ts    DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
    seq         UInt64               CODEC(DoubleDelta, ZSTD(3)),
    trade_id    String               CODEC(ZSTD(3)),
    price       Int64                CODEC(DoubleDelta, ZSTD(3)),
    qty         Int64                CODEC(DoubleDelta, ZSTD(3)),
    taker_side  Enum8('unknown' = 0, 'buy' = 1, 'sell' = 2)
)
ENGINE = MergeTree
PARTITION BY toDate(exchange_ts)
ORDER BY (venue, symbol, exchange_ts)
SETTINGS index_granularity = 8192
