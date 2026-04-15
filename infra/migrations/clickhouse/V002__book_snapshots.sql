-- Full order book snapshots. Bid and ask levels are stored as parallel
-- arrays so a row is self-contained and a single scan gives the whole
-- book at a point in time. Array(Int64) compresses well with DoubleDelta
-- because adjacent levels differ by tick size.

CREATE TABLE IF NOT EXISTS book_snapshots
(
    venue       LowCardinality(String),
    symbol      LowCardinality(String),
    exchange_ts DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
    local_ts    DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
    seq         UInt64               CODEC(DoubleDelta, ZSTD(3)),
    bid_prices  Array(Int64)         CODEC(DoubleDelta, ZSTD(3)),
    bid_qtys    Array(Int64)         CODEC(DoubleDelta, ZSTD(3)),
    ask_prices  Array(Int64)         CODEC(DoubleDelta, ZSTD(3)),
    ask_qtys    Array(Int64)         CODEC(DoubleDelta, ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toDate(exchange_ts)
ORDER BY (venue, symbol, exchange_ts)
SETTINGS index_granularity = 8192
