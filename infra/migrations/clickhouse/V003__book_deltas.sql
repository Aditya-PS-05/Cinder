-- Incremental book updates. `prev_seq` is the sequence this delta
-- builds on; replay is `(prev_seq,seq)` ordered per `(venue,symbol)`.
-- A level with qty==0 means "delete".

CREATE TABLE IF NOT EXISTS book_deltas
(
    venue       LowCardinality(String),
    symbol      LowCardinality(String),
    exchange_ts DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
    local_ts    DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
    seq         UInt64               CODEC(DoubleDelta, ZSTD(3)),
    prev_seq    UInt64               CODEC(DoubleDelta, ZSTD(3)),
    bid_prices  Array(Int64)         CODEC(DoubleDelta, ZSTD(3)),
    bid_qtys    Array(Int64)         CODEC(DoubleDelta, ZSTD(3)),
    ask_prices  Array(Int64)         CODEC(DoubleDelta, ZSTD(3)),
    ask_qtys    Array(Int64)         CODEC(DoubleDelta, ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toDate(exchange_ts)
ORDER BY (venue, symbol, exchange_ts)
SETTINGS index_granularity = 8192
