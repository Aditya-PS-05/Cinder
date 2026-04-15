-- Liquidation events. `side` is the side that was liquidated — a long
-- liquidation reports 'buy'.

CREATE TABLE IF NOT EXISTS liquidations
(
    venue       LowCardinality(String),
    symbol      LowCardinality(String),
    exchange_ts DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
    local_ts    DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
    price       Int64                CODEC(DoubleDelta, ZSTD(3)),
    qty         Int64                CODEC(DoubleDelta, ZSTD(3)),
    side        Enum8('unknown' = 0, 'buy' = 1, 'sell' = 2)
)
ENGINE = MergeTree
PARTITION BY toDate(exchange_ts)
ORDER BY (venue, symbol, exchange_ts)
SETTINGS index_granularity = 8192
