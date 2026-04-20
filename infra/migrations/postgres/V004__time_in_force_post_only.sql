-- Extend the `time_in_force` enum with `post_only` so orders that
-- must never take liquidity can be stored alongside the existing
-- gtc/ioc/fok/day labels. The application-side mapping lives in
-- `ts_storage::orders::tif_to_pg`.
--
-- `ALTER TYPE ... ADD VALUE` is forward-only and cannot run inside a
-- transaction block; guard with IF NOT EXISTS so replays on an
-- already-migrated database are a no-op.

ALTER TYPE time_in_force ADD VALUE IF NOT EXISTS 'post_only';
