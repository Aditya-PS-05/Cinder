//! Postgres persistence for orders and fills.
//!
//! Maps [`ts_core`] order/fill types to the `orders` and `fills`
//! tables defined by `V002__orders_and_fills.sql`. Enum text columns
//! (`order_side`, `order_kind`, `time_in_force`, `order_status`) are
//! written as string literals with SQL-level `::order_side` casts,
//! which avoids adding per-enum `ToSql` wrappers inside `ts-core`.
//!
//! The repo exposes three lifecycle operations:
//!   * [`OrderRepo::insert_order`] — first-time submission, generates `order_id`
//!   * [`OrderRepo::apply_exec_report`] — updates status / fill qty / avg price
//!   * [`OrderRepo::insert_fill`] — appends one fill row, idempotent on replay
//!
//! Plus helpers to idempotently register venues, accounts, and
//! instruments so the FK targets exist before the first order.

use chrono::{DateTime, Utc};
use ts_core::{ExecReport, OrderKind, OrderStatus, Side, TimeInForce};

use crate::error::StorageError;
use crate::postgres::Postgres;

/// Text label that matches the Postgres `order_side` enum. Returns an
/// error for [`Side::Unknown`] since the schema has no corresponding
/// label — unknown sides should not reach the persistence layer.
pub fn side_to_pg(s: Side) -> Result<&'static str, StorageError> {
    match s {
        Side::Buy => Ok("buy"),
        Side::Sell => Ok("sell"),
        Side::Unknown => Err(StorageError::InvalidEnum {
            field: "side",
            value: "unknown",
        }),
    }
}

/// Text label that matches the Postgres `order_kind` enum.
pub fn kind_to_pg(k: OrderKind) -> &'static str {
    match k {
        OrderKind::Limit => "limit",
        OrderKind::Market => "market",
    }
}

/// Text label that matches the Postgres `time_in_force` enum.
pub fn tif_to_pg(t: TimeInForce) -> &'static str {
    match t {
        TimeInForce::Gtc => "gtc",
        TimeInForce::Ioc => "ioc",
        TimeInForce::Fok => "fok",
        TimeInForce::PostOnly => "post_only",
    }
}

/// Text label that matches the Postgres `order_status` enum.
pub fn status_to_pg(s: OrderStatus) -> &'static str {
    match s {
        OrderStatus::New => "new",
        OrderStatus::PartiallyFilled => "partially_filled",
        OrderStatus::Filled => "filled",
        OrderStatus::Canceled => "canceled",
        OrderStatus::Rejected => "rejected",
        OrderStatus::Expired => "expired",
    }
}

/// Whether a status stops the order from ever progressing again. The
/// `terminal_at` column is stamped once for the first terminal report.
pub fn is_terminal(s: OrderStatus) -> bool {
    matches!(
        s,
        OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Expired
    )
}

/// Inputs for [`OrderRepo::insert_order`]. Captures the fields the
/// `orders` row needs at submission time — the subset of [`ts_core::NewOrder`]
/// plus account/strategy context that the runner owns.
#[derive(Clone, Debug)]
pub struct InsertOrderReq<'a> {
    pub cid: &'a str,
    pub account_id: i64,
    pub instrument_id: i64,
    pub strategy_id: &'a str,
    pub side: Side,
    pub kind: OrderKind,
    pub tif: TimeInForce,
    pub qty: i64,
    pub limit_price: Option<i64>,
    pub stop_price: Option<i64>,
    pub status: OrderStatus,
}

/// Inputs for [`OrderRepo::insert_fill`]. Kept a distinct struct from
/// [`ts_core::Fill`] because the row carries venue-level attributes
/// (`venue_fill_id`, fees) that the core type omits.
#[derive(Clone, Debug)]
pub struct InsertFillReq<'a> {
    pub order_id: i64,
    pub venue_fill_id: &'a str,
    pub price: i64,
    pub qty: i64,
    pub fee: i64,
    pub fee_asset: Option<&'a str>,
    pub is_maker: Option<bool>,
    pub exchange_ts: DateTime<Utc>,
}

/// Thin DAL wrapping a [`Postgres`] pool. One client is checked out
/// per call; callers that issue many writes back-to-back can amortize
/// by holding onto their own pool client instead, but for the runner's
/// sidecar cadence this is already fine.
pub struct OrderRepo<'a> {
    pg: &'a Postgres,
}

impl<'a> OrderRepo<'a> {
    pub fn new(pg: &'a Postgres) -> Self {
        Self { pg }
    }

    async fn client(&self) -> Result<deadpool_postgres::Client, StorageError> {
        self.pg
            .pool()
            .get()
            .await
            .map_err(|e| StorageError::PostgresPool(e.to_string()))
    }

    /// Idempotently register a venue row.
    pub async fn resolve_venue(&self, venue_id: &str, kind: &str) -> Result<(), StorageError> {
        let client = self.client().await?;
        client
            .execute(
                "INSERT INTO venues (venue_id, kind) VALUES ($1, $2)
                 ON CONFLICT (venue_id) DO NOTHING",
                &[&venue_id, &kind],
            )
            .await?;
        Ok(())
    }

    /// Idempotently register an account and return its `account_id`.
    /// `environment` must be one of `paper`, `testnet`, `live` to
    /// satisfy the CHECK constraint.
    pub async fn resolve_account(
        &self,
        name: &str,
        venue_id: &str,
        environment: &str,
    ) -> Result<i64, StorageError> {
        let client = self.client().await?;
        let row = client
            .query_one(
                "INSERT INTO accounts (name, venue_id, environment)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (name)
                 DO UPDATE SET name = EXCLUDED.name
                 RETURNING account_id",
                &[&name, &venue_id, &environment],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Idempotently register an instrument and return its `instrument_id`.
    pub async fn resolve_instrument(
        &self,
        venue_id: &str,
        symbol: &str,
        base_asset: &str,
        quote_asset: &str,
        price_scale: i16,
        qty_scale: i16,
    ) -> Result<i64, StorageError> {
        let client = self.client().await?;
        let row = client
            .query_one(
                "INSERT INTO instruments (venue_id, symbol, base_asset, quote_asset, price_scale, qty_scale)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (venue_id, symbol)
                 DO UPDATE SET symbol = EXCLUDED.symbol
                 RETURNING instrument_id",
                &[
                    &venue_id,
                    &symbol,
                    &base_asset,
                    &quote_asset,
                    &price_scale,
                    &qty_scale,
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Insert a newly-submitted order. Returns the generated `order_id`.
    /// The `client_order_id` is a write-once idempotency key — a second
    /// `insert_order` for the same cid fails with a UNIQUE violation by
    /// design. Use [`Self::apply_exec_report`] for follow-up state.
    pub async fn insert_order(&self, req: &InsertOrderReq<'_>) -> Result<i64, StorageError> {
        let client = self.client().await?;
        let row = client
            .query_one(
                "INSERT INTO orders (
                    client_order_id, account_id, instrument_id, strategy_id,
                    side, kind, tif, limit_price, stop_price, qty, status
                 ) VALUES (
                    $1, $2, $3, $4,
                    $5::order_side, $6::order_kind, $7::time_in_force,
                    $8, $9, $10, $11::order_status
                 ) RETURNING order_id",
                &[
                    &req.cid,
                    &req.account_id,
                    &req.instrument_id,
                    &req.strategy_id,
                    &side_to_pg(req.side)?,
                    &kind_to_pg(req.kind),
                    &tif_to_pg(req.tif),
                    &req.limit_price,
                    &req.stop_price,
                    &req.qty,
                    &status_to_pg(req.status),
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Apply an [`ExecReport`] to the orders row keyed by `cid`.
    ///
    /// The UPDATE stamps `submitted_at` on the first non-`new` report
    /// and `terminal_at` on the first terminal report, so re-applying
    /// the same report does not overwrite the original timestamps.
    /// Returns the number of rows affected; callers can treat `0` as
    /// "unknown cid" (typically means `insert_order` was skipped).
    pub async fn apply_exec_report(
        &self,
        cid: &str,
        report: &ExecReport,
    ) -> Result<u64, StorageError> {
        let client = self.client().await?;
        let status = status_to_pg(report.status);
        let filled: i64 = report.filled_qty.0;
        let avg_price: Option<i64> = report.avg_price.map(|p| p.0);
        let reason: Option<&str> = report.reason.as_deref();
        let terminal = is_terminal(report.status);

        let rows = client
            .execute(
                "UPDATE orders SET
                    status = $2::order_status,
                    filled_qty = $3,
                    avg_fill_price = $4,
                    reject_reason = $5,
                    submitted_at = COALESCE(
                        submitted_at,
                        CASE WHEN $2::order_status <> 'new' THEN now() ELSE NULL END
                    ),
                    terminal_at = CASE
                        WHEN $6 AND terminal_at IS NULL THEN now()
                        ELSE terminal_at
                    END
                 WHERE client_order_id = $1",
                &[&cid, &status, &filled, &avg_price, &reason, &terminal],
            )
            .await?;
        Ok(rows)
    }

    /// Look up `order_id` by `client_order_id`. Callers use this to
    /// resolve the FK before inserting a fill when the cid's row was
    /// created on a previous process lifetime.
    pub async fn lookup_order_id(&self, cid: &str) -> Result<Option<i64>, StorageError> {
        let client = self.client().await?;
        let row = client
            .query_opt(
                "SELECT order_id FROM orders WHERE client_order_id = $1",
                &[&cid],
            )
            .await?;
        Ok(row.map(|r| r.get::<_, i64>(0)))
    }

    /// Insert one fill. If `(order_id, venue_fill_id)` already exists
    /// the existing row is returned so replay is idempotent.
    pub async fn insert_fill(&self, req: &InsertFillReq<'_>) -> Result<i64, StorageError> {
        let client = self.client().await?;
        let row = client
            .query_one(
                "INSERT INTO fills (
                    order_id, venue_fill_id, price, qty, fee, fee_asset,
                    is_maker, exchange_ts
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (order_id, venue_fill_id)
                 DO UPDATE SET venue_fill_id = EXCLUDED.venue_fill_id
                 RETURNING fill_id",
                &[
                    &req.order_id,
                    &req.venue_fill_id,
                    &req.price,
                    &req.qty,
                    &req.fee,
                    &req.fee_asset,
                    &req.is_maker,
                    &req.exchange_ts,
                ],
            )
            .await?;
        Ok(row.get(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_labels_match_schema_enum() {
        assert_eq!(side_to_pg(Side::Buy).unwrap(), "buy");
        assert_eq!(side_to_pg(Side::Sell).unwrap(), "sell");
    }

    #[test]
    fn side_unknown_is_rejected() {
        let err = side_to_pg(Side::Unknown).unwrap_err();
        assert!(
            matches!(err, StorageError::InvalidEnum { field: "side", .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn kind_labels_match_schema_enum() {
        assert_eq!(kind_to_pg(OrderKind::Limit), "limit");
        assert_eq!(kind_to_pg(OrderKind::Market), "market");
    }

    #[test]
    fn tif_labels_match_schema_enum() {
        // Every variant maps to a label present in the
        // `time_in_force` Postgres enum (see
        // `infra/migrations/postgres/V002__orders_and_fills.sql` and
        // `V004__time_in_force_post_only.sql`). Adding a variant to
        // `ts_core::TimeInForce` without a matching migration would
        // break inserts at runtime; this pin catches the omission.
        assert_eq!(tif_to_pg(TimeInForce::Gtc), "gtc");
        assert_eq!(tif_to_pg(TimeInForce::Ioc), "ioc");
        assert_eq!(tif_to_pg(TimeInForce::Fok), "fok");
        assert_eq!(tif_to_pg(TimeInForce::PostOnly), "post_only");
    }

    #[test]
    fn status_labels_cover_every_variant() {
        assert_eq!(status_to_pg(OrderStatus::New), "new");
        assert_eq!(
            status_to_pg(OrderStatus::PartiallyFilled),
            "partially_filled"
        );
        assert_eq!(status_to_pg(OrderStatus::Filled), "filled");
        assert_eq!(status_to_pg(OrderStatus::Canceled), "canceled");
        assert_eq!(status_to_pg(OrderStatus::Rejected), "rejected");
        assert_eq!(status_to_pg(OrderStatus::Expired), "expired");
    }

    #[test]
    fn terminal_classification_matches_schema() {
        assert!(!is_terminal(OrderStatus::New));
        assert!(!is_terminal(OrderStatus::PartiallyFilled));
        assert!(is_terminal(OrderStatus::Filled));
        assert!(is_terminal(OrderStatus::Canceled));
        assert!(is_terminal(OrderStatus::Rejected));
        assert!(is_terminal(OrderStatus::Expired));
    }
}
