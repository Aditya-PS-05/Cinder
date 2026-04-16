//! Canonical order and fill types shared across risk, the paper /
//! live executors, and the Postgres orders table.
//!
//! Mirrors the `order_side`, `order_kind`, `time_in_force`, and
//! `order_status` enums defined in
//! `infra/migrations/postgres/V002__orders_and_fills.sql`.

use crate::decimal::{Price, Qty};
use crate::time::Timestamp;
use crate::venue::{Side, Symbol, Venue};

/// Idempotency key supplied by the caller. Opaque to the system.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientOrderId(pub String);

impl ClientOrderId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OrderKind {
    Limit,
    Market,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimeInForce {
    /// Good-til-cancel: unfilled remainder rests on the book.
    Gtc,
    /// Immediate-or-cancel: cancel any unfilled remainder.
    Ioc,
    /// Fill-or-kill: either fill in full or reject outright.
    Fok,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

#[derive(Clone, Debug)]
pub struct NewOrder {
    pub cid: ClientOrderId,
    pub venue: Venue,
    pub symbol: Symbol,
    pub side: Side,
    pub kind: OrderKind,
    pub tif: TimeInForce,
    pub qty: Qty,
    /// Required for `OrderKind::Limit`; ignored for `OrderKind::Market`.
    pub price: Option<Price>,
    pub ts: Timestamp,
}

#[derive(Clone, Debug)]
pub struct Fill {
    pub cid: ClientOrderId,
    pub venue: Venue,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub ts: Timestamp,
}

/// Terminal result of submitting a single order: every fill that
/// occurred plus the final [`OrderStatus`].
#[derive(Clone, Debug)]
pub struct ExecReport {
    pub cid: ClientOrderId,
    pub status: OrderStatus,
    pub filled_qty: Qty,
    pub avg_price: Option<Price>,
    /// Populated for `Rejected` / `Expired` outcomes.
    pub reason: Option<String>,
    pub fills: Vec<Fill>,
}

impl ExecReport {
    pub fn rejected(cid: ClientOrderId, reason: impl Into<String>) -> Self {
        Self {
            cid,
            status: OrderStatus::Rejected,
            filled_qty: Qty(0),
            avg_price: None,
            reason: Some(reason.into()),
            fills: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_builder_populates_fields() {
        let r = ExecReport::rejected(ClientOrderId::new("c1"), "bad qty");
        assert_eq!(r.status, OrderStatus::Rejected);
        assert_eq!(r.filled_qty, Qty(0));
        assert_eq!(r.reason.as_deref(), Some("bad qty"));
        assert!(r.fills.is_empty());
    }
}
