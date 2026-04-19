//! Pre-trade risk checks.
//!
//! [`RiskEngine`] enforces a symbol whitelist, per-symbol absolute
//! position cap, a per-order notional cap, and a global open-order
//! cap. It is synchronous and mutable — a single trading loop is
//! expected to own one instance and call [`Self::check`] before
//! submission, then [`Self::record_submit`] / [`Self::record_fill`]
//! / [`Self::record_complete`] around each order's lifecycle.

#![forbid(unsafe_code)]

pub mod clock_skew_guard;
pub mod clock_skew_tracker;
pub mod kill_switch;
pub mod pnl_guard;
pub mod staleness_guard;
pub use clock_skew_guard::{ClockSkewBreach, ClockSkewGuard, ClockSkewGuardConfig};
pub use clock_skew_tracker::{ClockSkewSnapshot, ClockSkewTracker};
pub use kill_switch::{KillSwitch, KillSwitchConfig, TripReason};
pub use pnl_guard::{GuardBreach, PnlGuard, PnlGuardConfig};
pub use staleness_guard::{StalenessBreach, StalenessGuard, StalenessGuardConfig};

use std::collections::{HashMap, HashSet};

use thiserror::Error;

use ts_core::{ClientOrderId, Fill, NewOrder, OrderKind, Price, Qty, Side, Symbol};

#[derive(Clone, Debug)]
pub struct RiskConfig {
    /// Per-symbol absolute position cap (mantissa at qty_scale).
    pub max_position_qty: Qty,
    /// Per-order notional cap (mantissa at price_scale + qty_scale).
    pub max_order_notional: i64,
    /// Global cap on concurrently-open orders.
    pub max_open_orders: usize,
    /// Allowed symbols. An empty set means "allow all".
    pub whitelist: HashSet<Symbol>,
}

impl RiskConfig {
    /// A config that permits anything. Useful as a test baseline that
    /// individual tests tighten one field at a time.
    pub fn permissive() -> Self {
        Self {
            max_position_qty: Qty(i64::MAX),
            max_order_notional: i64::MAX,
            max_open_orders: usize::MAX,
            whitelist: HashSet::new(),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RiskRejection {
    #[error("symbol {symbol} is not in the whitelist")]
    NotWhitelisted { symbol: String },

    #[error("order would breach position cap on {symbol}: proposed={proposed}, limit={limit}")]
    MaxPositionBreached {
        symbol: String,
        proposed: i64,
        limit: i64,
    },

    #[error("order notional {notional} exceeds limit {limit}")]
    MaxNotionalBreached { notional: i64, limit: i64 },

    #[error("too many open orders: {current} >= {limit}")]
    TooManyOpenOrders { current: usize, limit: usize },

    #[error("invalid order: {0}")]
    InvalidOrder(&'static str),
}

pub struct RiskEngine {
    cfg: RiskConfig,
    positions: HashMap<Symbol, i64>,
    open_orders: usize,
}

impl RiskEngine {
    pub fn new(cfg: RiskConfig) -> Self {
        Self {
            cfg,
            positions: HashMap::new(),
            open_orders: 0,
        }
    }

    pub fn config(&self) -> &RiskConfig {
        &self.cfg
    }

    pub fn position(&self, symbol: &Symbol) -> i64 {
        self.positions.get(symbol).copied().unwrap_or(0)
    }

    pub fn open_orders(&self) -> usize {
        self.open_orders
    }

    /// Pre-trade check. `ref_price` is used for the notional cap — the
    /// caller hands in the limit price for a limit order or a book-
    /// derived estimate (top-of-book opposite side) for a market order.
    pub fn check(&self, order: &NewOrder, ref_price: Price) -> Result<(), RiskRejection> {
        if order.qty.0 <= 0 {
            return Err(RiskRejection::InvalidOrder("non-positive qty"));
        }
        if matches!(order.kind, OrderKind::Limit) && order.price.is_none() {
            return Err(RiskRejection::InvalidOrder("limit without price"));
        }
        if !matches!(order.side, Side::Buy | Side::Sell) {
            return Err(RiskRejection::InvalidOrder("unknown side"));
        }
        if !self.cfg.whitelist.is_empty() && !self.cfg.whitelist.contains(&order.symbol) {
            return Err(RiskRejection::NotWhitelisted {
                symbol: order.symbol.as_str().to_string(),
            });
        }
        if self.open_orders >= self.cfg.max_open_orders {
            return Err(RiskRejection::TooManyOpenOrders {
                current: self.open_orders,
                limit: self.cfg.max_open_orders,
            });
        }

        let notional_i128 = (ref_price.0 as i128).saturating_mul(order.qty.0 as i128);
        let notional = notional_i128.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        if notional > self.cfg.max_order_notional {
            return Err(RiskRejection::MaxNotionalBreached {
                notional,
                limit: self.cfg.max_order_notional,
            });
        }

        let current = self.position(&order.symbol);
        let signed_qty = match order.side {
            Side::Buy => order.qty.0,
            Side::Sell => -order.qty.0,
            Side::Unknown => 0,
        };
        let proposed = current.saturating_add(signed_qty);
        let proposed_abs = (proposed as i128).abs();
        if proposed_abs > self.cfg.max_position_qty.0 as i128 {
            return Err(RiskRejection::MaxPositionBreached {
                symbol: order.symbol.as_str().to_string(),
                proposed,
                limit: self.cfg.max_position_qty.0,
            });
        }

        Ok(())
    }

    pub fn record_submit(&mut self, _order: &NewOrder) {
        self.open_orders += 1;
    }

    pub fn record_fill(&mut self, fill: &Fill) {
        let signed = match fill.side {
            Side::Buy => fill.qty.0,
            Side::Sell => -fill.qty.0,
            Side::Unknown => 0,
        };
        *self.positions.entry(fill.symbol.clone()).or_insert(0) += signed;
    }

    pub fn record_complete(&mut self, _cid: &ClientOrderId) {
        self.open_orders = self.open_orders.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{ClientOrderId, OrderKind, TimeInForce, Timestamp, Venue};

    fn order(side: Side, qty: i64, price: Option<Price>) -> NewOrder {
        NewOrder {
            cid: ClientOrderId::new("c1"),
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            side,
            kind: if price.is_some() {
                OrderKind::Limit
            } else {
                OrderKind::Market
            },
            tif: TimeInForce::Gtc,
            qty: Qty(qty),
            price,
            ts: Timestamp::default(),
        }
    }

    fn fill(side: Side, qty: i64, price: i64) -> Fill {
        Fill {
            cid: ClientOrderId::new("c1"),
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            side,
            price: Price(price),
            qty: Qty(qty),
            ts: Timestamp::default(),
            is_maker: None,
            fee: 0,
            fee_asset: None,
        }
    }

    #[test]
    fn permissive_accepts() {
        let e = RiskEngine::new(RiskConfig::permissive());
        e.check(&order(Side::Buy, 1, Some(Price(100))), Price(100))
            .unwrap();
    }

    #[test]
    fn rejects_non_positive_qty() {
        let e = RiskEngine::new(RiskConfig::permissive());
        let err = e
            .check(&order(Side::Buy, 0, Some(Price(100))), Price(100))
            .unwrap_err();
        assert_eq!(err, RiskRejection::InvalidOrder("non-positive qty"));
    }

    #[test]
    fn rejects_limit_without_price() {
        let e = RiskEngine::new(RiskConfig::permissive());
        let mut o = order(Side::Buy, 1, Some(Price(100)));
        o.price = None;
        o.kind = OrderKind::Limit;
        let err = e.check(&o, Price(100)).unwrap_err();
        assert_eq!(err, RiskRejection::InvalidOrder("limit without price"));
    }

    #[test]
    fn rejects_unknown_side() {
        let e = RiskEngine::new(RiskConfig::permissive());
        let err = e
            .check(&order(Side::Unknown, 1, Some(Price(100))), Price(100))
            .unwrap_err();
        assert_eq!(err, RiskRejection::InvalidOrder("unknown side"));
    }

    #[test]
    fn whitelist_excludes_non_members() {
        let mut cfg = RiskConfig::permissive();
        cfg.whitelist.insert(Symbol::from_static("ETHUSDT"));
        let e = RiskEngine::new(cfg);
        let err = e
            .check(&order(Side::Buy, 1, Some(Price(100))), Price(100))
            .unwrap_err();
        assert!(matches!(err, RiskRejection::NotWhitelisted { .. }));
    }

    #[test]
    fn notional_cap_blocks_oversized_orders() {
        let mut cfg = RiskConfig::permissive();
        cfg.max_order_notional = 5_000;
        let e = RiskEngine::new(cfg);
        let err = e
            .check(&order(Side::Buy, 100, Some(Price(100))), Price(100))
            .unwrap_err();
        assert_eq!(
            err,
            RiskRejection::MaxNotionalBreached {
                notional: 10_000,
                limit: 5_000,
            }
        );
    }

    #[test]
    fn position_cap_blocks_long_buildup() {
        let mut cfg = RiskConfig::permissive();
        cfg.max_position_qty = Qty(10);
        let mut e = RiskEngine::new(cfg);
        e.record_fill(&fill(Side::Buy, 8, 100));
        let err = e
            .check(&order(Side::Buy, 5, Some(Price(100))), Price(100))
            .unwrap_err();
        assert!(matches!(err, RiskRejection::MaxPositionBreached { .. }));
    }

    #[test]
    fn position_cap_blocks_short_buildup() {
        let mut cfg = RiskConfig::permissive();
        cfg.max_position_qty = Qty(10);
        let e = RiskEngine::new(cfg);
        let err = e
            .check(&order(Side::Sell, 11, Some(Price(100))), Price(100))
            .unwrap_err();
        assert!(matches!(err, RiskRejection::MaxPositionBreached { .. }));
    }

    #[test]
    fn open_order_cap_enforced() {
        let mut cfg = RiskConfig::permissive();
        cfg.max_open_orders = 1;
        let mut e = RiskEngine::new(cfg);
        let o = order(Side::Buy, 1, Some(Price(100)));
        e.check(&o, Price(100)).unwrap();
        e.record_submit(&o);
        let err = e.check(&o, Price(100)).unwrap_err();
        assert!(matches!(err, RiskRejection::TooManyOpenOrders { .. }));
        e.record_complete(&ClientOrderId::new("c1"));
        e.check(&o, Price(100)).unwrap();
    }

    #[test]
    fn record_fill_tracks_signed_position() {
        let mut e = RiskEngine::new(RiskConfig::permissive());
        e.record_fill(&fill(Side::Buy, 5, 100));
        assert_eq!(e.position(&Symbol::from_static("BTCUSDT")), 5);
        e.record_fill(&fill(Side::Sell, 3, 100));
        assert_eq!(e.position(&Symbol::from_static("BTCUSDT")), 2);
    }
}
