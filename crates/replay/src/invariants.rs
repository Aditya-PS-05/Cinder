//! Replay-time invariant checks.
//!
//! A registered [`Invariant`] is evaluated after every event that could
//! change the accountant or metrics — book updates, taker submissions,
//! cancels, timer ticks, shutdown sweeps. A breach is captured in a
//! [`InvariantViolation`] and surfaced through
//! [`crate::ReplaySummary::invariant_violations`], so replay-driven
//! tests can assert against strategy-level safety properties without
//! reinventing a verification harness.
//!
//! The harness never aborts replay on a breach — it records every
//! violation and keeps going. That lets one replay catch a flurry of
//! violations across the same session rather than masking later ones
//! behind the first.
//!
//! Invariants see the accountant + metrics via [`InvariantCtx`], not
//! the strategy-typed engine, so the trait stays object-safe and
//! independent of the `Strategy` type parameter.

use ts_core::Symbol;
use ts_pnl::Accountant;

use crate::ReplayMetrics;

/// Read-only view handed to an [`Invariant`] at check time.
pub struct InvariantCtx<'a> {
    pub symbol: &'a Symbol,
    pub accountant: &'a Accountant,
    pub metrics: &'a ReplayMetrics,
}

/// A replay-time safety check. Implementors may carry internal state
/// (e.g. the last-seen value of a monotone counter), hence `&mut self`.
///
/// Return `None` when the invariant holds, `Some(detail)` on breach.
/// `detail` should describe *what* was observed so a test failure is
/// self-explanatory.
pub trait Invariant: Send {
    fn name(&self) -> &'static str;
    fn check(&mut self, ctx: &InvariantCtx) -> Option<String>;
}

/// Recorded breach of a registered [`Invariant`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvariantViolation {
    /// Name of the breached invariant.
    pub invariant: &'static str,
    /// Value of `ReplayMetrics::events_ingested` at the moment of the
    /// check — lets callers localize a breach to a specific tick.
    pub after_events: u64,
    /// Human-readable description supplied by the invariant.
    pub detail: String,
}

/// |position| must never exceed `cap`. Useful as a strategy-level
/// sanity check for makers that claim an inventory bound: if the cap
/// gate ever misfires, this invariant catches it.
pub struct PositionBound {
    pub cap: i64,
}

impl Invariant for PositionBound {
    fn name(&self) -> &'static str {
        "position_bound"
    }

    fn check(&mut self, ctx: &InvariantCtx) -> Option<String> {
        let pos = ctx.accountant.position(ctx.symbol);
        (pos.unsigned_abs() > self.cap as u64).then(|| {
            format!(
                "|position|={} exceeds cap={} (position={})",
                pos.unsigned_abs(),
                self.cap,
                pos,
            )
        })
    }
}

/// `gross_filled_qty` is a cumulative counter and must never go
/// backwards. A breach would indicate fill accounting corruption —
/// e.g. a double-counted refund, an unsigned saturating underflow.
pub struct GrossFilledQtyMonotone {
    last: i128,
}

impl GrossFilledQtyMonotone {
    pub fn new() -> Self {
        Self { last: 0 }
    }
}

impl Default for GrossFilledQtyMonotone {
    fn default() -> Self {
        Self::new()
    }
}

impl Invariant for GrossFilledQtyMonotone {
    fn name(&self) -> &'static str {
        "gross_filled_qty_monotone"
    }

    fn check(&mut self, ctx: &InvariantCtx) -> Option<String> {
        let cur = ctx.metrics.gross_filled_qty;
        let breach = (cur < self.last)
            .then(|| format!("gross_filled_qty regressed: last={} now={}", self.last, cur));
        self.last = cur;
        breach
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{Fill, Price, Qty, Side, Symbol, Timestamp, Venue};

    fn sym() -> Symbol {
        Symbol::from_static("BTCUSDT")
    }

    fn buy_fill(qty: i64) -> Fill {
        Fill {
            cid: ts_core::ClientOrderId::new("ignored"),
            venue: Venue::BINANCE,
            symbol: sym(),
            side: Side::Buy,
            price: Price(100),
            qty: Qty(qty),
            ts: Timestamp::default(),
            is_maker: None,
            fee: 0,
            fee_asset: None,
        }
    }

    #[test]
    fn position_bound_holds_on_flat_book() {
        let s = sym();
        let a = Accountant::new();
        let m = ReplayMetrics::default();
        let ctx = InvariantCtx {
            symbol: &s,
            accountant: &a,
            metrics: &m,
        };
        let mut inv = PositionBound { cap: 10 };
        assert!(inv.check(&ctx).is_none());
    }

    #[test]
    fn position_bound_breached_when_accumulation_exceeds_cap() {
        let s = sym();
        let mut a = Accountant::new();
        a.on_fill(&buy_fill(15));
        let m = ReplayMetrics::default();
        let ctx = InvariantCtx {
            symbol: &s,
            accountant: &a,
            metrics: &m,
        };
        let mut inv = PositionBound { cap: 10 };
        let violation = inv.check(&ctx).expect("15 > cap=10 should breach");
        assert!(violation.contains("cap=10"));
        assert!(violation.contains("15"));
    }

    #[test]
    fn position_bound_also_catches_short_side() {
        let s = sym();
        let mut a = Accountant::new();
        // Sell 15 against a flat book — accountant records a -15
        // position. The bound is on absolute value, so the short leg
        // must breach the same cap.
        let mut f = buy_fill(15);
        f.side = Side::Sell;
        a.on_fill(&f);
        let m = ReplayMetrics::default();
        let ctx = InvariantCtx {
            symbol: &s,
            accountant: &a,
            metrics: &m,
        };
        let mut inv = PositionBound { cap: 10 };
        assert!(inv.check(&ctx).is_some());
    }

    #[test]
    fn gross_filled_qty_monotone_never_fires_on_a_growing_counter() {
        let s = sym();
        let a = Accountant::new();
        let mut m = ReplayMetrics::default();
        let mut inv = GrossFilledQtyMonotone::new();

        for step in [5, 10, 10, 20, 100] {
            m.gross_filled_qty = step;
            let ctx = InvariantCtx {
                symbol: &s,
                accountant: &a,
                metrics: &m,
            };
            assert!(inv.check(&ctx).is_none(), "monotone growth must not breach");
        }
    }

    #[test]
    fn gross_filled_qty_monotone_breaches_on_regression() {
        let s = sym();
        let a = Accountant::new();
        let mut m = ReplayMetrics::default();
        let mut inv = GrossFilledQtyMonotone::new();

        m.gross_filled_qty = 50;
        let ctx = InvariantCtx {
            symbol: &s,
            accountant: &a,
            metrics: &m,
        };
        assert!(inv.check(&ctx).is_none());

        m.gross_filled_qty = 30;
        let ctx = InvariantCtx {
            symbol: &s,
            accountant: &a,
            metrics: &m,
        };
        let v = inv.check(&ctx).expect("30 < 50 must breach");
        assert!(v.contains("50"));
        assert!(v.contains("30"));
    }
}
