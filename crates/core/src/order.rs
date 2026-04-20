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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OrderKind {
    Limit,
    Market,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TimeInForce {
    /// Good-til-cancel: unfilled remainder rests on the book.
    Gtc,
    /// Immediate-or-cancel: cancel any unfilled remainder.
    Ioc,
    /// Fill-or-kill: either fill in full or reject outright.
    Fok,
    /// Post-only (aka maker-only): reject the order outright if any
    /// part of it would immediately cross the opposite side and take
    /// liquidity. Guarantees the resulting order rests as a maker or
    /// does not exist at all — useful for makers who care about fee
    /// tier, queue position, or the guarantee that they never
    /// accidentally lift the offer on a missed cancel. On Binance
    /// spot, this maps to the `LIMIT_MAKER` order type rather than a
    /// TIF string; in the paper engine, it is enforced pre-submit by
    /// comparing the limit price against top-of-book.
    PostOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

impl OrderStatus {
    /// Terminal states: the order will not change again after reaching one.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Rejected | Self::Expired
        )
    }

    /// Whether the venue's execution flow may legally move us from
    /// `self` to `next`. The legal graph is:
    ///
    /// ```text
    /// New ──► PartiallyFilled ──► Filled
    ///   │            │
    ///   │            └─► Canceled / Expired
    ///   │
    ///   └─► Filled / Canceled / Rejected / Expired
    /// ```
    ///
    /// A self-transition is always legal so callers can re-apply the
    /// same status idempotently (common when reconciling with the
    /// venue). Once a terminal state is reached no further transition
    /// is allowed; `Rejected` is only reachable from `New` because a
    /// rejection means the venue never accepted the order.
    pub fn can_transition_to(self, next: OrderStatus) -> bool {
        use OrderStatus::*;
        if self == next {
            return true;
        }
        match (self, next) {
            (New, PartiallyFilled | Filled | Canceled | Rejected | Expired) => true,
            (PartiallyFilled, Filled | Canceled | Expired) => true,
            // Every other pair — including anything out of a terminal
            // state or backwards transitions like `Filled → New` — is
            // illegal.
            _ => false,
        }
    }

    /// Returns `next` on a legal transition, otherwise an
    /// [`InvalidTransition`] describing the attempted edge.
    pub fn try_transition_to(self, next: OrderStatus) -> Result<OrderStatus, InvalidTransition> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(InvalidTransition {
                from: self,
                to: next,
            })
        }
    }
}

/// Error returned by [`OrderStatus::try_transition_to`] when an
/// execution update would move an order along an illegal edge in the
/// lifecycle graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InvalidTransition {
    pub from: OrderStatus,
    pub to: OrderStatus,
}

impl core::fmt::Display for InvalidTransition {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "illegal order-status transition {:?} → {:?}",
            self.from, self.to
        )
    }
}

impl std::error::Error for InvalidTransition {}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Fill {
    pub cid: ClientOrderId,
    pub venue: Venue,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub ts: Timestamp,
    /// Liquidity side of this fill: `Some(true)` if we provided
    /// liquidity (maker), `Some(false)` if we took it (taker), `None`
    /// when the source cannot report it. Paper engines currently leave
    /// this unset; the Binance user-stream fills it from the exchange's
    /// `m` flag.
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_maker: Option<bool>,
    /// Commission charged on this fill, in quote-currency mantissa
    /// at `price_scale + qty_scale` (the same mantissa PnL uses). Zero
    /// when the commission was paid in a non-quote asset — the asset
    /// label lands in `fee_asset` so the value is still auditable
    /// even though we can't fold it into realized PnL without a
    /// cross-asset mark.
    #[cfg_attr(feature = "serde", serde(default))]
    pub fee: i64,
    /// Asset the commission was denominated in (e.g. `"USDT"`, `"BNB"`).
    /// `None` when the source doesn't report it.
    #[cfg_attr(feature = "serde", serde(default))]
    pub fee_asset: Option<String>,
}

/// Terminal result of submitting a single order: every fill that
/// occurred plus the final [`OrderStatus`].
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

    #[test]
    fn is_terminal_matches_spec() {
        assert!(!OrderStatus::New.is_terminal());
        assert!(!OrderStatus::PartiallyFilled.is_terminal());
        assert!(OrderStatus::Filled.is_terminal());
        assert!(OrderStatus::Canceled.is_terminal());
        assert!(OrderStatus::Rejected.is_terminal());
        assert!(OrderStatus::Expired.is_terminal());
    }

    #[test]
    fn self_transition_is_always_legal() {
        for s in [
            OrderStatus::New,
            OrderStatus::PartiallyFilled,
            OrderStatus::Filled,
            OrderStatus::Canceled,
            OrderStatus::Rejected,
            OrderStatus::Expired,
        ] {
            assert!(
                s.can_transition_to(s),
                "self-transition must be legal for {s:?}"
            );
            assert_eq!(s.try_transition_to(s), Ok(s));
        }
    }

    #[test]
    fn new_can_reach_every_other_state() {
        use OrderStatus::*;
        for next in [PartiallyFilled, Filled, Canceled, Rejected, Expired] {
            assert!(New.can_transition_to(next), "New → {next:?} must be legal");
        }
    }

    #[test]
    fn partially_filled_cannot_go_back_to_new_or_rejected() {
        use OrderStatus::*;
        assert!(!PartiallyFilled.can_transition_to(New));
        assert!(!PartiallyFilled.can_transition_to(Rejected));
        assert!(PartiallyFilled.can_transition_to(Filled));
        assert!(PartiallyFilled.can_transition_to(Canceled));
        assert!(PartiallyFilled.can_transition_to(Expired));
    }

    #[test]
    fn terminal_states_do_not_transition_anywhere_else() {
        use OrderStatus::*;
        for term in [Filled, Canceled, Rejected, Expired] {
            for other in [New, PartiallyFilled, Filled, Canceled, Rejected, Expired] {
                if term == other {
                    continue;
                }
                assert!(
                    !term.can_transition_to(other),
                    "{term:?} → {other:?} must be illegal"
                );
            }
        }
    }

    #[test]
    fn try_transition_surfaces_illegal_edge() {
        let err = OrderStatus::Filled
            .try_transition_to(OrderStatus::PartiallyFilled)
            .unwrap_err();
        assert_eq!(err.from, OrderStatus::Filled);
        assert_eq!(err.to, OrderStatus::PartiallyFilled);
        let msg = format!("{err}");
        assert!(msg.contains("Filled"));
        assert!(msg.contains("PartiallyFilled"));
    }
}
