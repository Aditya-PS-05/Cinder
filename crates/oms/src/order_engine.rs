//! `OrderEngine` trait — the shape shared between [`PaperEngine`] and
//! future venue-specific live engines (BinanceLiveEngine, …).
//!
//! The trait captures the four order-side operations any engine needs:
//!
//! * [`OrderEngine::submit`] — admit a new order. Paper engines return
//!   a terminal report immediately; live engines return an optimistic
//!   `New` and stream follow-on reports out of [`OrderEngine::reconcile`].
//! * [`OrderEngine::cancel`] — cancel a live order by cid.
//! * [`OrderEngine::query`] — cached last-known status, if the engine
//!   tracks one.
//! * [`OrderEngine::reconcile`] — drain async updates (fills from a
//!   user-data-stream, background acks, …) that have arrived since the
//!   last poll. Synchronous engines always return an empty
//!   [`EngineStep`].
//!
//! The trait is synchronous so paper simulations, replay harnesses, and
//! backtests don't need a tokio runtime. Live engines implement the
//! contract over a command channel + background worker: `submit`
//! buffers an intent and returns immediately, a background task runs
//! the HTTP call, and `reconcile` drains the completed reports.
//!
//! The `Error` associated type is reserved for engine-level transport
//! failures (network drop, auth rejection). Venue-level order rejections
//! remain encoded as [`ts_core::OrderStatus::Rejected`] inside the
//! returned [`ExecReport`] so the outward contract stays uniform across
//! paper and live.

use std::convert::Infallible;

use ts_core::{ClientOrderId, ExecReport, NewOrder, Timestamp};
use ts_strategy::Strategy;

use crate::{EngineStep, PaperEngine};

/// Shared contract between paper, replay, and live order engines.
pub trait OrderEngine {
    /// Engine-level transport error. Paper engines use `Infallible`;
    /// live engines surface network/venue failures here.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Submit a new order. The returned report is the engine's first
    /// acknowledgement — it may already be terminal (`Filled`, `Canceled`,
    /// `Rejected`) on a paper engine, or `New` with fills streaming in
    /// later via [`reconcile`](Self::reconcile) on a live engine.
    fn submit(&mut self, order: NewOrder, now: Timestamp) -> Result<ExecReport, Self::Error>;

    /// Cancel a live order by client order id. Returns a `Canceled`
    /// report on success, or `Rejected` if the cid is unknown.
    fn cancel(&mut self, cid: &ClientOrderId) -> Result<ExecReport, Self::Error>;

    /// Cached last-known status for `cid`, if the engine tracks history.
    /// Paper engines return `None` — transient reports are delivered
    /// through `submit`/`cancel` and not retained.
    fn query(&self, cid: &ClientOrderId) -> Option<ExecReport>;

    /// Drain any reports and fills that have arrived since the last
    /// call. Paper engines return an empty step because they have no
    /// out-of-band updates.
    fn reconcile(&mut self) -> Result<EngineStep, Self::Error>;

    /// Cumulative count of [`ExecReport`]s the engine has dropped
    /// because the prev → next transition violated the
    /// [`ts_core::OrderStatus`] state machine. Always non-decreasing.
    /// Engines with no out-of-band updates (e.g. [`PaperEngine`]) return
    /// `0` since they construct every report inline. Surfaced as a
    /// Prometheus counter by the live runner.
    fn illegal_transitions(&self) -> u64 {
        0
    }

    /// Client order ids the engine still considers live — i.e. orders
    /// that have been submitted and have not yet reached a terminal
    /// [`ts_core::OrderStatus`]. The default returns an empty vec for
    /// engines that don't track per-order state (backtest shims,
    /// stateless test doubles). The live runner uses this as a defence
    /// in depth on the kill-switch trip sweep: after the strategy's
    /// `on_shutdown` has emitted its cancel list, the sweep cancels
    /// every remaining open cid the engine still knows about, so a
    /// strategy bug can't silently leave orders hanging on the venue.
    ///
    /// Return order is not specified; callers that need determinism
    /// should sort.
    fn open_cids(&self) -> Vec<ClientOrderId> {
        Vec::new()
    }

    /// Trigger an out-of-band resync against the venue's authoritative
    /// order state. Live engines fire an async query per open cid; the
    /// resulting [`ExecReport`]s land in the same queue that
    /// [`reconcile`](Self::reconcile) drains, so correction flows
    /// through the normal path. Paper engines have no venue to query
    /// and return immediately.
    ///
    /// Semantics are "best effort, eventually consistent" — the method
    /// is synchronous; callers do not wait for the query results.
    /// Corrections arrive on the next `reconcile` tick that runs after
    /// the HTTP round-trip completes.
    fn resync_open_orders(&mut self) {}

    /// Cancel an open order and immediately submit a replacement.
    ///
    /// The default is a non-atomic compose of [`cancel`](Self::cancel)
    /// then [`submit`](Self::submit), with STOP-on-failure semantics: if
    /// the cancel leg returns [`ts_core::OrderStatus::Rejected`] the new
    /// order is **not** submitted, so the caller can't end up with
    /// double exposure when the first leg couldn't be retired. Callers
    /// that want post-failure submit (e.g. the venue rejected cancel
    /// because the order already filled) should inspect the returned
    /// [`CancelReplaceReport`] and re-issue explicitly.
    ///
    /// Venue-native cancel-replace primitives (Binance's `POST
    /// /api/v3/order/cancelReplace`) can override this to get an
    /// atomic round-trip.
    fn cancel_replace(
        &mut self,
        old_cid: &ClientOrderId,
        new_order: NewOrder,
        now: Timestamp,
    ) -> Result<crate::CancelReplaceReport, Self::Error> {
        let cancel = self.cancel(old_cid)?;
        if matches!(cancel.status, ts_core::OrderStatus::Rejected) {
            return Ok(crate::CancelReplaceReport {
                cancel,
                submit: None,
            });
        }
        let submit = self.submit(new_order, now)?;
        Ok(crate::CancelReplaceReport {
            cancel,
            submit: Some(submit),
        })
    }
}

impl<S: Strategy> OrderEngine for PaperEngine<S> {
    type Error = Infallible;

    fn submit(&mut self, order: NewOrder, now: Timestamp) -> Result<ExecReport, Self::Error> {
        Ok(self.submit_internal(order, now))
    }

    fn cancel(&mut self, cid: &ClientOrderId) -> Result<ExecReport, Self::Error> {
        Ok(self.cancel_internal(cid))
    }

    fn query(&self, _cid: &ClientOrderId) -> Option<ExecReport> {
        None
    }

    fn reconcile(&mut self) -> Result<EngineStep, Self::Error> {
        Ok(EngineStep::default())
    }

    fn open_cids(&self) -> Vec<ClientOrderId> {
        self.live_orders().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EngineConfig, RiskConfig};
    use ts_core::{
        BookLevel, BookSnapshot, MarketEvent, MarketPayload, OrderKind, OrderStatus, Price, Qty,
        Side, Symbol, TimeInForce, Venue,
    };
    use ts_strategy::{InventorySkewMaker, MakerConfig};

    fn venue() -> Venue {
        Venue::BINANCE
    }
    fn sym() -> Symbol {
        Symbol::from_static("BTCUSDT")
    }

    fn engine() -> PaperEngine<InventorySkewMaker> {
        PaperEngine::new(
            EngineConfig {
                venue: venue(),
                symbol: sym(),
                notional_fallback_price: None,
            },
            RiskConfig::permissive(),
            InventorySkewMaker::new(MakerConfig {
                venue: venue(),
                symbol: sym(),
                quote_qty: Qty(2),
                half_spread_ticks: 5,
                imbalance_widen_ticks: 0,
                vol_lambda: 0.94,
                vol_widen_coeff: 0.0,
                inventory_skew_ticks: 0,
                max_inventory: 20,
                cid_prefix: "t".into(),
            }),
        )
    }

    fn snapshot(bid: i64, ask: i64, seq: u64) -> MarketEvent {
        MarketEvent {
            venue: venue(),
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq,
            payload: MarketPayload::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(bid),
                    qty: Qty(10),
                }],
                asks: vec![BookLevel {
                    price: Price(ask),
                    qty: Qty(10),
                }],
            }),
        }
    }

    fn market(side: Side, cid: &str) -> NewOrder {
        NewOrder {
            cid: ClientOrderId::new(cid),
            venue: venue(),
            symbol: sym(),
            side,
            kind: OrderKind::Market,
            tif: TimeInForce::Ioc,
            qty: Qty(3),
            price: None,
            ts: Timestamp::default(),
        }
    }

    #[test]
    fn paper_engine_implements_order_engine_submit() {
        let mut e = engine();
        e.apply_event(&snapshot(100, 110, 1)).unwrap();
        let report =
            OrderEngine::submit(&mut e, market(Side::Buy, "t1"), Timestamp::default()).unwrap();
        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_qty, Qty(3));
    }

    #[test]
    fn paper_engine_trait_cancel_unknown_cid_is_rejected() {
        let mut e = engine();
        let r = OrderEngine::cancel(&mut e, &ClientOrderId::new("no-such")).unwrap();
        assert_eq!(r.status, OrderStatus::Rejected);
    }

    #[test]
    fn paper_engine_trait_query_is_always_none() {
        let e = engine();
        assert!(OrderEngine::query(&e, &ClientOrderId::new("any")).is_none());
    }

    #[test]
    fn paper_engine_trait_reconcile_is_empty() {
        let mut e = engine();
        let step = OrderEngine::reconcile(&mut e).unwrap();
        assert!(step.reports.is_empty());
        assert!(step.fills.is_empty());
    }

    /// A helper generic over the trait — proves the contract can be
    /// used polymorphically without knowing the concrete engine.
    fn place_buy<E: OrderEngine>(engine: &mut E, cid: &str) -> Result<ExecReport, E::Error> {
        engine.submit(
            NewOrder {
                cid: ClientOrderId::new(cid),
                venue: venue(),
                symbol: sym(),
                side: Side::Buy,
                kind: OrderKind::Market,
                tif: TimeInForce::Ioc,
                qty: Qty(2),
                price: None,
                ts: Timestamp::default(),
            },
            Timestamp::default(),
        )
    }

    #[test]
    fn generic_helper_drives_paper_engine_through_trait() {
        let mut e = engine();
        e.apply_event(&snapshot(100, 110, 1)).unwrap();
        let r = place_buy(&mut e, "g1").unwrap();
        assert!(matches!(
            r.status,
            OrderStatus::Filled | OrderStatus::PartiallyFilled
        ));
    }

    fn resting_limit_buy(cid: &str, price: i64) -> NewOrder {
        NewOrder {
            cid: ClientOrderId::new(cid),
            venue: venue(),
            symbol: sym(),
            side: Side::Buy,
            kind: OrderKind::Limit,
            tif: TimeInForce::Gtc,
            qty: Qty(1),
            price: Some(Price(price)),
            ts: Timestamp::default(),
        }
    }

    #[test]
    fn cancel_replace_happy_path_retires_old_and_submits_new() {
        let mut e = engine();
        e.apply_event(&snapshot(100, 110, 1)).unwrap();
        // Seed a resting limit — price below best-bid keeps it live.
        let first =
            OrderEngine::submit(&mut e, resting_limit_buy("r1", 50), Timestamp::default()).unwrap();
        assert_eq!(first.status, OrderStatus::New);
        assert!(e.live_orders().contains_key(&ClientOrderId::new("r1")));

        let report = OrderEngine::cancel_replace(
            &mut e,
            &ClientOrderId::new("r1"),
            resting_limit_buy("r2", 55),
            Timestamp::default(),
        )
        .unwrap();

        assert_eq!(report.cancel.status, OrderStatus::Canceled);
        let submit = report.submit.expect("submit leg fires on cancel success");
        assert_eq!(submit.status, OrderStatus::New);
        assert!(!e.live_orders().contains_key(&ClientOrderId::new("r1")));
        assert!(e.live_orders().contains_key(&ClientOrderId::new("r2")));
    }

    #[test]
    fn cancel_replace_stops_when_cancel_leg_rejected() {
        let mut e = engine();
        e.apply_event(&snapshot(100, 110, 1)).unwrap();

        let report = OrderEngine::cancel_replace(
            &mut e,
            &ClientOrderId::new("never-existed"),
            resting_limit_buy("r3", 55),
            Timestamp::default(),
        )
        .unwrap();

        assert_eq!(report.cancel.status, OrderStatus::Rejected);
        assert!(
            report.submit.is_none(),
            "STOP-on-failure: no new order when cancel leg rejects"
        );
        // And critically: the replacement cid must not be live.
        assert!(!e.live_orders().contains_key(&ClientOrderId::new("r3")));
    }
}
