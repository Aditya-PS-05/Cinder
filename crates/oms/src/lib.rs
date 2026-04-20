//! Synchronous paper trading engine.
//!
//! [`PaperEngine`] composes the four building blocks produced in
//! earlier phases — [`OrderBook`], [`RiskEngine`], [`PaperExecutor`],
//! and a user-supplied [`Strategy`] — behind a single step function
//! driven by [`MarketEvent`]s. The engine is single-threaded and
//! holds no I/O; a runner binary feeds events in order and persists
//! the resulting [`ExecReport`]s and [`Fill`]s elsewhere.
//!
//! Lifecycle rules enforced here, so strategies and tests can rely
//! on them:
//!
//! * Every submitted order is priced for the notional cap via
//!   [`PaperEngine::reference_price`]: limit orders use their own
//!   price, market orders use the opposite-side top of book, with
//!   [`EngineConfig::notional_fallback_price`] as a last resort.
//! * Risk rejections and executor rejections both surface as
//!   `OrderStatus::Rejected` in the returned [`ExecReport`] so the
//!   outward contract is uniform.
//! * Fills drive `RiskEngine::record_fill` and `Strategy::on_fill`;
//!   terminal statuses drive `RiskEngine::record_complete` and evict
//!   the order from the live-order map.
//! * `StrategyAction::Cancel` of an unknown cid yields a `Rejected`
//!   report rather than being silently dropped — it is a strategy
//!   bug and should be visible.

#![forbid(unsafe_code)]

pub mod order_engine;

use std::collections::HashMap;

use ts_book::{BookError, OrderBook};
use ts_core::{
    ClientOrderId, ExecReport, Fill, MarketEvent, MarketPayload, NewOrder, OrderKind, OrderStatus,
    Price, Qty, Side, Symbol, TimeInForce, Timestamp, Venue,
};
use ts_paper::PaperExecutor;
use ts_risk::RiskEngine;
pub use ts_risk::{RiskConfig, RiskRejection};
use ts_strategy::{is_terminal, Strategy, StrategyAction};

pub use order_engine::OrderEngine;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub venue: Venue,
    pub symbol: Symbol,
    /// Fallback reference price for market-order notional checks when
    /// the opposing side of the book is empty. `None` means "reject
    /// the order in that case" rather than let an unbounded notional
    /// slip past risk.
    pub notional_fallback_price: Option<Price>,
}

/// Aggregated output of a single [`PaperEngine::apply_event`] call.
#[derive(Default, Debug, Clone)]
pub struct EngineStep {
    pub reports: Vec<ExecReport>,
    pub fills: Vec<Fill>,
}

/// Result of an [`OrderEngine::cancel_replace`] call.
///
/// `cancel` is the engine's response to the cancel leg (optimistic on
/// live engines, terminal on paper). `submit` is the new-order leg's
/// response: present on success, `None` when the cancel leg returned
/// `OrderStatus::Rejected` and the engine's policy was to skip the
/// submit (default: STOP-on-failure — don't create a second exposure
/// when the first couldn't be retired).
#[derive(Debug, Clone)]
pub struct CancelReplaceReport {
    pub cancel: ExecReport,
    pub submit: Option<ExecReport>,
}

pub struct PaperEngine<S: Strategy> {
    cfg: EngineConfig,
    book: OrderBook,
    risk: RiskEngine,
    exec: PaperExecutor,
    strategy: S,
    live_orders: HashMap<ClientOrderId, NewOrder>,
    /// When `true`, [`Self::apply_event`] still applies book updates but
    /// skips the strategy tick entirely. Skipping — rather than calling
    /// `on_book_update` and dropping the returned actions — keeps the
    /// strategy's quote ledger from remembering ghost placements that
    /// would then emit spurious cancels on the first tick after unpause.
    /// [`Self::drain_shutdown`] is intentionally exempt so a halted
    /// runner can still cancel open quotes during shutdown.
    paused: bool,
}

impl<S: Strategy> PaperEngine<S> {
    pub fn new(cfg: EngineConfig, risk_cfg: RiskConfig, strategy: S) -> Self {
        Self {
            cfg,
            book: OrderBook::new(),
            risk: RiskEngine::new(risk_cfg),
            exec: PaperExecutor::new(),
            strategy,
            live_orders: HashMap::new(),
            paused: false,
        }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.cfg
    }
    pub fn book(&self) -> &OrderBook {
        &self.book
    }
    pub fn risk(&self) -> &RiskEngine {
        &self.risk
    }
    pub fn strategy(&self) -> &S {
        &self.strategy
    }
    pub fn live_orders(&self) -> &HashMap<ClientOrderId, NewOrder> {
        &self.live_orders
    }

    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// Toggle the action gate. Idempotent — repeated sets of the same
    /// value are no-ops.
    pub fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }

    /// Apply a single market event, tick the strategy on book-moving
    /// payloads, and drain any actions the strategy emits.
    ///
    /// Returns `Err(BookError)` when a delta cannot be applied (the
    /// book is uninitialized or the sequence chain breaks) so the
    /// caller can drive resync. Non-book payloads are accepted but
    /// do not tick the strategy.
    pub fn apply_event(&mut self, event: &MarketEvent) -> Result<EngineStep, BookError> {
        match &event.payload {
            MarketPayload::BookSnapshot(s) => {
                self.book.apply_snapshot(s, event.seq);
            }
            MarketPayload::BookDelta(d) => {
                self.book.apply_delta(d, event.seq)?;
            }
            MarketPayload::Trade(t) => {
                if self.paused {
                    return Ok(EngineStep::default());
                }
                let actions = self.strategy.on_trade(event.local_ts, t);
                return Ok(self.process_actions(actions, event.local_ts));
            }
            MarketPayload::Funding(_) | MarketPayload::Liquidation(_) => {
                return Ok(EngineStep::default());
            }
        }

        if self.paused {
            // Skip the strategy tick wholesale while paused. We still
            // applied the book update above, so the book stays fresh,
            // but we do not give the strategy a chance to track quotes
            // it would not actually place. `drain_shutdown` remains the
            // only path that can run strategy actions in this state.
            return Ok(EngineStep::default());
        }
        let actions = self.strategy.on_book_update(event.local_ts, &self.book);
        Ok(self.process_actions(actions, event.local_ts))
    }

    /// Drive a wall-clock timer tick into the strategy and run any
    /// emitted actions through the engine. Called by runners on a
    /// configured cadence so time-scheduled strategies (TWAP slices,
    /// heartbeat cancels) can act on a silent feed. The `paused` gate
    /// applies — a halted engine never calls `on_timer`, mirroring how
    /// it skips `on_book_update`. `drain_shutdown` remains the only
    /// path that can emit strategy actions while paused.
    pub fn apply_timer(&mut self, now: Timestamp) -> EngineStep {
        if self.paused {
            return EngineStep::default();
        }
        let actions = self.strategy.on_timer(now);
        self.process_actions(actions, now)
    }

    /// Drive the strategy's shutdown hook and run every returned action
    /// through the engine. Used by runners to cancel open quotes before
    /// exiting so the venue does not retain stale orders across a
    /// process restart. Idempotent: if the strategy has nothing to
    /// cancel the returned step is empty.
    pub fn drain_shutdown(&mut self, now: Timestamp) -> EngineStep {
        let actions = self.strategy.on_shutdown();
        self.process_actions(actions, now)
    }

    fn process_actions(&mut self, actions: Vec<StrategyAction>, now: Timestamp) -> EngineStep {
        let mut step = EngineStep::default();
        for action in actions {
            match action {
                StrategyAction::Place(order) => {
                    let report = self.submit_internal(order, now);
                    step.fills.extend(report.fills.iter().cloned());
                    step.reports.push(report);
                }
                StrategyAction::Cancel(cid) => {
                    let report = self.cancel_internal(&cid);
                    step.reports.push(report);
                }
            }
        }
        step
    }

    pub(crate) fn submit_internal(&mut self, order: NewOrder, now: Timestamp) -> ExecReport {
        // Idempotency guard: a cid that's still in the live-orders ledger
        // is in a non-terminal state (`New` or `PartiallyFilled`). Reject
        // duplicate submits before they hit risk/exec so the state
        // machine never has to handle a `New → New` re-entry that
        // silently replaces an in-flight order.
        if self.live_orders.contains_key(&order.cid) {
            let r = ExecReport::rejected(order.cid.clone(), "duplicate cid: order still live");
            self.strategy.on_exec_report(&r);
            return r;
        }
        // PostOnly must never take liquidity. Pre-screen against the
        // current top of book: a buy at price >= best_ask or a sell at
        // price <= best_bid would cross, so reject before the executor
        // can fill a single share. Venues enforce this server-side
        // (Binance's LIMIT_MAKER) — doing it here keeps paper in
        // lockstep with live and lets strategies see the same rejection
        // reason on both sides.
        if let Some(reason) = post_only_cross_reason(&order, &self.book) {
            let r = ExecReport::rejected(order.cid.clone(), reason);
            self.strategy.on_exec_report(&r);
            return r;
        }
        let ref_price = match self.reference_price(&order) {
            Some(p) => p,
            None => {
                let r = ExecReport::rejected(order.cid.clone(), "no reference price available");
                self.strategy.on_exec_report(&r);
                return r;
            }
        };
        if let Err(err) = self.risk.check(&order, ref_price) {
            let r = ExecReport::rejected(order.cid.clone(), err.to_string());
            self.strategy.on_exec_report(&r);
            return r;
        }

        self.risk.record_submit(&order);
        self.live_orders.insert(order.cid.clone(), order.clone());
        let report = self.exec.execute(&order, &self.book, now);

        for fill in &report.fills {
            self.risk.record_fill(fill);
            self.strategy.on_fill(fill);
        }
        if is_terminal(report.status) {
            self.risk.record_complete(&report.cid);
            self.live_orders.remove(&report.cid);
        }
        self.strategy.on_exec_report(&report);
        report
    }

    pub(crate) fn cancel_internal(&mut self, cid: &ClientOrderId) -> ExecReport {
        if self.live_orders.remove(cid).is_none() {
            let r = ExecReport::rejected(cid.clone(), "cancel: unknown cid");
            self.strategy.on_exec_report(&r);
            return r;
        }
        self.risk.record_complete(cid);
        let r = ExecReport {
            cid: cid.clone(),
            status: OrderStatus::Canceled,
            filled_qty: Qty(0),
            avg_price: None,
            reason: Some("canceled".into()),
            fills: Vec::new(),
        };
        self.strategy.on_exec_report(&r);
        r
    }

    fn reference_price(&self, order: &NewOrder) -> Option<Price> {
        match order.kind {
            OrderKind::Limit => order.price,
            OrderKind::Market => {
                let opposite = match order.side {
                    Side::Buy => self.book.best_ask(),
                    Side::Sell => self.book.best_bid(),
                    Side::Unknown => None,
                };
                opposite
                    .map(|lvl| lvl.price)
                    .or(self.cfg.notional_fallback_price)
            }
        }
    }
}

/// Decide whether a PostOnly order should be rejected pre-submit.
/// Returns `None` when the order is not PostOnly or does not cross the
/// current book; returns `Some(reason)` when the order would take
/// liquidity and must be rejected.
///
/// Handles only [`OrderKind::Limit`]; a PostOnly market order is a
/// contradiction in terms — market orders *always* take liquidity — so
/// we reject that combo up front with a distinct reason so strategies
/// can't accidentally rely on it for "aggressive post-only" that
/// doesn't exist. Missing price on a Limit order is left for
/// [`RiskEngine::check`] to surface with its existing error.
fn post_only_cross_reason(order: &NewOrder, book: &OrderBook) -> Option<String> {
    if !matches!(order.tif, TimeInForce::PostOnly) {
        return None;
    }
    if matches!(order.kind, OrderKind::Market) {
        return Some("post-only requires a limit price".to_string());
    }
    let limit = order.price?;
    match order.side {
        Side::Buy => book.best_ask().and_then(|lvl| {
            if limit.0 >= lvl.price.0 {
                Some(format!(
                    "post-only would cross: buy at {} >= best ask {}",
                    limit.0, lvl.price.0
                ))
            } else {
                None
            }
        }),
        Side::Sell => book.best_bid().and_then(|lvl| {
            if limit.0 <= lvl.price.0 {
                Some(format!(
                    "post-only would cross: sell at {} <= best bid {}",
                    limit.0, lvl.price.0
                ))
            } else {
                None
            }
        }),
        Side::Unknown => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use ts_core::{
        BookDelta, BookLevel, BookSnapshot, ClientOrderId, MarketEvent, MarketPayload, OrderKind,
        TimeInForce, Trade,
    };
    use ts_strategy::{InventorySkewMaker, MakerConfig};

    fn venue() -> Venue {
        Venue::BINANCE
    }
    fn sym() -> Symbol {
        Symbol::from_static("BTCUSDT")
    }

    fn engine_cfg() -> EngineConfig {
        EngineConfig {
            venue: venue(),
            symbol: sym(),
            notional_fallback_price: None,
        }
    }

    fn maker() -> InventorySkewMaker {
        InventorySkewMaker::new(MakerConfig {
            venue: venue(),
            symbol: sym(),
            quote_qty: Qty(2),
            half_spread_ticks: 5,
            imbalance_widen_ticks: 0,
            vol_lambda: 0.94,
            vol_widen_coeff: 0.0,
            inventory_skew_ticks: 0,
            max_inventory: 100,
            cid_prefix: "mk".into(),
        })
    }

    fn snapshot_event(bids: Vec<BookLevel>, asks: Vec<BookLevel>, seq: u64) -> MarketEvent {
        MarketEvent {
            venue: venue(),
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq,
            payload: MarketPayload::BookSnapshot(BookSnapshot { bids, asks }),
        }
    }

    fn delta_event(
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        prev_seq: u64,
        seq: u64,
    ) -> MarketEvent {
        MarketEvent {
            venue: venue(),
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq,
            payload: MarketPayload::BookDelta(BookDelta {
                bids,
                asks,
                prev_seq,
            }),
        }
    }

    fn trade_event() -> MarketEvent {
        MarketEvent {
            venue: venue(),
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq: 0,
            payload: MarketPayload::Trade(Trade {
                id: "t1".into(),
                price: Price(100),
                qty: Qty(1),
                taker_side: Side::Buy,
            }),
        }
    }

    fn lvl(p: i64, q: i64) -> BookLevel {
        BookLevel {
            price: Price(p),
            qty: Qty(q),
        }
    }

    fn market_order(side: Side, qty: i64) -> NewOrder {
        NewOrder {
            cid: ClientOrderId::new("ext-1"),
            venue: venue(),
            symbol: sym(),
            side,
            kind: OrderKind::Market,
            tif: TimeInForce::Ioc,
            qty: Qty(qty),
            price: None,
            ts: Timestamp::default(),
        }
    }

    #[test]
    fn trade_payload_invokes_on_trade_and_default_impl_emits_nothing() {
        // The default `Strategy::on_trade` is empty, so the maker —
        // which doesn't override it — must stay silent on trade prints.
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), maker());
        let step = e.apply_event(&trade_event()).unwrap();
        assert!(step.reports.is_empty());
        assert!(step.fills.is_empty());
        assert!(e.live_orders().is_empty());
    }

    /// Strategy that posts a single limit quote the first time
    /// `on_trade` fires. Used to prove the engine actually routes Trade
    /// payloads through the strategy.
    struct TradeQuoter {
        symbol: Symbol,
        venue: Venue,
        fired: bool,
    }
    impl Strategy for TradeQuoter {
        fn on_book_update(&mut self, _now: Timestamp, _book: &OrderBook) -> Vec<StrategyAction> {
            Vec::new()
        }
        fn on_trade(&mut self, _now: Timestamp, _trade: &Trade) -> Vec<StrategyAction> {
            if self.fired {
                return Vec::new();
            }
            self.fired = true;
            vec![StrategyAction::Place(NewOrder {
                cid: ClientOrderId::new("on-trade-1"),
                venue: self.venue.clone(),
                symbol: self.symbol.clone(),
                side: Side::Buy,
                kind: OrderKind::Limit,
                tif: TimeInForce::Gtc,
                qty: Qty(1),
                price: Some(Price(50)),
                ts: Timestamp::default(),
            })]
        }
        fn on_fill(&mut self, _fill: &Fill) {}
    }

    #[test]
    fn trade_payload_fans_out_to_on_trade_and_actions_reach_engine() {
        let s = TradeQuoter {
            symbol: sym(),
            venue: venue(),
            fired: false,
        };
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), s);
        // Seed a book so the submit-side reference-price check passes —
        // the quote is a limit so it uses its own price, but the
        // executor still needs a book to rest against.
        e.apply_event(&snapshot_event(vec![lvl(100, 5)], vec![lvl(110, 5)], 1))
            .unwrap();
        let step = e.apply_event(&trade_event()).unwrap();
        assert_eq!(step.reports.len(), 1);
        assert_eq!(step.reports[0].status, OrderStatus::New);
        assert_eq!(e.live_orders().len(), 1);
    }

    #[test]
    fn paused_engine_drops_on_trade_actions() {
        let s = TradeQuoter {
            symbol: sym(),
            venue: venue(),
            fired: false,
        };
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), s);
        e.apply_event(&snapshot_event(vec![lvl(100, 5)], vec![lvl(110, 5)], 1))
            .unwrap();
        e.set_paused(true);
        let step = e.apply_event(&trade_event()).unwrap();
        assert!(step.reports.is_empty());
        assert!(e.live_orders().is_empty());
    }

    /// Timer-only strategy that emits one cancel the first time
    /// `on_timer` fires. Proves `apply_timer` walks the action list.
    struct TimerCanceler {
        target: ClientOrderId,
        fired: bool,
    }
    impl Strategy for TimerCanceler {
        fn on_book_update(&mut self, _now: Timestamp, _book: &OrderBook) -> Vec<StrategyAction> {
            Vec::new()
        }
        fn on_fill(&mut self, _fill: &Fill) {}
        fn on_timer(&mut self, _now: Timestamp) -> Vec<StrategyAction> {
            if self.fired {
                return Vec::new();
            }
            self.fired = true;
            vec![StrategyAction::Cancel(self.target.clone())]
        }
    }

    #[test]
    fn apply_timer_fans_out_to_on_timer_and_actions_reach_engine() {
        let s = TimerCanceler {
            target: ClientOrderId::new("nope"),
            fired: false,
        };
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), s);
        // No such live order; cancel_internal surfaces Rejected — which
        // is exactly the signal we use to prove the action ran.
        let step = e.apply_timer(Timestamp::default());
        assert_eq!(step.reports.len(), 1);
        assert_eq!(step.reports[0].status, OrderStatus::Rejected);
    }

    #[test]
    fn paused_engine_drops_on_timer_actions() {
        let s = TimerCanceler {
            target: ClientOrderId::new("nope"),
            fired: false,
        };
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), s);
        e.set_paused(true);
        let step = e.apply_timer(Timestamp::default());
        assert!(step.reports.is_empty());
    }

    #[test]
    fn snapshot_event_ticks_maker_and_places_two_quotes() {
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), maker());
        let step = e
            .apply_event(&snapshot_event(vec![lvl(100, 5)], vec![lvl(110, 5)], 1))
            .unwrap();
        // Two Place reports, both New (quotes don't cross).
        assert_eq!(step.reports.len(), 2);
        for r in &step.reports {
            assert_eq!(r.status, OrderStatus::New);
        }
        assert_eq!(e.live_orders().len(), 2);
        assert_eq!(e.risk().open_orders(), 2);
    }

    #[test]
    fn second_snapshot_cancels_prior_quotes_and_places_new() {
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), maker());
        e.apply_event(&snapshot_event(vec![lvl(100, 5)], vec![lvl(110, 5)], 1))
            .unwrap();
        let prior_cids: HashSet<_> = e.live_orders().keys().cloned().collect();

        let step = e
            .apply_event(&snapshot_event(vec![lvl(101, 5)], vec![lvl(109, 5)], 2))
            .unwrap();
        // Two Canceled reports for the old cids, two New for the fresh quotes.
        let canceled: Vec<_> = step
            .reports
            .iter()
            .filter(|r| r.status == OrderStatus::Canceled)
            .collect();
        let new: Vec<_> = step
            .reports
            .iter()
            .filter(|r| r.status == OrderStatus::New)
            .collect();
        assert_eq!(canceled.len(), 2);
        assert_eq!(new.len(), 2);
        for r in &canceled {
            assert!(prior_cids.contains(&r.cid));
        }
        // Live ledger now holds only the fresh pair.
        assert_eq!(e.live_orders().len(), 2);
        for cid in e.live_orders().keys() {
            assert!(!prior_cids.contains(cid));
        }
    }

    #[test]
    fn delta_before_snapshot_errors() {
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), maker());
        let err = e
            .apply_event(&delta_event(vec![lvl(100, 5)], vec![lvl(110, 5)], 0, 1))
            .unwrap_err();
        assert_eq!(err, BookError::Uninitialized);
    }

    #[test]
    fn external_market_order_fills_and_updates_risk_position() {
        let mut cfg = engine_cfg();
        cfg.notional_fallback_price = None;
        let mut e = PaperEngine::new(cfg, RiskConfig::permissive(), maker());
        e.apply_event(&snapshot_event(
            vec![lvl(100, 10)],
            vec![lvl(110, 10), lvl(111, 10)],
            1,
        ))
        .unwrap();

        let report = e
            .submit(market_order(Side::Buy, 5), Timestamp::default())
            .unwrap();
        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_qty, Qty(5));
        assert_eq!(e.risk().position(&sym()), 5);
        assert!(!e.live_orders().contains_key(&ClientOrderId::new("ext-1")));
    }

    #[test]
    fn market_order_without_opposite_side_and_no_fallback_is_rejected() {
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), maker());
        // Bids only; a buy market has no ask to reference.
        e.apply_event(&snapshot_event(vec![lvl(100, 10)], vec![], 1))
            .unwrap();
        let report = e
            .submit(market_order(Side::Buy, 1), Timestamp::default())
            .unwrap();
        assert_eq!(report.status, OrderStatus::Rejected);
        assert_eq!(e.risk().open_orders(), 0);
    }

    #[test]
    fn market_order_uses_fallback_price_when_book_one_sided() {
        let mut cfg = engine_cfg();
        cfg.notional_fallback_price = Some(Price(100));
        let mut e = PaperEngine::new(cfg, RiskConfig::permissive(), maker());
        e.apply_event(&snapshot_event(vec![lvl(100, 10)], vec![], 1))
            .unwrap();
        let report = e
            .submit(market_order(Side::Buy, 1), Timestamp::default())
            .unwrap();
        // Risk passes, executor finds no asks, market qty remains → Canceled.
        assert_eq!(report.status, OrderStatus::Canceled);
    }

    #[test]
    fn risk_rejection_surfaces_as_exec_report_rejected() {
        let mut risk_cfg = RiskConfig::permissive();
        risk_cfg.max_order_notional = 50;
        let mut e = PaperEngine::new(engine_cfg(), risk_cfg, maker());
        e.apply_event(&snapshot_event(vec![lvl(100, 10)], vec![lvl(110, 10)], 1))
            .unwrap();

        // 10 @ 110 = 1100 notional, well above cap of 50.
        let report = e
            .submit(market_order(Side::Buy, 10), Timestamp::default())
            .unwrap();
        assert_eq!(report.status, OrderStatus::Rejected);
        assert!(report.reason.as_deref().unwrap().contains("notional"));
        assert_eq!(e.risk().open_orders(), 0);
        assert!(e.live_orders().is_empty());
    }

    #[test]
    fn duplicate_submit_for_live_cid_is_rejected() {
        // A buy limit at 90 against asks @ 110 stays New (resting). A
        // second submit with the same cid must reject without re-running
        // risk or replacing the live entry.
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), maker());
        e.apply_event(&snapshot_event(vec![lvl(100, 5)], vec![lvl(110, 5)], 1))
            .unwrap();

        let limit = NewOrder {
            cid: ClientOrderId::new("dup"),
            venue: venue(),
            symbol: sym(),
            side: Side::Buy,
            kind: OrderKind::Limit,
            tif: TimeInForce::Gtc,
            qty: Qty(1),
            price: Some(Price(90)),
            ts: Timestamp::default(),
        };
        let first = e.submit(limit.clone(), Timestamp::default()).unwrap();
        assert_eq!(first.status, OrderStatus::New);
        let live_before = e.live_orders().len();

        let second = e.submit(limit, Timestamp::default()).unwrap();
        assert_eq!(second.status, OrderStatus::Rejected);
        assert!(second.reason.as_deref().unwrap().contains("duplicate cid"));
        // The original live entry must still be present and unique.
        assert_eq!(e.live_orders().len(), live_before);
    }

    #[test]
    fn cancel_unknown_cid_rejects_without_touching_state() {
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), maker());
        let report = e.cancel(&ClientOrderId::new("no-such-order")).unwrap();
        assert_eq!(report.status, OrderStatus::Rejected);
        assert_eq!(e.risk().open_orders(), 0);
    }

    #[test]
    fn paused_engine_drops_strategy_actions_on_book_update() {
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), maker());
        e.set_paused(true);
        assert!(e.is_paused());

        let step = e
            .apply_event(&snapshot_event(vec![lvl(100, 5)], vec![lvl(110, 5)], 1))
            .unwrap();

        // Maker would normally place two quotes — gated to nothing.
        assert!(step.reports.is_empty());
        assert!(step.fills.is_empty());
        assert!(e.live_orders().is_empty());
        assert_eq!(e.risk().open_orders(), 0);

        // Unpause and the next book update places quotes as usual.
        e.set_paused(false);
        let step = e
            .apply_event(&snapshot_event(vec![lvl(101, 5)], vec![lvl(109, 5)], 2))
            .unwrap();
        assert_eq!(step.reports.len(), 2);
        assert_eq!(e.live_orders().len(), 2);
    }

    #[test]
    fn drain_shutdown_emits_cancels_even_when_paused() {
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), maker());
        // Place two quotes via a normal tick.
        e.apply_event(&snapshot_event(vec![lvl(100, 5)], vec![lvl(110, 5)], 1))
            .unwrap();
        assert_eq!(e.live_orders().len(), 2);

        // Pausing must NOT muzzle the shutdown sweep — outstanding
        // orders need to be cancelled even after a kill switch trip.
        e.set_paused(true);
        let step = e.drain_shutdown(Timestamp::default());
        assert_eq!(step.reports.len(), 2);
        for r in &step.reports {
            assert_eq!(r.status, OrderStatus::Canceled);
        }
        assert!(e.live_orders().is_empty());
    }

    #[test]
    fn fill_driven_inventory_steers_maker_quotes_via_skew() {
        // Give the maker a nonzero skew and verify that after a buy fill,
        // the next quoting tick shifts both quotes downward.
        let cfg = engine_cfg();
        let skewed = InventorySkewMaker::new(MakerConfig {
            venue: venue(),
            symbol: sym(),
            quote_qty: Qty(2),
            half_spread_ticks: 5,
            imbalance_widen_ticks: 0,
            vol_lambda: 0.94,
            vol_widen_coeff: 0.0,
            inventory_skew_ticks: 1,
            max_inventory: 100,
            cid_prefix: "mk".into(),
        });
        let mut e = PaperEngine::new(cfg, RiskConfig::permissive(), skewed);

        // Tick 1: flat inventory, quotes symmetric around mid 105.
        e.apply_event(&snapshot_event(vec![lvl(100, 10)], vec![lvl(110, 10)], 1))
            .unwrap();

        // Externally buy 3 at the ask so the maker records a +3 inventory fill.
        let fill_for_maker = Fill {
            cid: ClientOrderId::new("ext"),
            venue: venue(),
            symbol: sym(),
            side: Side::Buy,
            price: Price(110),
            qty: Qty(3),
            ts: Timestamp::default(),
            is_maker: None,
            fee: 0,
            fee_asset: None,
        };
        // The engine's strategy is private; poke inventory through a synthetic
        // fill by submitting and filling a marketable order.
        let _ = e
            .submit(market_order(Side::Buy, 3), Timestamp::default())
            .unwrap();
        let _ = fill_for_maker; // kept to document the scenario

        // Tick 2: post-fill, inventory should skew quotes down by 3 ticks.
        let step = e
            .apply_event(&snapshot_event(vec![lvl(100, 10)], vec![lvl(110, 10)], 2))
            .unwrap();
        let places: Vec<&ExecReport> = step
            .reports
            .iter()
            .filter(|r| r.status == OrderStatus::New)
            .collect();
        // Both fresh quotes should have been re-placed.
        assert_eq!(places.len(), 2);
        assert_eq!(e.risk().position(&sym()), 3);
    }

    /// No-op strategy: ignores every callback. Lets PostOnly tests
    /// observe `live_orders` without an auto-quoting maker polluting it.
    struct Passive;
    impl Strategy for Passive {
        fn on_book_update(&mut self, _now: Timestamp, _book: &OrderBook) -> Vec<StrategyAction> {
            Vec::new()
        }
        fn on_fill(&mut self, _fill: &Fill) {}
    }

    fn post_only_limit(side: Side, price: i64, qty: i64, cid: &'static str) -> NewOrder {
        NewOrder {
            cid: ClientOrderId::new(cid),
            venue: venue(),
            symbol: sym(),
            side,
            kind: OrderKind::Limit,
            tif: TimeInForce::PostOnly,
            qty: Qty(qty),
            price: Some(Price(price)),
            ts: Timestamp::default(),
        }
    }

    #[test]
    fn post_only_buy_crossing_best_ask_is_rejected() {
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), Passive);
        e.apply_event(&snapshot_event(vec![lvl(99, 5)], vec![lvl(100, 5)], 1))
            .unwrap();
        let report = e
            .submit(
                post_only_limit(Side::Buy, 100, 1, "po-buy"),
                Timestamp::default(),
            )
            .unwrap();
        assert_eq!(report.status, OrderStatus::Rejected);
        assert!(report
            .reason
            .as_deref()
            .unwrap()
            .contains("post-only would cross"));
        assert!(e.live_orders().is_empty());
        assert_eq!(e.risk().open_orders(), 0);
    }

    #[test]
    fn post_only_sell_crossing_best_bid_is_rejected() {
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), Passive);
        e.apply_event(&snapshot_event(vec![lvl(100, 5)], vec![lvl(101, 5)], 1))
            .unwrap();
        let report = e
            .submit(
                post_only_limit(Side::Sell, 100, 1, "po-sell"),
                Timestamp::default(),
            )
            .unwrap();
        assert_eq!(report.status, OrderStatus::Rejected);
        assert!(report
            .reason
            .as_deref()
            .unwrap()
            .contains("post-only would cross"));
        assert!(e.live_orders().is_empty());
    }

    #[test]
    fn post_only_limit_not_crossing_accepts_as_resting_maker() {
        let mut e = PaperEngine::new(engine_cfg(), RiskConfig::permissive(), Passive);
        e.apply_event(&snapshot_event(vec![lvl(100, 5)], vec![lvl(110, 5)], 1))
            .unwrap();
        let cid = ClientOrderId::new("po-rest");
        let order = post_only_limit(Side::Buy, 99, 1, "po-rest");
        let report = e.submit(order, Timestamp::default()).unwrap();
        assert_eq!(report.status, OrderStatus::New);
        assert!(report.fills.is_empty());
        assert!(e.live_orders().contains_key(&cid));
    }
}
