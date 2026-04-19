//! Live-trading runner.
//!
//! Mirrors [`EngineRunner`](crate::EngineRunner) but for venue-backed
//! engines like [`ts_binance::BinanceLiveEngine`]. The paper engine
//! owns book + strategy internally (via `Replay`); the live engine
//! only speaks to the venue, so [`LiveRunner`] plays the orchestrator
//! role itself:
//!
//! 1. drains [`MarketEvent`]s from a `tokio::sync::mpsc`,
//! 2. maintains a local [`OrderBook`] and ticks the strategy on every
//!    book-moving event,
//! 3. translates each [`StrategyAction`] into an [`OrderEngine`] call,
//! 4. polls [`OrderEngine::reconcile`] to drain venue-side
//!    [`ExecReport`]s (delivered by the user-data-stream), and
//! 5. feeds every report + fill back into the strategy so inventory
//!    stays correct.
//!
//! The runner is generic over the engine so tests can drive it with a
//! mock that doesn't need HTTP. The reconcile cadence is decoupled from
//! market events: a timer ticks reconcile even during quiet periods so
//! fills arrive bounded-latency after the user-data-stream lands them.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc, Notify};
use tracing::{debug, error, warn};

use ts_book::OrderBook;
use ts_core::{
    ClientOrderId, ExecReport, Fill, MarketEvent, MarketPayload, NewOrder, OrderKind, Price, Side,
    Timestamp,
};
use ts_oms::OrderEngine;
use ts_pnl::Accountant;
use ts_risk::{KillSwitch, PnlGuard, RiskConfig, RiskEngine};
use ts_strategy::{Strategy, StrategyAction};

use crate::audit::AuditEvent;
use crate::metrics::RunnerMetrics;

/// Mirror of [`ts_replay::ReplaySummary`] for the live path. The live
/// engine doesn't track realized / unrealized PnL itself — that lives
/// in the strategy / an external PnL module — so we keep this purely
/// to counter-based observability for now.
#[derive(Clone, Debug, Default)]
pub struct LiveSummary {
    pub events_ingested: u64,
    pub book_updates: u64,
    pub orders_submitted: u64,
    pub orders_new: u64,
    pub orders_filled: u64,
    pub orders_canceled: u64,
    pub orders_rejected: u64,
    pub orders_expired: u64,
    pub fills: u64,
    pub reconcile_errors: u64,
}

/// Driver around an [`OrderEngine`] + [`Strategy`] + local [`OrderBook`].
pub struct LiveRunner<E: OrderEngine> {
    engine: E,
    strategy: Box<dyn Strategy>,
    book: OrderBook,
    rx: mpsc::Receiver<MarketEvent>,
    shutdown: Arc<Notify>,
    reconcile_interval: Duration,
    summary_tx: Option<broadcast::Sender<LiveSummary>>,
    summary_interval: Duration,
    audit_tx: Option<mpsc::Sender<AuditEvent>>,
    metrics: Option<Arc<RunnerMetrics>>,
    kill_switch: Option<Arc<KillSwitch>>,
    summary: LiveSummary,
    /// Tracks realized-net and unrealized PnL for the [`PnlGuard`]. The
    /// accountant is always constructed; the guard is the opt-in piece.
    accountant: Accountant,
    pnl_guard: Option<PnlGuard>,
    /// Pre-trade gate. Owned by the runner so risk rejects surface as
    /// `OrderStatus::Rejected` reports without ever touching the venue.
    /// Defaults to `RiskConfig::permissive()` when no config is attached,
    /// matching the hardcoded baseline the paper runner used before the
    /// pre-trade section landed.
    risk: RiskEngine,
    /// Cids that have passed `risk.check` and been recorded via
    /// `risk.record_submit`. Kept so `risk.record_complete` only fires
    /// once per order and only for orders this runner is tracking — an
    /// externally-injected cid arriving via the user-data-stream
    /// (`engine.inbound_sender()`) wouldn't be in this set and so won't
    /// decrement the open-order counter.
    live_cids: HashSet<ClientOrderId>,
}

/// Remote control for a [`LiveRunner`]. Dropping the handle does not
/// stop the runner — call [`Self::shutdown`] explicitly.
#[derive(Clone)]
pub struct LiveRunnerHandle {
    shutdown: Arc<Notify>,
    summary_tx: Option<broadcast::Sender<LiveSummary>>,
}

impl LiveRunnerHandle {
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    pub fn subscribe_summaries(&self) -> Option<broadcast::Receiver<LiveSummary>> {
        self.summary_tx.as_ref().map(|tx| tx.subscribe())
    }
}

impl<E: OrderEngine> LiveRunner<E> {
    pub fn new(
        engine: E,
        strategy: Box<dyn Strategy>,
        rx: mpsc::Receiver<MarketEvent>,
    ) -> (Self, LiveRunnerHandle) {
        Self::builder(engine, strategy, rx).build()
    }

    pub fn builder(
        engine: E,
        strategy: Box<dyn Strategy>,
        rx: mpsc::Receiver<MarketEvent>,
    ) -> LiveRunnerBuilder<E> {
        LiveRunnerBuilder {
            engine,
            strategy,
            rx,
            reconcile_interval: Duration::from_millis(100),
            summary_interval: Duration::from_secs(0),
            summary_capacity: 0,
            audit_tx: None,
            metrics: None,
            kill_switch: None,
            pnl_guard: None,
            risk_config: RiskConfig::permissive(),
        }
    }

    /// Drive the runner until the event channel closes or the handle
    /// asks for shutdown. Returns the final counter summary. Engine
    /// submit / cancel / reconcile errors are logged and counted, not
    /// propagated — a live runner should stay up through transient
    /// venue failures.
    pub async fn run(mut self) -> LiveSummary {
        // Seed the PnL guard so its daily baseline is pinned to zero
        // (the start-of-session realized PnL) rather than whatever the
        // first post-fill snapshot happens to be. Without this first
        // observe, a batch of fills delivered before the first guard
        // tick would set the baseline to the already-lossy realized
        // total, hiding the breach.
        self.evaluate_pnl_guard();

        let mut reconcile = tokio::time::interval(self.reconcile_interval);
        reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        reconcile.tick().await; // skip the immediate tick

        let mut summary_ticker = if self.summary_interval.is_zero() {
            None
        } else {
            let mut i = tokio::time::interval(self.summary_interval);
            i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            i.tick().await;
            Some(i)
        };

        // Track the most recent event timestamp so shutdown-sweep
        // cancels can be stamped consistently with the event stream.
        let mut last_ts = Timestamp::default();

        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.notified() => {
                    debug!("live-runner: shutdown requested");
                    break;
                }
                maybe_event = self.rx.recv() => {
                    let Some(event) = maybe_event else {
                        debug!("live-runner: event channel closed");
                        break;
                    };
                    last_ts = event.local_ts;
                    if let Some(m) = self.metrics.as_ref() {
                        m.observe_event(&event);
                    }
                    self.handle_market_event(event).await;
                }
                _ = reconcile.tick() => {
                    self.drain_reconcile().await;
                }
                _ = async {
                    match summary_ticker.as_mut() {
                        Some(i) => { i.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    if let Some(tx) = self.summary_tx.as_ref() {
                        let _ = tx.send(self.summary.clone());
                    }
                }
            }
        }

        // Cancel any quotes the strategy still holds. Without this the
        // venue keeps resting orders across a process restart, which is
        // exactly the failure mode a trader most wants to avoid.
        self.drain_strategy_shutdown(last_ts).await;

        // One last reconcile so anything that landed between the final
        // event and shutdown (including the cancels above) isn't lost.
        self.drain_reconcile().await;
        self.summary.clone()
    }

    async fn drain_strategy_shutdown(&mut self, now: Timestamp) {
        let actions = self.strategy.on_shutdown();
        for action in actions {
            match action {
                StrategyAction::Cancel(cid) => match self.engine.cancel(&cid) {
                    Ok(report) => self.observe_report(report).await,
                    Err(err) => {
                        error!(error = %err, "live-runner: shutdown cancel failed");
                        self.summary.reconcile_errors += 1;
                    }
                },
                StrategyAction::Place(order) => {
                    self.submit_with_risk(order, now, "shutdown submit failed")
                        .await;
                }
            }
        }
    }

    async fn handle_market_event(&mut self, event: MarketEvent) {
        self.summary.events_ingested += 1;

        let ticked = match &event.payload {
            MarketPayload::BookSnapshot(s) => {
                self.book.apply_snapshot(s, event.seq);
                self.summary.book_updates += 1;
                true
            }
            MarketPayload::BookDelta(d) => match self.book.apply_delta(d, event.seq) {
                Ok(()) => {
                    self.summary.book_updates += 1;
                    true
                }
                Err(err) => {
                    warn!(error = ?err, "live-runner: book delta error, dropping");
                    return;
                }
            },
            _ => false,
        };

        if !ticked {
            return;
        }

        let actions = self.strategy.on_book_update(event.local_ts, &self.book);
        let halted = self.kill_switch.as_ref().is_some_and(|ks| ks.tripped());
        for action in actions {
            if halted {
                warn!("live-runner: kill switch tripped; dropping strategy action");
                continue;
            }
            match action {
                StrategyAction::Place(order) => {
                    self.submit_with_risk(order, event.local_ts, "submit failed")
                        .await;
                }
                StrategyAction::Cancel(cid) => match self.engine.cancel(&cid) {
                    Ok(report) => self.observe_report(report).await,
                    Err(err) => {
                        error!(error = %err, "live-runner: cancel failed");
                        self.summary.reconcile_errors += 1;
                    }
                },
            }
        }
    }

    /// Run the pre-trade gate, submit if it passes, and emit a synthetic
    /// `Rejected` report when it doesn't. `err_ctx` identifies the call
    /// site in the transport-error log so the shutdown-sweep path
    /// doesn't masquerade as the hot path. The reference price mirrors
    /// [`ts_oms::PaperEngine::reference_price`] sans fallback — limit
    /// orders use their own price, market orders probe the opposite
    /// side of the local book, and a missing opposite side is a reject.
    async fn submit_with_risk(&mut self, order: NewOrder, now: Timestamp, err_ctx: &str) {
        let ref_price = match self.reference_price(&order) {
            Some(p) => p,
            None => {
                let r = ExecReport::rejected(order.cid.clone(), "no reference price available");
                self.observe_report(r).await;
                return;
            }
        };
        if let Err(rej) = self.risk.check(&order, ref_price) {
            let r = ExecReport::rejected(order.cid.clone(), rej.to_string());
            self.observe_report(r).await;
            return;
        }
        // Pre-trade passed — reserve the slot before the submit so
        // back-to-back actions see the updated open-order count.
        self.risk.record_submit(&order);
        self.live_cids.insert(order.cid.clone());
        self.summary.orders_submitted += 1;

        let cid = order.cid.clone();
        match self.engine.submit(order, now) {
            Ok(report) => self.observe_report(report).await,
            Err(err) => {
                error!(error = %err, "live-runner: {err_ctx}");
                self.summary.reconcile_errors += 1;
                // Transport failed; roll back the reservation so the
                // open-order counter reflects reality. No observe_report
                // — there was no ack to attribute this to.
                if self.live_cids.remove(&cid) {
                    self.risk.record_complete(&cid);
                }
            }
        }
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
                opposite.map(|lvl| lvl.price)
            }
        }
    }

    async fn drain_reconcile(&mut self) {
        match self.engine.reconcile() {
            Ok(step) => {
                for report in step.reports {
                    self.observe_report(report).await;
                }
                for fill in step.fills {
                    self.observe_fill(fill).await;
                }
            }
            Err(err) => {
                warn!(error = %err, "live-runner: reconcile failed");
                self.summary.reconcile_errors += 1;
            }
        }
        // Mirror the engine's cumulative illegal-transition counter
        // into Prometheus on every tick. Cheap (single atomic load +
        // store) and the only call site, so the gauge tracks the engine
        // even on quiet reconcile passes.
        if let Some(m) = self.metrics.as_ref() {
            m.observe_illegal_transitions(self.engine.illegal_transitions());
        }
        self.evaluate_pnl_guard();
    }

    /// Compute the PnL snapshot (realized net, unrealized, position,
    /// mark) from the accountant and publish it to `RunnerMetrics`;
    /// then re-evaluate the configured [`PnlGuard`] and trip the kill
    /// switch on a breach. Guard evaluation is a no-op when the guard
    /// or kill switch is unattached, when the switch is already
    /// tripped, or when no threshold is crossed — metrics still
    /// publish on every call.
    fn evaluate_pnl_guard(&mut self) {
        // LiveRunner is single-symbol today; the local book maps to the
        // strategy's instrument, so any symbol the accountant tracks
        // gets this book's mid as its mark. If we ever multiplex more
        // symbols through one runner this closure needs a per-symbol
        // book lookup.
        let mark = self.book.mid().map(Price);
        let realized_net = self.accountant.realized_net_total();
        let unrealized = self.accountant.unrealized_total(|_| mark);
        let position = self.accountant.position_total();
        if let Some(m) = self.metrics.as_ref() {
            m.observe_pnl(realized_net, unrealized, position, mark);
            for (sym, book) in self.accountant.iter() {
                let unr = mark.map_or(0, |p| self.accountant.unrealized(sym, p));
                m.observe_pnl_symbol(
                    sym,
                    book.position,
                    book.realized - book.fees,
                    unr,
                    book.fees,
                    mark,
                );
            }
        }
        let Some(guard) = self.pnl_guard.as_mut() else {
            return;
        };
        let Some(ks) = self.kill_switch.as_ref() else {
            return;
        };
        if ks.tripped() {
            return;
        }
        if let Some(breach) = guard.observe(Instant::now(), realized_net, unrealized) {
            warn!(
                ?breach,
                "live-runner: pnl guard breach; tripping kill switch"
            );
            ks.trip(breach.to_trip_reason());
            if let Some(m) = self.metrics.as_ref() {
                m.observe_kill_switch(ks);
            }
        }
    }

    async fn observe_report(&mut self, report: ExecReport) {
        use ts_core::OrderStatus;
        match report.status {
            OrderStatus::New => self.summary.orders_new += 1,
            OrderStatus::Filled => self.summary.orders_filled += 1,
            OrderStatus::Canceled => self.summary.orders_canceled += 1,
            OrderStatus::Rejected => {
                self.summary.orders_rejected += 1;
                if let Some(ks) = self.kill_switch.as_ref() {
                    if ks.record_reject(std::time::Instant::now()) {
                        error!("live-runner: kill switch tripped on reject rate");
                    }
                }
            }
            OrderStatus::Expired => self.summary.orders_expired += 1,
            OrderStatus::PartiallyFilled => {}
        }
        // Release the risk-engine's open-order reservation exactly once
        // per terminal report, and only for cids we submitted through
        // our pre-trade gate — external cids (user-data-stream pushes)
        // never reserved a slot, so there is nothing to release.
        if report.status.is_terminal() && self.live_cids.remove(&report.cid) {
            self.risk.record_complete(&report.cid);
        }
        // Fills embedded in a report are re-exposed in `EngineStep::fills`
        // by BinanceLiveEngine::reconcile, so we only forward them from
        // `observe_fill`. Synchronous reports from submit/cancel always
        // carry empty fills (the optimistic ack).
        self.strategy.on_exec_report(&report);
        if let Some(tx) = self.audit_tx.as_ref() {
            if tx.send(AuditEvent::Report(report)).await.is_err() {
                debug!("live-runner: audit sink closed, detaching");
                self.audit_tx = None;
            }
        }
        self.observe_metrics();
    }

    async fn observe_fill(&mut self, fill: Fill) {
        self.strategy.on_fill(&fill);
        self.accountant.on_fill(&fill);
        self.risk.record_fill(&fill);
        self.summary.fills += 1;
        if let Some(tx) = self.audit_tx.as_ref() {
            if tx.send(AuditEvent::Fill(fill)).await.is_err() {
                debug!("live-runner: audit sink closed, detaching");
                self.audit_tx = None;
            }
        }
        self.observe_metrics();
    }

    fn observe_metrics(&self) {
        if let Some(m) = self.metrics.as_ref() {
            m.observe_live(&self.summary);
            if let Some(ks) = self.kill_switch.as_ref() {
                m.observe_kill_switch(ks);
            }
        }
    }
}

pub struct LiveRunnerBuilder<E: OrderEngine> {
    engine: E,
    strategy: Box<dyn Strategy>,
    rx: mpsc::Receiver<MarketEvent>,
    reconcile_interval: Duration,
    summary_interval: Duration,
    summary_capacity: usize,
    audit_tx: Option<mpsc::Sender<AuditEvent>>,
    metrics: Option<Arc<RunnerMetrics>>,
    kill_switch: Option<Arc<KillSwitch>>,
    pnl_guard: Option<PnlGuard>,
    risk_config: RiskConfig,
}

impl<E: OrderEngine> LiveRunnerBuilder<E> {
    pub fn reconcile_interval(mut self, d: Duration) -> Self {
        self.reconcile_interval = d;
        self
    }

    pub fn summary_tap(mut self, interval: Duration, capacity: usize) -> Self {
        self.summary_interval = interval;
        self.summary_capacity = capacity;
        self
    }

    pub fn audit(mut self, tx: mpsc::Sender<AuditEvent>) -> Self {
        self.audit_tx = Some(tx);
        self
    }

    pub fn metrics(mut self, m: Arc<RunnerMetrics>) -> Self {
        self.metrics = Some(m);
        self
    }

    /// Attach a process-wide [`KillSwitch`]. When `tripped()` returns
    /// true, every subsequent strategy submit/cancel is dropped on the
    /// floor with a warn-level log, and venue-side `OrderStatus::Rejected`
    /// reports feed the switch's reject-rate window.
    pub fn kill_switch(mut self, ks: Arc<KillSwitch>) -> Self {
        self.kill_switch = Some(ks);
        self
    }

    /// Attach a [`PnlGuard`]. The runner folds every fill into an
    /// internal [`Accountant`] and re-evaluates the guard at the end of
    /// every reconcile tick; a breach trips the attached [`KillSwitch`].
    /// The guard is a no-op when no [`KillSwitch`] is attached — there
    /// is no downstream to signal.
    pub fn pnl_guard(mut self, guard: PnlGuard) -> Self {
        self.pnl_guard = Some(guard);
        self
    }

    /// Seed the runner's pre-trade [`RiskEngine`]. Defaults to
    /// [`RiskConfig::permissive`]; pass a tightened config (position
    /// cap, notional cap, open-order cap, whitelist) to enforce
    /// per-order limits before submits hit the venue.
    pub fn risk_config(mut self, cfg: RiskConfig) -> Self {
        self.risk_config = cfg;
        self
    }

    pub fn build(self) -> (LiveRunner<E>, LiveRunnerHandle) {
        let shutdown = Arc::new(Notify::new());
        let summary_tx = if self.summary_interval.is_zero() || self.summary_capacity == 0 {
            None
        } else {
            Some(broadcast::channel(self.summary_capacity).0)
        };
        let handle = LiveRunnerHandle {
            shutdown: shutdown.clone(),
            summary_tx: summary_tx.clone(),
        };
        let runner = LiveRunner {
            engine: self.engine,
            strategy: self.strategy,
            book: OrderBook::new(),
            rx: self.rx,
            shutdown,
            reconcile_interval: self.reconcile_interval,
            summary_tx,
            summary_interval: self.summary_interval,
            audit_tx: self.audit_tx,
            metrics: self.metrics,
            kill_switch: self.kill_switch,
            summary: LiveSummary::default(),
            accountant: Accountant::new(),
            pnl_guard: self.pnl_guard,
            risk: RiskEngine::new(self.risk_config),
            live_cids: HashSet::new(),
        };
        (runner, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use ts_binance::live_engine::BinanceOrderApi;
    use ts_binance::{
        BinanceError, BinanceLiveEngine, CancelOrderRequest, NewOrderRequest, OrderAck,
        QueryOrderRequest,
    };
    use ts_core::{
        BookLevel, BookSnapshot, ClientOrderId, ExecReport, InstrumentSpec, MarketEvent,
        MarketPayload, OrderStatus, Price, Qty, Side, Symbol, Timestamp, Venue,
    };
    use ts_strategy::InventorySkewMaker;
    use ts_strategy::MakerConfig;

    /// Test double: returns queued OrderAck responses in FIFO order.
    struct QueuedApi {
        responses: Mutex<std::collections::VecDeque<Result<OrderAck, BinanceError>>>,
    }

    impl QueuedApi {
        fn new(items: Vec<Result<OrderAck, BinanceError>>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(items.into()),
            })
        }
        fn pop(&self) -> Result<OrderAck, BinanceError> {
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("no queued responses left")
        }
    }

    #[allow(clippy::manual_async_fn)]
    impl BinanceOrderApi for QueuedApi {
        fn new_order(
            &self,
            _req: NewOrderRequest,
        ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send {
            async move { self.pop() }
        }
        fn cancel_order(
            &self,
            _req: CancelOrderRequest,
        ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send {
            async move { self.pop() }
        }
        fn query_order(
            &self,
            _req: QueryOrderRequest,
        ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send {
            async move { self.pop() }
        }
    }

    fn sym() -> Symbol {
        Symbol::from_static("BTCUSDT")
    }
    fn venue() -> Venue {
        Venue::BINANCE
    }

    fn spec() -> InstrumentSpec {
        InstrumentSpec {
            venue: venue(),
            symbol: sym(),
            base: "BTC".into(),
            quote: "USDT".into(),
            price_scale: 2,
            qty_scale: 5,
            min_qty: Qty(0),
            min_notional: 0,
        }
    }

    fn specs() -> HashMap<Symbol, InstrumentSpec> {
        let mut m = HashMap::new();
        m.insert(sym(), spec());
        m
    }

    fn maker() -> Box<dyn Strategy> {
        Box::new(InventorySkewMaker::new(MakerConfig {
            venue: venue(),
            symbol: sym(),
            quote_qty: Qty(100_000),
            half_spread_ticks: 5,
            imbalance_widen_ticks: 0,
            vol_lambda: 0.94,
            vol_widen_coeff: 0.0,
            inventory_skew_ticks: 0,
            max_inventory: 10_000_000,
            cid_prefix: "live-t".into(),
        }))
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
                    qty: Qty(1_000_000),
                }],
                asks: vec![BookLevel {
                    price: Price(ask),
                    qty: Qty(1_000_000),
                }],
            }),
        }
    }

    fn ack(cid: &str, status: &str) -> OrderAck {
        OrderAck {
            symbol: "BTCUSDT".into(),
            order_id: 1,
            client_order_id: cid.into(),
            transact_time: Some(0),
            price: "0".into(),
            orig_qty: "1.00000".into(),
            executed_qty: "0.00000".into(),
            cummulative_quote_qty: "0.00".into(),
            status: status.into(),
            time_in_force: "GTC".into(),
            order_type: "LIMIT".into(),
            side: "BUY".into(),
        }
    }

    #[tokio::test]
    async fn book_update_produces_submit_calls_and_counters_track_them() {
        // The maker places two quotes on every book update — bid + ask.
        // Pre-load the QueuedApi with two NEW acks so submit_internal
        // can complete both HTTP calls inside reconcile.
        let api = QueuedApi::new(vec![Ok(ack("bid-1", "NEW")), Ok(ack("ask-1", "NEW"))]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 32);

        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .build();
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(10_000, 10_010, 1)).await.unwrap();

        // Give the runtime time to process the event, spawn HTTP tasks,
        // drain reconcile, and forward reports.
        for _ in 0..50 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let summary = task.await.unwrap();

        assert_eq!(summary.events_ingested, 1);
        assert_eq!(summary.book_updates, 1);
        assert_eq!(summary.orders_submitted, 2, "maker places bid + ask");
        assert!(summary.orders_new >= 2, "both acks should be NEW");
    }

    #[tokio::test]
    async fn dropped_event_channel_ends_runner_cleanly() {
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let (tx, rx) = mpsc::channel(8);
        let (runner, _handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(50))
            .build();
        let task = tokio::spawn(runner.run());

        drop(tx); // channel closed → runner drains and exits
        let summary = task.await.unwrap();
        assert_eq!(summary.events_ingested, 0);
    }

    #[tokio::test]
    async fn user_stream_push_reaches_strategy_via_reconcile() {
        // No orders get submitted, but we push a FILLED ack directly
        // onto the engine's inbound sender (simulating a user-data-stream
        // frame) and confirm the runner's reconcile loop surfaces it.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);
        let inbound = engine.inbound_sender();

        let (_tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .build();
        let task = tokio::spawn(runner.run());

        let report = ExecReport {
            cid: ClientOrderId::new("external-1"),
            status: OrderStatus::Filled,
            filled_qty: Qty(100_000),
            avg_price: Some(Price(10_000)),
            reason: None,
            fills: vec![ts_core::Fill {
                cid: ClientOrderId::new("external-1"),
                venue: venue(),
                symbol: sym(),
                side: Side::Buy,
                price: Price(10_000),
                qty: Qty(100_000),
                ts: Timestamp::default(),
                is_maker: None,
                fee: 0,
                fee_asset: None,
            }],
        };
        inbound.send(report).await.unwrap();

        for _ in 0..20 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let summary = task.await.unwrap();
        assert_eq!(summary.orders_filled, 1);
        assert_eq!(summary.fills, 1, "fill should reach the strategy");
    }

    #[tokio::test]
    async fn pnl_guard_breach_trips_kill_switch() {
        use ts_risk::{KillSwitch, PnlGuardConfig, TripReason};
        // A single-fill realized loss: SELL at 80 after a BUY at 100
        // yields realized=-200 before fees, which > 50 → daily-loss
        // breach. Using one combined push (both fills in one report)
        // ensures they fold into the accountant in the same reconcile
        // tick, so the guard re-evaluates once and trips.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);
        let inbound = engine.inbound_sender();

        let ks = Arc::new(KillSwitch::default());
        let guard = PnlGuard::new(PnlGuardConfig {
            max_drawdown: None,
            max_daily_loss: Some(50),
            day_length: Duration::from_secs(60),
        });

        let (_tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .kill_switch(Arc::clone(&ks))
            .pnl_guard(guard)
            .build();
        let task = tokio::spawn(runner.run());

        let report = ExecReport {
            cid: ClientOrderId::new("lossy"),
            status: OrderStatus::Filled,
            filled_qty: Qty(0),
            avg_price: None,
            reason: None,
            fills: vec![
                ts_core::Fill {
                    cid: ClientOrderId::new("lossy-open"),
                    venue: venue(),
                    symbol: sym(),
                    side: Side::Buy,
                    price: Price(100),
                    qty: Qty(10),
                    ts: Timestamp::default(),
                    is_maker: None,
                    fee: 0,
                    fee_asset: None,
                },
                ts_core::Fill {
                    cid: ClientOrderId::new("lossy-close"),
                    venue: venue(),
                    symbol: sym(),
                    side: Side::Sell,
                    price: Price(80),
                    qty: Qty(10),
                    ts: Timestamp::default(),
                    is_maker: None,
                    fee: 0,
                    fee_asset: None,
                },
            ],
        };
        inbound.send(report).await.unwrap();

        for _ in 0..40 {
            if ks.tripped() {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let _ = task.await.unwrap();

        assert!(ks.tripped(), "kill switch should trip on realized loss");
        assert_eq!(ks.reason(), Some(TripReason::DailyLoss));
    }

    #[tokio::test]
    async fn illegal_transition_count_is_mirrored_into_runner_metrics() {
        // Push two reports for the same cid: a FILLED then a stale
        // PARTIALLY_FILLED. The engine drops the stale one and bumps its
        // illegal_transitions counter; the runner must mirror that into
        // RunnerMetrics on the next reconcile tick so /metrics surfaces
        // it as ts_illegal_transitions_total.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);
        let inbound = engine.inbound_sender();

        let metrics = RunnerMetrics::new();
        let (_tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .metrics(Arc::clone(&metrics))
            .build();
        let task = tokio::spawn(runner.run());

        let cid = ClientOrderId::new("c-stale");
        inbound
            .send(ExecReport {
                cid: cid.clone(),
                status: OrderStatus::Filled,
                filled_qty: Qty(100_000),
                avg_price: Some(Price(10_000)),
                reason: None,
                fills: vec![],
            })
            .await
            .unwrap();
        inbound
            .send(ExecReport {
                cid: cid.clone(),
                status: OrderStatus::PartiallyFilled,
                filled_qty: Qty(50_000),
                avg_price: Some(Price(10_000)),
                reason: None,
                fills: vec![],
            })
            .await
            .unwrap();

        // Wait for at least one reconcile tick to drain both reports
        // and publish the engine's illegal_transitions count.
        let mut saw_one = false;
        for _ in 0..50 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            if metrics
                .encode_prometheus()
                .contains("ts_illegal_transitions_total 1")
            {
                saw_one = true;
                break;
            }
        }

        handle.shutdown();
        let _ = task.await.unwrap();

        assert!(
            saw_one,
            "expected ts_illegal_transitions_total 1 in metrics, got:\n{}",
            metrics.encode_prometheus()
        );
    }

    #[tokio::test]
    async fn pnl_guard_does_not_trip_below_limit() {
        use ts_risk::{KillSwitch, PnlGuardConfig};
        // Open + close with a small realized loss of 20; threshold 50.
        // The guard must stay armed and the kill switch unchanged.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);
        let inbound = engine.inbound_sender();

        let ks = Arc::new(KillSwitch::default());
        let guard = PnlGuard::new(PnlGuardConfig {
            max_drawdown: None,
            max_daily_loss: Some(50),
            day_length: Duration::from_secs(60),
        });

        let (_tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .kill_switch(Arc::clone(&ks))
            .pnl_guard(guard)
            .build();
        let task = tokio::spawn(runner.run());

        let report = ExecReport {
            cid: ClientOrderId::new("mild"),
            status: OrderStatus::Filled,
            filled_qty: Qty(0),
            avg_price: None,
            reason: None,
            fills: vec![
                ts_core::Fill {
                    cid: ClientOrderId::new("mild-open"),
                    venue: venue(),
                    symbol: sym(),
                    side: Side::Buy,
                    price: Price(100),
                    qty: Qty(2),
                    ts: Timestamp::default(),
                    is_maker: None,
                    fee: 0,
                    fee_asset: None,
                },
                ts_core::Fill {
                    cid: ClientOrderId::new("mild-close"),
                    venue: venue(),
                    symbol: sym(),
                    side: Side::Sell,
                    price: Price(90),
                    qty: Qty(2),
                    ts: Timestamp::default(),
                    is_maker: None,
                    fee: 0,
                    fee_asset: None,
                },
            ],
        };
        inbound.send(report).await.unwrap();

        for _ in 0..10 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        handle.shutdown();
        let _ = task.await.unwrap();

        assert!(!ks.tripped(), "modest loss must stay below the limit");
    }

    #[tokio::test]
    async fn pre_trade_whitelist_rejects_orders_before_reaching_venue() {
        // Tighten the pre-trade gate so the maker's BTCUSDT symbol is
        // NOT whitelisted. QueuedApi is empty — if the runner tried to
        // submit, its pop() would panic. Instead the risk engine
        // short-circuits, each rejected Place surfaces as a Rejected
        // report via observe_report, and orders_submitted stays at 0.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let mut risk_cfg = RiskConfig::permissive();
        risk_cfg.whitelist.insert(Symbol::from_static("ETHUSDT"));

        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .risk_config(risk_cfg)
            .build();
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(10_000, 10_010, 1)).await.unwrap();

        for _ in 0..30 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let summary = task.await.unwrap();

        assert_eq!(
            summary.orders_submitted, 0,
            "risk must short-circuit before submit"
        );
        assert!(
            summary.orders_rejected >= 2,
            "both maker quotes must surface as Rejected, got {}",
            summary.orders_rejected
        );
        // Transport never fired, so there can be no reconcile_errors.
        assert_eq!(summary.reconcile_errors, 0);
    }

    #[tokio::test]
    async fn pre_trade_notional_cap_blocks_oversized_order() {
        // A per-order notional cap of 1 blocks any meaningful quote.
        // With an empty QueuedApi, a single submit reaching the engine
        // would panic — proving the risk gate gatekeeps synchronously.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let mut risk_cfg = RiskConfig::permissive();
        risk_cfg.max_order_notional = 1;

        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .risk_config(risk_cfg)
            .build();
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(10_000, 10_010, 1)).await.unwrap();

        for _ in 0..30 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let summary = task.await.unwrap();

        assert_eq!(summary.orders_submitted, 0);
        assert!(summary.orders_rejected >= 2);
    }
}
