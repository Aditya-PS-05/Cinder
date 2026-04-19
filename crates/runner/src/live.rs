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

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, Notify};
use tracing::{debug, error, warn};

use ts_book::OrderBook;
use ts_core::{ExecReport, Fill, MarketEvent, MarketPayload, Timestamp};
use ts_oms::OrderEngine;
use ts_risk::KillSwitch;
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
        }
    }

    /// Drive the runner until the event channel closes or the handle
    /// asks for shutdown. Returns the final counter summary. Engine
    /// submit / cancel / reconcile errors are logged and counted, not
    /// propagated — a live runner should stay up through transient
    /// venue failures.
    pub async fn run(mut self) -> LiveSummary {
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
                    self.summary.orders_submitted += 1;
                    match self.engine.submit(order, now) {
                        Ok(report) => self.observe_report(report).await,
                        Err(err) => {
                            error!(error = %err, "live-runner: shutdown submit failed");
                            self.summary.reconcile_errors += 1;
                        }
                    }
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
                    self.summary.orders_submitted += 1;
                    match self.engine.submit(order, event.local_ts) {
                        Ok(report) => self.observe_report(report).await,
                        Err(err) => {
                            error!(error = %err, "live-runner: submit failed");
                            self.summary.reconcile_errors += 1;
                        }
                    }
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
}
