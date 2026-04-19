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
use std::sync::atomic::{AtomicU64, Ordering};
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
use ts_risk::{ClockSkewGuard, KillSwitch, PnlGuard, RiskConfig, RiskEngine, StalenessGuard};
use ts_strategy::{Strategy, StrategyAction};

use crate::audit::AuditEvent;
use crate::intent_log::IntentLogWriter;
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
    /// Cancels fired by the kill-switch trip sweep against cids that
    /// `Strategy::on_shutdown` failed to return but that the engine
    /// still considered live. Non-zero here indicates a strategy bug:
    /// the runner cleaned up, but the strategy's shutdown path is not
    /// trustworthy and should be fixed.
    pub trip_sweep_orphan_cancels: u64,
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
    /// Cadence at which [`Strategy::on_timer`] fires, independent of
    /// market events and reconcile ticks. Zero disables the tick. Pairs
    /// with the same knob on [`crate::EngineRunner`] so paper and live
    /// share one configuration surface for time-scheduled strategies.
    timer_interval: Duration,
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
    /// Shared handle to the user-data-stream listener's cumulative
    /// illegal-transition counter. `None` when no listener is attached
    /// (e.g. the paper runner's live-engine tests). The runner loads
    /// the counter once per reconcile tick and mirrors it into the
    /// `source="stream"` metric series so operators can see
    /// stream-vs-REST provenance side by side.
    stream_illegal_counter: Option<Arc<AtomicU64>>,
    /// Tracks whether the post-trip cancel sweep has already run for
    /// the current halted epoch. Flips to `true` on the first
    /// reconcile tick where the kill switch is tripped; flips back to
    /// `false` when the switch clears so a later re-trip runs a fresh
    /// sweep. Without this flag the strategy's `on_shutdown` would
    /// either be called repeatedly (leaking no-op cancel-reject
    /// reports) or never (leaving orders live on the venue until
    /// process exit).
    swept_after_trip: bool,
    /// Most recent event timestamp, kept as a field so the
    /// reconcile-driven trip sweep can stamp cancel submissions with
    /// a timestamp consistent with the event stream. Updated on
    /// every market event, mirroring the previous `last_ts` local in
    /// `run`.
    last_event_ts: Timestamp,
    /// Crash-safety WAL. When attached, every pre-trade-passed submit
    /// is fsynced to this writer *before* the engine's HTTP call fires,
    /// and every terminal [`ExecReport`] writes a completion tombstone
    /// so restart-time replay only surfaces cids whose outcome the
    /// operator never observed. `None` disables the WAL entirely —
    /// correctness is preserved (the runner never reads the log), but
    /// crash recovery is manual.
    intent_log: Option<IntentLogWriter>,
    /// Detects prolonged silence on the market-data feed. The runner
    /// stamps the guard on every event and re-evaluates it on every
    /// reconcile tick; a breach trips the attached [`KillSwitch`] with
    /// [`ts_risk::TripReason::FeedStaleness`], so we stop quoting
    /// against a frozen book.
    staleness_guard: Option<StalenessGuard>,
    /// Detects local-vs-exchange clock drift. Each market event's
    /// `(exchange_ts, local_ts)` is folded through the guard; a breach
    /// trips the attached [`KillSwitch`] with
    /// [`ts_risk::TripReason::ClockSkew`] so latency-sensitive logic
    /// does not operate on bogus timing.
    clock_skew_guard: Option<ClockSkewGuard>,
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
            timer_interval: Duration::ZERO,
            audit_tx: None,
            metrics: None,
            kill_switch: None,
            pnl_guard: None,
            risk_config: RiskConfig::permissive(),
            stream_illegal_counter: None,
            intent_log: None,
            staleness_guard: None,
            clock_skew_guard: None,
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

        let mut timer_ticker = if self.timer_interval.is_zero() {
            None
        } else {
            let mut i = tokio::time::interval(self.timer_interval);
            i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            i.tick().await; // burn the immediate fire
            Some(i)
        };

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
                    self.last_event_ts = event.local_ts;
                    if let Some(m) = self.metrics.as_ref() {
                        m.observe_event(&event);
                    }
                    self.handle_market_event(event).await;
                }
                _ = reconcile.tick() => {
                    self.drain_reconcile().await;
                }
                _ = async {
                    match timer_ticker.as_mut() {
                        Some(i) => { i.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    self.handle_timer_tick().await;
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
        self.drain_strategy_shutdown(self.last_event_ts).await;

        // One last reconcile so anything that landed between the final
        // event and shutdown (including the cancels above) isn't lost.
        self.drain_reconcile().await;
        self.summary.clone()
    }

    /// Drive the kill-switch trip sweep at most once per halted
    /// epoch. Called at the tail of every reconcile tick, so the
    /// sweep fires within one reconcile interval of the switch
    /// tripping. `Place` actions coming back from `on_shutdown` are
    /// ignored — the whole point of a trip is to stop opening new
    /// exposure; only `Cancel` actions are forwarded to the engine.
    async fn apply_trip_sweep(&mut self) {
        let Some(ks) = self.kill_switch.as_ref() else {
            return;
        };
        let tripped = ks.tripped();
        if !tripped {
            // Operator cleared the switch; re-arm the sweep so a later
            // re-trip fires another one.
            if self.swept_after_trip {
                debug!("live-runner: kill switch cleared; re-arming trip sweep");
                self.swept_after_trip = false;
            }
            return;
        }
        if self.swept_after_trip {
            return;
        }
        self.swept_after_trip = true;
        warn!("live-runner: kill switch tripped; running cancel sweep");
        let actions = self.strategy.on_shutdown();
        let mut handled: HashSet<ClientOrderId> = HashSet::new();
        for action in actions {
            match action {
                StrategyAction::Cancel(cid) => {
                    handled.insert(cid.clone());
                    match self.engine.cancel(&cid) {
                        Ok(report) => self.observe_report(report).await,
                        Err(err) => {
                            error!(error = %err, "live-runner: trip-sweep cancel failed");
                            self.summary.reconcile_errors += 1;
                        }
                    }
                }
                StrategyAction::Place(order) => {
                    // `on_shutdown` is contractually cancel-only; a
                    // Place here would be a strategy bug. Log once
                    // and drop — we never want to open new risk after
                    // a trip.
                    warn!(cid = ?order.cid, "live-runner: dropping Place from on_shutdown during trip sweep");
                }
            }
        }
        // Defense in depth: the strategy might have forgotten an open
        // cid (bug, half-ticked state, etc.). The engine is the
        // authoritative source for "what's actually live on the
        // venue," so cancel any cids it still tracks that the
        // strategy didn't hand us. Snapshot first — each successful
        // cancel mutates `open_cids()` on engines that track live
        // state inline.
        let orphans: Vec<ClientOrderId> = self
            .engine
            .open_cids()
            .into_iter()
            .filter(|cid| !handled.contains(cid))
            .collect();
        for cid in orphans {
            warn!(
                cid = %cid.as_str(),
                "live-runner: trip sweep cancelling cid strategy did not surface"
            );
            match self.engine.cancel(&cid) {
                Ok(report) => {
                    self.summary.trip_sweep_orphan_cancels += 1;
                    self.observe_report(report).await;
                }
                Err(err) => {
                    error!(error = %err, "live-runner: trip-sweep orphan cancel failed");
                    self.summary.reconcile_errors += 1;
                }
            }
        }
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

    /// Fire [`Strategy::on_timer`] on the runner-driven cadence and
    /// dispatch emitted actions through the same path the market-event
    /// branch uses. Halted runners short-circuit: ticking and then
    /// discarding actions would advance the strategy's cid tracking
    /// for orders we're not actually placing, exactly the invariant
    /// [`Self::handle_market_event`] enforces by skipping `on_book_update`
    /// while the kill switch is tripped.
    async fn handle_timer_tick(&mut self) {
        if self.kill_switch.as_ref().is_some_and(|ks| ks.tripped()) {
            return;
        }
        let actions = self.strategy.on_timer(self.last_event_ts);
        self.dispatch_actions(actions, self.last_event_ts).await;
    }

    async fn handle_market_event(&mut self, event: MarketEvent) {
        self.summary.events_ingested += 1;
        // Stamp the staleness guard first — a feed that stays silent
        // through shutdown is still considered alive for whatever
        // events *did* arrive before the gap started.
        if let Some(g) = self.staleness_guard.as_mut() {
            g.observe_event(Instant::now());
        }
        // Fold event timestamps through the clock-skew guard. Inlined
        // rather than delegated to a helper because the borrow
        // checker prefers `self.field.method()` over a mut method
        // call that also touches `self.kill_switch`.
        if let Some(g) = self.clock_skew_guard.as_mut() {
            if let Some(breach) = g.observe(event.exchange_ts, event.local_ts) {
                if let Some(ks) = self.kill_switch.as_ref() {
                    if !ks.tripped() {
                        warn!(
                            ?breach,
                            "live-runner: clock-skew guard breach; tripping kill switch"
                        );
                        ks.trip(breach.to_trip_reason());
                        if let Some(m) = self.metrics.as_ref() {
                            m.observe_kill_switch(ks);
                        }
                    }
                }
            }
        }

        enum TickKind<'a> {
            Book,
            Trade(&'a ts_core::Trade),
            Skip,
        }

        let kind = match &event.payload {
            MarketPayload::BookSnapshot(s) => {
                self.book.apply_snapshot(s, event.seq);
                self.summary.book_updates += 1;
                TickKind::Book
            }
            MarketPayload::BookDelta(d) => match self.book.apply_delta(d, event.seq) {
                Ok(()) => {
                    self.summary.book_updates += 1;
                    TickKind::Book
                }
                Err(err) => {
                    warn!(error = ?err, "live-runner: book delta error, dropping");
                    return;
                }
            },
            MarketPayload::Trade(t) => TickKind::Trade(t),
            _ => TickKind::Skip,
        };

        if matches!(kind, TickKind::Skip) {
            return;
        }

        // While halted, skip the strategy tick entirely. Ticking and
        // dropping emitted actions would advance the strategy's
        // internal cid tracking for orders we never placed — a silent
        // drift between what the maker thinks is live and what's
        // actually on the venue. The paper engine enforces the same
        // invariant (`PaperEngine::apply_event` short-circuits while
        // `paused`); this keeps the two runners aligned. The trip
        // sweep in `drain_reconcile` handles cleanup of already-live
        // orders.
        if self.kill_switch.as_ref().is_some_and(|ks| ks.tripped()) {
            return;
        }

        let actions = match kind {
            TickKind::Book => self.strategy.on_book_update(event.local_ts, &self.book),
            TickKind::Trade(t) => self.strategy.on_trade(event.local_ts, t),
            TickKind::Skip => unreachable!(),
        };
        self.dispatch_actions(actions, event.local_ts).await;
    }

    /// Route a batch of strategy actions through the venue: `Place` goes
    /// through the pre-trade gate + engine submit, `Cancel` goes
    /// straight to `engine.cancel`. Shared between the book-update,
    /// trade, and timer paths so the dispatch rules stay identical.
    async fn dispatch_actions(&mut self, actions: Vec<StrategyAction>, now: Timestamp) {
        for action in actions {
            match action {
                StrategyAction::Place(order) => {
                    self.submit_with_risk(order, now, "submit failed").await;
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

        // Durably record the intent before the engine fires its HTTP
        // task. A crash between this fsync and the ack guarantees the
        // intent survives to restart-time replay; a crash *before* the
        // fsync loses the intent, matching what the venue knows (it
        // never received the request). A failed WAL write is treated
        // as a synthetic Rejected so we never leak an intent the
        // operator can't see.
        if let Some(w) = self.intent_log.as_mut() {
            if let Err(err) = w.record_submit(&order).await {
                error!(error = %err, "live-runner: intent-log submit write failed");
                self.live_cids.remove(&order.cid);
                self.risk.record_complete(&order.cid);
                self.summary.orders_submitted -= 1;
                let r = ExecReport::rejected(
                    order.cid.clone(),
                    format!("intent log write failed: {err}"),
                );
                self.observe_report(r).await;
                return;
            }
        }

        let cid = order.cid.clone();
        match self.engine.submit(order, now) {
            Ok(report) => self.observe_report(report).await,
            Err(err) => {
                error!(error = %err, "live-runner: {err_ctx}");
                self.summary.reconcile_errors += 1;
                // Transport failed; roll back the reservation so the
                // open-order counter reflects reality. No observe_report
                // — there was no ack to attribute this to. The intent
                // log still carries the submit line so operators know
                // the runner *attempted* the order; write a completion
                // tombstone so restart replay doesn't re-surface it as
                // an open intent.
                if self.live_cids.remove(&cid) {
                    self.risk.record_complete(&cid);
                }
                if let Some(w) = self.intent_log.as_mut() {
                    if let Err(err) = w.record_complete(&cid).await {
                        warn!(error = %err, "live-runner: intent-log completion write failed after transport error");
                    }
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
        // Edge-triggered cancel sweep on kill-switch trip. The first
        // reconcile tick that observes `halted` runs the strategy's
        // `on_shutdown` sweep and forwards its cancels through the
        // venue. This bounds the time between trip and
        // cancel-on-venue to one reconcile interval, rather than
        // leaving orders resting until process exit. The flag resets
        // when the switch clears so a re-trip runs another sweep.
        self.apply_trip_sweep().await;
        // Mirror the cumulative illegal-transition counters into
        // Prometheus on every tick. Two sources: the engine (REST
        // reconcile path) and, when attached, the user-data-stream
        // listener — each surfaces under its own `source=` label so
        // operators can tell whether stale frames come in via push or
        // pull. Cheap (single atomic load + store per source) and the
        // only publish site, so the counters track even on quiet
        // reconcile passes.
        if let Some(m) = self.metrics.as_ref() {
            m.observe_engine_illegal_transitions(self.engine.illegal_transitions());
            if let Some(c) = self.stream_illegal_counter.as_ref() {
                m.observe_stream_illegal_transitions(c.load(Ordering::Relaxed));
            }
        }
        self.evaluate_staleness_guard();
        self.evaluate_pnl_guard();
    }

    /// Re-evaluate the configured [`StalenessGuard`] and trip the kill
    /// switch on a breach. A no-op when the guard or kill switch is
    /// unattached, when the switch is already tripped, or when the
    /// guard hasn't yet seen a baseline event.
    fn evaluate_staleness_guard(&mut self) {
        let Some(guard) = self.staleness_guard.as_mut() else {
            return;
        };
        let Some(ks) = self.kill_switch.as_ref() else {
            return;
        };
        if ks.tripped() {
            return;
        }
        if let Some(breach) = guard.check(Instant::now()) {
            warn!(
                ?breach,
                "live-runner: staleness guard breach; tripping kill switch"
            );
            ks.trip(breach.to_trip_reason());
            if let Some(m) = self.metrics.as_ref() {
                m.observe_kill_switch(ks);
            }
        }
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
        let terminal_for_tracked_cid =
            report.status.is_terminal() && self.live_cids.remove(&report.cid);
        if terminal_for_tracked_cid {
            self.risk.record_complete(&report.cid);
            // Write the WAL completion tombstone so a crash after this
            // point leaves no dangling intent on restart-time replay.
            // A failed fsync here degrades to "operator sees orphan on
            // next startup" — noisy but safe — so we log and continue.
            if let Some(w) = self.intent_log.as_mut() {
                if let Err(err) = w.record_complete(&report.cid).await {
                    warn!(error = %err, cid = ?report.cid, "live-runner: intent-log completion write failed");
                }
            }
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
    timer_interval: Duration,
    audit_tx: Option<mpsc::Sender<AuditEvent>>,
    metrics: Option<Arc<RunnerMetrics>>,
    kill_switch: Option<Arc<KillSwitch>>,
    pnl_guard: Option<PnlGuard>,
    risk_config: RiskConfig,
    stream_illegal_counter: Option<Arc<AtomicU64>>,
    intent_log: Option<IntentLogWriter>,
    staleness_guard: Option<StalenessGuard>,
    clock_skew_guard: Option<ClockSkewGuard>,
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

    /// Fire [`Strategy::on_timer`] on this cadence even when the feed
    /// is silent. Zero disables the tick entirely. Matches the
    /// [`crate::EngineRunner::with_timer_interval`] knob on the paper
    /// side so both runners share one knob for time-scheduled
    /// strategies.
    pub fn timer_interval(mut self, d: Duration) -> Self {
        self.timer_interval = d;
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

    /// Wire the user-data-stream listener's illegal-transition counter
    /// (e.g. [`ts_binance::UserDataStreamClient::illegal_transitions_counter`])
    /// into the runner so its value reaches the
    /// `ts_illegal_transitions_total{source="stream"}` Prometheus series
    /// on every reconcile tick. Without this, the stream series stays
    /// pegged at zero even while the listener drops reordered frames.
    pub fn stream_illegal_counter(mut self, counter: Arc<AtomicU64>) -> Self {
        self.stream_illegal_counter = Some(counter);
        self
    }

    /// Attach a crash-safety [`IntentLogWriter`]. When set, every
    /// pre-trade-passed submit is fsynced to the log *before* the
    /// engine's HTTP task fires, and every terminal report writes a
    /// completion tombstone. Open intents are surfaced at next startup
    /// via [`crate::intent_log::replay_open_intents`].
    pub fn intent_log(mut self, writer: IntentLogWriter) -> Self {
        self.intent_log = Some(writer);
        self
    }

    /// Attach a [`StalenessGuard`] to monitor market-data feed gaps.
    /// Every market event stamps the guard with the current wall-clock
    /// time; every reconcile tick re-evaluates it. A breach trips the
    /// attached [`KillSwitch`] with
    /// [`ts_risk::TripReason::FeedStaleness`]. The guard is a no-op
    /// when no kill switch is attached — there is no downstream to
    /// signal.
    pub fn staleness_guard(mut self, guard: StalenessGuard) -> Self {
        self.staleness_guard = Some(guard);
        self
    }

    /// Attach a [`ClockSkewGuard`] to monitor local-vs-exchange clock
    /// drift. Each market event's `(exchange_ts, local_ts)` is folded
    /// through the guard; a breach trips the attached [`KillSwitch`]
    /// with [`ts_risk::TripReason::ClockSkew`]. No-op when no kill
    /// switch is attached.
    pub fn clock_skew_guard(mut self, guard: ClockSkewGuard) -> Self {
        self.clock_skew_guard = Some(guard);
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
            timer_interval: self.timer_interval,
            audit_tx: self.audit_tx,
            metrics: self.metrics,
            kill_switch: self.kill_switch,
            summary: LiveSummary::default(),
            accountant: Accountant::new(),
            pnl_guard: self.pnl_guard,
            risk: RiskEngine::new(self.risk_config),
            live_cids: HashSet::new(),
            stream_illegal_counter: self.stream_illegal_counter,
            swept_after_trip: false,
            last_event_ts: Timestamp::default(),
            intent_log: self.intent_log,
            staleness_guard: self.staleness_guard,
            clock_skew_guard: self.clock_skew_guard,
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
        MarketPayload, NewOrder, OrderKind, OrderStatus, Price, Qty, Side, Symbol, TimeInForce,
        Timestamp, Venue,
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
    async fn engine_illegal_transition_count_is_mirrored_into_runner_metrics() {
        // Push two reports for the same cid: a FILLED then a stale
        // PARTIALLY_FILLED. The engine drops the stale one and bumps its
        // illegal_transitions counter; the runner must mirror that into
        // RunnerMetrics on the next reconcile tick so /metrics surfaces
        // it under the `source="engine"` label.
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
                .contains("ts_illegal_transitions_total{source=\"engine\"} 1")
            {
                saw_one = true;
                break;
            }
        }

        handle.shutdown();
        let _ = task.await.unwrap();

        let body = metrics.encode_prometheus();
        assert!(
            saw_one,
            "expected ts_illegal_transitions_total{{source=\"engine\"}} 1, got:\n{body}"
        );
        // Without a stream-counter wired, the stream series must stay
        // pegged at zero — proving the engine bump didn't leak across
        // labels.
        assert!(
            body.contains("ts_illegal_transitions_total{source=\"stream\"} 0"),
            "stream series should stay at 0, got:\n{body}"
        );
    }

    #[tokio::test]
    async fn stream_illegal_counter_is_published_under_stream_label() {
        // A fake stream counter stands in for UserDataStreamClient's
        // Arc<AtomicU64>. Bumping it between reconcile ticks must
        // surface under the `source="stream"` label without touching
        // the engine-label series.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let stream_counter = Arc::new(AtomicU64::new(0));
        let metrics = RunnerMetrics::new();
        let (_tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .metrics(Arc::clone(&metrics))
            .stream_illegal_counter(Arc::clone(&stream_counter))
            .build();
        let task = tokio::spawn(runner.run());

        // Simulate the listener dropping four reordered frames. The
        // counter is monotone so the runner publishes whatever it sees.
        stream_counter.store(4, Ordering::Relaxed);

        let mut saw_stream = false;
        for _ in 0..50 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            if metrics
                .encode_prometheus()
                .contains("ts_illegal_transitions_total{source=\"stream\"} 4")
            {
                saw_stream = true;
                break;
            }
        }

        handle.shutdown();
        let _ = task.await.unwrap();

        let body = metrics.encode_prometheus();
        assert!(
            saw_stream,
            "expected ts_illegal_transitions_total{{source=\"stream\"}} 4, got:\n{body}"
        );
        // Engine label stays at zero — no stale REST acks in this test.
        assert!(
            body.contains("ts_illegal_transitions_total{source=\"engine\"} 0"),
            "engine series should stay at 0, got:\n{body}"
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
    async fn kill_switch_trip_cancels_open_quotes_via_reconcile_sweep() {
        use ts_risk::{KillSwitch, TripReason};
        // Phase: when the kill switch trips, the live runner must not
        // wait until process exit to cancel open quotes. Seed the
        // maker with two NEW acks so both quotes land on the venue,
        // then trip the switch and queue two Canceled acks for the
        // sweep. The reconcile tick drains the sweep's HTTP calls;
        // orders_canceled must reflect them. Without the sweep,
        // QueuedApi's pop() wouldn't be called for cancels during the
        // run and orders_canceled would stay at 0.
        let api = QueuedApi::new(vec![
            Ok(ack("bid-1", "NEW")),
            Ok(ack("ask-1", "NEW")),
            Ok(ack("bid-1", "CANCELED")),
            Ok(ack("ask-1", "CANCELED")),
        ]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 32);

        let ks = Arc::new(KillSwitch::default());
        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .kill_switch(Arc::clone(&ks))
            .build();
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(10_000, 10_010, 1)).await.unwrap();

        // Wait for both NEW acks to drain.
        for _ in 0..40 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        ks.trip(TripReason::Manual);

        // Wait for the reconcile-driven trip sweep to fire and both
        // cancel acks to drain.
        for _ in 0..80 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let summary = task.await.unwrap();

        // BinanceLiveEngine emits an optimistic Canceled synchronously
        // AND an async Canceled via reconcile, so each real cancel
        // surfaces as two Canceled reports to the runner. The trip
        // sweep cancels both quotes → 4 reports. The floor >= 2 would
        // pass even without the sweep; the ceiling check proves the
        // sweep didn't duplicate itself. orders_submitted stays at 2
        // because that counter increments once per risk-passed Place.
        assert_eq!(summary.orders_submitted, 2);
        assert_eq!(
            summary.orders_canceled, 4,
            "trip sweep must cancel both open quotes; summary={summary:?}"
        );
    }

    /// Strategy that submits exactly one order on its first
    /// `on_book_update` and then does nothing — crucially, its
    /// `on_shutdown` is empty, simulating the class of strategy bug
    /// where the shutdown path forgets to return live cids. The
    /// runner's trip sweep must catch this via `engine.open_cids()`
    /// rather than trusting the strategy's output alone.
    struct ForgetfulCanceler {
        submitted: bool,
    }
    impl Strategy for ForgetfulCanceler {
        fn on_book_update(&mut self, now: Timestamp, _book: &OrderBook) -> Vec<StrategyAction> {
            if self.submitted {
                return Vec::new();
            }
            self.submitted = true;
            vec![StrategyAction::Place(NewOrder {
                cid: ClientOrderId::new("forgotten-1"),
                venue: venue(),
                symbol: sym(),
                side: Side::Buy,
                kind: OrderKind::Limit,
                tif: TimeInForce::Gtc,
                qty: Qty(1_000),
                price: Some(Price(9_900)),
                ts: now,
            })]
        }
        fn on_fill(&mut self, _fill: &Fill) {}
        // Deliberately empty: the whole point of the test is that the
        // runner's defense-in-depth cancels the live cid anyway.
        fn on_shutdown(&mut self) -> Vec<StrategyAction> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn trip_sweep_cancels_engine_tracked_cids_the_strategy_forgot() {
        use ts_risk::{KillSwitch, TripReason};
        // A strategy bug leaves a live cid on the venue with nothing
        // in `on_shutdown` to cancel it. The runner must still flatten
        // it when the kill switch trips: iterating `engine.open_cids()`
        // picks up "forgotten-1", cancels it, and bumps the orphan
        // counter so operators can see the strategy failed its
        // contract.
        let api = QueuedApi::new(vec![
            Ok(ack("forgotten-1", "NEW")),
            Ok(ack("forgotten-1", "CANCELED")),
        ]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 16);

        let ks = Arc::new(KillSwitch::default());
        let strategy: Box<dyn Strategy> = Box::new(ForgetfulCanceler { submitted: false });
        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, strategy, rx)
            .reconcile_interval(Duration::from_millis(10))
            .kill_switch(Arc::clone(&ks))
            .build();
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(10_000, 10_010, 1)).await.unwrap();

        // Let the NEW ack drain so the engine's symbol_by_cid has
        // "forgotten-1" registered as live.
        for _ in 0..40 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        ks.trip(TripReason::Manual);

        // Wait for the reconcile-driven trip sweep to fire. The
        // strategy returns nothing from on_shutdown, so the only path
        // that can consume the queued CANCELED ack is the orphan
        // sweep through engine.open_cids().
        for _ in 0..80 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let summary = task.await.unwrap();

        assert_eq!(
            summary.trip_sweep_orphan_cancels, 1,
            "orphan sweep must cancel the engine-tracked cid the strategy forgot; summary={summary:?}"
        );
        // BinanceLiveEngine's cancel returns an optimistic Canceled
        // synchronously plus an async one via reconcile — 2 reports
        // per real cancel, matching the pattern in the earlier
        // trip-sweep test.
        assert_eq!(summary.orders_canceled, 2);
        assert_eq!(summary.orders_submitted, 1);
        assert_eq!(summary.reconcile_errors, 0);
    }

    #[tokio::test]
    async fn trip_sweep_fires_once_per_halted_epoch() {
        use ts_risk::{KillSwitch, TripReason};
        // After the initial sweep consumes two cancel acks, any extra
        // reconcile ticks must not re-enter on_shutdown — the queue
        // only holds two cancel responses, so a second attempt would
        // exhaust the queue and panic on pop(). Verifying the panic
        // does NOT fire proves the edge-triggered semantics hold.
        let api = QueuedApi::new(vec![
            Ok(ack("bid-1", "NEW")),
            Ok(ack("ask-1", "NEW")),
            Ok(ack("bid-1", "CANCELED")),
            Ok(ack("ask-1", "CANCELED")),
        ]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 32);

        let ks = Arc::new(KillSwitch::default());
        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .kill_switch(Arc::clone(&ks))
            .build();
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(10_000, 10_010, 1)).await.unwrap();
        for _ in 0..40 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        ks.trip(TripReason::Manual);
        // Many reconcile ticks while halted. If the sweep re-fired
        // on every tick, the strategy would emit no cancels (tracked
        // cids already cleared) so the queue still wouldn't exhaust,
        // but the runner would ALSO skip the strategy tick while
        // halted — meaning no new Place submissions, so the queue's
        // four entries must remain at four consumed total.
        for _ in 0..120 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            tx.send(snapshot(10_001, 10_011, 2)).await.ok();
        }

        handle.shutdown();
        let summary = task.await.unwrap();

        // Two initial submits (risk-passed, counted once) and two real
        // cancels from the sweep. Each cancel doubles in the
        // Canceled-report counter (optimistic + reconcile), so 4 is
        // the exact ceiling: a re-entered sweep would cancel-reject
        // the already-dead cids and push orders_rejected up,
        // something this assertion catches too.
        assert_eq!(summary.orders_submitted, 2);
        assert_eq!(summary.orders_canceled, 4);
        assert_eq!(summary.orders_rejected, 0);
        assert_eq!(summary.reconcile_errors, 0);
    }

    #[tokio::test]
    async fn halted_runner_skips_strategy_tick() {
        use ts_risk::{KillSwitch, TripReason};
        // Trip the switch BEFORE any market event. An empty QueuedApi
        // would panic on pop() if the strategy tick produced any
        // submit, so surviving the test with zero calls is the
        // assertion that the strategy was not ticked while halted.
        // The trip sweep itself is a no-op — the strategy has no
        // tracked cids.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let ks = Arc::new(KillSwitch::default());
        ks.trip(TripReason::Manual);

        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .kill_switch(Arc::clone(&ks))
            .build();
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(10_000, 10_010, 1)).await.unwrap();
        for _ in 0..40 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let summary = task.await.unwrap();

        assert_eq!(summary.orders_submitted, 0);
        assert_eq!(summary.orders_canceled, 0);
        assert_eq!(summary.events_ingested, 1);
        assert_eq!(summary.book_updates, 1);
    }

    /// Strategy that emits one Cancel the first time a trade lands.
    /// Book updates are a no-op so we can observe the trade-driven
    /// branch in isolation. Cancel of an unknown cid surfaces as a
    /// Rejected report synchronously, letting the test assert the
    /// fanout without any HTTP traffic.
    struct TradeReactiveCanceler {
        fired: bool,
    }
    impl Strategy for TradeReactiveCanceler {
        fn on_book_update(&mut self, _now: Timestamp, _book: &OrderBook) -> Vec<StrategyAction> {
            Vec::new()
        }
        fn on_trade(&mut self, _now: Timestamp, _trade: &ts_core::Trade) -> Vec<StrategyAction> {
            if self.fired {
                return Vec::new();
            }
            self.fired = true;
            vec![StrategyAction::Cancel(ClientOrderId::new("no-such-order"))]
        }
        fn on_fill(&mut self, _fill: &Fill) {}
    }

    #[tokio::test]
    async fn trade_payload_reaches_strategy_on_trade_and_actions_dispatch() {
        // An empty QueuedApi proves the cancel path doesn't hit HTTP:
        // BinanceLiveEngine::cancel short-circuits unknown cids to a
        // synchronous Rejected report. If the trade payload weren't
        // routed to on_trade, orders_rejected would stay at 0.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let strategy: Box<dyn Strategy> = Box::new(TradeReactiveCanceler { fired: false });
        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, strategy, rx)
            .reconcile_interval(Duration::from_millis(10))
            .build();
        let task = tokio::spawn(runner.run());

        tx.send(MarketEvent {
            venue: venue(),
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq: 1,
            payload: MarketPayload::Trade(ts_core::Trade {
                id: "t1".into(),
                price: Price(10_000),
                qty: Qty(1_000),
                taker_side: Side::Buy,
            }),
        })
        .await
        .unwrap();

        for _ in 0..30 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let summary = task.await.unwrap();

        assert_eq!(summary.events_ingested, 1);
        assert_eq!(summary.book_updates, 0, "trade must not bump book counter");
        assert_eq!(
            summary.orders_rejected, 1,
            "Cancel of an unknown cid from on_trade must surface as Rejected"
        );
        // No transport errors — the unknown-cid short-circuit never
        // touches QueuedApi, so pop() is never called.
        assert_eq!(summary.reconcile_errors, 0);
    }

    /// Strategy that emits exactly one Cancel from `on_timer` — used
    /// to prove the live runner's timer branch routes actions through
    /// `dispatch_actions`. The target cid is unknown to the engine so
    /// `BinanceLiveEngine::cancel` short-circuits synchronously with
    /// a Rejected report and never hits `QueuedApi::pop`.
    struct TimerCancelOnce {
        fired: bool,
    }
    impl Strategy for TimerCancelOnce {
        fn on_book_update(&mut self, _now: Timestamp, _book: &OrderBook) -> Vec<StrategyAction> {
            Vec::new()
        }
        fn on_fill(&mut self, _fill: &Fill) {}
        fn on_timer(&mut self, _now: Timestamp) -> Vec<StrategyAction> {
            if self.fired {
                return Vec::new();
            }
            self.fired = true;
            vec![StrategyAction::Cancel(ClientOrderId::new("no-such-order"))]
        }
    }

    #[tokio::test]
    async fn timer_tick_invokes_on_timer_and_dispatches_actions() {
        // Empty QueuedApi: a single submit would panic on pop(), and
        // a normal cancel RPC would too. The only way orders_rejected
        // can reach 1 without any market events is the runner's
        // timer branch calling on_timer → dispatch_actions → engine.cancel,
        // which synthesises a Rejected for the unknown cid synchronously.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let strategy: Box<dyn Strategy> = Box::new(TimerCancelOnce { fired: false });
        let (_tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, strategy, rx)
            .reconcile_interval(Duration::from_millis(50))
            .timer_interval(Duration::from_millis(20))
            .build();
        let task = tokio::spawn(runner.run());

        for _ in 0..40 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let summary = task.await.unwrap();
        assert_eq!(summary.events_ingested, 0, "no market events should flow");
        assert!(
            summary.orders_rejected >= 1,
            "timer-driven cancel must surface Rejected; got {:?}",
            summary
        );
        assert_eq!(summary.reconcile_errors, 0);
    }

    #[tokio::test]
    async fn halted_live_runner_skips_timer_ticks() {
        use ts_risk::{KillSwitch, TripReason};
        // An empty QueuedApi guarantees any submit call would panic
        // on pop(). Tripping the switch before any tick fires and then
        // advancing time must yield orders_rejected == 0 — proof
        // `handle_timer_tick` short-circuits before touching
        // `dispatch_actions`.
        let api = QueuedApi::new(vec![]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let ks = Arc::new(KillSwitch::default());
        ks.trip(TripReason::Manual);

        let strategy: Box<dyn Strategy> = Box::new(TimerCancelOnce { fired: false });
        let (_tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, strategy, rx)
            .reconcile_interval(Duration::from_millis(50))
            .timer_interval(Duration::from_millis(20))
            .kill_switch(Arc::clone(&ks))
            .build();
        let task = tokio::spawn(runner.run());

        for _ in 0..40 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let summary = task.await.unwrap();
        assert_eq!(summary.orders_rejected, 0);
        assert_eq!(summary.orders_submitted, 0);
        assert_eq!(summary.reconcile_errors, 0);
    }

    #[tokio::test]
    async fn intent_log_records_submit_then_complete_on_terminal_report() {
        use crate::intent_log::{replay_open_intents, IntentLogWriter};
        use std::sync::atomic::{AtomicU64, Ordering as AOrdering};

        // Unique path per test run so parallel-test interleavings don't
        // step on each other.
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ts-live-intent-{}-{}.ndjson",
            std::process::id(),
            N.fetch_add(1, AOrdering::Relaxed)
        ));

        // Writer lives inside the runner. Seed the queued API with two
        // NEW acks + two FILLED async acks so each quote reaches a
        // terminal status via the reconcile path.
        let api = QueuedApi::new(vec![Ok(ack("bid-1", "FILLED")), Ok(ack("ask-1", "FILLED"))]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 32);

        let wal = IntentLogWriter::open(&path).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .intent_log(wal)
            .build();
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(10_000, 10_010, 1)).await.unwrap();

        for _ in 0..80 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let _ = task.await.unwrap();

        // Replay the log — both intents were submitted and terminally
        // filled, so the open set must be empty. Without the WAL's
        // completion tombstone path firing in `observe_report`, the two
        // submits would still be dangling.
        let open = replay_open_intents(&path).await.unwrap();
        assert!(
            open.is_empty(),
            "terminal fills must clear the WAL; orphans={open:?}"
        );

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn intent_log_preserves_open_submits_before_terminal_status() {
        use crate::intent_log::{replay_open_intents, IntentLogWriter};
        use std::sync::atomic::{AtomicU64, Ordering as AOrdering};

        // Two NEW acks mean both submits are live but neither has
        // reached a terminal status. Simulate a hard crash by
        // aborting the task — this skips the graceful shutdown
        // cancel-sweep that would otherwise write completion
        // tombstones — and prove the WAL's submits are on durable
        // storage.
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ts-live-intent-orph-{}-{}.ndjson",
            std::process::id(),
            N.fetch_add(1, AOrdering::Relaxed)
        ));

        let api = QueuedApi::new(vec![Ok(ack("bid-1", "NEW")), Ok(ack("ask-1", "NEW"))]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 32);

        let wal = IntentLogWriter::open(&path).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        let (runner, _handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .intent_log(wal)
            .build();
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(10_000, 10_010, 1)).await.unwrap();

        for _ in 0..60 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Crash: drop the task without letting `on_shutdown` run.
        // The writer's file handle goes away with it, but every
        // submit already fsynced before returning to the runner.
        task.abort();
        let _ = task.await;

        let open = replay_open_intents(&path).await.unwrap();
        assert_eq!(
            open.len(),
            2,
            "two NEW submits should both surface as open intents after a crash"
        );
        let _ = tokio::fs::remove_file(&path).await;
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

    #[tokio::test]
    async fn staleness_guard_trips_kill_switch_on_silent_feed() {
        use ts_risk::{KillSwitch, StalenessGuard, StalenessGuardConfig, TripReason};
        // Configure a 30 ms max_idle. After a single baseline event
        // the feed stays silent; several reconcile ticks later, the
        // gap exceeds 30 ms and the guard trips the kill switch.
        let api = QueuedApi::new(vec![
            Ok(ack("bid-1", "NEW")),
            Ok(ack("ask-1", "NEW")),
            Ok(ack("bid-2", "CANCELED")),
            Ok(ack("ask-2", "CANCELED")),
        ]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let ks = Arc::new(KillSwitch::default());
        let guard = StalenessGuard::new(StalenessGuardConfig {
            max_idle: Some(Duration::from_millis(30)),
        });

        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .kill_switch(Arc::clone(&ks))
            .staleness_guard(guard)
            .build();
        let task = tokio::spawn(runner.run());

        // One event seeds the guard's baseline.
        tx.send(snapshot(10_000, 10_010, 1)).await.unwrap();

        // Wait long enough for several reconcile ticks past the idle
        // threshold. 150 ms > 30 ms limit by a healthy margin.
        for _ in 0..30 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let _ = task.await.unwrap();

        assert!(ks.tripped(), "silent feed past max_idle must trip");
        assert_eq!(ks.reason(), Some(TripReason::FeedStaleness));
    }

    #[tokio::test]
    async fn clock_skew_guard_trips_on_event_with_large_skew() {
        use ts_risk::{ClockSkewGuard, ClockSkewGuardConfig, KillSwitch, TripReason};
        let api = QueuedApi::new(vec![Ok(ack("bid-1", "NEW")), Ok(ack("ask-1", "NEW"))]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let ks = Arc::new(KillSwitch::default());
        // 1 ms limit; inject an event with 500 ms skew.
        let guard = ClockSkewGuard::new(ClockSkewGuardConfig {
            max_abs_skew: Some(Duration::from_millis(1)),
        });

        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .kill_switch(Arc::clone(&ks))
            .clock_skew_guard(guard)
            .build();
        let task = tokio::spawn(runner.run());

        let skewed = MarketEvent {
            venue: venue(),
            symbol: sym(),
            exchange_ts: Timestamp(1_000_000_000),
            local_ts: Timestamp(1_500_000_000),
            seq: 1,
            payload: MarketPayload::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(10_000),
                    qty: Qty(1_000_000),
                }],
                asks: vec![BookLevel {
                    price: Price(10_010),
                    qty: Qty(1_000_000),
                }],
            }),
        };
        tx.send(skewed).await.unwrap();

        for _ in 0..10 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let _ = task.await.unwrap();

        assert!(ks.tripped(), "large event skew must trip the switch");
        assert_eq!(ks.reason(), Some(TripReason::ClockSkew));
    }

    #[tokio::test]
    async fn clock_skew_guard_does_not_trip_on_within_limit_events() {
        use ts_risk::{ClockSkewGuard, ClockSkewGuardConfig, KillSwitch};
        let api = QueuedApi::new(vec![Ok(ack("bid-1", "NEW")), Ok(ack("ask-1", "NEW"))]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 8);

        let ks = Arc::new(KillSwitch::default());
        let guard = ClockSkewGuard::new(ClockSkewGuardConfig {
            max_abs_skew: Some(Duration::from_secs(1)),
        });

        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .kill_switch(Arc::clone(&ks))
            .clock_skew_guard(guard)
            .build();
        let task = tokio::spawn(runner.run());

        // 10 ms skew, well within 1s limit.
        let ok_event = MarketEvent {
            venue: venue(),
            symbol: sym(),
            exchange_ts: Timestamp(1_000_000_000),
            local_ts: Timestamp(1_010_000_000),
            seq: 1,
            payload: MarketPayload::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(10_000),
                    qty: Qty(1_000_000),
                }],
                asks: vec![BookLevel {
                    price: Price(10_010),
                    qty: Qty(1_000_000),
                }],
            }),
        };
        tx.send(ok_event).await.unwrap();

        for _ in 0..10 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.shutdown();
        let _ = task.await.unwrap();

        assert!(
            !ks.tripped(),
            "event skew under limit must not trip the switch"
        );
    }

    #[tokio::test]
    async fn staleness_guard_does_not_trip_when_feed_stays_alive() {
        use ts_risk::{KillSwitch, StalenessGuard, StalenessGuardConfig};
        // Keep the feed ticking every few ms — well under max_idle.
        // The guard should never fire.
        let api = QueuedApi::new(vec![
            Ok(ack("bid-1", "NEW")),
            Ok(ack("ask-1", "NEW")),
            Ok(ack("bid-2", "CANCELED")),
            Ok(ack("ask-2", "CANCELED")),
            Ok(ack("bid-3", "NEW")),
            Ok(ack("ask-3", "NEW")),
            Ok(ack("bid-4", "CANCELED")),
            Ok(ack("ask-4", "CANCELED")),
        ]);
        let engine = BinanceLiveEngine::new(Arc::clone(&api), specs(), venue(), 32);

        let ks = Arc::new(KillSwitch::default());
        let guard = StalenessGuard::new(StalenessGuardConfig {
            max_idle: Some(Duration::from_millis(500)),
        });

        let (tx, rx) = mpsc::channel(8);
        let (runner, handle) = LiveRunner::builder(engine, maker(), rx)
            .reconcile_interval(Duration::from_millis(10))
            .kill_switch(Arc::clone(&ks))
            .staleness_guard(guard)
            .build();
        let task = tokio::spawn(runner.run());

        for seq in 1..5u64 {
            tx.send(snapshot(10_000 + seq as i64, 10_010 + seq as i64, seq))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
        }

        handle.shutdown();
        let _ = task.await.unwrap();

        assert!(
            !ks.tripped(),
            "feed under max_idle must never trip the switch"
        );
    }
}
