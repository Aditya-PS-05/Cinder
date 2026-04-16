//! Async orchestration for [`ts_replay::Replay`].
//!
//! [`EngineRunner`] owns a [`Replay`] and pumps [`MarketEvent`]s from a
//! `tokio::sync::mpsc` receiver through it until the channel closes or
//! the returned [`RunnerHandle`] asks for shutdown. An optional periodic
//! tap broadcasts [`ReplaySummary`]s so dashboards or log sinks can
//! read live metrics without mutating the runner.
//!
//! The runner is deliberately ignorant of the event source. A companion
//! [`bridge_bus`] helper drains a `ts_core::bus::Subscription<MarketEvent>`
//! (the shape the Binance connector publishes on) into a tokio channel
//! so the blocking bus can feed an async engine. Other sources — file
//! replays, test fixtures — just push onto the same mpsc.

#![forbid(unsafe_code)]

pub mod audit;
pub mod paper_cfg;
pub mod tape;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, Notify};
use tokio::task::{spawn_blocking, JoinHandle};
use tracing::{debug, warn};

use ts_core::{bus::Subscription, MarketEvent};
use ts_replay::{Replay, ReplaySummary};
use ts_strategy::Strategy;

use crate::audit::AuditEvent;

pub struct EngineRunner<S: Strategy> {
    replay: Replay<S>,
    rx: mpsc::Receiver<MarketEvent>,
    shutdown: Arc<Notify>,
    summary_tx: Option<broadcast::Sender<ReplaySummary>>,
    summary_interval: Duration,
    audit_tx: Option<mpsc::Sender<AuditEvent>>,
}

/// Remote control for an [`EngineRunner`]. Dropping the handle does not
/// stop the runner — call [`Self::shutdown`] explicitly so the caller
/// observes a deterministic lifecycle.
#[derive(Clone)]
pub struct RunnerHandle {
    shutdown: Arc<Notify>,
    summary_tx: Option<broadcast::Sender<ReplaySummary>>,
}

impl RunnerHandle {
    /// Ask the runner to finish processing the current event and return.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Subscribe to the periodic summary stream. Returns `None` if the
    /// runner was not built with a summary interval.
    pub fn subscribe_summaries(&self) -> Option<broadcast::Receiver<ReplaySummary>> {
        self.summary_tx.as_ref().map(|tx| tx.subscribe())
    }
}

impl<S: Strategy> EngineRunner<S> {
    /// Build a runner with no periodic summary tap.
    pub fn new(replay: Replay<S>, rx: mpsc::Receiver<MarketEvent>) -> (Self, RunnerHandle) {
        Self::with_summary_tap(replay, rx, Duration::from_secs(0), 0)
    }

    /// Build a runner that broadcasts a summary every `interval`. A zero
    /// duration disables the tap (equivalent to [`Self::new`]). `capacity`
    /// is the broadcast channel backlog — slow subscribers drop stale
    /// summaries rather than blocking the runner.
    pub fn with_summary_tap(
        replay: Replay<S>,
        rx: mpsc::Receiver<MarketEvent>,
        interval: Duration,
        capacity: usize,
    ) -> (Self, RunnerHandle) {
        let shutdown = Arc::new(Notify::new());
        let summary_tx = if interval.is_zero() || capacity == 0 {
            None
        } else {
            Some(broadcast::channel(capacity).0)
        };
        let handle = RunnerHandle {
            shutdown: shutdown.clone(),
            summary_tx: summary_tx.clone(),
        };
        let runner = Self {
            replay,
            rx,
            shutdown,
            summary_tx,
            summary_interval: interval,
            audit_tx: None,
        };
        (runner, handle)
    }

    /// Attach a channel that receives every [`ExecReport`] and [`Fill`]
    /// emitted by the engine. The runner calls `send().await` so capture
    /// is lossless under backpressure — size the channel to absorb your
    /// longest expected writer stall. Pair with
    /// [`audit::spawn_audit_writer`] to persist to disk.
    ///
    /// [`ExecReport`]: ts_core::ExecReport
    /// [`Fill`]: ts_core::Fill
    pub fn with_audit(mut self, audit_tx: mpsc::Sender<AuditEvent>) -> Self {
        self.audit_tx = Some(audit_tx);
        self
    }

    /// Drive the replay loop until the event channel closes or the
    /// handle signals shutdown. Returns the final summary.
    pub async fn run(mut self) -> ReplaySummary {
        let mut interval_ticker = if self.summary_interval.is_zero() {
            None
        } else {
            let mut i = tokio::time::interval(self.summary_interval);
            i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Burn the immediate-fire tick so the first summary lands
            // one interval into the run.
            i.tick().await;
            Some(i)
        };

        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.notified() => {
                    debug!("runner: shutdown requested");
                    break;
                }
                maybe_event = self.rx.recv() => {
                    let Some(event) = maybe_event else {
                        debug!("runner: event channel closed");
                        break;
                    };
                    match self.replay.step(&event) {
                        Ok(step) => {
                            if let Some(tx) = self.audit_tx.as_ref() {
                                for r in &step.reports {
                                    if tx.send(AuditEvent::Report(r.clone())).await.is_err() {
                                        debug!("runner: audit sink closed, detaching");
                                        self.audit_tx = None;
                                        break;
                                    }
                                }
                                if let Some(tx) = self.audit_tx.as_ref() {
                                    for f in &step.fills {
                                        if tx.send(AuditEvent::Fill(f.clone())).await.is_err() {
                                            debug!("runner: audit sink closed, detaching");
                                            self.audit_tx = None;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            warn!(error = ?err, "runner: book error, dropping event");
                        }
                    }
                }
                _ = async {
                    match interval_ticker.as_mut() {
                        Some(i) => { i.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    if let Some(tx) = self.summary_tx.as_ref() {
                        let _ = tx.send(self.replay.summary());
                    }
                }
            }
        }
        self.replay.summary()
    }
}

/// Spawn a blocking task that drains a [`ts_core::bus::Subscription`]
/// onto a tokio `mpsc::Sender`. Returns the tokio `JoinHandle` so the
/// caller can await shutdown. The bridge exits when the subscription
/// closes (its `iter()` completes) or the receiver is dropped.
pub fn bridge_bus(sub: Subscription<MarketEvent>, tx: mpsc::Sender<MarketEvent>) -> JoinHandle<()> {
    spawn_blocking(move || {
        for event in sub.iter() {
            if tx.blocking_send(event).is_err() {
                // Receiver is gone; stop draining.
                break;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use ts_core::{
        bus::Bus, BookLevel, BookSnapshot, MarketPayload, Price, Qty, Symbol, Timestamp, Venue,
    };
    use ts_oms::{EngineConfig, PaperEngine, RiskConfig};
    use ts_strategy::{InventorySkewMaker, MakerConfig};

    fn venue() -> Venue {
        Venue::BINANCE
    }
    fn sym() -> Symbol {
        Symbol::from_static("BTCUSDT")
    }

    fn build_replay() -> Replay<InventorySkewMaker> {
        let engine = PaperEngine::new(
            EngineConfig {
                venue: venue(),
                symbol: sym(),
                notional_fallback_price: Some(Price(1)),
            },
            RiskConfig::permissive(),
            InventorySkewMaker::new(MakerConfig {
                venue: venue(),
                symbol: sym(),
                quote_qty: Qty(2),
                half_spread_ticks: 5,
                inventory_skew_ticks: 0,
                max_inventory: 20,
                cid_prefix: "r".into(),
            }),
        );
        Replay::new(engine)
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

    #[tokio::test]
    async fn runs_to_channel_close_and_returns_summary() {
        let (tx, rx) = mpsc::channel(16);
        let (runner, _handle) = EngineRunner::new(build_replay(), rx);
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(100, 110, 1)).await.unwrap();
        tx.send(snapshot(101, 111, 2)).await.unwrap();
        drop(tx); // Closing the channel shuts the runner down.

        let summary = task.await.unwrap();
        assert_eq!(summary.metrics.events_ingested, 2);
        assert_eq!(summary.metrics.book_updates, 2);
    }

    #[tokio::test]
    async fn shutdown_handle_stops_runner() {
        let (tx, rx) = mpsc::channel(16);
        let (runner, handle) = EngineRunner::new(build_replay(), rx);
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(100, 110, 1)).await.unwrap();
        // Give the runner a chance to process the event.
        tokio::task::yield_now().await;
        handle.shutdown();

        let summary = task.await.unwrap();
        assert!(summary.metrics.events_ingested >= 1);
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_summary_tap_broadcasts_state() {
        let (tx, rx) = mpsc::channel(16);
        let (runner, handle) =
            EngineRunner::with_summary_tap(build_replay(), rx, Duration::from_millis(50), 8);
        let mut summaries = handle.subscribe_summaries().unwrap();
        let task = tokio::spawn(runner.run());

        tx.send(snapshot(100, 110, 1)).await.unwrap();
        // Advance past one interval so the tap fires at least once.
        tokio::time::sleep(Duration::from_millis(75)).await;

        let got = summaries.recv().await.expect("tap should fire");
        assert!(got.metrics.events_ingested >= 1);

        handle.shutdown();
        let _ = task.await.unwrap();
    }

    #[tokio::test]
    async fn bridge_bus_forwards_events_to_runner() {
        let bus: Arc<Bus<MarketEvent>> = Bus::new();
        let sub = bus.subscribe(64);
        let (tx, rx) = mpsc::channel(64);
        let bridge = bridge_bus(sub, tx);

        let (runner, handle) = EngineRunner::new(build_replay(), rx);
        let task = tokio::spawn(runner.run());

        // Publish on the sync bus from a blocking task so the bridge
        // drains it onto the tokio channel.
        let bus_for_pub = bus.clone();
        tokio::task::spawn_blocking(move || {
            bus_for_pub.publish(snapshot(100, 110, 1));
            bus_for_pub.publish(snapshot(101, 111, 2));
        })
        .await
        .unwrap();

        // Let the bridge + runner drain.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        bus.close();
        let _ = bridge.await;
        handle.shutdown();

        let summary = task.await.unwrap();
        assert!(summary.metrics.events_ingested >= 2);
    }

    #[tokio::test]
    async fn audit_tap_forwards_reports_and_fills() {
        use crate::audit::AuditEvent;
        let (tx, rx) = mpsc::channel(16);
        let (audit_tx, mut audit_rx) = mpsc::channel::<AuditEvent>(64);
        let (runner, handle) = EngineRunner::new(build_replay(), rx);
        let runner = runner.with_audit(audit_tx);
        let task = tokio::spawn(runner.run());

        // A snapshot drives the maker to place quotes, which produces
        // at least one OrderStatus::New report (no fills, since paper
        // fills require opposing flow).
        tx.send(snapshot(100, 110, 1)).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        handle.shutdown();
        let _summary = task.await.unwrap();

        let mut reports = 0;
        while let Ok(evt) = audit_rx.try_recv() {
            if matches!(evt, AuditEvent::Report(_)) {
                reports += 1;
            }
        }
        assert!(reports > 0, "expected at least one report on the audit tap");
    }

    #[tokio::test]
    async fn audit_tap_detaches_when_receiver_drops() {
        use crate::audit::AuditEvent;
        let (tx, rx) = mpsc::channel(16);
        let (audit_tx, audit_rx) = mpsc::channel::<AuditEvent>(1);
        drop(audit_rx); // sink closed before any event flows.
        let (runner, handle) = EngineRunner::new(build_replay(), rx);
        let runner = runner.with_audit(audit_tx);
        let task = tokio::spawn(runner.run());

        // Runner must keep going even with a dead audit sink.
        tx.send(snapshot(100, 110, 1)).await.unwrap();
        tx.send(snapshot(101, 111, 2)).await.unwrap();
        tokio::task::yield_now().await;
        handle.shutdown();

        let summary = task.await.unwrap();
        assert!(summary.metrics.events_ingested >= 2);
    }

    #[tokio::test]
    async fn book_error_is_logged_and_swallowed() {
        use ts_core::BookDelta;
        let (tx, rx) = mpsc::channel(16);
        let (runner, handle) = EngineRunner::new(build_replay(), rx);
        let task = tokio::spawn(runner.run());

        // Send a delta before any snapshot — engine returns BookError::Uninitialized.
        tx.send(MarketEvent {
            venue: venue(),
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq: 1,
            payload: MarketPayload::BookDelta(BookDelta {
                bids: vec![BookLevel {
                    price: Price(100),
                    qty: Qty(1),
                }],
                asks: vec![],
                prev_seq: 0,
            }),
        })
        .await
        .unwrap();
        // Then a valid snapshot to prove the runner survived.
        tx.send(snapshot(100, 110, 2)).await.unwrap();
        tokio::task::yield_now().await;

        handle.shutdown();
        let summary = task.await.unwrap();
        assert_eq!(summary.metrics.events_ingested, 2);
    }
}
