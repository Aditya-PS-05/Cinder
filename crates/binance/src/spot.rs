//! Binance spot WebSocket client with reconnect loop and depth resync.
//!
//! [`SpotStreamClient::run`] is the entry point: it opens a WebSocket to
//! `wss://stream.binance.com:9443/ws`, subscribes each symbol to
//! `<sym>@trade` + `<sym>@depth@100ms`, fires a `/api/v3/depth` REST
//! snapshot per symbol concurrently, and aligns the two into a single
//! bus stream. On any transport or alignment error the session ends,
//! the reconnect loop backs off, and a fresh snapshot is pulled.
//!
//! The hot path interleaves two futures with `tokio::select!`:
//!
//! * `ws.next()` — frames are decoded by [`decode_frame`]. `BookDelta`
//!   events are routed through [`SymbolResync`]; everything else
//!   (trades, etc.) publishes straight to the bus.
//! * `snapshot_fetches.next()` — a [`FuturesUnordered`] of REST depth
//!   fetches. When one lands, it is installed via
//!   [`SymbolResync::apply_snapshot`] and the snapshot event plus every
//!   surviving buffered delta is published in order.
//!
//! Slow bus consumers drop messages, never the producer. Alignment
//! gaps return [`BinanceError::Align`] so the outer loop reconnects
//! and starts from a fresh snapshot.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use futures::SinkExt;
use serde_json::json;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use ts_core::{bus::Bus, InstrumentSpec, MarketEvent, MarketPayload, Symbol};

use crate::decode::decode_frame;
use crate::error::BinanceError;
use crate::rest::{fetch_depth_snapshot, DepthSnapshot};
use crate::resync::{PushOutcome, SymbolResync};

/// Depth snapshot limit; Binance supports up to 5000.
const DEPTH_SNAPSHOT_LIMIT: u32 = 1000;
/// Per-request REST timeout for depth snapshot fetches.
const REST_TIMEOUT: Duration = Duration::from_secs(10);

/// Runtime configuration for a single spot stream session.
#[derive(Clone, Debug)]
pub struct SpotStreamConfig {
    /// Upstream WebSocket URL. Defaults to the public mainnet endpoint.
    pub ws_url: String,
    /// REST base URL used to pull `/api/v3/depth` snapshots.
    pub rest_base: String,
    /// Uppercase venue-local symbols to subscribe to (e.g. `["BTCUSDT"]`).
    pub symbols: Vec<String>,
    /// Instrument metadata keyed by canonical [`Symbol`].
    pub specs: HashMap<Symbol, InstrumentSpec>,
    /// First reconnect delay. Doubles up to [`Self::max_backoff`] on
    /// consecutive failures.
    pub initial_backoff: Duration,
    /// Upper bound on the reconnect delay.
    pub max_backoff: Duration,
}

impl SpotStreamConfig {
    pub fn new(symbols: Vec<String>, specs: HashMap<Symbol, InstrumentSpec>) -> Self {
        Self {
            ws_url: "wss://stream.binance.com:9443/ws".to_string(),
            rest_base: "https://api.binance.com".to_string(),
            symbols,
            specs,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
        }
    }
}

pub struct SpotStreamClient {
    cfg: SpotStreamConfig,
    bus: Arc<Bus<MarketEvent>>,
    /// Cumulative count of forced resyncs across the lifetime of this
    /// client. Bumped each time [`SymbolResync`] reports
    /// [`PushOutcome::Resync`] (a sequence-number gap that broke the
    /// chain). Persists across reconnect cycles so operators can see the
    /// long-run rate of desync events on a feed; the outer reconnect
    /// loop reuses the same counter via [`Self::resync_counter`].
    resync_counter: Arc<AtomicU64>,
}

impl SpotStreamClient {
    pub fn new(cfg: SpotStreamConfig, bus: Arc<Bus<MarketEvent>>) -> Self {
        Self {
            cfg,
            bus,
            resync_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Cumulative count of forced resyncs (sequence-number desyncs that
    /// flipped a [`SymbolResync`] into [`crate::AlignState::Lost`]) over
    /// this client's lifetime. Each desync also forces a fresh REST
    /// snapshot via the reconnect loop, so this is also the count of
    /// auto-resubscribe events.
    pub fn resync_count(&self) -> u64 {
        self.resync_counter.load(Ordering::Relaxed)
    }

    /// Shared handle to the resync counter so an external observer (e.g.
    /// a metrics endpoint or a periodic logger) can read the live value
    /// without holding a reference to the client.
    pub fn resync_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.resync_counter)
    }

    /// Run the reconnect loop forever. Returns only if the caller aborts
    /// the task or [`Bus::close`] has been called and there is nothing
    /// left to publish.
    pub async fn run(&self) {
        let mut backoff = self.cfg.initial_backoff;
        loop {
            match self.session().await {
                Ok(()) => {
                    info!("binance spot session ended cleanly, reconnecting");
                    backoff = self.cfg.initial_backoff;
                }
                Err(e) => {
                    error!(error = %e, "binance spot session failed");
                }
            }
            warn!(?backoff, "reconnecting to binance spot");
            sleep(backoff).await;
            backoff = (backoff * 2).min(self.cfg.max_backoff);
        }
    }

    async fn session(&self) -> Result<(), BinanceError> {
        info!(url = %self.cfg.ws_url, "connecting to binance spot");
        let (mut ws, _resp) = connect_async(&self.cfg.ws_url).await?;

        // Single combined subscribe request. Each symbol gets trades +
        // 100ms depth; that's enough to prove the pipeline end to end.
        let params: Vec<String> = self
            .cfg
            .symbols
            .iter()
            .flat_map(|s| {
                let lower = s.to_lowercase();
                [format!("{lower}@trade"), format!("{lower}@depth@100ms")]
            })
            .collect();
        let sub_msg = json!({
            "method": "SUBSCRIBE",
            "params": params,
            "id": 1,
        });
        ws.send(Message::Text(sub_msg.to_string())).await?;
        info!(symbols = ?self.cfg.symbols, "subscribed");

        // Per-symbol alignment state. Only symbols with a known spec get
        // a resync instance — unknown symbols are dropped by the decoder
        // anyway, so there is no book to align for them.
        let mut resyncs: HashMap<Symbol, SymbolResync> = self
            .cfg
            .specs
            .keys()
            .cloned()
            .map(|s| (s, SymbolResync::new()))
            .collect();

        // Concurrent REST snapshot fetches, one per symbol. When a
        // future resolves we install its snapshot into the matching
        // resync and publish the backlog.
        let http = reqwest::Client::builder().timeout(REST_TIMEOUT).build()?;
        let rest_base = self.cfg.rest_base.clone();
        let mut snapshot_fetches = FuturesUnordered::new();
        for (sym, spec) in self.cfg.specs.iter() {
            let http = http.clone();
            let base = rest_base.clone();
            let sym_cloned = sym.clone();
            let spec = spec.clone();
            snapshot_fetches.push(async move {
                let res =
                    fetch_depth_snapshot(&http, &base, &sym_cloned, DEPTH_SNAPSHOT_LIMIT, &spec)
                        .await;
                (sym_cloned, res)
            });
        }

        loop {
            tokio::select! {
                biased;
                maybe_frame = ws.next() => {
                    let Some(frame) = maybe_frame else { break; };
                    match frame? {
                        Message::Text(t) => self.handle_payload(t.as_bytes(), &mut resyncs)?,
                        Message::Binary(b) => self.handle_payload(&b, &mut resyncs)?,
                        Message::Ping(p) => {
                            ws.send(Message::Pong(p)).await?;
                        }
                        Message::Pong(_) => {}
                        Message::Close(frame) => {
                            info!(?frame, "binance spot closed connection");
                            break;
                        }
                        Message::Frame(_) => {}
                    }
                }
                Some((sym, snap_res)) = snapshot_fetches.next(), if !snapshot_fetches.is_empty() => {
                    let snap = snap_res?;
                    self.install_snapshot(sym, snap, &mut resyncs)?;
                }
            }
        }
        Ok(())
    }

    fn handle_payload(
        &self,
        body: &[u8],
        resyncs: &mut HashMap<Symbol, SymbolResync>,
    ) -> Result<(), BinanceError> {
        match decode_frame(body, &self.cfg.specs) {
            Ok(Some(evt)) => self.route_event(evt, resyncs),
            Ok(None) => {
                debug!("non-event frame, skipped");
                Ok(())
            }
            Err(BinanceError::Unsupported(tag)) => {
                debug!(tag, "unsupported event tag");
                Ok(())
            }
            Err(e) => {
                warn!(error = %e, "failed to decode frame");
                Ok(())
            }
        }
    }

    fn route_event(
        &self,
        evt: MarketEvent,
        resyncs: &mut HashMap<Symbol, SymbolResync>,
    ) -> Result<(), BinanceError> {
        // Non-depth events bypass alignment entirely.
        if !matches!(evt.payload, MarketPayload::BookDelta(_)) {
            self.publish(evt);
            return Ok(());
        }
        let Some(resync) = resyncs.get_mut(&evt.symbol) else {
            // No spec registered; decoder normally filters these, but be
            // defensive — drop the delta rather than publish misaligned.
            return Ok(());
        };
        let sym = evt.symbol.clone();
        let outcome = resync.push_delta(evt);
        self.dispatch_book_outcome(outcome, &sym)
    }

    /// Dispatch the resync state machine's verdict on a single delta.
    /// Extracted so the resync-counter bump and structured warn live in
    /// one place reachable from the unit tests; production callers go
    /// through [`Self::route_event`].
    fn dispatch_book_outcome(
        &self,
        outcome: PushOutcome,
        sym: &Symbol,
    ) -> Result<(), BinanceError> {
        match outcome {
            PushOutcome::Buffered => Ok(()),
            PushOutcome::Ready(out) => {
                self.publish(out);
                Ok(())
            }
            PushOutcome::Resync => {
                let count = self.resync_counter.fetch_add(1, Ordering::Relaxed) + 1;
                warn!(
                    symbol = %sym,
                    count,
                    "depth stream gapped; forcing reconnect"
                );
                Err(BinanceError::Align {
                    detail: format!(
                        "live stream gapped on {sym}; forcing reconnect (cumulative={count})"
                    ),
                })
            }
        }
    }

    fn install_snapshot(
        &self,
        sym: Symbol,
        snap: DepthSnapshot,
        resyncs: &mut HashMap<Symbol, SymbolResync>,
    ) -> Result<(), BinanceError> {
        let Some(resync) = resyncs.get_mut(&sym) else {
            return Ok(());
        };
        let snap_event = MarketEvent {
            venue: ts_core::Venue::BINANCE,
            symbol: sym.clone(),
            exchange_ts: ts_core::Timestamp::default(),
            local_ts: ts_core::Timestamp::default(),
            seq: snap.last_update_id,
            payload: MarketPayload::BookSnapshot(snap.snapshot),
        };
        let out = resync.apply_snapshot(snap_event, snap.last_update_id)?;
        info!(symbol = %sym, events = out.len(), "installed depth snapshot");
        for evt in out {
            self.publish(evt);
        }
        Ok(())
    }

    fn publish(&self, evt: MarketEvent) {
        let (delivered, dropped) = self.bus.publish(evt);
        if dropped > 0 {
            warn!(delivered, dropped, "slow consumer on bus");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{
        BookDelta, BookLevel, BookSnapshot, MarketEvent, MarketPayload, Price, Qty, Timestamp,
        Venue,
    };

    fn sym() -> Symbol {
        Symbol::from_static("BTCUSDT")
    }

    fn client() -> SpotStreamClient {
        let cfg = SpotStreamConfig::new(vec!["BTCUSDT".into()], HashMap::new());
        SpotStreamClient::new(cfg, Bus::new())
    }

    fn delta_event(prev_seq: u64, seq: u64) -> MarketEvent {
        MarketEvent {
            venue: Venue::BINANCE,
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq,
            payload: MarketPayload::BookDelta(BookDelta {
                bids: vec![BookLevel {
                    price: Price(100),
                    qty: Qty(1),
                }],
                asks: vec![],
                prev_seq,
            }),
        }
    }

    /// `Buffered` and `Ready` outcomes must never bump the resync counter
    /// — the counter is the sole signal operators use to detect a feed
    /// that's silently desynchronising, so any false positive here would
    /// cry wolf on a healthy session.
    #[test]
    fn dispatch_book_outcome_does_not_bump_counter_on_buffered_or_ready() {
        let c = client();
        c.dispatch_book_outcome(PushOutcome::Buffered, &sym())
            .unwrap();
        c.dispatch_book_outcome(PushOutcome::Ready(delta_event(0, 1)), &sym())
            .unwrap();
        assert_eq!(c.resync_count(), 0);
    }

    /// A `Resync` outcome must increment the counter once and surface a
    /// `BinanceError::Align` to the caller so the outer reconnect loop
    /// re-snapshots and resubscribes. The error string also embeds the
    /// cumulative count so it lands in operator logs alongside the
    /// structured `count` field on the warn line.
    #[test]
    fn dispatch_book_outcome_bumps_counter_and_errors_on_resync() {
        let c = client();
        let err = c
            .dispatch_book_outcome(PushOutcome::Resync, &sym())
            .unwrap_err();
        assert!(matches!(err, BinanceError::Align { .. }));
        let detail = err.to_string();
        assert!(
            detail.contains("BTCUSDT"),
            "error should name the gapped symbol, got `{detail}`"
        );
        assert!(
            detail.contains("cumulative=1"),
            "error should embed cumulative count, got `{detail}`"
        );
        assert_eq!(c.resync_count(), 1);
    }

    /// Counter is cumulative across calls; multiple gaps within the
    /// lifetime of one client should reflect in `resync_count`. Mirrors
    /// how the production loop holds onto the same `SpotStreamClient`
    /// across reconnect cycles via the same `Arc<AtomicU64>`.
    #[test]
    fn dispatch_book_outcome_counter_is_cumulative() {
        let c = client();
        for _ in 0..3 {
            let _ = c.dispatch_book_outcome(PushOutcome::Resync, &sym());
        }
        assert_eq!(c.resync_count(), 3);
        // External observers reading the shared counter see the same
        // value — important for downstream metrics endpoints that hold
        // an `Arc` rather than the client itself.
        assert_eq!(c.resync_counter().load(Ordering::Relaxed), 3);
    }

    /// End-to-end through `route_event`: a Lost-state resync emits
    /// `PushOutcome::Resync` on every push, and the routing path bumps
    /// the counter accordingly. Non-delta events bypass alignment
    /// (and therefore the counter) entirely.
    #[test]
    fn route_event_increments_counter_on_lost_state_delta() {
        let c = client();
        let mut resyncs: HashMap<Symbol, SymbolResync> = HashMap::new();
        let mut s = SymbolResync::new();
        // Drive the resync into Lost state: snapshot at 20 with a
        // post-snapshot gap (delta U=30 has no bridge).
        let _ = s.apply_snapshot(
            MarketEvent {
                venue: Venue::BINANCE,
                symbol: sym(),
                exchange_ts: Timestamp::default(),
                local_ts: Timestamp::default(),
                seq: 20,
                payload: MarketPayload::BookSnapshot(BookSnapshot {
                    bids: vec![],
                    asks: vec![],
                }),
            },
            20,
        );
        // Push a delta whose first_update_id is past the bridge window.
        let _ = s.push_delta(delta_event(29, 30));
        resyncs.insert(sym(), s);

        // Now an additional delta hits the Lost state and forces resync.
        let err = c
            .route_event(delta_event(31, 32), &mut resyncs)
            .unwrap_err();
        assert!(matches!(err, BinanceError::Align { .. }));
        assert_eq!(c.resync_count(), 1);

        // A non-delta event must not advance the counter even on a Lost
        // resync — alignment doesn't apply, so the bus publish path runs.
        let trade = MarketEvent {
            venue: Venue::BINANCE,
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq: 33,
            payload: MarketPayload::Trade(ts_core::Trade {
                id: "t1".into(),
                price: Price(100),
                qty: Qty(1),
                taker_side: ts_core::Side::Buy,
            }),
        };
        c.route_event(trade, &mut resyncs).unwrap();
        assert_eq!(c.resync_count(), 1);
    }
}
