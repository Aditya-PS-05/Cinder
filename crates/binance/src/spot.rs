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
}

impl SpotStreamClient {
    pub fn new(cfg: SpotStreamConfig, bus: Arc<Bus<MarketEvent>>) -> Self {
        Self { cfg, bus }
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
        match resync.push_delta(evt) {
            PushOutcome::Buffered => Ok(()),
            PushOutcome::Ready(out) => {
                self.publish(out);
                Ok(())
            }
            PushOutcome::Resync => Err(BinanceError::Align {
                detail: "live stream gapped; forcing reconnect".to_string(),
            }),
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
