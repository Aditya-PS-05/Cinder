//! Binance spot WebSocket client with reconnect loop.
//!
//! [`SpotStreamClient::run`] is the entry point: it opens a WebSocket to
//! `wss://stream.binance.com:9443/ws`, sends a single `SUBSCRIBE` message
//! for every configured symbol (`<sym>@trade` and `<sym>@depth@100ms`),
//! and pumps every decoded [`MarketEvent`] onto a [`Bus`]. On any
//! transport error it backs off exponentially and reconnects.
//!
//! The hot path is deliberately simple: one future pulls frames,
//! [`decode_frame`] turns them into events, and [`Bus::publish`] hands
//! them off non-blocking. Slow consumers drop messages, never the
//! producer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use ts_core::{bus::Bus, InstrumentSpec, MarketEvent, Symbol};

use crate::decode::decode_frame;
use crate::error::BinanceError;

/// Runtime configuration for a single spot stream session.
#[derive(Clone, Debug)]
pub struct SpotStreamConfig {
    /// Upstream WebSocket URL. Defaults to the public mainnet endpoint.
    pub ws_url: String,
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

        while let Some(frame) = ws.next().await {
            match frame? {
                Message::Text(t) => self.handle_payload(t.as_bytes()),
                Message::Binary(b) => self.handle_payload(&b),
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
        Ok(())
    }

    fn handle_payload(&self, body: &[u8]) {
        match decode_frame(body, &self.cfg.specs) {
            Ok(Some(evt)) => {
                let (delivered, dropped) = self.bus.publish(evt);
                if dropped > 0 {
                    warn!(delivered, dropped, "slow consumer on bus");
                }
            }
            Ok(None) => {
                debug!("non-event frame, skipped");
            }
            Err(BinanceError::Unsupported(tag)) => {
                debug!(tag, "unsupported event tag");
            }
            Err(e) => {
                warn!(error = %e, "failed to decode frame");
            }
        }
    }
}
