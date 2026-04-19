//! Binance spot user-data-stream decoder and WebSocket session.
//!
//! The user-data-stream delivers venue-side order updates:
//! `executionReport` frames that tell us when an order is accepted,
//! partially filled, fully filled, canceled, or expired — and every
//! trade fill along the way. This module provides:
//!
//! * [`decode_execution_report`] — pure JSON → [`ExecReport`]
//!   transformation, unit-testable without I/O.
//! * [`UserDataStreamClient::run`] — a WebSocket session that opens the
//!   stream keyed by a Binance-issued listenKey, decodes every frame,
//!   and pushes the resulting [`ExecReport`]s onto an mpsc. A background
//!   timer refreshes the listenKey every 30 minutes.
//!
//! Reconnect and resubscribe on transport failure are intentionally out
//! of scope — the session exits with an error and a higher layer
//! decides whether to retry. Binance requires a fresh listenKey on
//! every reconnect, so the retry logic has to coordinate REST + WS.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use ts_core::{
    decimal::parse_decimal, ClientOrderId, ExecReport, Fill, InstrumentSpec, OrderStatus, Price,
    Qty, Side, Symbol, Timestamp, Venue,
};

use crate::error::BinanceError;
use crate::order_rest::SignedClient;

/// Mainnet user-data-stream WebSocket base. Append `/<listenKey>` to
/// open a session.
pub const MAINNET_USER_WS_BASE: &str = "wss://stream.binance.com:9443/ws";
/// Testnet user-data-stream WebSocket base.
pub const TESTNET_USER_WS_BASE: &str = "wss://testnet.binance.vision/ws";

/// Configuration for a single user-data-stream session.
#[derive(Clone, Debug)]
pub struct UserDataStreamConfig {
    /// WebSocket base URL (without the trailing `/<listenKey>`).
    pub ws_base: String,
    /// Canonical venue tag — stamped onto every emitted [`Fill`].
    pub venue: Venue,
    /// Instrument metadata keyed by venue-local symbol
    /// (uppercase, e.g. `"BTCUSDT"`). Needed to quantize price/qty
    /// strings at the correct fixed-point scale.
    pub specs: HashMap<Symbol, InstrumentSpec>,
    /// How often to refresh the listenKey. Binance expires keys after
    /// 60 minutes; 30 minutes is the idiomatic safety margin.
    pub keepalive_interval: Duration,
}

impl UserDataStreamConfig {
    pub fn new(
        ws_base: impl Into<String>,
        venue: Venue,
        specs: HashMap<Symbol, InstrumentSpec>,
    ) -> Self {
        Self {
            ws_base: ws_base.into(),
            venue,
            specs,
            keepalive_interval: Duration::from_secs(30 * 60),
        }
    }
}

/// Session runner for Binance spot user-data-stream. Parametrised over
/// the REST client so tests can skip HTTP entirely.
pub struct UserDataStreamClient {
    cfg: UserDataStreamConfig,
    rest: Arc<SignedClient>,
    guard: TransitionGuard,
}

impl UserDataStreamClient {
    pub fn new(cfg: UserDataStreamConfig, rest: Arc<SignedClient>) -> Self {
        Self {
            cfg,
            rest,
            guard: TransitionGuard::new(),
        }
    }

    /// Cumulative count of user-stream [`ExecReport`]s dropped because
    /// the cached prev → next status transition violated
    /// [`OrderStatus::can_transition_to`]. Runs ahead of the engine so
    /// stale frames never reach the reconcile channel; the engine keeps
    /// a matching counter for the REST ack path, and together they let
    /// operators see where stale lifecycle frames enter.
    pub fn illegal_transitions(&self) -> u64 {
        self.guard.illegal_count()
    }

    /// Shared handle for the listener's illegal-transition counter so
    /// the runner can publish it into Prometheus on its own cadence
    /// without holding a reference back to the client. The returned
    /// `Arc<AtomicU64>` is the same counter [`Self::illegal_transitions`]
    /// reads — cloning it is cheap and reads stay lock-free.
    pub fn illegal_transitions_counter(&self) -> Arc<AtomicU64> {
        self.guard.illegal_counter()
    }

    /// Run one session end-to-end: create a listenKey, open the WS,
    /// decode frames onto `report_tx`, refresh the listenKey on a
    /// timer, close the listenKey on shutdown. Returns when the WS
    /// closes cleanly or any error occurs — the caller decides whether
    /// to reconnect.
    pub async fn run(&self, report_tx: mpsc::Sender<ExecReport>) -> Result<(), BinanceError> {
        let listen_key = self.rest.create_listen_key().await?;
        info!(%listen_key, "binance user-data-stream listenKey created");

        let url = format!("{}/{listen_key}", self.cfg.ws_base.trim_end_matches('/'));
        let result = self.run_session(&url, &listen_key, report_tx).await;

        // Best-effort cleanup — log and move on if close fails.
        if let Err(e) = self.rest.close_listen_key(&listen_key).await {
            warn!(error = %e, "failed to close listenKey");
        }
        result
    }

    async fn run_session(
        &self,
        url: &str,
        listen_key: &str,
        report_tx: mpsc::Sender<ExecReport>,
    ) -> Result<(), BinanceError> {
        info!(%url, "connecting to binance user-data-stream");
        let (mut ws, _resp) = connect_async(url).await?;

        let mut ka = interval(self.cfg.keepalive_interval);
        ka.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // `interval` fires immediately on the first tick — skip it so we
        // don't hit Binance with a redundant keepalive the instant the
        // listenKey was minted.
        ka.tick().await;

        loop {
            tokio::select! {
                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Text(body))) => {
                            if let Err(e) = self.dispatch(body.as_bytes(), &report_tx).await {
                                warn!(error = %e, "user-stream frame decode failed");
                            }
                        }
                        Some(Ok(Message::Binary(body))) => {
                            if let Err(e) = self.dispatch(&body, &report_tx).await {
                                warn!(error = %e, "user-stream frame decode failed");
                            }
                        }
                        Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {
                            debug!("user-stream control frame");
                        }
                        Some(Ok(Message::Close(frame))) => {
                            info!(?frame, "binance user-data-stream closed by server");
                            return Ok(());
                        }
                        Some(Err(e)) => return Err(BinanceError::from(e)),
                        None => {
                            info!("binance user-data-stream ended");
                            return Ok(());
                        }
                    }
                }
                _ = ka.tick() => {
                    if let Err(e) = self.rest.keepalive_listen_key(listen_key).await {
                        error!(error = %e, "listenKey keepalive failed");
                        return Err(e);
                    }
                    debug!("listenKey keepalive ok");
                }
            }
        }
    }

    async fn dispatch(
        &self,
        body: &[u8],
        report_tx: &mpsc::Sender<ExecReport>,
    ) -> Result<(), BinanceError> {
        match decode_user_stream_frame(body, &self.cfg.specs, &self.cfg.venue)? {
            Some(UserStreamEvent::Report(report)) => {
                if !self.guard.admit(&report) {
                    return Ok(());
                }
                if report_tx.send(report).await.is_err() {
                    return Err(BinanceError::Unsupported(
                        "report channel closed by consumer".into(),
                    ));
                }
            }
            None => {}
        }
        Ok(())
    }
}

/// Prev → next [`OrderStatus`] sanity check for the user-data-stream.
///
/// Binance occasionally reorders frames — a stale `PARTIALLY_FILLED`
/// arriving after `FILLED`, a late `NEW` after a `CANCELED`, etc. The
/// engine's reconcile loop already guards against illegal edges, but
/// filtering at the listener means the engine channel stays clean and
/// the counter here pins blame to the stream rather than the REST path.
/// Admitted reports update the cache; rejected ones bump
/// `illegal_count` and are dropped.
#[derive(Debug, Default)]
pub(crate) struct TransitionGuard {
    cache: Mutex<HashMap<ClientOrderId, OrderStatus>>,
    illegal: Arc<AtomicU64>,
}

impl TransitionGuard {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Validate `report.status` against the cached last-known status for
    /// `report.cid`. Returns `true` when the listener should forward —
    /// either no prior state is cached (first sighting) or the edge is
    /// legal. Illegal edges log a warn, bump the counter, and return
    /// `false` without mutating the cache.
    pub(crate) fn admit(&self, report: &ExecReport) -> bool {
        let mut cache = self.cache.lock().expect("transition guard cache poisoned");
        let prev = cache.get(&report.cid).copied();
        match prev {
            Some(p) if !p.can_transition_to(report.status) => {
                drop(cache);
                self.illegal.fetch_add(1, Ordering::Relaxed);
                warn!(
                    cid = %report.cid.as_str(),
                    from = ?p,
                    to = ?report.status,
                    "user-stream: dropping illegal OrderStatus transition"
                );
                false
            }
            _ => {
                cache.insert(report.cid.clone(), report.status);
                true
            }
        }
    }

    pub(crate) fn illegal_count(&self) -> u64 {
        self.illegal.load(Ordering::Relaxed)
    }

    pub(crate) fn illegal_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.illegal)
    }
}

/// Decoded user-stream payloads we forward to the engine. Anything we
/// don't map (balance updates, listKey-status, …) is skipped at the
/// decode layer and returned as `Ok(None)`.
#[derive(Debug, Clone)]
pub enum UserStreamEvent {
    Report(ExecReport),
}

/// Pure decoder for a single user-data-stream frame. Returns
/// `Ok(None)` for recognised-but-unmapped frames (balance updates, list
/// statuses, …) so callers can skip rather than error.
pub fn decode_user_stream_frame(
    body: &[u8],
    specs: &HashMap<Symbol, InstrumentSpec>,
    venue: &Venue,
) -> Result<Option<UserStreamEvent>, BinanceError> {
    let v: Value = serde_json::from_slice(body)?;
    let tag = v.get("e").and_then(Value::as_str).unwrap_or("");
    match tag {
        "executionReport" => {
            let (report, _fills) = decode_execution_report_value(&v, specs, venue)?;
            Ok(Some(UserStreamEvent::Report(report)))
        }
        // Known event kinds we intentionally skip for now.
        "outboundAccountPosition" | "balanceUpdate" | "listStatus" | "" => Ok(None),
        other => Err(BinanceError::Unsupported(other.to_string())),
    }
}

/// Decode a raw `executionReport` payload into an [`ExecReport`] plus
/// any [`Fill`]s it carries. Exposed for direct unit testing.
pub fn decode_execution_report(
    body: &[u8],
    specs: &HashMap<Symbol, InstrumentSpec>,
    venue: &Venue,
) -> Result<(ExecReport, Vec<Fill>), BinanceError> {
    let v: Value = serde_json::from_slice(body)?;
    decode_execution_report_value(&v, specs, venue)
}

fn decode_execution_report_value(
    v: &Value,
    specs: &HashMap<Symbol, InstrumentSpec>,
    venue: &Venue,
) -> Result<(ExecReport, Vec<Fill>), BinanceError> {
    let raw: RawExecutionReport = serde_json::from_value(v.clone())?;
    let symbol_key = Symbol::new(&raw.symbol);
    let spec = specs
        .get(&symbol_key)
        .ok_or_else(|| BinanceError::UnknownSymbol(raw.symbol.clone()))?;

    let cid = ClientOrderId::new(&raw.client_order_id);
    let status = ts_status(&raw.order_status);
    let side = ts_side(&raw.side);

    let filled_qty = Qty::from_str(&raw.cum_filled_qty, spec.qty_scale)?;
    let cum_quote_qty = Qty::from_str(&raw.cum_quote_qty, spec.price_scale)?;
    let avg_price = avg_price_from_cumulative(filled_qty, cum_quote_qty, spec.qty_scale);

    let ts = Timestamp::from_unix_millis(raw.transaction_time);

    // Binance emits one executionReport per fill; `l` (last exec qty)
    // > 0 means this frame represents a trade, and (L, l) give its
    // price/qty. Skip when `l == 0` (NEW, CANCELED, EXPIRED, …).
    let last_qty = Qty::from_str(&raw.last_exec_qty, spec.qty_scale)?;
    let mut fills = Vec::new();
    if last_qty.0 > 0 {
        let last_price = Price::from_str(&raw.last_exec_price, spec.price_scale)?;
        let (fee, fee_asset) = decode_commission(&raw.commission, &raw.commission_asset, spec)?;
        fills.push(Fill {
            cid: cid.clone(),
            venue: venue.clone(),
            symbol: spec.symbol.clone(),
            side,
            price: last_price,
            qty: last_qty,
            ts,
            is_maker: Some(raw.is_maker),
            fee,
            fee_asset,
        });
    }

    let reason = match status {
        OrderStatus::Rejected => Some(format!(
            "venue rejected: {}",
            if raw.reject_reason.is_empty() || raw.reject_reason == "NONE" {
                raw.order_status.clone()
            } else {
                raw.reject_reason.clone()
            }
        )),
        OrderStatus::Expired => Some(format!("venue status: {}", raw.order_status)),
        _ => None,
    };

    let report = ExecReport {
        cid,
        status,
        filled_qty,
        avg_price,
        reason,
        fills: fills.clone(),
    };
    Ok((report, fills))
}

fn ts_status(s: &str) -> OrderStatus {
    match s {
        "NEW" | "PENDING_NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "PENDING_CANCEL" => OrderStatus::Canceled,
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus::Expired,
        _ => OrderStatus::Rejected,
    }
}

fn ts_side(s: &str) -> Side {
    match s {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        _ => Side::Unknown,
    }
}

/// `avg_price = cum_quote_qty / filled_qty`, shifted into the
/// price_scale. Computed in i128 to avoid overflow; returns `None` if
/// filled_qty is zero or the scaled result overflows i64.
fn avg_price_from_cumulative(filled: Qty, cum_quote: Qty, qty_scale: u8) -> Option<Price> {
    if filled.0 <= 0 {
        return None;
    }
    let num = (cum_quote.0 as i128).checked_mul(10i128.pow(u32::from(qty_scale)))?;
    let px = num.checked_div(filled.0 as i128)?;
    i64::try_from(px).ok().map(Price)
}

#[derive(Debug, Deserialize)]
struct RawExecutionReport {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "X")]
    order_status: String,
    #[serde(rename = "r", default)]
    reject_reason: String,
    #[serde(rename = "l", default)]
    last_exec_qty: String,
    #[serde(rename = "L", default)]
    last_exec_price: String,
    #[serde(rename = "z", default)]
    cum_filled_qty: String,
    #[serde(rename = "Z", default)]
    cum_quote_qty: String,
    #[serde(rename = "T")]
    transaction_time: i64,
    /// Binance `m`: true when this trade side was the maker. Only
    /// present on executionReport frames that carry an actual fill.
    #[serde(rename = "m", default)]
    is_maker: bool,
    /// Commission charged on this trade, reported as a decimal string.
    /// Blank on non-fill frames.
    #[serde(rename = "n", default)]
    commission: String,
    /// Asset the commission was denominated in (`"USDT"`, `"BNB"`, …).
    /// Blank on non-fill frames.
    #[serde(rename = "N", default)]
    commission_asset: String,
}

/// Decode the `n` / `N` commission pair off a fill frame.
///
/// When the commission asset matches `spec.quote`, the numeric value is
/// parsed at `price_scale + qty_scale` so it lands in the same mantissa
/// PnL accumulates in. Cross-asset commissions (BNB discount, base-asset
/// rebates) return `fee = 0` with the asset label preserved so operators
/// can still see them in the audit trail — converting them to quote
/// terms needs a cross-asset mark that the decoder does not own.
fn decode_commission(
    commission: &str,
    asset: &str,
    spec: &InstrumentSpec,
) -> Result<(i64, Option<String>), crate::error::BinanceError> {
    if commission.is_empty() || asset.is_empty() {
        return Ok((0, None));
    }
    let label = Some(asset.to_string());
    if !asset.eq_ignore_ascii_case(&spec.quote) {
        return Ok((0, label));
    }
    let scale = spec.price_scale + spec.qty_scale;
    let mantissa = parse_decimal(commission, scale)?;
    Ok((mantissa, label))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> InstrumentSpec {
        InstrumentSpec {
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
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
        m.insert(Symbol::from_static("BTCUSDT"), spec());
        m
    }

    /// Sample `executionReport` frame modelled after the Binance spot docs.
    /// Trimmed to the fields the decoder actually reads.
    fn sample_frame(
        status: &str,
        last_q: &str,
        last_p: &str,
        cum_q: &str,
        cum_quote: &str,
    ) -> String {
        format!(
            r#"{{
                "e": "executionReport",
                "s": "BTCUSDT",
                "c": "mycid-1",
                "S": "BUY",
                "X": "{status}",
                "r": "NONE",
                "l": "{last_q}",
                "L": "{last_p}",
                "z": "{cum_q}",
                "Z": "{cum_quote}",
                "T": 1700000000500
            }}"#
        )
    }

    #[test]
    fn decoder_accepts_new_order_report_with_no_fill() {
        let body = sample_frame("NEW", "0.00000", "0.00", "0.00000", "0.00");
        let (report, fills) =
            decode_execution_report(body.as_bytes(), &specs(), &Venue::BINANCE).unwrap();
        assert_eq!(report.status, OrderStatus::New);
        assert_eq!(report.filled_qty, Qty(0));
        assert_eq!(report.avg_price, None);
        assert!(fills.is_empty());
        assert!(report.reason.is_none());
    }

    #[test]
    fn decoder_emits_fill_on_partial_fill_frame() {
        // 0.5 BTC filled at 30000 on this frame, cumulative 0.5 @ 30000.
        let body = sample_frame(
            "PARTIALLY_FILLED",
            "0.50000",
            "30000.00",
            "0.50000",
            "15000.00",
        );
        let (report, fills) =
            decode_execution_report(body.as_bytes(), &specs(), &Venue::BINANCE).unwrap();
        assert_eq!(report.status, OrderStatus::PartiallyFilled);
        assert_eq!(report.filled_qty, Qty(50_000)); // 0.5 at qty_scale=5
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].qty, Qty(50_000));
        assert_eq!(fills[0].price, Price(3_000_000)); // 30000 at price_scale=2
        assert_eq!(fills[0].side, Side::Buy);
        assert_eq!(fills[0].cid.as_str(), "mycid-1");
        // avg = 15000 / 0.5 = 30000 — in i64 price units (scale 2) that's 3_000_000.
        assert_eq!(report.avg_price, Some(Price(3_000_000)));
    }

    #[test]
    fn decoder_computes_avg_price_across_two_fills() {
        // Second frame of a two-fill execution: last=0.5@31000, cumulative 1.0 filled @ 15000+15500=30500.
        let body = sample_frame("FILLED", "0.50000", "31000.00", "1.00000", "30500.00");
        let (report, fills) =
            decode_execution_report(body.as_bytes(), &specs(), &Venue::BINANCE).unwrap();
        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_qty, Qty(100_000));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, Price(3_100_000));
        // 30500 / 1.0 = 30500 @ price_scale=2 → 3_050_000.
        assert_eq!(report.avg_price, Some(Price(3_050_000)));
    }

    #[test]
    fn decoder_marks_canceled_report_without_fills() {
        let body = sample_frame("CANCELED", "0.00000", "0.00", "0.00000", "0.00");
        let (report, fills) =
            decode_execution_report(body.as_bytes(), &specs(), &Venue::BINANCE).unwrap();
        assert_eq!(report.status, OrderStatus::Canceled);
        assert!(fills.is_empty());
    }

    #[test]
    fn decoder_attaches_reason_to_rejected_report() {
        let body = r#"{
            "e": "executionReport",
            "s": "BTCUSDT",
            "c": "mycid-1",
            "S": "BUY",
            "X": "REJECTED",
            "r": "INSUFFICIENT_BALANCE",
            "l": "0.00000",
            "L": "0.00",
            "z": "0.00000",
            "Z": "0.00",
            "T": 1700000000500
        }"#;
        let (report, _fills) =
            decode_execution_report(body.as_bytes(), &specs(), &Venue::BINANCE).unwrap();
        assert_eq!(report.status, OrderStatus::Rejected);
        assert!(report
            .reason
            .as_deref()
            .unwrap()
            .contains("INSUFFICIENT_BALANCE"));
    }

    #[test]
    fn decoder_errors_on_unknown_symbol() {
        let body =
            sample_frame("NEW", "0.00000", "0.00", "0.00000", "0.00").replace("BTCUSDT", "ETHUSDT");
        let err = decode_execution_report(body.as_bytes(), &specs(), &Venue::BINANCE).unwrap_err();
        assert!(matches!(err, BinanceError::UnknownSymbol(_)));
    }

    #[test]
    fn frame_dispatcher_skips_balance_updates_but_flags_unknown_events() {
        let balance = r#"{"e":"outboundAccountPosition","u":1700000000000}"#;
        assert!(
            decode_user_stream_frame(balance.as_bytes(), &specs(), &Venue::BINANCE)
                .unwrap()
                .is_none()
        );

        let blank = r#"{"result":null,"id":1}"#;
        assert!(
            decode_user_stream_frame(blank.as_bytes(), &specs(), &Venue::BINANCE)
                .unwrap()
                .is_none()
        );

        let mystery = r#"{"e":"mysteryEvent"}"#;
        assert!(matches!(
            decode_user_stream_frame(mystery.as_bytes(), &specs(), &Venue::BINANCE).unwrap_err(),
            BinanceError::Unsupported(_)
        ));
    }

    #[test]
    fn frame_dispatcher_forwards_execution_report() {
        let body = sample_frame("NEW", "0.00000", "0.00", "0.00000", "0.00");
        let ev = decode_user_stream_frame(body.as_bytes(), &specs(), &Venue::BINANCE)
            .unwrap()
            .unwrap();
        let UserStreamEvent::Report(report) = ev;
        assert_eq!(report.status, OrderStatus::New);
    }

    #[test]
    fn avg_price_helper_is_zero_safe() {
        assert_eq!(avg_price_from_cumulative(Qty(0), Qty(0), 5), None);
    }

    fn report(cid: &str, status: OrderStatus) -> ExecReport {
        ExecReport {
            cid: ClientOrderId::new(cid),
            status,
            filled_qty: Qty(0),
            avg_price: None,
            reason: None,
            fills: Vec::new(),
        }
    }

    #[test]
    fn transition_guard_admits_first_report_for_any_cid() {
        let g = TransitionGuard::new();
        // With no cached prev, any status is legal — including terminal
        // ones like Filled (in case the first frame we see is post-fill).
        assert!(g.admit(&report("c1", OrderStatus::New)));
        assert!(g.admit(&report("c2", OrderStatus::Filled)));
        assert_eq!(g.illegal_count(), 0);
    }

    #[test]
    fn transition_guard_allows_new_partial_filled_progression() {
        let g = TransitionGuard::new();
        assert!(g.admit(&report("c1", OrderStatus::New)));
        assert!(g.admit(&report("c1", OrderStatus::PartiallyFilled)));
        assert!(g.admit(&report("c1", OrderStatus::Filled)));
        assert_eq!(g.illegal_count(), 0);
    }

    #[test]
    fn transition_guard_drops_stale_partial_after_filled() {
        let g = TransitionGuard::new();
        assert!(g.admit(&report("stale", OrderStatus::Filled)));
        // Classic reorder case: a PARTIALLY_FILLED frame arriving after
        // the terminal FILLED. Must be dropped and counted.
        assert!(!g.admit(&report("stale", OrderStatus::PartiallyFilled)));
        assert_eq!(g.illegal_count(), 1);
        // A second stale frame increments the counter again; the cache
        // still reflects the terminal FILLED — not the rejected update.
        assert!(!g.admit(&report("stale", OrderStatus::PartiallyFilled)));
        assert_eq!(g.illegal_count(), 2);
        // A fresh, legal transition (terminal → terminal is not) would
        // still fail — Filled is absorbing. Verify the cache stayed pinned.
        assert!(!g.admit(&report("stale", OrderStatus::New)));
        assert_eq!(g.illegal_count(), 3);
    }

    #[test]
    fn transition_guard_counter_is_shared_across_clones() {
        // The shared handle lets the runner publish the stream's
        // illegal count into Prometheus without holding a reference to
        // the client. Bumps made through the guard must be visible
        // through the cloned handle immediately.
        let g = TransitionGuard::new();
        let shared = g.illegal_counter();
        assert_eq!(shared.load(Ordering::Relaxed), 0);
        assert!(g.admit(&report("shared", OrderStatus::Filled)));
        assert!(!g.admit(&report("shared", OrderStatus::New)));
        assert_eq!(shared.load(Ordering::Relaxed), 1);
        assert_eq!(shared.load(Ordering::Relaxed), g.illegal_count());
    }

    #[test]
    fn transition_guard_isolates_cids() {
        let g = TransitionGuard::new();
        // An illegal transition on one cid must not leak into another:
        // each cid has its own cached prev.
        assert!(g.admit(&report("a", OrderStatus::Filled)));
        assert!(!g.admit(&report("a", OrderStatus::New)));
        // cid "b" is untouched — first sighting for "b" is always legal.
        assert!(g.admit(&report("b", OrderStatus::New)));
        assert!(g.admit(&report("b", OrderStatus::Filled)));
        assert_eq!(g.illegal_count(), 1);
    }
}
