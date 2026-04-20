//! Live Binance spot order engine implementing [`ts_oms::OrderEngine`].
//!
//! Mediates between the synchronous [`OrderEngine`] surface the runner
//! expects and the async [`SignedClient`] that talks to the exchange.
//!
//! `submit`/`cancel` translate a `ts_core` order into the Binance wire
//! shape using the registered [`InstrumentSpec`], spawn a background
//! tokio task that calls the REST API, and return an optimistic
//! `OrderStatus::New` / `OrderStatus::Canceled` report immediately. The
//! venue ack (success or HTTP failure) is pushed onto an internal mpsc
//! that [`OrderEngine::reconcile`] drains at the runner's cadence — so
//! every state change, pending or terminal, flows through one channel.
//!
//! [`OrderEngine::query`] reads the last-known report from a cache that
//! `reconcile` updates as it drains.
//!
//! Fills are not synthesized here: the REST ack only carries cumulative
//! executed quantity and quote notional, not the per-trade record. The
//! runner will get granular [`Fill`]s once the user-data-stream listener
//! (phase 29) lands — at which point they'll flow through the same
//! reconcile channel.
//!
//! Testability: the engine is generic over a [`BinanceOrderApi`] trait
//! so tests can inject a deterministic mock without standing up HTTP.
//! [`SignedClient`] is the only production impl.

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::mpsc;
use tracing::warn;

use ts_core::{
    ClientOrderId, ExecReport, InstrumentSpec, NewOrder, OrderKind, OrderStatus, Price, Qty, Side,
    Symbol, TimeInForce, Timestamp, Venue,
};
use ts_oms::{EngineStep, OrderEngine};

use crate::error::BinanceError;
use crate::order_rest::{
    CancelOrderRequest, NewOrderRequest, OrderAck, OrderSelector, OrderType, QueryOrderRequest,
    Side as BinanceSide, SignedClient, TimeInForce as BinanceTimeInForce,
};

/// The four Binance spot order operations the live engine needs.
/// Abstracts [`SignedClient`] so tests can inject a deterministic mock
/// without standing up HTTP.
///
/// RPITIT with an explicit `+ Send` bound is used instead of `async fn`
/// so the returned futures are guaranteed to be `Send` — `async fn` in
/// traits does not propagate `Send` without an extra workaround, and the
/// background worker spawned in `submit`/`cancel` requires `Send`.
#[allow(clippy::manual_async_fn)]
pub trait BinanceOrderApi: Send + Sync + 'static {
    fn new_order(
        &self,
        req: NewOrderRequest,
    ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send;
    fn cancel_order(
        &self,
        req: CancelOrderRequest,
    ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send;
    fn query_order(
        &self,
        req: QueryOrderRequest,
    ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send;
}

#[allow(clippy::manual_async_fn)]
impl BinanceOrderApi for SignedClient {
    fn new_order(
        &self,
        req: NewOrderRequest,
    ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send {
        async move { SignedClient::new_order(self, &req).await }
    }
    fn cancel_order(
        &self,
        req: CancelOrderRequest,
    ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send {
        async move { SignedClient::cancel_order(self, &req).await }
    }
    fn query_order(
        &self,
        req: QueryOrderRequest,
    ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send {
        async move { SignedClient::query_order(self, &req).await }
    }
}

/// Synchronous errors the live engine can surface. Venue-level order
/// rejections (risk, balance, filter) remain encoded as
/// `OrderStatus::Rejected` inside the returned `ExecReport`.
#[derive(Error, Debug)]
pub enum LiveEngineError {
    #[error("no instrument spec registered for symbol {0}")]
    UnknownSymbol(Symbol),
    #[error("unsupported order side: {0:?}")]
    UnsupportedSide(Side),
}

/// Live Binance order engine. Generic over a [`BinanceOrderApi`] so
/// tests can substitute the transport.
pub struct BinanceLiveEngine<A: BinanceOrderApi> {
    api: Arc<A>,
    specs: HashMap<Symbol, InstrumentSpec>,
    venue: Venue,
    tx: mpsc::Sender<ExecReport>,
    rx: mpsc::Receiver<ExecReport>,
    cache: HashMap<ClientOrderId, ExecReport>,
    /// Remember which symbol each open cid belongs to — Binance cancel
    /// requires symbol + cid on the wire. Entries are evicted on
    /// terminal status in `reconcile`.
    symbol_by_cid: HashMap<ClientOrderId, Symbol>,
    /// Cumulative count of [`ExecReport`]s dropped during
    /// [`Self::reconcile`] because the prev → next transition violated
    /// [`OrderStatus::can_transition_to`]. Exposed for tests and
    /// observability — runners surface this as a Prometheus counter.
    illegal_transitions: u64,
}

impl<A: BinanceOrderApi> BinanceLiveEngine<A> {
    pub fn new(
        api: Arc<A>,
        specs: HashMap<Symbol, InstrumentSpec>,
        venue: Venue,
        channel_capacity: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(channel_capacity.max(1));
        Self {
            api,
            specs,
            venue,
            tx,
            rx,
            cache: HashMap::new(),
            symbol_by_cid: HashMap::new(),
            illegal_transitions: 0,
        }
    }

    /// Number of [`ExecReport`]s that [`Self::reconcile`] has dropped
    /// because the cached prev → next transition was illegal under
    /// [`OrderStatus::can_transition_to`]. Always non-decreasing.
    pub fn illegal_transitions(&self) -> u64 {
        self.illegal_transitions
    }

    pub fn api(&self) -> &Arc<A> {
        &self.api
    }

    pub fn venue(&self) -> &Venue {
        &self.venue
    }

    pub fn cached_reports(&self) -> &HashMap<ClientOrderId, ExecReport> {
        &self.cache
    }

    /// Clone of the inbound [`ExecReport`] sender. External producers —
    /// typically a [`crate::user_stream::UserDataStreamClient`] — push
    /// venue-side order updates here so they land in the same queue
    /// that [`OrderEngine::reconcile`] drains. This is what lets live
    /// fills flow from Binance's WebSocket back into the engine.
    pub fn inbound_sender(&self) -> mpsc::Sender<ExecReport> {
        self.tx.clone()
    }

    fn spec_for(&self, sym: &Symbol) -> Result<&InstrumentSpec, LiveEngineError> {
        self.specs
            .get(sym)
            .ok_or_else(|| LiveEngineError::UnknownSymbol(sym.clone()))
    }

    fn to_new_order_request(&self, order: &NewOrder) -> Result<NewOrderRequest, LiveEngineError> {
        let spec = self.spec_for(&order.symbol)?;
        let side = binance_side(order.side)?;
        let qty_str = order.qty.to_string(spec.qty_scale);
        // PostOnly on Binance spot is the LIMIT_MAKER order type, not a
        // TIF variant — the venue rejects the order if any part would
        // take liquidity and never carries a `timeInForce` field.
        // Everything else (GTC/IOC/FOK) rides as a LIMIT with the
        // matching TIF string.
        let (order_type, tif, price) = match (order.kind, order.tif) {
            (OrderKind::Limit, TimeInForce::PostOnly) => (
                OrderType::LimitMaker,
                None,
                order.price.map(|p| p.to_string(spec.price_scale)),
            ),
            (OrderKind::Limit, _) => (
                OrderType::Limit,
                binance_tif(order.tif),
                order.price.map(|p| p.to_string(spec.price_scale)),
            ),
            (OrderKind::Market, _) => (OrderType::Market, None, None),
        };
        Ok(NewOrderRequest {
            symbol: spec.symbol.as_str().to_string(),
            side,
            order_type,
            time_in_force: tif,
            quantity: Some(qty_str),
            quote_order_qty: None,
            price,
            new_client_order_id: Some(order.cid.as_str().to_string()),
        })
    }
}

fn binance_side(side: Side) -> Result<BinanceSide, LiveEngineError> {
    match side {
        Side::Buy => Ok(BinanceSide::Buy),
        Side::Sell => Ok(BinanceSide::Sell),
        Side::Unknown => Err(LiveEngineError::UnsupportedSide(side)),
    }
}

/// Return the Binance `timeInForce` string for a non-PostOnly TIF, or
/// `None` when the caller supplies `TimeInForce::PostOnly` (which
/// should already have been routed to `OrderType::LimitMaker` by
/// [`BinanceLiveEngine::to_new_order_request`]).
fn binance_tif(tif: TimeInForce) -> Option<BinanceTimeInForce> {
    match tif {
        TimeInForce::Gtc => Some(BinanceTimeInForce::Gtc),
        TimeInForce::Ioc => Some(BinanceTimeInForce::Ioc),
        TimeInForce::Fok => Some(BinanceTimeInForce::Fok),
        TimeInForce::PostOnly => None,
    }
}

/// Map a Binance status string to [`OrderStatus`]. Unknown strings
/// default to `Rejected` — failing loudly is preferable to silently
/// treating an unfamiliar state as live.
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

/// Translate a Binance `OrderAck` into an [`ExecReport`] at the
/// instrument's fixed-point scale. `avg_price = cummulativeQuoteQty /
/// executedQty`, computed in i128 to avoid overflow at the scale
/// shift. Returns `avg_price = None` when executedQty is zero or the
/// division overflows i64.
fn ack_to_report(ack: &OrderAck, spec: &InstrumentSpec) -> ExecReport {
    let cid = ClientOrderId::new(ack.client_order_id.clone());
    let status = ts_status(&ack.status);
    let filled_qty = Qty::from_str(&ack.executed_qty, spec.qty_scale).unwrap_or(Qty(0));

    let avg_price = if filled_qty.0 > 0 {
        let quote = Qty::from_str(&ack.cummulative_quote_qty, spec.price_scale).ok();
        match quote {
            Some(q) => {
                let num = (q.0 as i128).checked_mul(10i128.pow(u32::from(spec.qty_scale)));
                num.and_then(|n| n.checked_div(filled_qty.0 as i128))
                    .and_then(|p| i64::try_from(p).ok())
                    .map(Price)
            }
            None => None,
        }
    } else {
        None
    };

    let reason = if matches!(status, OrderStatus::Rejected | OrderStatus::Expired) {
        Some(format!("venue status: {}", ack.status))
    } else {
        None
    };

    ExecReport {
        cid,
        status,
        filled_qty,
        avg_price,
        reason,
        fills: Vec::new(),
    }
}

impl<A: BinanceOrderApi> OrderEngine for BinanceLiveEngine<A> {
    type Error = LiveEngineError;

    fn submit(&mut self, order: NewOrder, _now: Timestamp) -> Result<ExecReport, Self::Error> {
        let req = self.to_new_order_request(&order)?;
        let spec = self.spec_for(&order.symbol)?.clone();
        self.symbol_by_cid
            .insert(order.cid.clone(), order.symbol.clone());

        let api = Arc::clone(&self.api);
        let tx = self.tx.clone();
        let cid_bg = order.cid.clone();
        tokio::spawn(async move {
            let report = match api.new_order(req).await {
                Ok(ack) => ack_to_report(&ack, &spec),
                Err(err) => ExecReport::rejected(cid_bg, err.to_string()),
            };
            let _ = tx.send(report).await;
        });

        Ok(ExecReport {
            cid: order.cid,
            status: OrderStatus::New,
            filled_qty: Qty(0),
            avg_price: None,
            reason: Some("pending venue ack".into()),
            fills: Vec::new(),
        })
    }

    fn cancel(&mut self, cid: &ClientOrderId) -> Result<ExecReport, Self::Error> {
        let Some(symbol) = self.symbol_by_cid.get(cid).cloned() else {
            return Ok(ExecReport::rejected(cid.clone(), "cancel: unknown cid"));
        };
        let spec = self.spec_for(&symbol)?.clone();
        let req = CancelOrderRequest {
            symbol: spec.symbol.as_str().to_string(),
            selector: OrderSelector::OrigClientOrderId(cid.as_str().to_string()),
        };

        let api = Arc::clone(&self.api);
        let tx = self.tx.clone();
        let cid_bg = cid.clone();
        tokio::spawn(async move {
            let report = match api.cancel_order(req).await {
                Ok(ack) => ack_to_report(&ack, &spec),
                Err(err) => ExecReport::rejected(cid_bg, err.to_string()),
            };
            let _ = tx.send(report).await;
        });

        Ok(ExecReport {
            cid: cid.clone(),
            status: OrderStatus::Canceled,
            filled_qty: Qty(0),
            avg_price: None,
            reason: Some("pending venue cancel ack".into()),
            fills: Vec::new(),
        })
    }

    fn query(&self, cid: &ClientOrderId) -> Option<ExecReport> {
        self.cache.get(cid).cloned()
    }

    fn illegal_transitions(&self) -> u64 {
        self.illegal_transitions
    }

    fn open_cids(&self) -> Vec<ClientOrderId> {
        // `symbol_by_cid` mirrors the set of cids the engine still
        // considers live — `reconcile` removes the entry on terminal
        // status, so this is always consistent with the cached last-
        // known status without a separate filter pass.
        self.symbol_by_cid.keys().cloned().collect()
    }

    fn reconcile(&mut self) -> Result<EngineStep, Self::Error> {
        let mut step = EngineStep::default();
        while let Ok(report) = self.rx.try_recv() {
            // Validate the prev → next transition against the cached
            // last-known status. The cache is empty for the first ack on
            // a cid (any status is a legal entry); for later acks, an
            // illegal edge usually means a stale REST ack arrived after
            // a fresher user-stream update — drop it so the cache stays
            // monotonic with the venue's true lifecycle. Embedded fills
            // are dropped with the report for the same reason: they
            // belong to a state we've already moved past.
            if let Some(prev) = self.cache.get(&report.cid) {
                if !prev.status.can_transition_to(report.status) {
                    self.illegal_transitions += 1;
                    warn!(
                        cid = %report.cid.as_str(),
                        from = ?prev.status,
                        to = ?report.status,
                        "binance-live-engine: dropping report on illegal status transition"
                    );
                    continue;
                }
            }
            if report.status.is_terminal() {
                self.symbol_by_cid.remove(&report.cid);
            }
            self.cache.insert(report.cid.clone(), report.clone());
            step.fills.extend(report.fills.iter().cloned());
            step.reports.push(report);
        }
        Ok(step)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use ts_core::{OrderKind, TimeInForce};

    /// Queue-backed mock. Callers enqueue canned `OrderAck` responses
    /// (or errors via `reject`) before driving the engine.
    #[derive(Default)]
    struct QueuedApi {
        new_order: Mutex<VecDeque<Result<OrderAck, BinanceError>>>,
        cancel_order: Mutex<VecDeque<Result<OrderAck, BinanceError>>>,
        query_order: Mutex<VecDeque<Result<OrderAck, BinanceError>>>,
    }

    impl QueuedApi {
        fn push_new(&self, ack: OrderAck) {
            self.new_order.lock().unwrap().push_back(Ok(ack));
        }
        fn push_cancel(&self, ack: OrderAck) {
            self.cancel_order.lock().unwrap().push_back(Ok(ack));
        }
    }

    impl BinanceOrderApi for QueuedApi {
        fn new_order(
            &self,
            _req: NewOrderRequest,
        ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send {
            let next = self.new_order.lock().unwrap().pop_front();
            async move {
                next.unwrap_or_else(|| Err(BinanceError::Unsupported("no canned new_order".into())))
            }
        }
        fn cancel_order(
            &self,
            _req: CancelOrderRequest,
        ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send {
            let next = self.cancel_order.lock().unwrap().pop_front();
            async move {
                next.unwrap_or_else(|| {
                    Err(BinanceError::Unsupported("no canned cancel_order".into()))
                })
            }
        }
        fn query_order(
            &self,
            _req: QueryOrderRequest,
        ) -> impl std::future::Future<Output = Result<OrderAck, BinanceError>> + Send {
            let next = self.query_order.lock().unwrap().pop_front();
            async move {
                next.unwrap_or_else(|| {
                    Err(BinanceError::Unsupported("no canned query_order".into()))
                })
            }
        }
    }

    fn spec() -> InstrumentSpec {
        InstrumentSpec {
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            base: "BTC".into(),
            quote: "USDT".into(),
            price_scale: 2,
            qty_scale: 6,
            min_qty: Qty(0),
            min_notional: 0,
        }
    }

    fn specs_map() -> HashMap<Symbol, InstrumentSpec> {
        let mut m = HashMap::new();
        m.insert(Symbol::from_static("BTCUSDT"), spec());
        m
    }

    fn ack(cid: &str, status: &str, executed: &str, quote: &str) -> OrderAck {
        OrderAck {
            symbol: "BTCUSDT".into(),
            order_id: 42,
            client_order_id: cid.into(),
            transact_time: Some(0),
            price: "65000.00".into(),
            orig_qty: "0.001000".into(),
            executed_qty: executed.into(),
            cummulative_quote_qty: quote.into(),
            status: status.into(),
            time_in_force: "GTC".into(),
            order_type: "LIMIT".into(),
            side: "BUY".into(),
        }
    }

    fn new_limit_order(cid: &str, side: Side, price: i64, qty: i64) -> NewOrder {
        NewOrder {
            cid: ClientOrderId::new(cid),
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            side,
            kind: OrderKind::Limit,
            tif: TimeInForce::Gtc,
            qty: Qty(qty),
            price: Some(Price(price)),
            ts: Timestamp::default(),
        }
    }

    #[test]
    fn ts_status_covers_known_binance_strings() {
        assert_eq!(ts_status("NEW"), OrderStatus::New);
        assert_eq!(ts_status("PARTIALLY_FILLED"), OrderStatus::PartiallyFilled);
        assert_eq!(ts_status("FILLED"), OrderStatus::Filled);
        assert_eq!(ts_status("CANCELED"), OrderStatus::Canceled);
        assert_eq!(ts_status("PENDING_CANCEL"), OrderStatus::Canceled);
        assert_eq!(ts_status("EXPIRED"), OrderStatus::Expired);
        // Anything not in the list collapses to Rejected.
        assert_eq!(ts_status("TRADE_PREVENTED"), OrderStatus::Rejected);
    }

    #[test]
    fn ack_to_report_filled_computes_avg_price() {
        let s = spec();
        // Filled 0.001 BTC for 65.00 USDT → avg = 65.00 / 0.001 = 65000.00.
        let a = ack("c1", "FILLED", "0.001000", "65.00");
        let r = ack_to_report(&a, &s);
        assert_eq!(r.status, OrderStatus::Filled);
        assert_eq!(r.filled_qty, Qty(1_000));
        assert_eq!(r.avg_price, Some(Price(6_500_000)));
        assert!(r.reason.is_none());
    }

    #[test]
    fn ack_to_report_new_has_no_avg_price() {
        let s = spec();
        let a = ack("c1", "NEW", "0", "0");
        let r = ack_to_report(&a, &s);
        assert_eq!(r.status, OrderStatus::New);
        assert_eq!(r.filled_qty, Qty(0));
        assert_eq!(r.avg_price, None);
    }

    #[test]
    fn ack_to_report_rejected_attaches_reason() {
        let s = spec();
        let mut a = ack("c1", "EXPIRED", "0", "0");
        a.status = "EXPIRED_IN_MATCH".into();
        let r = ack_to_report(&a, &s);
        assert_eq!(r.status, OrderStatus::Expired);
        assert_eq!(r.reason.as_deref(), Some("venue status: EXPIRED_IN_MATCH"));
    }

    #[test]
    fn to_new_order_request_encodes_limit_order_at_instrument_scale() {
        let api = Arc::new(QueuedApi::default());
        let eng = BinanceLiveEngine::new(api, specs_map(), Venue::BINANCE, 8);
        let order = new_limit_order("abc", Side::Buy, 6_500_000, 1_000);
        let req = eng.to_new_order_request(&order).unwrap();
        assert_eq!(req.symbol, "BTCUSDT");
        assert_eq!(req.side, BinanceSide::Buy);
        assert_eq!(req.order_type, OrderType::Limit);
        assert_eq!(req.time_in_force, Some(BinanceTimeInForce::Gtc));
        assert_eq!(req.price.as_deref(), Some("65000"));
        assert_eq!(req.quantity.as_deref(), Some("0.001"));
        assert_eq!(req.new_client_order_id.as_deref(), Some("abc"));
    }

    /// PostOnly limit routes to the `LIMIT_MAKER` order type with no
    /// `timeInForce` field — Binance's spot API rejects a
    /// `timeInForce` on `LIMIT_MAKER`, so sending one would bounce
    /// the order server-side. The price and qty encodings stay
    /// identical to a GTC limit; only the type and TIF differ.
    #[test]
    fn to_new_order_request_maps_post_only_to_limit_maker_without_tif() {
        let api = Arc::new(QueuedApi::default());
        let eng = BinanceLiveEngine::new(api, specs_map(), Venue::BINANCE, 8);
        let mut order = new_limit_order("abc", Side::Buy, 6_500_000, 1_000);
        order.tif = TimeInForce::PostOnly;
        let req = eng.to_new_order_request(&order).unwrap();
        assert_eq!(req.order_type, OrderType::LimitMaker);
        assert_eq!(req.time_in_force, None);
        // Price + qty still rendered at instrument scale.
        assert_eq!(req.price.as_deref(), Some("65000"));
        assert_eq!(req.quantity.as_deref(), Some("0.001"));
    }

    /// Cross-check the free `binance_tif` helper directly: each
    /// non-PostOnly variant yields the matching Binance string, and
    /// PostOnly collapses to `None` so callers that forget to route
    /// it through `LIMIT_MAKER` see a missing field rather than a
    /// wrong TIF.
    #[test]
    fn binance_tif_returns_none_only_for_post_only() {
        assert_eq!(binance_tif(TimeInForce::Gtc), Some(BinanceTimeInForce::Gtc));
        assert_eq!(binance_tif(TimeInForce::Ioc), Some(BinanceTimeInForce::Ioc));
        assert_eq!(binance_tif(TimeInForce::Fok), Some(BinanceTimeInForce::Fok));
        assert_eq!(binance_tif(TimeInForce::PostOnly), None);
    }

    #[test]
    fn unknown_symbol_on_submit_returns_error_synchronously() {
        let api = Arc::new(QueuedApi::default());
        let mut eng = BinanceLiveEngine::new(api, HashMap::new(), Venue::BINANCE, 8);
        let order = new_limit_order("abc", Side::Buy, 6_500_000, 1_000);
        let err = eng.submit(order, Timestamp::default()).unwrap_err();
        assert!(matches!(err, LiveEngineError::UnknownSymbol(_)));
    }

    #[tokio::test]
    async fn submit_returns_pending_new_then_reconcile_surfaces_filled() {
        let api = Arc::new(QueuedApi::default());
        api.push_new(ack("abc", "FILLED", "0.001000", "65.00"));
        let mut eng = BinanceLiveEngine::new(Arc::clone(&api), specs_map(), Venue::BINANCE, 8);

        let optimistic = eng
            .submit(
                new_limit_order("abc", Side::Buy, 6_500_000, 1_000),
                Timestamp::default(),
            )
            .unwrap();
        assert_eq!(optimistic.status, OrderStatus::New);
        assert_eq!(optimistic.reason.as_deref(), Some("pending venue ack"));

        // Wait for the background task to push the ack.
        for _ in 0..50 {
            let step = eng.reconcile().unwrap();
            if !step.reports.is_empty() {
                assert_eq!(step.reports.len(), 1);
                let r = &step.reports[0];
                assert_eq!(r.status, OrderStatus::Filled);
                assert_eq!(r.filled_qty, Qty(1_000));
                assert_eq!(r.avg_price, Some(Price(6_500_000)));
                // Cache is populated and symbol_by_cid is cleared on terminal.
                assert!(eng
                    .query(&ClientOrderId::new("abc"))
                    .is_some_and(|r| r.status == OrderStatus::Filled));
                assert!(eng.symbol_by_cid.is_empty());
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("expected a reconciled report within 50 yields");
    }

    #[tokio::test]
    async fn cancel_unknown_cid_is_rejected_synchronously_without_http() {
        let api = Arc::new(QueuedApi::default());
        let mut eng = BinanceLiveEngine::new(api, specs_map(), Venue::BINANCE, 8);
        let r = eng.cancel(&ClientOrderId::new("never-submitted")).unwrap();
        assert_eq!(r.status, OrderStatus::Rejected);
        assert_eq!(r.reason.as_deref(), Some("cancel: unknown cid"));
    }

    #[tokio::test]
    async fn cancel_after_submit_uses_symbol_from_cid_map() {
        let api = Arc::new(QueuedApi::default());
        api.push_new(ack("abc", "NEW", "0", "0"));
        api.push_cancel(ack("abc", "CANCELED", "0", "0"));
        let mut eng = BinanceLiveEngine::new(Arc::clone(&api), specs_map(), Venue::BINANCE, 8);

        eng.submit(
            new_limit_order("abc", Side::Buy, 6_500_000, 1_000),
            Timestamp::default(),
        )
        .unwrap();

        let optimistic_cancel = eng.cancel(&ClientOrderId::new("abc")).unwrap();
        assert_eq!(optimistic_cancel.status, OrderStatus::Canceled);
        assert_eq!(
            optimistic_cancel.reason.as_deref(),
            Some("pending venue cancel ack")
        );

        // Drain both background acks and assert the terminal state lands.
        let mut saw_canceled = false;
        for _ in 0..100 {
            let step = eng.reconcile().unwrap();
            for r in step.reports {
                if r.status == OrderStatus::Canceled {
                    saw_canceled = true;
                }
            }
            if saw_canceled {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(saw_canceled, "expected a canceled report");
        assert!(eng.symbol_by_cid.is_empty());
    }

    #[tokio::test]
    async fn http_error_surfaces_as_rejected_report() {
        let api = Arc::new(QueuedApi::default());
        api.new_order
            .lock()
            .unwrap()
            .push_back(Err(BinanceError::RestStatus {
                status: 400,
                body: "LOT_SIZE".into(),
            }));
        let mut eng = BinanceLiveEngine::new(Arc::clone(&api), specs_map(), Venue::BINANCE, 8);

        eng.submit(
            new_limit_order("xyz", Side::Buy, 6_500_000, 1_000),
            Timestamp::default(),
        )
        .unwrap();

        for _ in 0..50 {
            let step = eng.reconcile().unwrap();
            if let Some(r) = step.reports.first() {
                assert_eq!(r.status, OrderStatus::Rejected);
                assert!(r.reason.as_deref().unwrap().contains("LOT_SIZE"));
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("expected a rejected report from the HTTP error path");
    }

    #[tokio::test]
    async fn reconcile_drops_illegal_status_transition_and_counts_it() {
        // Push two reports onto the inbound channel directly (as the
        // user-data-stream listener would): a FILLED and then a stale
        // PARTIALLY_FILLED for the same cid. The second is illegal
        // (Filled → PartiallyFilled) and must be dropped without
        // touching the cache or the returned step.
        let api = Arc::new(QueuedApi::default());
        let mut eng = BinanceLiveEngine::new(Arc::clone(&api), specs_map(), Venue::BINANCE, 8);
        let inbound = eng.inbound_sender();

        let filled = ExecReport {
            cid: ClientOrderId::new("c-stale"),
            status: OrderStatus::Filled,
            filled_qty: Qty(1_000),
            avg_price: Some(Price(6_500_000)),
            reason: None,
            fills: Vec::new(),
        };
        let stale_partial = ExecReport {
            cid: ClientOrderId::new("c-stale"),
            status: OrderStatus::PartiallyFilled,
            filled_qty: Qty(500),
            avg_price: Some(Price(6_500_000)),
            reason: None,
            fills: Vec::new(),
        };
        inbound.send(filled).await.unwrap();
        inbound.send(stale_partial).await.unwrap();

        // Yield so the channel is observable from try_recv.
        tokio::task::yield_now().await;

        let step = eng.reconcile().unwrap();
        assert_eq!(step.reports.len(), 1, "only the FILLED should pass");
        assert_eq!(step.reports[0].status, OrderStatus::Filled);
        assert_eq!(eng.illegal_transitions(), 1);
        // Cache must reflect the legal terminal state, not the stale partial.
        let cached = eng.query(&ClientOrderId::new("c-stale")).unwrap();
        assert_eq!(cached.status, OrderStatus::Filled);
    }

    #[tokio::test]
    async fn reconcile_accepts_normal_progression_new_partial_filled() {
        let api = Arc::new(QueuedApi::default());
        let mut eng = BinanceLiveEngine::new(Arc::clone(&api), specs_map(), Venue::BINANCE, 8);
        let inbound = eng.inbound_sender();

        for (status, qty) in [
            (OrderStatus::New, 0),
            (OrderStatus::PartiallyFilled, 500),
            (OrderStatus::Filled, 1_000),
        ] {
            inbound
                .send(ExecReport {
                    cid: ClientOrderId::new("c-prog"),
                    status,
                    filled_qty: Qty(qty),
                    avg_price: None,
                    reason: None,
                    fills: Vec::new(),
                })
                .await
                .unwrap();
        }
        tokio::task::yield_now().await;

        let step = eng.reconcile().unwrap();
        assert_eq!(step.reports.len(), 3);
        assert_eq!(step.reports[2].status, OrderStatus::Filled);
        assert_eq!(eng.illegal_transitions(), 0);
    }

    #[tokio::test]
    async fn query_returns_none_until_reconcile_runs() {
        let api = Arc::new(QueuedApi::default());
        api.push_new(ack("abc", "NEW", "0", "0"));
        let mut eng = BinanceLiveEngine::new(Arc::clone(&api), specs_map(), Venue::BINANCE, 8);

        eng.submit(
            new_limit_order("abc", Side::Buy, 6_500_000, 1_000),
            Timestamp::default(),
        )
        .unwrap();

        // Before reconcile, the cache is empty.
        assert!(eng.query(&ClientOrderId::new("abc")).is_none());

        // After reconcile drains the channel, the cached NEW lands.
        for _ in 0..50 {
            eng.reconcile().unwrap();
            if let Some(r) = eng.query(&ClientOrderId::new("abc")) {
                assert_eq!(r.status, OrderStatus::New);
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("expected a cached report after reconcile");
    }
}
