//! Signed REST client for Binance spot order endpoints.
//!
//! Supports the `SIGNED` subset of `/api/v3` the trading path needs:
//! `new_order`, `cancel_order`, `query_order`, and the `account`
//! snapshot used as an authentication smoke test. Mainnet and testnet
//! base URLs are exposed as constants; pick one at construction time.
//!
//! Requests are signed with HMAC-SHA256 over the query string. The
//! client appends `recvWindow` + `timestamp`, signs the concatenated
//! parameter block, and forwards the result as `…&signature=…`. The
//! API key rides in the `X-MBX-APIKEY` header.
//!
//! The module is deliberately unopinionated about fixed-point types —
//! Binance speaks decimal strings for quantity and price, so the
//! request structs carry `String` too. Higher-level code is free to
//! convert `ts_core::Price` / `ts_core::Qty` into strings before
//! calling [`SignedClient::new_order`].

use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::{Client, Method};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::error::BinanceError;

/// Binance production spot base URL.
pub const MAINNET_BASE: &str = "https://api.binance.com";
/// Binance spot testnet base URL (free signup, paper funds).
pub const TESTNET_BASE: &str = "https://testnet.binance.vision";

/// Signed REST client. Holds API credentials and a single reqwest
/// `Client` for connection reuse.
#[derive(Debug, Clone)]
pub struct SignedClient {
    http: Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    recv_window_ms: u64,
}

impl SignedClient {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Self {
        Self::with_http(Client::new(), base_url, api_key, api_secret)
    }

    pub fn with_http(
        http: Client,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Self {
        Self {
            http,
            base_url: base_url.into(),
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            recv_window_ms: 5_000,
        }
    }

    /// Shortcut for [`TESTNET_BASE`].
    pub fn testnet(api_key: impl Into<String>, api_secret: impl Into<String>) -> Self {
        Self::new(TESTNET_BASE, api_key, api_secret)
    }

    /// Shortcut for [`MAINNET_BASE`]. Use with care.
    pub fn mainnet(api_key: impl Into<String>, api_secret: impl Into<String>) -> Self {
        Self::new(MAINNET_BASE, api_key, api_secret)
    }

    /// Override the `recvWindow` (ms). Binance rejects requests where
    /// `timestamp` is more than this much older than the server clock.
    pub fn with_recv_window(mut self, ms: u64) -> Self {
        self.recv_window_ms = ms;
        self
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn new_order(&self, req: &NewOrderRequest) -> Result<OrderAck, BinanceError> {
        self.send_signed(Method::POST, "/api/v3/order", req.to_params())
            .await
    }

    pub async fn cancel_order(&self, req: &CancelOrderRequest) -> Result<OrderAck, BinanceError> {
        self.send_signed(Method::DELETE, "/api/v3/order", req.to_params())
            .await
    }

    pub async fn query_order(&self, req: &QueryOrderRequest) -> Result<OrderAck, BinanceError> {
        self.send_signed(Method::GET, "/api/v3/order", req.to_params())
            .await
    }

    pub async fn account(&self) -> Result<AccountSummary, BinanceError> {
        self.send_signed(Method::GET, "/api/v3/account", Vec::new())
            .await
    }

    /// Request a fresh user-data-stream `listenKey`. Unsigned (API key
    /// header only); the key expires 60 minutes after creation unless
    /// refreshed with [`keepalive_listen_key`](Self::keepalive_listen_key).
    pub async fn create_listen_key(&self) -> Result<String, BinanceError> {
        #[derive(Deserialize)]
        struct Resp {
            #[serde(rename = "listenKey")]
            listen_key: String,
        }
        let resp: Resp = self
            .send_keyed(Method::POST, "/api/v3/userDataStream", &[])
            .await?;
        Ok(resp.listen_key)
    }

    /// Extend a listenKey's validity by 60 minutes. Binance expects a
    /// keepalive every 30 minutes to stay well clear of expiry.
    pub async fn keepalive_listen_key(&self, listen_key: &str) -> Result<(), BinanceError> {
        let _: serde_json::Value = self
            .send_keyed(
                Method::PUT,
                "/api/v3/userDataStream",
                &[("listenKey", listen_key)],
            )
            .await?;
        Ok(())
    }

    /// Close a listenKey, ending the user-data-stream on Binance's side.
    pub async fn close_listen_key(&self, listen_key: &str) -> Result<(), BinanceError> {
        let _: serde_json::Value = self
            .send_keyed(
                Method::DELETE,
                "/api/v3/userDataStream",
                &[("listenKey", listen_key)],
            )
            .await?;
        Ok(())
    }

    /// API-key-authenticated but unsigned request — used by endpoints
    /// like the listenKey lifecycle that don't require HMAC.
    pub(crate) async fn send_keyed<T: for<'de> Deserialize<'de>>(
        &self,
        method: Method,
        path: &str,
        params: &[(&'static str, &str)],
    ) -> Result<T, BinanceError> {
        let base = self.base_url.trim_end_matches('/');
        let url = if params.is_empty() {
            format!("{base}{path}")
        } else {
            let owned: Vec<(&'static str, String)> =
                params.iter().map(|(k, v)| (*k, (*v).to_string())).collect();
            format!("{base}{path}?{}", build_query(&owned))
        };
        let resp = self
            .http
            .request(method, &url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        let status = resp.status();
        let bytes = resp.bytes().await?;
        if !status.is_success() {
            return Err(BinanceError::RestStatus {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        serde_json::from_slice(&bytes).map_err(Into::into)
    }

    async fn send_signed<T: for<'de> Deserialize<'de>>(
        &self,
        method: Method,
        path: &str,
        mut params: Vec<(&'static str, String)>,
    ) -> Result<T, BinanceError> {
        params.push(("recvWindow", self.recv_window_ms.to_string()));
        params.push(("timestamp", now_ms().to_string()));
        let query = build_query(&params);
        let signature = hmac_sha256_hex(self.api_secret.as_bytes(), query.as_bytes());
        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url.trim_end_matches('/'),
            path,
            query,
            signature
        );
        let resp = self
            .http
            .request(method, &url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        let status = resp.status();
        let bytes = resp.bytes().await?;
        if !status.is_success() {
            return Err(BinanceError::RestStatus {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        serde_json::from_slice(&bytes).map_err(Into::into)
    }
}

/// New-order request. Mirrors the Binance payload — callers translate
/// `ts_core` types into Binance strings as needed.
#[derive(Debug, Clone)]
pub struct NewOrderRequest {
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub quantity: Option<String>,
    pub quote_order_qty: Option<String>,
    pub price: Option<String>,
    pub new_client_order_id: Option<String>,
}

impl NewOrderRequest {
    fn to_params(&self) -> Vec<(&'static str, String)> {
        let mut p = Vec::with_capacity(8);
        p.push(("symbol", self.symbol.clone()));
        p.push(("side", self.side.as_str().to_string()));
        p.push(("type", self.order_type.as_str().to_string()));
        if let Some(tif) = self.time_in_force {
            p.push(("timeInForce", tif.as_str().to_string()));
        }
        if let Some(q) = self.quantity.as_ref() {
            p.push(("quantity", q.clone()));
        }
        if let Some(q) = self.quote_order_qty.as_ref() {
            p.push(("quoteOrderQty", q.clone()));
        }
        if let Some(price) = self.price.as_ref() {
            p.push(("price", price.clone()));
        }
        if let Some(cid) = self.new_client_order_id.as_ref() {
            p.push(("newClientOrderId", cid.clone()));
        }
        p
    }
}

#[derive(Debug, Clone)]
pub struct CancelOrderRequest {
    pub symbol: String,
    pub selector: OrderSelector,
}

impl CancelOrderRequest {
    fn to_params(&self) -> Vec<(&'static str, String)> {
        let mut p = Vec::with_capacity(2);
        p.push(("symbol", self.symbol.clone()));
        self.selector.push_params(&mut p);
        p
    }
}

#[derive(Debug, Clone)]
pub struct QueryOrderRequest {
    pub symbol: String,
    pub selector: OrderSelector,
}

impl QueryOrderRequest {
    fn to_params(&self) -> Vec<(&'static str, String)> {
        let mut p = Vec::with_capacity(2);
        p.push(("symbol", self.symbol.clone()));
        self.selector.push_params(&mut p);
        p
    }
}

/// Cancel / query must select an order either by its venue-assigned
/// numeric id or the original client-order-id. Encoding this as an
/// enum rules out the invalid "neither" case at compile time.
#[derive(Debug, Clone)]
pub enum OrderSelector {
    OrderId(i64),
    OrigClientOrderId(String),
}

impl OrderSelector {
    fn push_params(&self, out: &mut Vec<(&'static str, String)>) {
        match self {
            OrderSelector::OrderId(id) => out.push(("orderId", id.to_string())),
            OrderSelector::OrigClientOrderId(cid) => out.push(("origClientOrderId", cid.clone())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
    LimitMaker,
    StopLossLimit,
    TakeProfitLimit,
}

impl OrderType {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderType::Limit => "LIMIT",
            OrderType::Market => "MARKET",
            OrderType::LimitMaker => "LIMIT_MAKER",
            OrderType::StopLossLimit => "STOP_LOSS_LIMIT",
            OrderType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
}

impl TimeInForce {
    pub fn as_str(self) -> &'static str {
        match self {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
        }
    }
}

/// Subset of the Binance order-ack payload the trading path uses.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OrderAck {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    #[serde(rename = "transactTime", default)]
    pub transact_time: Option<i64>,
    #[serde(default)]
    pub price: String,
    #[serde(rename = "origQty", default)]
    pub orig_qty: String,
    #[serde(rename = "executedQty", default)]
    pub executed_qty: String,
    #[serde(rename = "cummulativeQuoteQty", default)]
    pub cummulative_quote_qty: String,
    pub status: String,
    #[serde(rename = "timeInForce", default)]
    pub time_in_force: String,
    #[serde(rename = "type", default)]
    pub order_type: String,
    #[serde(default)]
    pub side: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountSummary {
    #[serde(rename = "makerCommission", default)]
    pub maker_commission: i64,
    #[serde(rename = "takerCommission", default)]
    pub taker_commission: i64,
    #[serde(rename = "canTrade", default)]
    pub can_trade: bool,
    pub balances: Vec<Balance>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Balance {
    pub asset: String,
    pub free: String,
    pub locked: String,
}

// --- signing helpers ---------------------------------------------------------

/// HMAC-SHA256 over `msg` keyed by `secret`, hex-encoded lowercase.
pub fn hmac_sha256_hex(secret: &[u8], msg: &[u8]) -> String {
    hex::encode(hmac_sha256(secret, msg))
}

fn hmac_sha256(secret: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut key_block = [0u8; BLOCK];
    if secret.len() > BLOCK {
        let digest = Sha256::digest(secret);
        key_block[..32].copy_from_slice(&digest);
    } else {
        key_block[..secret.len()].copy_from_slice(secret);
    }
    let mut ikey = [0u8; BLOCK];
    let mut okey = [0u8; BLOCK];
    for i in 0..BLOCK {
        ikey[i] = key_block[i] ^ 0x36;
        okey[i] = key_block[i] ^ 0x5c;
    }
    let mut inner = Sha256::new();
    inner.update(ikey);
    inner.update(msg);
    let inner_digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(okey);
    outer.update(inner_digest);
    let outer_digest = outer.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&outer_digest);
    out
}

/// URL-encode a query value using RFC 3986 unreserved set. Digits,
/// letters, and `-_.~` pass through; everything else is percent-encoded.
pub fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for &b in value.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(to_hex(b >> 4));
                out.push(to_hex(b & 0x0f));
            }
        }
    }
    out
}

fn to_hex(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'A' + (nibble - 10)) as char,
        _ => unreachable!(),
    }
}

/// Build a Binance-compatible query string: `k1=v1&k2=v2`, values
/// percent-encoded. Key order is preserved — Binance signs the exact
/// string it will later receive.
pub fn build_query(params: &[(&str, String)]) -> String {
    let mut out = String::new();
    for (i, (k, v)) in params.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        out.push_str(k);
        out.push('=');
        out.push_str(&percent_encode(v));
    }
    out
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Binance's canonical HMAC-SHA256 example, lifted from the public
    // docs (SIGNED endpoint security section). Keeping the vector in
    // tree means a regression in our signing code fails loudly.
    #[test]
    fn hmac_sha256_matches_binance_doc_vector() {
        let secret = b"NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
        let msg = b"symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let sig = hmac_sha256_hex(secret, msg);
        assert_eq!(
            sig,
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
    }

    #[test]
    fn hmac_handles_long_key() {
        // Keys longer than the SHA-256 block (64 bytes) get hashed first.
        // The output is still 32 bytes; we only sanity-check length here.
        let long = vec![b'x'; 200];
        let sig = hmac_sha256_hex(&long, b"hello");
        assert_eq!(sig.len(), 64);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn percent_encode_passes_unreserved_untouched() {
        assert_eq!(percent_encode("BTCUSDT"), "BTCUSDT");
        assert_eq!(percent_encode("abc-_.~"), "abc-_.~");
    }

    #[test]
    fn percent_encode_encodes_reserved_bytes() {
        assert_eq!(percent_encode("a b"), "a%20b");
        assert_eq!(percent_encode("&=?"), "%26%3D%3F");
    }

    #[test]
    fn build_query_preserves_order_and_encodes_values() {
        let params = vec![
            ("symbol", "BTCUSDT".to_string()),
            ("side", "BUY".to_string()),
            ("price", "0.10".to_string()),
            ("note", "hi there".to_string()),
        ];
        assert_eq!(
            build_query(&params),
            "symbol=BTCUSDT&side=BUY&price=0.10&note=hi%20there"
        );
    }

    #[test]
    fn new_order_request_serializes_limit_params() {
        let req = NewOrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::Gtc),
            quantity: Some("0.001".into()),
            quote_order_qty: None,
            price: Some("65000.00".into()),
            new_client_order_id: Some("pr-42".into()),
        };
        let qs = build_query(&req.to_params());
        assert_eq!(
            qs,
            "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.001&price=65000.00&newClientOrderId=pr-42"
        );
    }

    #[test]
    fn cancel_and_query_select_by_orig_cid() {
        let cancel = CancelOrderRequest {
            symbol: "BTCUSDT".into(),
            selector: OrderSelector::OrigClientOrderId("pr-42".into()),
        };
        assert_eq!(
            build_query(&cancel.to_params()),
            "symbol=BTCUSDT&origClientOrderId=pr-42"
        );
        let query = QueryOrderRequest {
            symbol: "BTCUSDT".into(),
            selector: OrderSelector::OrderId(9876),
        };
        assert_eq!(
            build_query(&query.to_params()),
            "symbol=BTCUSDT&orderId=9876"
        );
    }

    #[test]
    fn order_ack_decodes_binance_payload() {
        let body = br#"{
            "symbol": "BTCUSDT",
            "orderId": 28,
            "orderListId": -1,
            "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP",
            "transactTime": 1507725176595,
            "price": "65000.00000000",
            "origQty": "0.00100000",
            "executedQty": "0.00000000",
            "cummulativeQuoteQty": "0.00000000",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY"
        }"#;
        let ack: OrderAck = serde_json::from_slice(body).unwrap();
        assert_eq!(ack.symbol, "BTCUSDT");
        assert_eq!(ack.order_id, 28);
        assert_eq!(ack.client_order_id, "6gCrw2kRUAF9CvJDGP16IP");
        assert_eq!(ack.transact_time, Some(1507725176595));
        assert_eq!(ack.status, "NEW");
        assert_eq!(ack.order_type, "LIMIT");
        assert_eq!(ack.side, "BUY");
    }

    #[test]
    fn account_summary_decodes_subset() {
        let body = br#"{
            "makerCommission": 10,
            "takerCommission": 10,
            "buyerCommission": 0,
            "sellerCommission": 0,
            "canTrade": true,
            "canWithdraw": true,
            "canDeposit": true,
            "updateTime": 1600000000000,
            "accountType": "SPOT",
            "balances": [
                {"asset":"BTC","free":"0.00100000","locked":"0.00000000"},
                {"asset":"USDT","free":"1000.00000000","locked":"0.00000000"}
            ],
            "permissions": ["SPOT"]
        }"#;
        let acct: AccountSummary = serde_json::from_slice(body).unwrap();
        assert_eq!(acct.maker_commission, 10);
        assert!(acct.can_trade);
        assert_eq!(acct.balances.len(), 2);
        assert_eq!(acct.balances[1].asset, "USDT");
        assert_eq!(acct.balances[1].free, "1000.00000000");
    }

    #[test]
    fn testnet_constructor_points_at_testnet_base() {
        let c = SignedClient::testnet("k", "s");
        assert_eq!(c.base_url(), TESTNET_BASE);
        let c = SignedClient::mainnet("k", "s");
        assert_eq!(c.base_url(), MAINNET_BASE);
    }
}
