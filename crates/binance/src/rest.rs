//! Binance spot REST endpoints that the WebSocket streams depend on.
//!
//! The only thing Phase 4 needs is `/api/v3/depth`: a server-side
//! snapshot of the L2 book plus its `lastUpdateId` checkpoint. The
//! [`BinanceBookSync`] state machine stitches this onto the live
//! `@depth@100ms` stream.
//!
//! [`BinanceBookSync`]: crate::book::BinanceBookSync

use reqwest::Client;
use serde::Deserialize;

use ts_core::{BookLevel, BookSnapshot, InstrumentSpec, Price, Qty, Symbol};

use crate::error::BinanceError;

/// A REST depth snapshot with its alignment checkpoint.
#[derive(Debug, Clone)]
pub struct DepthSnapshot {
    pub last_update_id: u64,
    pub snapshot: BookSnapshot,
}

#[derive(Debug, Deserialize)]
struct RawDepth {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

/// Parse a raw `/api/v3/depth` response body. Split out so tests can
/// exercise the decoder without a live HTTP client.
pub fn parse_depth_snapshot(
    body: &[u8],
    spec: &InstrumentSpec,
) -> Result<DepthSnapshot, BinanceError> {
    let raw: RawDepth = serde_json::from_slice(body)?;
    let bids = levels(&raw.bids, spec)?;
    let asks = levels(&raw.asks, spec)?;
    Ok(DepthSnapshot {
        last_update_id: raw.last_update_id,
        snapshot: BookSnapshot { bids, asks },
    })
}

fn levels(raw: &[[String; 2]], spec: &InstrumentSpec) -> Result<Vec<BookLevel>, BinanceError> {
    let mut out = Vec::with_capacity(raw.len());
    for [p, q] in raw {
        out.push(BookLevel {
            price: Price::from_str(p, spec.price_scale)?,
            qty: Qty::from_str(q, spec.qty_scale)?,
        });
    }
    Ok(out)
}

/// Fetch `/api/v3/depth?symbol=X&limit=L` and parse it.
pub async fn fetch_depth_snapshot(
    http: &Client,
    base_url: &str,
    symbol: &Symbol,
    limit: u32,
    spec: &InstrumentSpec,
) -> Result<DepthSnapshot, BinanceError> {
    let url = format!("{}/api/v3/depth", base_url.trim_end_matches('/'));
    let resp = http
        .get(&url)
        .query(&[
            ("symbol", symbol.as_str().to_string()),
            ("limit", limit.to_string()),
        ])
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
    parse_depth_snapshot(&bytes, spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{Symbol, Venue};

    fn spec() -> InstrumentSpec {
        InstrumentSpec {
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            base: "BTC".into(),
            quote: "USDT".into(),
            price_scale: 2,
            qty_scale: 8,
            min_qty: Qty(0),
            min_notional: 0,
        }
    }

    #[test]
    fn parses_fixture() {
        let body = include_bytes!("../testdata/depth_snapshot.json");
        let snap = parse_depth_snapshot(body, &spec()).unwrap();
        assert_eq!(snap.last_update_id, 100);
        assert_eq!(snap.snapshot.bids.len(), 2);
        assert_eq!(snap.snapshot.asks.len(), 2);
        // 65000.00 at price_scale 2.
        assert_eq!(snap.snapshot.bids[0].price, Price(6_500_000));
        // 0.8 at qty_scale 8.
        assert_eq!(snap.snapshot.asks[0].qty, Qty(80_000_000));
    }
}
