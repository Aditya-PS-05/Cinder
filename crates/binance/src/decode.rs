//! Pure JSON → [`MarketEvent`] decoder for Binance spot frames.
//!
//! Binance tags every event with an `e` field ("trade", "depthUpdate",
//! ...). We parse the frame as a [`serde_json::Value`] once, inspect the
//! tag, then deserialize into the concrete shape we need. This is a tad
//! slower than a direct `from_slice` into an untagged enum, but it keeps
//! the decoder robust against (a) subscription ACK frames without any
//! `e` tag and (b) new event kinds that we have not mapped yet.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use ts_core::{
    BookDelta, BookLevel, InstrumentSpec, MarketEvent, MarketPayload, Price, Qty, Side, Symbol,
    Timestamp, Trade, Venue,
};

use crate::error::BinanceError;

/// Decode a single Binance spot WebSocket frame.
///
/// * `body` is the raw text payload of a `Text` WebSocket message.
/// * `specs` maps venue-local symbol (uppercase, e.g. `"BTCUSDT"`) to
///   the [`InstrumentSpec`] whose scales control mantissa conversion.
///
/// Returns `Ok(None)` for frames we intentionally skip (subscription
/// ACKs, non-event replies); `Err(BinanceError::Unsupported(..))` for
/// recognized JSON that has no canonical mapping yet.
pub fn decode_frame(
    body: &[u8],
    specs: &HashMap<Symbol, InstrumentSpec>,
) -> Result<Option<MarketEvent>, BinanceError> {
    let v: Value = serde_json::from_slice(body)?;

    // Subscription ACK / response to an id-bearing command: `{ "result": null, "id": 1 }`.
    if v.get("e").is_none() {
        return Ok(None);
    }

    let tag = v
        .get("e")
        .and_then(Value::as_str)
        .ok_or_else(|| BinanceError::Unsupported("missing e tag".into()))?
        .to_string();

    let local_ts = Timestamp::now();

    match tag.as_str() {
        "trade" => {
            let raw: RawTrade = serde_json::from_value(v)?;
            let spec = lookup_spec(specs, &raw.symbol)?;
            Ok(Some(map_trade(raw, spec, local_ts)?))
        }
        "depthUpdate" => {
            let raw: RawDepth = serde_json::from_value(v)?;
            let spec = lookup_spec(specs, &raw.symbol)?;
            Ok(Some(map_depth(raw, spec, local_ts)?))
        }
        other => Err(BinanceError::Unsupported(other.to_string())),
    }
}

fn lookup_spec<'a>(
    specs: &'a HashMap<Symbol, InstrumentSpec>,
    symbol: &str,
) -> Result<&'a InstrumentSpec, BinanceError> {
    let key = Symbol::new(symbol);
    specs
        .get(&key)
        .ok_or_else(|| BinanceError::UnknownSymbol(symbol.to_string()))
}

fn map_trade(
    raw: RawTrade,
    spec: &InstrumentSpec,
    local_ts: Timestamp,
) -> Result<MarketEvent, BinanceError> {
    let price = Price::from_str(&raw.price, spec.price_scale)?;
    let qty = Qty::from_str(&raw.qty, spec.qty_scale)?;
    // Binance reports `m = true` when the buyer is the maker, which means
    // the *seller* was the taker. Invert to get taker_side.
    let taker_side = if raw.is_buyer_maker {
        Side::Sell
    } else {
        Side::Buy
    };
    Ok(MarketEvent {
        venue: Venue::BINANCE,
        symbol: Symbol::new(raw.symbol),
        exchange_ts: Timestamp::from_unix_millis(raw.trade_time_ms),
        local_ts,
        seq: raw.trade_id,
        payload: MarketPayload::Trade(Trade {
            id: raw.trade_id.to_string(),
            price,
            qty,
            taker_side,
        }),
    })
}

fn map_depth(
    raw: RawDepth,
    spec: &InstrumentSpec,
    local_ts: Timestamp,
) -> Result<MarketEvent, BinanceError> {
    let bids = levels(&raw.bids, spec)?;
    let asks = levels(&raw.asks, spec)?;
    // Binance chains depth updates: each frame advances from
    // `first_update_id` to `last_update_id`. Callers that need to stitch
    // against a snapshot compare `prev_seq + 1 == snapshot.lastUpdateId`.
    let prev_seq = raw.first_update_id.saturating_sub(1);
    Ok(MarketEvent {
        venue: Venue::BINANCE,
        symbol: Symbol::new(raw.symbol),
        exchange_ts: Timestamp::from_unix_millis(raw.event_time_ms),
        local_ts,
        seq: raw.last_update_id,
        payload: MarketPayload::BookDelta(BookDelta {
            bids,
            asks,
            prev_seq,
        }),
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

// ---------- wire-level shapes ----------

#[derive(Debug, Deserialize)]
struct RawTrade {
    #[serde(rename = "E")]
    #[allow(dead_code)]
    event_time_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
    #[serde(rename = "T")]
    trade_time_ms: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

#[derive(Debug, Deserialize)]
struct RawDepth {
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    last_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn btcusdt_spec() -> InstrumentSpec {
        InstrumentSpec {
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            base: "BTC".into(),
            quote: "USDT".into(),
            price_scale: 2,
            qty_scale: 8,
            min_qty: Qty(1),
            min_notional: 0,
        }
    }

    fn specs() -> HashMap<Symbol, InstrumentSpec> {
        let mut m = HashMap::new();
        m.insert(Symbol::from_static("BTCUSDT"), btcusdt_spec());
        m
    }

    #[test]
    fn decodes_trade_fixture() {
        let body = include_bytes!("../testdata/trade.json");
        let evt = decode_frame(body, &specs()).unwrap().unwrap();
        assert_eq!(evt.venue, Venue::BINANCE);
        assert_eq!(evt.symbol.as_str(), "BTCUSDT");
        assert_eq!(evt.seq, 12345);
        match evt.payload {
            MarketPayload::Trade(t) => {
                // 65000.50 @ scale 2
                assert_eq!(t.price, Price(6_500_050));
                // 0.01 @ scale 8
                assert_eq!(t.qty, Qty(1_000_000));
                // Fixture: "m": true -> buyer is maker -> taker is seller.
                assert_eq!(t.taker_side, Side::Sell);
                assert_eq!(t.id, "12345");
            }
            _ => panic!("expected Trade payload"),
        }
    }

    #[test]
    fn decodes_depth_fixture() {
        let body = include_bytes!("../testdata/depth_update.json");
        let evt = decode_frame(body, &specs()).unwrap().unwrap();
        assert_eq!(evt.seq, 200);
        match evt.payload {
            MarketPayload::BookDelta(d) => {
                assert_eq!(d.prev_seq, 99); // first_update_id 100 → prev 99
                assert_eq!(d.bids.len(), 2);
                assert_eq!(d.asks.len(), 1);
                assert_eq!(d.bids[0].price, Price(6_499_900));
                // Deletion: level with qty 0 must round-trip as Qty(0).
                assert_eq!(d.bids[1].qty, Qty(0));
            }
            _ => panic!("expected BookDelta payload"),
        }
    }

    #[test]
    fn ack_frame_returns_none() {
        let body = br#"{"result":null,"id":1}"#;
        assert!(decode_frame(body, &specs()).unwrap().is_none());
    }

    #[test]
    fn unknown_symbol_errors() {
        let body = br#"{
            "e":"trade","E":1,"s":"ETHUSDT","t":1,"p":"1","q":"1",
            "T":1,"m":false
        }"#;
        let err = decode_frame(body, &specs()).unwrap_err();
        assert!(matches!(err, BinanceError::UnknownSymbol(s) if s == "ETHUSDT"));
    }

    #[test]
    fn unsupported_event_tag_errors() {
        let body = br#"{"e":"kline","E":1,"s":"BTCUSDT"}"#;
        let err = decode_frame(body, &specs()).unwrap_err();
        assert!(matches!(err, BinanceError::Unsupported(s) if s == "kline"));
    }
}
