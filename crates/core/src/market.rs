//! Canonical market data envelope and payload variants.

use crate::decimal::{Price, Qty};
use crate::time::Timestamp;
use crate::venue::{Side, Symbol, Venue};

/// Wire envelope every market data producer emits. `exchange_ts` is the
/// venue-reported timestamp; `local_ts` is wall-clock at the moment the
/// connector decoded the message. Their delta is the visible latency for
/// that hop.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MarketEvent {
    pub venue: Venue,
    pub symbol: Symbol,
    pub exchange_ts: Timestamp,
    pub local_ts: Timestamp,
    /// Venue-reported sequence number (0 if the venue does not provide one).
    pub seq: u64,
    pub payload: MarketPayload,
}

impl MarketEvent {
    /// Local-minus-exchange timestamp in nanoseconds. Returns 0 if either
    /// timestamp is unset. Negative values indicate clock skew and are
    /// surfaced as-is so callers can alarm on them.
    pub fn latency_nanos(&self) -> i64 {
        if self.exchange_ts.is_unset() || self.local_ts.is_unset() {
            return 0;
        }
        self.local_ts.0 - self.exchange_ts.0
    }
}

/// Sum type over every payload that can flow through the market data bus.
/// Rust's native enum gives us exhaustive matching for free.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "kind", rename_all = "snake_case"))]
pub enum MarketPayload {
    Trade(Trade),
    BookSnapshot(BookSnapshot),
    BookDelta(BookDelta),
    Funding(Funding),
    Liquidation(Liquidation),
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Trade {
    pub id: String,
    pub price: Price,
    pub qty: Qty,
    /// Which side initiated the trade.
    pub taker_side: Side,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BookLevel {
    pub price: Price,
    pub qty: Qty,
}

/// Full order book snapshot. Bids are sorted descending, asks ascending.
/// Producers enforce the invariant; consumers may rely on it.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BookSnapshot {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

/// Incremental book update. A level with `qty == 0` means "delete".
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BookDelta {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    /// Sequence this delta builds on (0 if the venue does not chain).
    pub prev_seq: u64,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Funding {
    /// Signed fraction; 0.0001 = 1 bp per funding interval.
    pub rate: f64,
    pub next_funding_time: Timestamp,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Liquidation {
    pub price: Price,
    pub qty: Qty,
    /// Side that was liquidated (a long liquidation reports [`Side::Buy`]).
    pub side: Side,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(payload: MarketPayload) -> MarketEvent {
        MarketEvent {
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            exchange_ts: Timestamp::from_unix_millis(1_700_000_000_000),
            local_ts: Timestamp::from_unix_millis(1_700_000_000_015),
            seq: 42,
            payload,
        }
    }

    #[test]
    fn latency_nanos_simple() {
        let e = sample_event(MarketPayload::Trade(Trade {
            id: "t1".into(),
            price: Price(6_500_000),
            qty: Qty(1_000_000),
            taker_side: Side::Buy,
        }));
        assert_eq!(e.latency_nanos(), 15_000_000); // 15 ms in ns
    }

    #[test]
    fn latency_zero_when_unset() {
        let mut e = sample_event(MarketPayload::Trade(Trade {
            id: "t1".into(),
            price: Price(0),
            qty: Qty(0),
            taker_side: Side::Unknown,
        }));
        e.exchange_ts = Timestamp::default();
        assert_eq!(e.latency_nanos(), 0);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn json_roundtrip_preserves_payload() {
        let e = sample_event(MarketPayload::BookSnapshot(BookSnapshot {
            bids: vec![BookLevel {
                price: Price(99),
                qty: Qty(1),
            }],
            asks: vec![BookLevel {
                price: Price(101),
                qty: Qty(1),
            }],
        }));
        let j = serde_json::to_string(&e).expect("serialize");
        let back: MarketEvent = serde_json::from_str(&j).expect("deserialize");
        assert_eq!(back.seq, e.seq);
        assert_eq!(back.symbol.as_str(), e.symbol.as_str());
        match back.payload {
            MarketPayload::BookSnapshot(bs) => {
                assert_eq!(bs.bids[0].price, Price(99));
                assert_eq!(bs.asks[0].price, Price(101));
            }
            _ => panic!("payload variant drifted through roundtrip"),
        }
    }

    #[test]
    fn payload_match_is_exhaustive() {
        // This test exists purely to assert that the match is exhaustive
        // at compile time. Adding a variant without updating this match
        // will break the build.
        let variants = [
            MarketPayload::Trade(Trade {
                id: "t".into(),
                price: Price(1),
                qty: Qty(1),
                taker_side: Side::Buy,
            }),
            MarketPayload::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(99),
                    qty: Qty(1),
                }],
                asks: vec![BookLevel {
                    price: Price(101),
                    qty: Qty(1),
                }],
            }),
            MarketPayload::BookDelta(BookDelta::default()),
            MarketPayload::Funding(Funding {
                rate: 0.0001,
                next_funding_time: Timestamp::default(),
            }),
            MarketPayload::Liquidation(Liquidation {
                price: Price(100),
                qty: Qty(1),
                side: Side::Sell,
            }),
        ];
        let mut seen = 0u8;
        for v in variants {
            match v {
                MarketPayload::Trade(_) => seen |= 1,
                MarketPayload::BookSnapshot(_) => seen |= 2,
                MarketPayload::BookDelta(_) => seen |= 4,
                MarketPayload::Funding(_) => seen |= 8,
                MarketPayload::Liquidation(_) => seen |= 16,
            }
        }
        assert_eq!(seen, 0b11111);
    }
}
