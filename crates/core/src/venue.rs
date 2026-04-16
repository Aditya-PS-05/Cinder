//! Venues, symbols, sides, and per-instrument quantization metadata.

use std::borrow::Cow;
use std::fmt;

use crate::decimal::Qty;

/// Venue identifies a trading venue. Backed by a `Cow<'static, str>` so
/// well-known venues live in the binary without allocation, but runtime
/// discoveries can still be constructed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Venue(Cow<'static, str>);

impl Venue {
    // ----- CEX -----
    pub const BINANCE: Venue = Venue(Cow::Borrowed("binance"));
    pub const BINANCE_FUTURES: Venue = Venue(Cow::Borrowed("binance-futures"));
    pub const COINBASE: Venue = Venue(Cow::Borrowed("coinbase"));
    pub const OKX: Venue = Venue(Cow::Borrowed("okx"));
    pub const BYBIT: Venue = Venue(Cow::Borrowed("bybit"));
    pub const DERIBIT: Venue = Venue(Cow::Borrowed("deribit"));

    // ----- DEX -----
    pub const UNISWAP_V2: Venue = Venue(Cow::Borrowed("uniswap-v2"));
    pub const UNISWAP_V3: Venue = Venue(Cow::Borrowed("uniswap-v3"));
    pub const CURVE: Venue = Venue(Cow::Borrowed("curve"));
    pub const BALANCER: Venue = Venue(Cow::Borrowed("balancer"));
    pub const AERODROME: Venue = Venue(Cow::Borrowed("aerodrome"));

    pub fn new(name: impl Into<String>) -> Self {
        Venue(Cow::Owned(name.into()))
    }
    pub const fn from_static(name: &'static str) -> Self {
        Venue(Cow::Borrowed(name))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Venue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Venue-local instrument identifier (e.g. `"BTCUSDT"` on Binance, a pool
/// address on a DEX).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Symbol(Cow<'static, str>);

impl Symbol {
    pub fn new(s: impl Into<String>) -> Self {
        Symbol(Cow::Owned(s.into()))
    }
    pub const fn from_static(s: &'static str) -> Self {
        Symbol(Cow::Borrowed(s))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Trade direction / order side.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(u8)]
pub enum Side {
    #[default]
    Unknown = 0,
    Buy = 1,
    Sell = 2,
}

impl Side {
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
            Side::Unknown => Side::Unknown,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
            Side::Unknown => "unknown",
        })
    }
}

/// Quantization rules and metadata for a single instrument on a single venue.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstrumentSpec {
    pub venue: Venue,
    pub symbol: Symbol,
    pub base: String,  // e.g. "BTC"
    pub quote: String, // e.g. "USDT"
    pub price_scale: u8,
    pub qty_scale: u8,
    pub min_qty: Qty,
    /// Minimum notional in units of `10^-(price_scale + qty_scale)`. 0 if none.
    pub min_notional: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn venue_constants_and_runtime() {
        assert_eq!(Venue::BINANCE.as_str(), "binance");
        assert_eq!(Venue::UNISWAP_V3.as_str(), "uniswap-v3");

        let runtime = Venue::new("hyperliquid");
        assert_eq!(runtime.as_str(), "hyperliquid");
        assert_eq!(format!("{runtime}"), "hyperliquid");
    }

    #[test]
    fn symbol_from_static_is_const() {
        const BTCUSDT: Symbol = Symbol::from_static("BTCUSDT");
        assert_eq!(BTCUSDT.as_str(), "BTCUSDT");
    }

    #[test]
    fn side_opposite_and_display() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
        assert_eq!(Side::Unknown.opposite(), Side::Unknown);
        assert_eq!(format!("{}", Side::Buy), "buy");
        assert_eq!(format!("{}", Side::Sell), "sell");
    }

    #[test]
    fn instrument_spec_construction() {
        let spec = InstrumentSpec {
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            base: "BTC".into(),
            quote: "USDT".into(),
            price_scale: 2,
            qty_scale: 6,
            min_qty: Qty(1),
            min_notional: 0,
        };
        assert_eq!(spec.venue, Venue::BINANCE);
        assert_eq!(spec.symbol.as_str(), "BTCUSDT");
    }
}
