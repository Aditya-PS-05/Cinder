//! Binance spot market-data connector.
//!
//! Exposes three layers the rest of the workspace consumes:
//!
//! * [`decode`] — pure JSON → [`ts_core::MarketEvent`] transformation.
//!   No I/O, easy to unit-test against fixture frames.
//! * [`spot`] — WebSocket session plus reconnect loop. Reads frames,
//!   hands them to the decoder, publishes onto a [`ts_core::bus::Bus`].
//! * A thin CLI (`ts-md-binance`) that wires the two together against
//!   a user-provided symbol list.
//!
//! The decoder takes a `HashMap<Symbol, InstrumentSpec>` so price/qty
//! mantissas land at the correct fixed-point scale for each instrument.

#![forbid(unsafe_code)]

pub mod book;
pub mod decode;
pub mod error;
pub mod rest;
pub mod spot;

pub use book::{BinanceBookSync, BookUpdate, SyncState};
pub use error::BinanceError;
pub use rest::{fetch_depth_snapshot, parse_depth_snapshot, DepthSnapshot};
pub use spot::{SpotStreamClient, SpotStreamConfig};
