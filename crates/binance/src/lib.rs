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
pub mod live_engine;
pub mod order_rest;
pub mod rest;
pub mod resync;
pub mod spot;
pub mod user_stream;

pub use book::{BinanceBookSync, BookUpdate, SyncState};
pub use error::BinanceError;
pub use live_engine::{BinanceLiveEngine, BinanceOrderApi, LiveEngineError};
pub use order_rest::{
    AccountSummary, Balance, CancelOrderRequest, NewOrderRequest, OrderAck, OrderSelector,
    OrderType, QueryOrderRequest, Side, SignedClient, TimeInForce, MAINNET_BASE, TESTNET_BASE,
};
pub use rest::{fetch_depth_snapshot, parse_depth_snapshot, DepthSnapshot};
pub use resync::{AlignError, AlignState, PushOutcome, SymbolResync};
pub use spot::{SpotStreamClient, SpotStreamConfig};
pub use user_stream::{
    decode_execution_report, decode_user_stream_frame, UserDataStreamClient, UserDataStreamConfig,
    UserStreamEvent, MAINNET_USER_WS_BASE, TESTNET_USER_WS_BASE,
};
