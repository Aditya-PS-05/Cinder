//! Shared primitives used across every trading-system service.
//!
//! The hot path moves fixed-point [`Price`] and [`Qty`] values around
//! inside a [`MarketEvent`] envelope and publishes them through a typed
//! [`bus::Bus`] that never blocks producers. No external dependencies;
//! everything here is standard library.

#![forbid(unsafe_code)]

#[cfg(feature = "serde")]
pub mod audit;
pub mod bus;
pub mod decimal;
pub mod market;
pub mod order;
pub mod time;
pub mod venue;

#[cfg(feature = "serde")]
pub use audit::AuditEvent;
pub use decimal::{DecimalError, Price, Qty};
pub use market::{
    BookDelta, BookLevel, BookSnapshot, Funding, Liquidation, MarketEvent, MarketPayload, Trade,
};
pub use order::{
    ClientOrderId, ExecReport, Fill, InvalidTransition, NewOrder, OrderKind, OrderStatus,
    TimeInForce,
};
pub use time::Timestamp;
pub use venue::{InstrumentSpec, Side, Symbol, Venue};
