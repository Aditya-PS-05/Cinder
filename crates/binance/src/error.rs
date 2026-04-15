//! Error type for the Binance connector.

use thiserror::Error;

use ts_core::DecimalError;

#[derive(Debug, Error)]
pub enum BinanceError {
    #[error("websocket error: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),

    #[error("json decode error: {0}")]
    Json(#[from] serde_json::Error),

    /// Frame referenced a symbol the caller did not register a spec for.
    /// Dropping such a frame is almost always the right call — we cannot
    /// quantize price/qty without knowing the instrument's scales.
    #[error("unknown symbol: {0}")]
    UnknownSymbol(String),

    #[error("decimal: {0}")]
    Decimal(#[from] DecimalError),

    /// Frame parsed cleanly but the connector has no mapping for the
    /// reported `e` tag. Returned as an error so callers can log-and-skip
    /// rather than silently drop.
    #[error("unsupported event tag: {0}")]
    Unsupported(String),
}

impl From<tokio_tungstenite::tungstenite::Error> for BinanceError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        BinanceError::WebSocket(Box::new(e))
    }
}
