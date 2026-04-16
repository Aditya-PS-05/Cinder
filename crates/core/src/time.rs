//! Wall-clock timestamps used on the market data bus.
//!
//! Stored as a single `i64` nanoseconds since the Unix epoch. Zero means
//! "unset" — connectors that lack an exchange timestamp leave it at zero
//! so [`MarketEvent::latency`] can distinguish "no data" from "negative
//! skew" without a separate Option.
//!
//! [`MarketEvent::latency`]: crate::market::MarketEvent::latency

use std::time::{SystemTime, UNIX_EPOCH};

/// Nanoseconds since the Unix epoch. Zero sentinels "unset".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// Current wall-clock time.
    pub fn now() -> Self {
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Timestamp(d.as_nanos() as i64)
    }

    pub fn from_unix_millis(ms: i64) -> Self {
        Timestamp(ms * 1_000_000)
    }

    pub fn from_unix_micros(us: i64) -> Self {
        Timestamp(us * 1_000)
    }

    pub fn from_unix_nanos(ns: i64) -> Self {
        Timestamp(ns)
    }

    pub fn as_unix_nanos(self) -> i64 {
        self.0
    }

    pub fn is_unset(self) -> bool {
        self.0 == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_conversions() {
        assert_eq!(
            Timestamp::from_unix_millis(1_700_000_000_500).0,
            1_700_000_000_500_000_000
        );
        assert_eq!(
            Timestamp::from_unix_micros(1_700_000_000_500_000).0,
            1_700_000_000_500_000_000
        );
        assert_eq!(Timestamp::from_unix_nanos(1).0, 1);
    }

    #[test]
    fn default_is_unset() {
        assert!(Timestamp::default().is_unset());
    }

    #[test]
    fn now_is_after_unset() {
        assert!(Timestamp::now() > Timestamp::default());
    }
}
