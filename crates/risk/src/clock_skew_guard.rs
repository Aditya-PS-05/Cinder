//! Clock-skew guard.
//!
//! [`ClockSkewGuard`] watches the absolute gap between a venue-reported
//! timestamp (e.g. `MarketEvent::exchange_ts`) and the local wall-clock
//! timestamp the connector stamped when the message was decoded
//! (e.g. `MarketEvent::local_ts`). When that gap exceeds the configured
//! [`ClockSkewGuardConfig::max_abs_skew`], [`ClockSkewGuard::observe`]
//! returns a [`ClockSkewBreach`] which the runner converts into a
//! [`TripReason::ClockSkew`] and uses to halt trading.
//!
//! The guard measures an absolute skew — either direction is alarming:
//!
//! * Large positive skew (local far ahead of exchange) — our clock
//!   has jumped forward (NTP step, VM pause, processing stall).
//! * Large negative skew (local behind exchange) — our clock is
//!   dragging or the venue is publishing timestamps from the future.
//!
//! Either way, latency-sensitive logic (TTL checks, cancel-before-
//! expiry windows, staleness math) is unreliable, so we halt.
//!
//! The guard is dependency-free; it speaks in [`ts_core::Timestamp`]
//! nanoseconds and `Duration`, matching the rest of this crate. The
//! runner calls [`ClockSkewGuard::observe`] on each market event.

use std::time::Duration;

use ts_core::Timestamp;

use crate::kill_switch::TripReason;

/// Threshold controlling how much absolute skew is tolerated before
/// the guard breaches. `None` disables the check.
#[derive(Clone, Copy, Debug)]
pub struct ClockSkewGuardConfig {
    pub max_abs_skew: Option<Duration>,
}

impl ClockSkewGuardConfig {
    /// No threshold; guard never breaches. Useful as a test baseline
    /// and the default when staleness monitoring is unconfigured.
    pub fn permissive() -> Self {
        Self { max_abs_skew: None }
    }
}

/// What the guard caught. The runner converts this into a
/// [`TripReason`] via [`ClockSkewBreach::to_trip_reason`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockSkewBreach {
    /// `|local - exchange|` exceeded `limit`. `skew_nanos` is signed
    /// so operators can tell which direction drifted.
    SkewExceeded {
        exchange_ts: Timestamp,
        local_ts: Timestamp,
        skew_nanos: i64,
        limit_nanos: i64,
    },
}

impl ClockSkewBreach {
    pub fn to_trip_reason(self) -> TripReason {
        TripReason::ClockSkew
    }
}

/// Stateful guard. One instance per feed under surveillance.
#[derive(Debug)]
pub struct ClockSkewGuard {
    cfg: ClockSkewGuardConfig,
    /// Stays `true` after a breach so the same limit does not fire on
    /// every subsequent event. [`ClockSkewGuard::reset`] clears it —
    /// unlike the staleness guard, fresh events do not auto-rearm,
    /// since a persistent drift would keep re-firing and drown the
    /// log otherwise.
    tripped: bool,
    /// Most recent observed skew, in nanoseconds. `None` before the
    /// first observation. Exposed for dashboards.
    last_skew_nanos: Option<i64>,
}

impl ClockSkewGuard {
    pub fn new(cfg: ClockSkewGuardConfig) -> Self {
        Self {
            cfg,
            tripped: false,
            last_skew_nanos: None,
        }
    }

    pub fn config(&self) -> &ClockSkewGuardConfig {
        &self.cfg
    }

    pub fn is_tripped(&self) -> bool {
        self.tripped
    }

    /// Most recently observed skew (local − exchange, in nanoseconds).
    /// `None` before the first observation. Retained even across
    /// tripped state so operators can see the drift magnitude.
    pub fn last_skew_nanos(&self) -> Option<i64> {
        self.last_skew_nanos
    }

    /// Fold one event's timestamp pair through the guard. Returns a
    /// breach at most once per trip epoch. Events with an unset
    /// timestamp on either side are ignored — we cannot compute a
    /// meaningful skew without both sides — rather than surfacing as
    /// false positives.
    pub fn observe(
        &mut self,
        exchange_ts: Timestamp,
        local_ts: Timestamp,
    ) -> Option<ClockSkewBreach> {
        if exchange_ts.is_unset() || local_ts.is_unset() {
            return None;
        }
        let skew = local_ts.0.saturating_sub(exchange_ts.0);
        self.last_skew_nanos = Some(skew);
        if self.tripped {
            return None;
        }
        let limit = self.cfg.max_abs_skew?;
        let limit_nanos = i64::try_from(limit.as_nanos()).unwrap_or(i64::MAX);
        if skew.unsigned_abs() > limit_nanos.unsigned_abs() {
            self.tripped = true;
            return Some(ClockSkewBreach::SkewExceeded {
                exchange_ts,
                local_ts,
                skew_nanos: skew,
                limit_nanos,
            });
        }
        None
    }

    /// Clear the tripped flag so the guard can breach again. Operator
    /// action only; the last-skew reading is retained.
    pub fn reset(&mut self) {
        self.tripped = false;
    }
}

impl Default for ClockSkewGuard {
    fn default() -> Self {
        Self::new(ClockSkewGuardConfig::permissive())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts_ns(nanos: i64) -> Timestamp {
        Timestamp(nanos)
    }

    fn cfg(max_abs_ns: Option<i64>) -> ClockSkewGuardConfig {
        ClockSkewGuardConfig {
            max_abs_skew: max_abs_ns.map(|n| Duration::from_nanos(n as u64)),
        }
    }

    #[test]
    fn permissive_guard_never_breaches() {
        let mut g = ClockSkewGuard::new(ClockSkewGuardConfig::permissive());
        assert!(g.observe(ts_ns(1_000_000), ts_ns(1_500_000_000)).is_none());
    }

    #[test]
    fn unset_timestamps_are_ignored() {
        let mut g = ClockSkewGuard::new(cfg(Some(1_000)));
        // Default Timestamp is unset; guard should treat these as missing.
        assert!(g.observe(Timestamp::default(), ts_ns(5_000_000)).is_none());
        assert!(g.observe(ts_ns(5_000_000), Timestamp::default()).is_none());
        // last_skew stays None because nothing meaningful was observed.
        assert!(g.last_skew_nanos().is_none());
    }

    #[test]
    fn positive_skew_past_limit_breaches_once() {
        // 2 ms limit; local is 5 ms ahead of exchange.
        let mut g = ClockSkewGuard::new(cfg(Some(2_000_000)));
        let breach = g
            .observe(ts_ns(1_000_000_000), ts_ns(1_005_000_000))
            .expect("5ms skew > 2ms limit must breach");
        match breach {
            ClockSkewBreach::SkewExceeded {
                skew_nanos,
                limit_nanos,
                ..
            } => {
                assert_eq!(skew_nanos, 5_000_000);
                assert_eq!(limit_nanos, 2_000_000);
            }
        }
        assert_eq!(breach.to_trip_reason(), TripReason::ClockSkew);
        // Further events do not re-fire even if they still exceed.
        assert!(g
            .observe(ts_ns(1_000_000_000), ts_ns(1_100_000_000))
            .is_none());
        assert!(g.is_tripped());
        // last_skew still tracks even once tripped — operators want the
        // live reading for dashboards.
        assert_eq!(g.last_skew_nanos(), Some(100_000_000));
    }

    #[test]
    fn negative_skew_past_limit_also_breaches() {
        // Our local clock is 5 ms behind exchange.
        let mut g = ClockSkewGuard::new(cfg(Some(2_000_000)));
        let breach = g
            .observe(ts_ns(1_005_000_000), ts_ns(1_000_000_000))
            .expect("|-5ms| > 2ms limit must breach");
        match breach {
            ClockSkewBreach::SkewExceeded { skew_nanos, .. } => {
                assert_eq!(skew_nanos, -5_000_000);
            }
        }
    }

    #[test]
    fn within_limit_does_not_breach() {
        // 2 ms limit; 1 ms positive skew is fine.
        let mut g = ClockSkewGuard::new(cfg(Some(2_000_000)));
        assert!(g
            .observe(ts_ns(1_000_000_000), ts_ns(1_001_000_000))
            .is_none());
        assert_eq!(g.last_skew_nanos(), Some(1_000_000));
    }

    #[test]
    fn exact_limit_does_not_breach() {
        let mut g = ClockSkewGuard::new(cfg(Some(2_000_000)));
        // Exactly 2 ms skew — equal is not greater, so no breach.
        assert!(g
            .observe(ts_ns(1_000_000_000), ts_ns(1_002_000_000))
            .is_none());
    }

    #[test]
    fn reset_rearms_without_dropping_history() {
        let mut g = ClockSkewGuard::new(cfg(Some(1_000_000)));
        let _ = g
            .observe(ts_ns(1_000_000_000), ts_ns(1_005_000_000))
            .unwrap();
        assert!(g.is_tripped());
        g.reset();
        assert!(!g.is_tripped());
        // Last skew retained.
        assert_eq!(g.last_skew_nanos(), Some(5_000_000));
        // Still outside limit → breaches again after reset.
        let breach = g
            .observe(ts_ns(1_000_000_000), ts_ns(1_005_000_000))
            .unwrap();
        assert!(matches!(breach, ClockSkewBreach::SkewExceeded { .. }));
    }
}
