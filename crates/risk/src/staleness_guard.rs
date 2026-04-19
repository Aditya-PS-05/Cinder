//! Data-feed staleness guard.
//!
//! [`StalenessGuard`] watches the wall-clock gap between market-data
//! events and fires a [`StalenessBreach`] when the gap exceeds a
//! configured [`StalenessGuardConfig::max_idle`]. Quoting against a
//! frozen book is a fast way to get picked off, so the runner wires a
//! breach into the [`KillSwitch`] and halts until the feed recovers.
//!
//! The guard is dependency-free — it speaks in [`std::time::Instant`]
//! and `Duration`, matching the rest of this crate. The runner owns
//! the integration: call [`StalenessGuard::observe_event`] each time a
//! market event lands (book snapshot, delta, trade) and
//! [`StalenessGuard::check`] on a separate cadence (reconcile tick) so
//! a silent feed still produces a breach.
//!
//! [`KillSwitch`]: crate::KillSwitch

use std::time::{Duration, Instant};

use crate::kill_switch::TripReason;

/// Threshold controlling how long the feed may go silent before the
/// guard breaches. `None` disables the check.
#[derive(Clone, Copy, Debug)]
pub struct StalenessGuardConfig {
    pub max_idle: Option<Duration>,
}

impl StalenessGuardConfig {
    /// No threshold set; guard never breaches. Useful as a test
    /// baseline and the default when the operator has not configured
    /// staleness monitoring.
    pub fn permissive() -> Self {
        Self { max_idle: None }
    }
}

/// What the guard caught. The runner converts this into a
/// [`TripReason`] via [`StalenessBreach::to_trip_reason`] and trips
/// the kill switch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StalenessBreach {
    /// The gap between the last-seen event and the check exceeded
    /// `limit`.
    FeedStale {
        last_event_at: Instant,
        now: Instant,
        idle: Duration,
        limit: Duration,
    },
}

impl StalenessBreach {
    pub fn to_trip_reason(self) -> TripReason {
        TripReason::FeedStaleness
    }
}

/// Stateful guard. One instance per feed under surveillance.
#[derive(Debug)]
pub struct StalenessGuard {
    cfg: StalenessGuardConfig,
    last_event_at: Option<Instant>,
    /// Stays `true` after a breach so the same limit does not fire on
    /// every subsequent silent tick. [`StalenessGuard::reset`] clears
    /// it; [`StalenessGuard::observe_event`] also re-arms, since any
    /// fresh event means the feed is alive again.
    tripped: bool,
}

impl StalenessGuard {
    pub fn new(cfg: StalenessGuardConfig) -> Self {
        Self {
            cfg,
            last_event_at: None,
            tripped: false,
        }
    }

    pub fn config(&self) -> &StalenessGuardConfig {
        &self.cfg
    }

    /// Timestamp of the most recent event seen, or `None` before the
    /// first call.
    pub fn last_event_at(&self) -> Option<Instant> {
        self.last_event_at
    }

    /// Whether the guard is currently tripped.
    pub fn is_tripped(&self) -> bool {
        self.tripped
    }

    /// Record a fresh market event. Re-arms the guard because a live
    /// event means the feed has recovered — if the operator had not
    /// yet reset the kill switch, future checks will start fresh
    /// against the new baseline.
    pub fn observe_event(&mut self, now: Instant) {
        self.last_event_at = Some(now);
        self.tripped = false;
    }

    /// Evaluate the gap between the last event and `now`. Returns a
    /// breach at most once per silent stretch. A guard with no
    /// baseline yet (no event ever seen) never breaches — callers
    /// typically invoke `check` from a reconcile timer, and firing
    /// before the feed has even started would create a false
    /// positive on startup.
    pub fn check(&mut self, now: Instant) -> Option<StalenessBreach> {
        if self.tripped {
            return None;
        }
        let limit = self.cfg.max_idle?;
        let last = self.last_event_at?;
        let idle = now.saturating_duration_since(last);
        (idle > limit).then(|| {
            self.tripped = true;
            StalenessBreach::FeedStale {
                last_event_at: last,
                now,
                idle,
                limit,
            }
        })
    }

    /// Clear the tripped flag so the guard can breach again. Operator
    /// action only; the baseline event timestamp is retained so
    /// `check` keeps measuring against the last real event.
    pub fn reset(&mut self) {
        self.tripped = false;
    }
}

impl Default for StalenessGuard {
    fn default() -> Self {
        Self::new(StalenessGuardConfig::permissive())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(max_idle_ms: Option<u64>) -> StalenessGuardConfig {
        StalenessGuardConfig {
            max_idle: max_idle_ms.map(Duration::from_millis),
        }
    }

    #[test]
    fn permissive_guard_never_breaches() {
        let mut g = StalenessGuard::new(StalenessGuardConfig::permissive());
        let t0 = Instant::now();
        g.observe_event(t0);
        assert!(g.check(t0 + Duration::from_secs(3600)).is_none());
    }

    #[test]
    fn check_without_any_event_is_a_no_op() {
        // No baseline yet — firing would be a false startup positive.
        let mut g = StalenessGuard::new(cfg(Some(100)));
        let t0 = Instant::now();
        assert!(g.check(t0 + Duration::from_secs(10)).is_none());
    }

    #[test]
    fn breach_fires_once_after_silent_stretch() {
        let mut g = StalenessGuard::new(cfg(Some(100)));
        let t0 = Instant::now();
        g.observe_event(t0);
        assert!(g.check(t0 + Duration::from_millis(50)).is_none());
        let breach = g
            .check(t0 + Duration::from_millis(150))
            .expect("idle 150ms > limit 100ms must breach");
        match breach {
            StalenessBreach::FeedStale { idle, limit, .. } => {
                assert_eq!(idle, Duration::from_millis(150));
                assert_eq!(limit, Duration::from_millis(100));
            }
        }
        assert_eq!(breach.to_trip_reason(), TripReason::FeedStaleness);
        // Further silent ticks do not re-fire.
        assert!(g.check(t0 + Duration::from_millis(500)).is_none());
        assert!(g.is_tripped());
    }

    #[test]
    fn exact_limit_does_not_trip() {
        let mut g = StalenessGuard::new(cfg(Some(100)));
        let t0 = Instant::now();
        g.observe_event(t0);
        // Idle of exactly 100 ms — not > 100, so no breach.
        assert!(g.check(t0 + Duration::from_millis(100)).is_none());
    }

    #[test]
    fn fresh_event_rearms_and_moves_the_baseline() {
        let mut g = StalenessGuard::new(cfg(Some(100)));
        let t0 = Instant::now();
        g.observe_event(t0);
        let _ = g.check(t0 + Duration::from_millis(200)).unwrap();
        assert!(g.is_tripped());
        // New event lands — guard re-arms and uses the new baseline.
        let t1 = t0 + Duration::from_millis(300);
        g.observe_event(t1);
        assert!(!g.is_tripped());
        assert!(g.check(t1 + Duration::from_millis(50)).is_none());
        let _ = g
            .check(t1 + Duration::from_millis(150))
            .expect("idle from t1 should breach again");
    }

    #[test]
    fn manual_reset_rearms_without_moving_baseline() {
        let mut g = StalenessGuard::new(cfg(Some(100)));
        let t0 = Instant::now();
        g.observe_event(t0);
        let _ = g.check(t0 + Duration::from_millis(200)).unwrap();
        g.reset();
        assert!(!g.is_tripped());
        assert_eq!(g.last_event_at(), Some(t0));
        // Still measured against t0 — next silent check breaches again.
        let breach = g.check(t0 + Duration::from_millis(300)).unwrap();
        assert!(matches!(breach, StalenessBreach::FeedStale { .. }));
    }
}
