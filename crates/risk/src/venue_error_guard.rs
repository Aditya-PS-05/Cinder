//! Venue-error circuit breaker.
//!
//! [`VenueErrorGuard`] watches HTTP/transport failures against a venue
//! (5xx, timeouts, connection-refused, auth rejections) — distinct from
//! order-level rejects, which the [`KillSwitch`] already counts via
//! [`KillSwitch::record_reject`]. A burst of transport errors means
//! the venue itself is unhealthy or our credentials are wrong; quoting
//! further is pointless and risks stacking up retries that land after
//! the book has moved. The runner wires a breach into the
//! [`KillSwitch`] and halts trading until the operator investigates.
//!
//! The guard is dependency-free — it speaks in [`std::time::Instant`]
//! and uses a bounded sliding window, same shape as the reject-rate
//! tracking inside [`KillSwitch`]. One instance per venue.
//!
//! [`KillSwitch`]: crate::KillSwitch
//! [`KillSwitch::record_reject`]: crate::KillSwitch::record_reject

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::kill_switch::TripReason;

/// Threshold + window controlling how many transport errors may be
/// observed before the guard breaches. `max_errors == None` disables
/// the check (permissive mode).
#[derive(Clone, Copy, Debug)]
pub struct VenueErrorGuardConfig {
    /// Maximum number of errors tolerated inside `window`. `None`
    /// disables the guard. A count *strictly greater* than this trips.
    pub max_errors: Option<usize>,
    /// Sliding window over which errors are counted.
    pub window: Duration,
}

impl VenueErrorGuardConfig {
    pub fn permissive() -> Self {
        Self {
            max_errors: None,
            window: Duration::from_secs(60),
        }
    }
}

/// What the guard caught. The runner converts this into a
/// [`TripReason::VenueErrors`] and trips the kill switch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VenueErrorBreach {
    /// More than `limit` errors fell inside the sliding `window`
    /// ending at `now`.
    RateExceeded {
        observed: usize,
        limit: usize,
        window: Duration,
        now: Instant,
    },
}

impl VenueErrorBreach {
    pub fn to_trip_reason(self) -> TripReason {
        TripReason::VenueErrors
    }
}

/// Stateful guard. One instance per venue under surveillance.
#[derive(Debug)]
pub struct VenueErrorGuard {
    cfg: VenueErrorGuardConfig,
    /// Timestamps of recent errors, in arrival order. Pruned on every
    /// `record_error` call so the deque never grows unbounded.
    errors: VecDeque<Instant>,
    /// Stays `true` after a breach so the same sliding window does not
    /// fire on every subsequent error. [`VenueErrorGuard::reset`]
    /// clears it; the operator is expected to clear the kill switch
    /// and the guard in lockstep.
    tripped: bool,
}

impl VenueErrorGuard {
    pub fn new(cfg: VenueErrorGuardConfig) -> Self {
        Self {
            cfg,
            errors: VecDeque::new(),
            tripped: false,
        }
    }

    pub fn config(&self) -> &VenueErrorGuardConfig {
        &self.cfg
    }

    pub fn is_tripped(&self) -> bool {
        self.tripped
    }

    /// Number of errors currently inside the sliding window. Primarily
    /// for tests and metrics; does not prune.
    pub fn window_len(&self) -> usize {
        self.errors.len()
    }

    /// Record a transport error observed now. Returns a breach once
    /// the count inside the window exceeds the configured limit.
    /// Further calls after a breach prune but do not re-fire; the
    /// operator clears the guard via [`Self::reset`].
    pub fn record_error(&mut self, now: Instant) -> Option<VenueErrorBreach> {
        let limit = self.cfg.max_errors?;
        self.prune(now);
        self.errors.push_back(now);
        if self.tripped {
            return None;
        }
        if self.errors.len() > limit {
            self.tripped = true;
            return Some(VenueErrorBreach::RateExceeded {
                observed: self.errors.len(),
                limit,
                window: self.cfg.window,
                now,
            });
        }
        None
    }

    /// Clear the tripped flag. Does not empty the error window —
    /// existing errors are still counted against the next breach,
    /// which means repeated resets without the underlying problem
    /// being fixed will trip again immediately on the next error.
    pub fn reset(&mut self) {
        self.tripped = false;
    }

    fn prune(&mut self, now: Instant) {
        let Some(cutoff) = now.checked_sub(self.cfg.window) else {
            return;
        };
        while let Some(&front) = self.errors.front() {
            if front < cutoff {
                self.errors.pop_front();
            } else {
                break;
            }
        }
    }
}

impl Default for VenueErrorGuard {
    fn default() -> Self {
        Self::new(VenueErrorGuardConfig::permissive())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(max: Option<usize>, window_ms: u64) -> VenueErrorGuardConfig {
        VenueErrorGuardConfig {
            max_errors: max,
            window: Duration::from_millis(window_ms),
        }
    }

    #[test]
    fn permissive_never_breaches() {
        let mut g = VenueErrorGuard::new(VenueErrorGuardConfig::permissive());
        let t0 = Instant::now();
        for i in 0..1000 {
            assert!(g.record_error(t0 + Duration::from_millis(i)).is_none());
        }
        assert!(!g.is_tripped());
    }

    #[test]
    fn breach_fires_when_count_exceeds_limit() {
        let mut g = VenueErrorGuard::new(cfg(Some(2), 1000));
        let t0 = Instant::now();
        assert!(g.record_error(t0).is_none());
        assert!(g.record_error(t0 + Duration::from_millis(10)).is_none());
        let breach = g
            .record_error(t0 + Duration::from_millis(20))
            .expect("third error inside window must breach");
        match breach {
            VenueErrorBreach::RateExceeded {
                observed,
                limit,
                window,
                ..
            } => {
                assert_eq!(observed, 3);
                assert_eq!(limit, 2);
                assert_eq!(window, Duration::from_millis(1000));
            }
        }
        assert_eq!(breach.to_trip_reason(), TripReason::VenueErrors);
        assert!(g.is_tripped());
    }

    #[test]
    fn old_errors_outside_window_do_not_count() {
        let mut g = VenueErrorGuard::new(cfg(Some(2), 100));
        let t0 = Instant::now();
        g.record_error(t0);
        g.record_error(t0 + Duration::from_millis(10));
        // Far past the window — the front entries must be pruned and
        // the third error must not trip.
        assert!(g.record_error(t0 + Duration::from_millis(500)).is_none());
        assert_eq!(g.window_len(), 1);
        assert!(!g.is_tripped());
    }

    #[test]
    fn breach_fires_at_most_once_until_reset() {
        let mut g = VenueErrorGuard::new(cfg(Some(1), 1000));
        let t0 = Instant::now();
        g.record_error(t0);
        let first = g
            .record_error(t0 + Duration::from_millis(1))
            .expect("second error must breach");
        assert!(matches!(first, VenueErrorBreach::RateExceeded { .. }));
        // Further errors within the window prune-and-push but do not
        // re-fire until reset.
        assert!(g.record_error(t0 + Duration::from_millis(2)).is_none());
        assert!(g.record_error(t0 + Duration::from_millis(3)).is_none());
        assert!(g.is_tripped());

        g.reset();
        assert!(!g.is_tripped());
        // With the window still full, the very next error breaches
        // again — operator must address the root cause, not just the
        // guard.
        let second = g
            .record_error(t0 + Duration::from_millis(4))
            .expect("post-reset error must breach since window is still full");
        assert!(matches!(second, VenueErrorBreach::RateExceeded { .. }));
    }

    #[test]
    fn zero_limit_trips_on_very_first_error() {
        // A zero tolerance means "any transport error is fatal".
        let mut g = VenueErrorGuard::new(cfg(Some(0), 1000));
        let breach = g
            .record_error(Instant::now())
            .expect("zero-tolerance must trip on first error");
        match breach {
            VenueErrorBreach::RateExceeded {
                observed, limit, ..
            } => {
                assert_eq!(observed, 1);
                assert_eq!(limit, 0);
            }
        }
    }

    #[test]
    fn window_sized_boundary_is_exclusive_on_the_old_side() {
        // Errors exactly `window` old are pruned (cutoff is inclusive
        // for "keep"); earlier ones are dropped. Test guards against
        // off-by-one regressions.
        let mut g = VenueErrorGuard::new(cfg(Some(3), 100));
        let t0 = Instant::now();
        g.record_error(t0);
        // Jump to t0 + 100ms: the t0 entry is exactly `window` old and
        // stays (prune drops only entries strictly earlier than cutoff).
        g.record_error(t0 + Duration::from_millis(100));
        assert_eq!(g.window_len(), 2);
    }
}
