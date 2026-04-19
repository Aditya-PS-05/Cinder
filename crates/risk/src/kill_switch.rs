//! Global kill switch — trip on reject-rate spikes or an operator
//! signal, block further order submission until explicitly reset.
//!
//! The switch is intentionally small: an atomic flag + reason code for
//! the fast-path check, plus a mutex-guarded ring of recent reject
//! timestamps for the rate trigger. Everything else (halt-file
//! watching, metrics export) lives in the runner so this crate stays
//! dependency-free beyond `ts-core`.
//!
//! ```text
//!   record_reject ──► window.push(now)
//!                     window.trim(now - period)
//!                     if len > threshold → trip(RejectRate)
//!
//!   operator tool ──► trip(Manual)
//!
//!   submit path   ──► if tripped() → skip, don't touch engine
//! ```

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Why the switch tripped. Use [`TripReason::from_u8`] when reading the
/// reason atomic back out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TripReason {
    /// `record_reject` fired enough times inside the configured window.
    RejectRate,
    /// Trip caller was a halt-file watcher or another operator tool.
    Manual,
    /// `trip(External)` — caller tripped from outside without a richer
    /// reason (e.g. parent process signalled a shutdown).
    External,
    /// Equity drawdown from peak exceeded the configured limit.
    MaxDrawdown,
    /// Realized-net loss since the start of the current day exceeded
    /// the configured limit.
    DailyLoss,
    /// Market-data feed went silent for longer than the configured
    /// idle threshold. Trading on stale marks is a fast way to pick
    /// off our own quotes, so we halt until the feed recovers.
    FeedStaleness,
    /// Gap between venue-reported and local timestamps exceeded the
    /// configured limit. Large skew is either our clock drifting or
    /// the venue's — either way, latency-sensitive logic (TTL, window
    /// checks, cancel-before-expiry) is unreliable and we halt.
    ClockSkew,
}

impl TripReason {
    fn as_u8(self) -> u8 {
        match self {
            TripReason::RejectRate => 1,
            TripReason::Manual => 2,
            TripReason::External => 3,
            TripReason::MaxDrawdown => 4,
            TripReason::DailyLoss => 5,
            TripReason::FeedStaleness => 6,
            TripReason::ClockSkew => 7,
        }
    }
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(TripReason::RejectRate),
            2 => Some(TripReason::Manual),
            3 => Some(TripReason::External),
            4 => Some(TripReason::MaxDrawdown),
            5 => Some(TripReason::DailyLoss),
            6 => Some(TripReason::FeedStaleness),
            7 => Some(TripReason::ClockSkew),
            _ => None,
        }
    }
}

/// Thresholds governing automatic reject-rate tripping.
#[derive(Clone, Copy, Debug)]
pub struct KillSwitchConfig {
    /// How many rejects inside [`Self::window`] trip the switch.
    pub reject_threshold: u32,
    /// Sliding window horizon. Older entries are discarded.
    pub window: Duration,
}

impl KillSwitchConfig {
    /// Trip after 10 rejects inside 5 s. A defensible default for a
    /// low-volume maker; production deployments should tune from
    /// measured reject rates.
    pub fn default_live() -> Self {
        Self {
            reject_threshold: 10,
            window: Duration::from_secs(5),
        }
    }
}

/// Process-wide kill switch. Cheap to clone via `Arc` and safe to share
/// across runner + watcher tasks.
#[derive(Debug)]
pub struct KillSwitch {
    tripped: AtomicBool,
    reason: AtomicU8,
    rejects: Mutex<VecDeque<Instant>>,
    cfg: KillSwitchConfig,
}

impl KillSwitch {
    pub fn new(cfg: KillSwitchConfig) -> Self {
        Self {
            tripped: AtomicBool::new(false),
            reason: AtomicU8::new(0),
            rejects: Mutex::new(VecDeque::new()),
            cfg,
        }
    }

    /// Fast-path check. Safe to call on the hot order submission path.
    pub fn tripped(&self) -> bool {
        self.tripped.load(Ordering::Relaxed)
    }

    /// Current trip reason, or `None` if the switch is still armed.
    pub fn reason(&self) -> Option<TripReason> {
        TripReason::from_u8(self.reason.load(Ordering::Relaxed))
    }

    /// Record a venue-side reject at `now`. If the reject count inside
    /// the configured window exceeds the threshold, trip the switch
    /// with [`TripReason::RejectRate`]. Returns `true` if this call was
    /// the one that tripped it.
    pub fn record_reject(&self, now: Instant) -> bool {
        let mut tripped_here = false;
        let cutoff = now.checked_sub(self.cfg.window).unwrap_or(now);
        let mut rejects = self.rejects.lock().expect("rejects mutex poisoned");
        while rejects.front().is_some_and(|t| *t < cutoff) {
            rejects.pop_front();
        }
        rejects.push_back(now);
        if !self.tripped() && rejects.len() as u32 > self.cfg.reject_threshold {
            drop(rejects);
            self.trip(TripReason::RejectRate);
            tripped_here = true;
        }
        tripped_here
    }

    /// Force-trip with the given reason. Idempotent: the first caller
    /// wins the reason slot so later trips do not clobber it.
    pub fn trip(&self, reason: TripReason) {
        if !self.tripped.swap(true, Ordering::Relaxed) {
            self.reason.store(reason.as_u8(), Ordering::Relaxed);
        }
    }

    /// Clear the tripped state and drop the reject window. Operator
    /// action — do not call from the submit path.
    pub fn reset(&self) {
        self.tripped.store(false, Ordering::Relaxed);
        self.reason.store(0, Ordering::Relaxed);
        self.rejects.lock().expect("rejects mutex poisoned").clear();
    }
}

impl Default for KillSwitch {
    fn default() -> Self {
        Self::new(KillSwitchConfig::default_live())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ks(threshold: u32, window_ms: u64) -> KillSwitch {
        KillSwitch::new(KillSwitchConfig {
            reject_threshold: threshold,
            window: Duration::from_millis(window_ms),
        })
    }

    #[test]
    fn fresh_switch_is_armed() {
        let s = ks(3, 100);
        assert!(!s.tripped());
        assert_eq!(s.reason(), None);
    }

    #[test]
    fn reject_threshold_trips_with_reason() {
        let s = ks(2, 1_000);
        let now = Instant::now();
        assert!(!s.record_reject(now));
        assert!(!s.record_reject(now));
        // Third reject inside the 1 s window pushes count past the threshold.
        assert!(s.record_reject(now));
        assert!(s.tripped());
        assert_eq!(s.reason(), Some(TripReason::RejectRate));
    }

    #[test]
    fn old_rejects_age_out_of_window() {
        let s = ks(2, 50);
        let past = Instant::now() - Duration::from_millis(200);
        // Park two rejects at the threshold — len == 2 is not > 2, so
        // they alone don't trip. A later record_reject with a fresh
        // `now` must trim both of them before counting, keeping the
        // switch armed.
        s.record_reject(past);
        s.record_reject(past);
        assert!(!s.tripped());
        let now = Instant::now();
        assert!(!s.record_reject(now));
        assert!(!s.tripped());
    }

    #[test]
    fn manual_trip_locks_reason_from_first_caller() {
        let s = ks(2, 1_000);
        s.trip(TripReason::Manual);
        assert!(s.tripped());
        assert_eq!(s.reason(), Some(TripReason::Manual));
        // Second trip must not overwrite the reason slot.
        s.trip(TripReason::External);
        assert_eq!(s.reason(), Some(TripReason::Manual));
    }

    #[test]
    fn reset_clears_state_and_window() {
        let s = ks(1, 1_000);
        let now = Instant::now();
        s.record_reject(now);
        s.record_reject(now);
        assert!(s.tripped());
        s.reset();
        assert!(!s.tripped());
        assert_eq!(s.reason(), None);
        // Fresh window — needs >threshold again.
        assert!(!s.record_reject(now));
        assert!(!s.tripped());
    }
}
