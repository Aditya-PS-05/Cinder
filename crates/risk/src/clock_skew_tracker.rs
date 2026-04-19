//! Running-stats tracker for per-venue clock skew.
//!
//! The live runner already has a [`ClockSkewGuard`](crate::ClockSkewGuard)
//! that trips the kill switch when a single `exchange_ts - local_ts`
//! delta exceeds a configured bound. That's the enforcement layer. This
//! tracker is the *observation* layer: it maintains online mean,
//! variance, last, and max-absolute per venue so operators can see the
//! steady-state skew of the exchange's clock without having to wait for
//! a breach.
//!
//! The math is Welford's online algorithm — one pass, constant memory,
//! numerically stable — so publishing a fresh snapshot to a metrics
//! endpoint is O(1) per venue. No allocation on the hot path once the
//! venue's entry has been inserted.
//!
//! Units are milliseconds throughout. Callers pass `skew_ms` as a
//! signed `i64` so positive values mean "exchange clock is ahead of
//! local" and negatives mean "behind"; the tracker preserves the sign
//! in `mean_ms` / `last_ms` and reports the unsigned magnitude only on
//! `max_abs_ms`. The tracker never trips anything by itself — attach a
//! [`ClockSkewGuard`](crate::ClockSkewGuard) for that.

use std::collections::HashMap;

use ts_core::Venue;

/// Online Welford accumulator for skew observations on a single venue.
#[derive(Clone, Debug, Default)]
struct VenueStats {
    count: u64,
    mean_ms: f64,
    m2: f64,
    last_ms: i64,
    max_abs_ms: u64,
}

impl VenueStats {
    fn observe(&mut self, skew_ms: i64) {
        self.count += 1;
        let x = skew_ms as f64;
        let delta = x - self.mean_ms;
        self.mean_ms += delta / self.count as f64;
        let delta2 = x - self.mean_ms;
        self.m2 += delta * delta2;
        self.last_ms = skew_ms;
        let abs = skew_ms.unsigned_abs();
        if abs > self.max_abs_ms {
            self.max_abs_ms = abs;
        }
    }

    fn snapshot(&self) -> ClockSkewSnapshot {
        // Sample stddev uses `n - 1`; fall back to 0 for n <= 1 where
        // the statistic isn't defined.
        let stddev_ms = if self.count >= 2 {
            (self.m2 / (self.count - 1) as f64).sqrt()
        } else {
            0.0
        };
        ClockSkewSnapshot {
            samples: self.count,
            mean_ms: self.mean_ms,
            stddev_ms,
            last_ms: self.last_ms,
            max_abs_ms: self.max_abs_ms,
        }
    }
}

/// Immutable read of a venue's current running-stats.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClockSkewSnapshot {
    /// Count of observations since the tracker was built.
    pub samples: u64,
    /// Running mean of `exchange_ts - local_ts` in milliseconds.
    /// Positive means the venue clock is ahead of ours.
    pub mean_ms: f64,
    /// Sample standard deviation of the observed deltas. `0.0` when
    /// `samples < 2` (undefined).
    pub stddev_ms: f64,
    /// Most recent observation, preserving sign.
    pub last_ms: i64,
    /// Largest absolute-value delta seen so far. Useful for sizing a
    /// [`ClockSkewGuard`](crate::ClockSkewGuard) bound against live
    /// conditions.
    pub max_abs_ms: u64,
}

/// Per-venue clock-skew running-stats tracker. Cheap to create; the
/// map fills in lazily the first time each venue is observed.
#[derive(Clone, Debug, Default)]
pub struct ClockSkewTracker {
    stats: HashMap<Venue, VenueStats>,
}

impl ClockSkewTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an observation for `venue`. `skew_ms` is signed —
    /// positive means the venue's timestamp leads local wall-clock.
    pub fn observe(&mut self, venue: Venue, skew_ms: i64) {
        self.stats.entry(venue).or_default().observe(skew_ms);
    }

    /// Current snapshot for `venue`. `None` if no observation has ever
    /// been recorded for it.
    pub fn snapshot(&self, venue: Venue) -> Option<ClockSkewSnapshot> {
        self.stats.get(&venue).map(VenueStats::snapshot)
    }

    /// Iterate over every venue and its current snapshot. Order is not
    /// specified; callers that need determinism should collect and
    /// sort. Intended as the cheap read the metrics crate calls each
    /// scrape.
    pub fn iter(&self) -> impl Iterator<Item = (Venue, ClockSkewSnapshot)> + '_ {
        self.stats.iter().map(|(v, s)| (v.clone(), s.snapshot()))
    }

    /// Number of venues currently tracked. A venue becomes tracked on
    /// its first [`observe`](Self::observe) call.
    pub fn len(&self) -> usize {
        self.stats.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stats.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binance() -> Venue {
        Venue::BINANCE
    }

    #[test]
    fn empty_tracker_has_no_entries() {
        let t = ClockSkewTracker::new();
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
        assert!(t.snapshot(binance()).is_none());
        assert_eq!(t.iter().count(), 0);
    }

    #[test]
    fn single_sample_mean_equals_value_and_stddev_is_zero() {
        let mut t = ClockSkewTracker::new();
        t.observe(binance(), 42);
        let snap = t.snapshot(binance()).unwrap();
        assert_eq!(snap.samples, 1);
        assert!((snap.mean_ms - 42.0).abs() < 1e-9);
        assert_eq!(snap.stddev_ms, 0.0, "stddev undefined for n=1");
        assert_eq!(snap.last_ms, 42);
        assert_eq!(snap.max_abs_ms, 42);
    }

    #[test]
    fn running_mean_matches_naive_average() {
        let mut t = ClockSkewTracker::new();
        let samples = [10, 20, 30, 40, 50];
        for s in samples {
            t.observe(binance(), s);
        }
        let snap = t.snapshot(binance()).unwrap();
        let naive = samples.iter().sum::<i64>() as f64 / samples.len() as f64;
        assert_eq!(snap.samples, 5);
        assert!((snap.mean_ms - naive).abs() < 1e-9);
        assert_eq!(snap.last_ms, 50);
    }

    #[test]
    fn stddev_matches_naive_sample_variance() {
        // Cross-check Welford's online result against the textbook
        // two-pass sample variance computation on the same data.
        // Values mean = 5, sum of squared deviations = 32, sample
        // variance = 32 / (n-1) = 32/7, sample stddev ≈ 2.138.
        let samples: [i64; 8] = [2, 4, 4, 4, 5, 5, 7, 9];
        let n = samples.len() as f64;
        let mean = samples.iter().sum::<i64>() as f64 / n;
        let ssd: f64 = samples
            .iter()
            .map(|&s| {
                let d = s as f64 - mean;
                d * d
            })
            .sum();
        let expected_stddev = (ssd / (n - 1.0)).sqrt();

        let mut t = ClockSkewTracker::new();
        for s in samples {
            t.observe(binance(), s);
        }
        let snap = t.snapshot(binance()).unwrap();
        assert!(
            (snap.stddev_ms - expected_stddev).abs() < 1e-9,
            "got {} expected {expected_stddev}",
            snap.stddev_ms
        );
    }

    #[test]
    fn max_abs_tracks_unsigned_magnitude_of_both_polarities() {
        let mut t = ClockSkewTracker::new();
        t.observe(binance(), 5);
        t.observe(binance(), -12);
        t.observe(binance(), 3);
        // Negative has the largest magnitude.
        let snap = t.snapshot(binance()).unwrap();
        assert_eq!(snap.max_abs_ms, 12);
        // Last preserves sign.
        assert_eq!(snap.last_ms, 3);
    }

    #[test]
    fn max_abs_never_regresses() {
        let mut t = ClockSkewTracker::new();
        t.observe(binance(), 100);
        t.observe(binance(), 10);
        t.observe(binance(), -20);
        assert_eq!(t.snapshot(binance()).unwrap().max_abs_ms, 100);
    }

    #[test]
    fn multiple_venues_are_isolated() {
        let mut t = ClockSkewTracker::new();
        t.observe(binance(), 5);
        t.observe(Venue::COINBASE, -100);
        t.observe(binance(), 15);

        let b = t.snapshot(binance()).unwrap();
        let c = t.snapshot(Venue::COINBASE).unwrap();

        assert_eq!(b.samples, 2);
        assert!((b.mean_ms - 10.0).abs() < 1e-9);
        assert_eq!(b.max_abs_ms, 15);

        assert_eq!(c.samples, 1);
        assert_eq!(c.last_ms, -100);
        assert_eq!(c.max_abs_ms, 100);

        assert_eq!(t.len(), 2);
    }

    #[test]
    fn iter_surfaces_every_tracked_venue() {
        let mut t = ClockSkewTracker::new();
        t.observe(binance(), 1);
        t.observe(Venue::COINBASE, 2);
        let mut seen: Vec<_> = t.iter().map(|(v, _)| v).collect();
        seen.sort_by_key(|v| v.as_str().to_string());
        assert_eq!(seen, vec![Venue::BINANCE, Venue::COINBASE]);
    }

    #[test]
    fn zero_skew_is_recorded_as_a_sample_not_a_no_op() {
        let mut t = ClockSkewTracker::new();
        t.observe(binance(), 0);
        let snap = t.snapshot(binance()).unwrap();
        assert_eq!(snap.samples, 1);
        assert_eq!(snap.mean_ms, 0.0);
        assert_eq!(snap.last_ms, 0);
        assert_eq!(snap.max_abs_ms, 0);
    }

    #[test]
    fn large_magnitudes_do_not_overflow_running_stats() {
        // Feed extreme values near i64 bounds to make sure the f64
        // accumulator keeps the stats usable rather than returning NaN
        // or Inf. Can't test exact equality (f64 precision), but mean
        // must stay finite and match sign.
        let mut t = ClockSkewTracker::new();
        t.observe(binance(), i64::MAX / 2);
        t.observe(binance(), i64::MIN / 2);
        let snap = t.snapshot(binance()).unwrap();
        assert!(snap.mean_ms.is_finite());
        assert!(snap.stddev_ms.is_finite());
        assert_eq!(snap.samples, 2);
    }

    #[test]
    fn new_observations_mutate_snapshot_monotonically_in_sample_count() {
        // `samples` must strictly increase with each observe call.
        let mut t = ClockSkewTracker::new();
        let mut last_count = 0;
        for i in 0..10 {
            t.observe(binance(), i);
            let c = t.snapshot(binance()).unwrap().samples;
            assert!(c > last_count, "samples went backward: {last_count} -> {c}");
            last_count = c;
        }
    }
}
