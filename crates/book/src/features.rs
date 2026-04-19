//! Reusable feature primitives for strategy / signal code.
//!
//! Three stateful building blocks live here:
//!
//! * [`Ewma`] — generic exponential moving average over any `f64` stream
//!   (microprice, imbalance, trade intensity, …). The first observation
//!   seeds the estimate directly so cold-start readings are unbiased.
//! * [`RollingWindow`] — fixed-capacity FIFO of `f64` samples with O(1)
//!   running sum and sum-of-squares, from which `mean`, `variance`, and
//!   `stddev` are served without re-scanning the buffer.
//! * [`Vpin`] — bulk-volume VPIN estimator. Signed trade flow is packed
//!   into equal-volume buckets; each completed bucket emits an imbalance
//!   reading, and VPIN is the mean imbalance across the most recent
//!   `window` buckets.
//!
//! The volatility primitive [`crate::vol::EwmaVol`] remains specialized
//! to mid-price tick deltas — this module is for the non-volatility
//! features a feature pipeline typically wants alongside it.

use std::collections::VecDeque;

use ts_core::Side;

/// Generic exponential moving average.
///
/// ```text
///   ema_t = alpha * x_t + (1 - alpha) * ema_{t-1}
/// ```
///
/// `alpha` must lie in `(0, 1]`. `alpha == 1` is valid and produces an
/// estimator that always equals the last observation (useful as a
/// degenerate baseline in tests). The first observation seeds the value
/// directly — subsequent observations blend via the recursion.
#[derive(Debug, Clone)]
pub struct Ewma {
    alpha: f64,
    value: f64,
    samples: u64,
}

impl Ewma {
    /// Create an EMA with the given smoothing factor.
    ///
    /// Panics if `alpha` is not in `(0, 1]`. The smoothing factor is a
    /// development-time invariant, not a runtime input, so validation
    /// lives at construction rather than each `update`.
    pub fn new(alpha: f64) -> Self {
        assert!(
            alpha > 0.0 && alpha <= 1.0,
            "Ewma::new: alpha must be in (0, 1], got {alpha}"
        );
        Self {
            alpha,
            value: 0.0,
            samples: 0,
        }
    }

    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    pub fn samples(&self) -> u64 {
        self.samples
    }

    /// Fold `x` into the running average.
    pub fn update(&mut self, x: f64) {
        if self.samples == 0 {
            self.value = x;
        } else {
            self.value = self.alpha * x + (1.0 - self.alpha) * self.value;
        }
        self.samples += 1;
    }

    /// Current estimate. `None` until the first `update`.
    pub fn value(&self) -> Option<f64> {
        (self.samples > 0).then_some(self.value)
    }

    /// Drop all running state but keep the configured smoothing factor.
    pub fn reset(&mut self) {
        self.value = 0.0;
        self.samples = 0;
    }
}

/// Fixed-capacity FIFO of `f64` samples with O(1) moment queries.
///
/// Each push that would exceed `cap` evicts the oldest sample; the
/// running `sum` and `sum_sq` are corrected in constant time so
/// `mean` / `variance` / `stddev` never walk the buffer.
///
/// `variance` and `stddev` return a *population* statistic (divide by
/// `n`, not `n - 1`). This matches how feature pipelines typically feed
/// normalization layers.
#[derive(Debug, Clone)]
pub struct RollingWindow {
    cap: usize,
    buf: VecDeque<f64>,
    sum: f64,
    sum_sq: f64,
}

impl RollingWindow {
    /// Create a rolling window that holds the most recent `cap` samples.
    /// Panics if `cap == 0` — a zero-cap window cannot produce any
    /// statistic, so it's rejected up front rather than silently no-op'd.
    pub fn new(cap: usize) -> Self {
        assert!(cap > 0, "RollingWindow::new: cap must be positive");
        Self {
            cap,
            buf: VecDeque::with_capacity(cap),
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.buf.len() >= self.cap
    }

    /// Append a sample; evict the oldest if already at capacity.
    pub fn push(&mut self, x: f64) {
        if self.buf.len() >= self.cap {
            let old = self.buf.pop_front().expect("cap > 0");
            self.sum -= old;
            self.sum_sq -= old * old;
        }
        self.buf.push_back(x);
        self.sum += x;
        self.sum_sq += x * x;
    }

    pub fn mean(&self) -> Option<f64> {
        if self.buf.is_empty() {
            return None;
        }
        Some(self.sum / self.buf.len() as f64)
    }

    /// Population variance. `None` until the window has at least 2
    /// samples (a single-sample population variance is zero but also
    /// meaningless; `None` forces the caller to handle the cold-start).
    pub fn variance(&self) -> Option<f64> {
        if self.buf.len() < 2 {
            return None;
        }
        let n = self.buf.len() as f64;
        let mean = self.sum / n;
        // Clamp at 0 — floating-point roundoff can produce a tiny
        // negative value when all samples are effectively equal.
        Some(((self.sum_sq / n) - mean * mean).max(0.0))
    }

    pub fn stddev(&self) -> Option<f64> {
        self.variance().map(f64::sqrt)
    }

    /// Iterate over the buffered samples, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = &f64> {
        self.buf.iter()
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.sum = 0.0;
        self.sum_sq = 0.0;
    }
}

/// Volume-synchronized probability of informed trading.
///
/// Trades are classified by their `taker_side` and packed into equal-
/// volume buckets of size [`Vpin::bucket_vol`]. When a bucket fills,
/// its imbalance `|buy - sell| / bucket_vol` is pushed onto a rolling
/// window of the most recent `window` buckets; [`Vpin::value`] returns
/// the mean of that window.
///
/// Unknown-side trades (`Side::Unknown`) are ignored — the estimator
/// needs a side label per trade and cannot synthesize one without
/// falling back to a price-based BVC scheme. Trades that would overshoot
/// the current bucket are split across bucket boundaries so no volume
/// is lost and bucket sizes stay exact.
#[derive(Debug, Clone)]
pub struct Vpin {
    bucket_vol: i64,
    window: usize,
    cur_buy: i64,
    cur_sell: i64,
    imbalances: VecDeque<f64>,
    completed: u64,
}

impl Vpin {
    /// Create a fresh estimator with the given bucket volume (in the
    /// caller's own quantity units) and a rolling window of `window`
    /// buckets. Panics if either argument is zero.
    pub fn new(bucket_vol: i64, window: usize) -> Self {
        assert!(
            bucket_vol > 0,
            "Vpin::new: bucket_vol must be positive, got {bucket_vol}"
        );
        assert!(window > 0, "Vpin::new: window must be positive");
        Self {
            bucket_vol,
            window,
            cur_buy: 0,
            cur_sell: 0,
            imbalances: VecDeque::with_capacity(window),
            completed: 0,
        }
    }

    pub fn bucket_vol(&self) -> i64 {
        self.bucket_vol
    }

    pub fn window(&self) -> usize {
        self.window
    }

    /// Total buckets completed over the estimator's lifetime.
    pub fn completed(&self) -> u64 {
        self.completed
    }

    /// Volume packed into the current (not-yet-complete) bucket.
    pub fn pending_volume(&self) -> i64 {
        self.cur_buy + self.cur_sell
    }

    /// Feed one trade. `qty` must be positive; zero-qty and unknown-side
    /// trades are dropped. The trade is split across bucket boundaries
    /// if it would overshoot the current bucket so that the VPIN
    /// accounting stays exact.
    pub fn ingest(&mut self, side: Side, qty: i64) {
        if qty <= 0 {
            return;
        }
        let mut remaining = qty;
        while remaining > 0 {
            let capacity = self.bucket_vol - self.pending_volume();
            // Defensive: pending_volume should never exceed bucket_vol,
            // but guard against it so we never take a negative chunk.
            if capacity <= 0 {
                self.complete_bucket();
                continue;
            }
            let take = remaining.min(capacity);
            match side {
                Side::Buy => self.cur_buy += take,
                Side::Sell => self.cur_sell += take,
                Side::Unknown => return,
            }
            remaining -= take;
            if self.pending_volume() >= self.bucket_vol {
                self.complete_bucket();
            }
        }
    }

    /// VPIN estimate: mean bucket imbalance across the rolling window.
    /// `None` until the first bucket has completed.
    pub fn value(&self) -> Option<f64> {
        if self.imbalances.is_empty() {
            return None;
        }
        let sum: f64 = self.imbalances.iter().sum();
        Some(sum / self.imbalances.len() as f64)
    }

    /// Most recent completed bucket imbalance (oldest-first order for
    /// the full window is available via [`Vpin::bucket_imbalances`]).
    pub fn last_bucket(&self) -> Option<f64> {
        self.imbalances.back().copied()
    }

    /// Snapshot the rolling window of bucket imbalances, oldest first.
    pub fn bucket_imbalances(&self) -> Vec<f64> {
        self.imbalances.iter().copied().collect()
    }

    /// Drop all running state but keep bucket configuration.
    pub fn reset(&mut self) {
        self.cur_buy = 0;
        self.cur_sell = 0;
        self.imbalances.clear();
        self.completed = 0;
    }

    fn complete_bucket(&mut self) {
        let diff = (self.cur_buy - self.cur_sell).abs() as f64;
        let imb = diff / self.bucket_vol as f64;
        if self.imbalances.len() >= self.window {
            self.imbalances.pop_front();
        }
        self.imbalances.push_back(imb);
        self.cur_buy = 0;
        self.cur_sell = 0;
        self.completed += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, tol: f64) -> bool {
        (a - b).abs() < tol
    }

    // ---------- Ewma ----------

    #[test]
    fn ewma_fresh_has_no_value() {
        let e = Ewma::new(0.5);
        assert_eq!(e.samples(), 0);
        assert_eq!(e.value(), None);
    }

    #[test]
    fn ewma_first_update_seeds_without_blend() {
        let mut e = Ewma::new(0.2);
        e.update(10.0);
        // First observation seeds directly — not (alpha * 10).
        assert_eq!(e.value(), Some(10.0));
        assert_eq!(e.samples(), 1);
    }

    #[test]
    fn ewma_recursion_is_respected() {
        // alpha=0.5 on a [10, 20] stream: seed at 10, blend to 15.
        let mut e = Ewma::new(0.5);
        e.update(10.0);
        e.update(20.0);
        assert!(approx(e.value().unwrap(), 15.0, 1e-12));
        e.update(20.0);
        assert!(approx(e.value().unwrap(), 17.5, 1e-12));
    }

    #[test]
    fn ewma_alpha_one_tracks_last_observation() {
        let mut e = Ewma::new(1.0);
        for x in [3.0, 7.0, -2.0, 100.0] {
            e.update(x);
            assert_eq!(e.value(), Some(x));
        }
    }

    #[test]
    fn ewma_constant_stream_converges_to_constant() {
        let mut e = Ewma::new(0.1);
        for _ in 0..1000 {
            e.update(42.0);
        }
        assert!(approx(e.value().unwrap(), 42.0, 1e-9));
    }

    #[test]
    fn ewma_reset_clears_samples_but_not_alpha() {
        let mut e = Ewma::new(0.3);
        e.update(5.0);
        e.reset();
        assert_eq!(e.samples(), 0);
        assert_eq!(e.value(), None);
        assert!(approx(e.alpha(), 0.3, 1e-12));
        // Post-reset behaviour matches a fresh estimator.
        e.update(7.0);
        assert_eq!(e.value(), Some(7.0));
    }

    #[test]
    #[should_panic(expected = "alpha must be in (0, 1]")]
    fn ewma_alpha_zero_rejected() {
        let _ = Ewma::new(0.0);
    }

    #[test]
    #[should_panic(expected = "alpha must be in (0, 1]")]
    fn ewma_alpha_above_one_rejected() {
        let _ = Ewma::new(1.0001);
    }

    // ---------- RollingWindow ----------

    #[test]
    fn window_fresh_is_empty() {
        let w = RollingWindow::new(3);
        assert!(w.is_empty());
        assert!(!w.is_full());
        assert_eq!(w.len(), 0);
        assert_eq!(w.mean(), None);
        assert_eq!(w.variance(), None);
        assert_eq!(w.stddev(), None);
    }

    #[test]
    fn window_mean_handles_single_sample() {
        let mut w = RollingWindow::new(4);
        w.push(5.0);
        assert_eq!(w.mean(), Some(5.0));
        // Variance needs 2+ samples.
        assert_eq!(w.variance(), None);
    }

    #[test]
    fn window_tracks_mean_and_population_variance() {
        let mut w = RollingWindow::new(4);
        for x in [1.0, 2.0, 3.0, 4.0] {
            w.push(x);
        }
        assert_eq!(w.len(), 4);
        assert!(w.is_full());
        // mean = 2.5, population variance = ((1-2.5)^2 + (2-2.5)^2 +
        // (3-2.5)^2 + (4-2.5)^2) / 4 = (2.25+0.25+0.25+2.25)/4 = 1.25
        assert!(approx(w.mean().unwrap(), 2.5, 1e-12));
        assert!(approx(w.variance().unwrap(), 1.25, 1e-12));
        assert!(approx(w.stddev().unwrap(), 1.25f64.sqrt(), 1e-12));
    }

    #[test]
    fn window_evicts_oldest_on_overflow() {
        let mut w = RollingWindow::new(3);
        for x in [1.0, 2.0, 3.0, 4.0, 5.0] {
            w.push(x);
        }
        // Window now holds [3, 4, 5].
        let kept: Vec<f64> = w.iter().copied().collect();
        assert_eq!(kept, vec![3.0, 4.0, 5.0]);
        assert!(approx(w.mean().unwrap(), 4.0, 1e-12));
        // population variance of [3,4,5] = ((1+0+1)/3) = 2/3
        assert!(approx(w.variance().unwrap(), 2.0 / 3.0, 1e-12));
    }

    #[test]
    fn window_constant_stream_has_zero_variance() {
        let mut w = RollingWindow::new(8);
        for _ in 0..8 {
            w.push(7.5);
        }
        assert!(approx(w.mean().unwrap(), 7.5, 1e-12));
        // Should be exactly zero, clamp or not.
        assert!(approx(w.variance().unwrap(), 0.0, 1e-12));
        assert!(approx(w.stddev().unwrap(), 0.0, 1e-12));
    }

    #[test]
    fn window_variance_never_negative_after_churn() {
        // Push many values through eviction churn so the incremental
        // sum_sq bookkeeping can accumulate FP error; variance must
        // still surface non-negative (the `max(0.0)` clamp guards this).
        let mut w = RollingWindow::new(16);
        for i in 0..10_000i32 {
            // A tiny alternating perturbation around 1e6 produces
            // catastrophic cancellation in a naive (sum_sq - n*mean^2)
            // computation — exercise that path.
            let base = 1.0e6_f64;
            let jitter = if i % 2 == 0 { 1.0e-3 } else { -1.0e-3 };
            w.push(base + jitter);
        }
        assert!(w.variance().unwrap() >= 0.0);
    }

    #[test]
    fn window_reset_clears_state() {
        let mut w = RollingWindow::new(3);
        w.push(1.0);
        w.push(2.0);
        w.reset();
        assert!(w.is_empty());
        assert_eq!(w.mean(), None);
        // And behaves like a fresh one.
        w.push(10.0);
        assert_eq!(w.mean(), Some(10.0));
    }

    #[test]
    #[should_panic(expected = "cap must be positive")]
    fn window_zero_cap_rejected() {
        let _ = RollingWindow::new(0);
    }

    // ---------- Vpin ----------

    #[test]
    fn vpin_fresh_has_no_value() {
        let v = Vpin::new(100, 5);
        assert_eq!(v.completed(), 0);
        assert_eq!(v.pending_volume(), 0);
        assert_eq!(v.value(), None);
    }

    #[test]
    fn vpin_balanced_flow_gives_zero_imbalance() {
        let mut v = Vpin::new(100, 4);
        // 50 buy + 50 sell = one bucket at exact parity.
        v.ingest(Side::Buy, 50);
        v.ingest(Side::Sell, 50);
        assert_eq!(v.completed(), 1);
        assert_eq!(v.last_bucket(), Some(0.0));
        assert_eq!(v.value(), Some(0.0));
        assert_eq!(v.pending_volume(), 0);
    }

    #[test]
    fn vpin_one_sided_bucket_saturates_at_one() {
        let mut v = Vpin::new(100, 4);
        v.ingest(Side::Buy, 100);
        assert_eq!(v.completed(), 1);
        assert!(approx(v.last_bucket().unwrap(), 1.0, 1e-12));
        assert!(approx(v.value().unwrap(), 1.0, 1e-12));
    }

    #[test]
    fn vpin_splits_oversized_trade_across_buckets() {
        let mut v = Vpin::new(100, 4);
        // A single 250-lot buy must close two full buy-only buckets
        // and leave 50 in the pending bucket.
        v.ingest(Side::Buy, 250);
        assert_eq!(v.completed(), 2);
        assert_eq!(v.pending_volume(), 50);
        assert_eq!(v.cur_buy_for_test(), 50);
        assert!(approx(v.last_bucket().unwrap(), 1.0, 1e-12));
    }

    #[test]
    fn vpin_window_of_mixed_buckets_averages_correctly() {
        // window=3, bucket=100. Buckets: 1.0, 0.0, 0.5 → mean 0.5.
        let mut v = Vpin::new(100, 3);
        v.ingest(Side::Buy, 100); // bucket1: 1.0
        v.ingest(Side::Buy, 50);
        v.ingest(Side::Sell, 50); // bucket2: 0.0
        v.ingest(Side::Buy, 75);
        v.ingest(Side::Sell, 25); // bucket3: |75-25|/100 = 0.5
        assert_eq!(v.completed(), 3);
        assert!(approx(v.value().unwrap(), 0.5, 1e-12));
        assert_eq!(
            v.bucket_imbalances(),
            vec![1.0, 0.0, 0.5],
            "imbalances should be oldest-first"
        );
    }

    #[test]
    fn vpin_rolling_window_evicts_old_buckets() {
        // window=2 — after 3 buckets only the last two should count.
        let mut v = Vpin::new(10, 2);
        // Bucket 1: 1.0 (all buy)
        v.ingest(Side::Buy, 10);
        // Bucket 2: 0.0 (balanced)
        v.ingest(Side::Buy, 5);
        v.ingest(Side::Sell, 5);
        // Bucket 3: 0.6 (8 buy, 2 sell → |8-2|/10)
        v.ingest(Side::Buy, 8);
        v.ingest(Side::Sell, 2);
        assert_eq!(v.completed(), 3);
        // Should only contain buckets 2 and 3: mean = (0 + 0.6)/2 = 0.3
        assert_eq!(v.bucket_imbalances(), vec![0.0, 0.6]);
        assert!(approx(v.value().unwrap(), 0.3, 1e-12));
    }

    #[test]
    fn vpin_ignores_unknown_side_and_nonpositive_qty() {
        let mut v = Vpin::new(10, 3);
        v.ingest(Side::Unknown, 5);
        v.ingest(Side::Buy, 0);
        v.ingest(Side::Buy, -3);
        assert_eq!(v.completed(), 0);
        assert_eq!(v.pending_volume(), 0);
        assert_eq!(v.value(), None);
    }

    #[test]
    fn vpin_reset_clears_state_but_keeps_configuration() {
        let mut v = Vpin::new(10, 4);
        v.ingest(Side::Buy, 10);
        v.ingest(Side::Buy, 5);
        assert_eq!(v.completed(), 1);
        v.reset();
        assert_eq!(v.completed(), 0);
        assert_eq!(v.pending_volume(), 0);
        assert_eq!(v.value(), None);
        assert_eq!(v.bucket_vol(), 10);
        assert_eq!(v.window(), 4);
    }

    #[test]
    fn vpin_trade_landing_exactly_on_boundary_does_not_over_complete() {
        // Pending bucket is already full after a single flush, and the
        // next zero-remaining take path must not spin.
        let mut v = Vpin::new(100, 2);
        v.ingest(Side::Buy, 100);
        assert_eq!(v.completed(), 1);
        assert_eq!(v.pending_volume(), 0);
        // Another exact-fill bucket — completes cleanly without extras.
        v.ingest(Side::Sell, 100);
        assert_eq!(v.completed(), 2);
        assert_eq!(v.pending_volume(), 0);
    }

    #[test]
    #[should_panic(expected = "bucket_vol must be positive")]
    fn vpin_zero_bucket_rejected() {
        let _ = Vpin::new(0, 4);
    }

    #[test]
    #[should_panic(expected = "window must be positive")]
    fn vpin_zero_window_rejected() {
        let _ = Vpin::new(100, 0);
    }

    // Helper accessor used only in tests — keeps the impl minimal in prod.
    impl Vpin {
        pub(super) fn cur_buy_for_test(&self) -> i64 {
            self.cur_buy
        }
    }
}
