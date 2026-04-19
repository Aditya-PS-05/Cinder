//! Stateful volatility estimator for mid-price tick deltas.
//!
//! [`EwmaVol`] tracks the exponentially-weighted running variance of
//! successive mid-price deltas using the standard RiskMetrics recursion
//!
//! ```text
//!   var_t = lambda * var_{t-1} + (1 - lambda) * delta_t^2
//! ```
//!
//! The first non-trivial delta seeds the variance directly (no decay
//! blend yet) so the reading is unbiased out of the gate; subsequent
//! deltas blend in via the recursion. Output is in the same fixed-point
//! tick scale the book uses, so `sigma()` composes with `half_spread_ticks`
//! and friends without any unit conversion.

use std::cmp::Ordering;

/// EWMA volatility of mid-price tick deltas.
///
/// The tracker is seeded by calling [`update`](Self::update) with
/// successive top-of-book mid prices. The first call primes the state;
/// the second (and every call after) produces a variance reading. A
/// fresh tracker has no variance and reports `None` from both
/// [`variance`](Self::variance) and [`sigma`](Self::sigma).
#[derive(Debug, Clone)]
pub struct EwmaVol {
    lambda: f64,
    variance: f64,
    last_mid: Option<i64>,
    samples: u64,
}

impl EwmaVol {
    /// Create a tracker with the given decay factor.
    ///
    /// `lambda` must be strictly in `(0, 1)`. A common choice is 0.94
    /// (RiskMetrics-style, ~16 effective samples); lower values give
    /// more weight to recent shocks, higher values give a longer memory.
    ///
    /// Panics if `lambda` is not in the open interval — the value is
    /// treated as a development-time invariant rather than a runtime
    /// input, so validation lives at construction rather than deferred.
    pub fn new(lambda: f64) -> Self {
        assert!(
            lambda > 0.0 && lambda < 1.0,
            "EwmaVol::new: lambda must be in (0, 1), got {lambda}"
        );
        Self {
            lambda,
            variance: 0.0,
            last_mid: None,
            samples: 0,
        }
    }

    pub fn lambda(&self) -> f64 {
        self.lambda
    }

    /// Number of deltas folded into the running variance. Zero until
    /// the tracker has seen at least two mids.
    pub fn samples(&self) -> u64 {
        self.samples
    }

    /// Feed the next top-of-book mid (same tick scale `OrderBook::mid`
    /// returns). The first call primes the tracker without producing a
    /// reading; the second call seeds the variance directly from the
    /// squared delta; every call after blends via the EWMA recursion.
    pub fn update(&mut self, mid: i64) {
        let Some(prev) = self.last_mid else {
            self.last_mid = Some(mid);
            return;
        };
        // Promote to f64 before subtraction so we never risk i64 overflow
        // on a bizarre tick gap, and so the squared-delta math matches
        // the output type anyway.
        let delta = mid as f64 - prev as f64;
        let delta_sq = delta * delta;
        match self.samples.cmp(&0) {
            Ordering::Equal => {
                // First real observation: seed without blending so the
                // cold-start reading isn't biased down by a factor of
                // (1 - lambda).
                self.variance = delta_sq;
            }
            _ => {
                self.variance = self.lambda * self.variance + (1.0 - self.lambda) * delta_sq;
            }
        }
        self.samples += 1;
        self.last_mid = Some(mid);
    }

    /// Running variance of mid-price tick deltas, in (tick units)^2.
    /// `None` until the tracker has seen at least two mids.
    pub fn variance(&self) -> Option<f64> {
        (self.samples > 0).then_some(self.variance)
    }

    /// Running standard deviation of mid-price tick deltas, in tick
    /// units. `None` until the tracker has seen at least two mids.
    pub fn sigma(&self) -> Option<f64> {
        self.variance().map(f64::sqrt)
    }

    /// Drop all running state but keep the configured decay factor.
    /// Useful when a session boundary invalidates the running estimate
    /// (reconnect, venue switch, instrument change).
    pub fn reset(&mut self) {
        self.variance = 0.0;
        self.last_mid = None;
        self.samples = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, tol: f64) -> bool {
        (a - b).abs() < tol
    }

    #[test]
    fn fresh_tracker_has_no_reading() {
        let v = EwmaVol::new(0.9);
        assert_eq!(v.samples(), 0);
        assert_eq!(v.variance(), None);
        assert_eq!(v.sigma(), None);
        assert!((v.lambda() - 0.9).abs() < 1e-12);
    }

    #[test]
    fn first_update_primes_without_reading() {
        let mut v = EwmaVol::new(0.9);
        v.update(100);
        assert_eq!(v.samples(), 0);
        assert_eq!(v.variance(), None);
        assert_eq!(v.sigma(), None);
    }

    #[test]
    fn second_update_seeds_variance_directly() {
        let mut v = EwmaVol::new(0.9);
        v.update(100);
        v.update(103);
        // First real observation — seeded without EWMA blend so the
        // sigma equals |delta| rather than (1-lambda) * delta^2 sqrt.
        assert_eq!(v.samples(), 1);
        assert!(approx(v.variance().unwrap(), 9.0, 1e-12));
        assert!(approx(v.sigma().unwrap(), 3.0, 1e-12));
    }

    #[test]
    fn identical_mids_produce_zero_volatility() {
        let mut v = EwmaVol::new(0.9);
        for _ in 0..10 {
            v.update(100);
        }
        assert_eq!(v.sigma(), Some(0.0));
        assert_eq!(v.samples(), 9);
    }

    #[test]
    fn negative_delta_has_same_magnitude_as_positive() {
        let mut up = EwmaVol::new(0.7);
        up.update(100);
        up.update(107);

        let mut down = EwmaVol::new(0.7);
        down.update(107);
        down.update(100);

        assert!(approx(up.sigma().unwrap(), down.sigma().unwrap(), 1e-12));
    }

    #[test]
    fn shock_decays_on_subsequent_calm_ticks() {
        // Seeded with a delta of 10 (variance 100), then every tick
        // repeats the same mid — variance should decay by lambda each
        // step: 100 -> 100*lambda -> 100*lambda^2 -> ...
        let lambda = 0.5;
        let mut v = EwmaVol::new(lambda);
        v.update(100);
        v.update(110); // variance seeded at 100
        assert!(approx(v.variance().unwrap(), 100.0, 1e-9));

        v.update(110); // delta 0; variance = 0.5*100 + 0.5*0 = 50
        assert!(approx(v.variance().unwrap(), 50.0, 1e-9));

        v.update(110); // variance = 0.5*50 = 25
        assert!(approx(v.variance().unwrap(), 25.0, 1e-9));

        v.update(110); // variance = 0.5*25 = 12.5
        assert!(approx(v.variance().unwrap(), 12.5, 1e-9));

        // sigma is sqrt of variance at each step.
        assert!(approx(v.sigma().unwrap(), 12.5f64.sqrt(), 1e-9));
    }

    #[test]
    fn ewma_blend_formula_is_respected_after_seed() {
        // Exercise the recursion explicitly: var_t = 0.5 * var_{t-1}
        // + 0.5 * delta^2. Seed at variance 4 (delta 2), then feed a
        // delta of 6 -> variance = 0.5*4 + 0.5*36 = 20.
        let mut v = EwmaVol::new(0.5);
        v.update(100);
        v.update(102);
        assert!(approx(v.variance().unwrap(), 4.0, 1e-12));
        v.update(108);
        assert!(approx(v.variance().unwrap(), 20.0, 1e-12));
        assert!(approx(v.sigma().unwrap(), 20f64.sqrt(), 1e-12));
        assert_eq!(v.samples(), 2);
    }

    #[test]
    fn variance_converges_toward_true_variance_for_constant_magnitude_walk() {
        // Feed alternating +/- 4 ticks. Every delta squared is 16, so
        // the EWMA variance should converge to 16 regardless of lambda.
        let mut v = EwmaVol::new(0.8);
        let mut mid = 1000i64;
        let mut sign = 1i64;
        v.update(mid);
        for _ in 0..200 {
            mid += sign * 4;
            sign = -sign;
            v.update(mid);
        }
        assert!(
            approx(v.variance().unwrap(), 16.0, 1e-3),
            "variance should converge to 16.0, got {}",
            v.variance().unwrap()
        );
        assert!(approx(v.sigma().unwrap(), 4.0, 1e-3));
    }

    #[test]
    fn reset_clears_state_but_keeps_lambda() {
        let mut v = EwmaVol::new(0.9);
        v.update(100);
        v.update(105);
        assert!(v.sigma().is_some());
        v.reset();
        assert_eq!(v.samples(), 0);
        assert_eq!(v.variance(), None);
        assert_eq!(v.sigma(), None);
        assert!((v.lambda() - 0.9).abs() < 1e-12);
        // After reset the tracker behaves exactly like a fresh one:
        // one priming update, then a seeding update.
        v.update(100);
        assert_eq!(v.sigma(), None);
        v.update(107);
        assert!(approx(v.sigma().unwrap(), 7.0, 1e-12));
    }

    #[test]
    #[should_panic(expected = "lambda must be in (0, 1)")]
    fn lambda_zero_rejected() {
        let _ = EwmaVol::new(0.0);
    }

    #[test]
    #[should_panic(expected = "lambda must be in (0, 1)")]
    fn lambda_one_rejected() {
        let _ = EwmaVol::new(1.0);
    }

    #[test]
    #[should_panic(expected = "lambda must be in (0, 1)")]
    fn lambda_negative_rejected() {
        let _ = EwmaVol::new(-0.1);
    }
}
