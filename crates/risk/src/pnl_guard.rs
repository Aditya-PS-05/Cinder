//! Drawdown and daily-loss guard.
//!
//! [`PnlGuard`] watches a running equity series (realized-net plus
//! unrealized) and fires a [`GuardBreach`] when either of two limits
//! trips:
//!
//! * **Max drawdown** — the gap between the all-time peak equity seen
//!   and the current reading exceeds a configured mantissa amount.
//! * **Daily loss** — the drop in *realized-net* PnL since the start
//!   of the current day exceeds a configured mantissa amount. A
//!   "day" is a sliding [`PnlGuardConfig::day_length`] window that
//!   resets the baseline the first time [`PnlGuard::observe`] runs
//!   past the window's end.
//!
//! The guard is intentionally dependency-free — it speaks in `i128`
//! PnL mantissas and [`std::time::Instant`], matching the kill switch
//! next door. The runner owns the wiring to the [`KillSwitch`]: on a
//! breach, call [`GuardBreach::to_trip_reason`] and pass it to
//! [`KillSwitch::trip`] so no new orders are submitted and the
//! shutdown sweep cancels anything open.
//!
//! [`KillSwitch`]: crate::KillSwitch
//! [`KillSwitch::trip`]: crate::KillSwitch::trip

use std::time::{Duration, Instant};

use crate::kill_switch::TripReason;

/// Thresholds for [`PnlGuard`]. `None` on either limit disables it.
#[derive(Clone, Copy, Debug)]
pub struct PnlGuardConfig {
    /// Maximum allowed drop from the peak equity. Mantissa matches
    /// `price_scale * qty_scale` (the same units [`ts_pnl::Accountant`]
    /// uses). `None` disables the drawdown check.
    pub max_drawdown: Option<i128>,
    /// Maximum allowed drop in realized-net PnL since the start of the
    /// current day. Same mantissa as `max_drawdown`. `None` disables
    /// the daily check.
    pub max_daily_loss: Option<i128>,
    /// Sliding-window length for the daily baseline. Typically 24 h.
    pub day_length: Duration,
}

impl PnlGuardConfig {
    /// No thresholds set; guard never breaches. Useful as a test
    /// baseline that individual tests tighten one field at a time.
    pub fn permissive() -> Self {
        Self {
            max_drawdown: None,
            max_daily_loss: None,
            day_length: Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// Which threshold fired. The runner converts this into a
/// [`TripReason`] via [`GuardBreach::to_trip_reason`] and hands it to
/// the kill switch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GuardBreach {
    /// Equity dropped from peak by more than `max_drawdown`.
    MaxDrawdown {
        peak: i128,
        equity: i128,
        drawdown: i128,
        limit: i128,
    },
    /// Realized-net dropped since the start of the current day by
    /// more than `max_daily_loss`.
    DailyLoss {
        day_start_realized: i128,
        realized: i128,
        loss: i128,
        limit: i128,
    },
}

impl GuardBreach {
    pub fn to_trip_reason(self) -> TripReason {
        match self {
            GuardBreach::MaxDrawdown { .. } => TripReason::MaxDrawdown,
            GuardBreach::DailyLoss { .. } => TripReason::DailyLoss,
        }
    }
}

/// Stateful guard. One instance per trading loop.
#[derive(Debug)]
pub struct PnlGuard {
    cfg: PnlGuardConfig,
    peak_equity: Option<i128>,
    day_start_realized: Option<i128>,
    day_started_at: Option<Instant>,
    /// Stays `true` after a breach so the same limit doesn't fire on
    /// every subsequent tick. `reset()` clears it.
    drawdown_tripped: bool,
    daily_loss_tripped: bool,
}

impl PnlGuard {
    pub fn new(cfg: PnlGuardConfig) -> Self {
        Self {
            cfg,
            peak_equity: None,
            day_start_realized: None,
            day_started_at: None,
            drawdown_tripped: false,
            daily_loss_tripped: false,
        }
    }

    pub fn config(&self) -> &PnlGuardConfig {
        &self.cfg
    }

    /// Peak equity observed so far, or `None` before the first call.
    pub fn peak_equity(&self) -> Option<i128> {
        self.peak_equity
    }

    /// Realized-net baseline for the current day, or `None` before the
    /// first call.
    pub fn day_start_realized(&self) -> Option<i128> {
        self.day_start_realized
    }

    /// Fold a fresh equity reading into the guard. Returns at most one
    /// breach per call. The caller passes realized-net (from
    /// [`ts_pnl::Accountant::realized_net_total`]) and unrealized
    /// (from [`ts_pnl::Accountant::unrealized_total`]); the guard
    /// sums them for drawdown purposes and keeps realized-net alone
    /// for the daily limit.
    ///
    /// Already-tripped limits do not re-fire — the caller's kill
    /// switch already knows about them. Call [`PnlGuard::reset`]
    /// after manual intervention to re-arm.
    pub fn observe(
        &mut self,
        now: Instant,
        realized_net: i128,
        unrealized: i128,
    ) -> Option<GuardBreach> {
        let equity = realized_net.saturating_add(unrealized);

        // Initialize / roll the daily baseline. A fresh guard seeds the
        // baseline on the first call; subsequent calls roll when the
        // window has elapsed since the last seed.
        let needs_roll = match self.day_started_at {
            None => true,
            Some(started) => now.saturating_duration_since(started) >= self.cfg.day_length,
        };
        if needs_roll {
            self.day_started_at = Some(now);
            self.day_start_realized = Some(realized_net);
            // A rolled day re-arms the daily check — peak is untouched
            // because drawdown is an all-time metric.
            self.daily_loss_tripped = false;
        }

        // Peak tracking — monotonic high-water mark.
        self.peak_equity = Some(match self.peak_equity {
            Some(p) => p.max(equity),
            None => equity,
        });

        // Daily loss first: a sharp realized drop is the louder alarm
        // and we want the runner to see exactly one breach per tick.
        if !self.daily_loss_tripped {
            if let Some(limit) = self.cfg.max_daily_loss {
                let baseline = self.day_start_realized.expect("baseline seeded above");
                let loss = baseline.saturating_sub(realized_net);
                if loss > limit {
                    self.daily_loss_tripped = true;
                    return Some(GuardBreach::DailyLoss {
                        day_start_realized: baseline,
                        realized: realized_net,
                        loss,
                        limit,
                    });
                }
            }
        }

        if !self.drawdown_tripped {
            if let Some(limit) = self.cfg.max_drawdown {
                let peak = self.peak_equity.expect("peak seeded above");
                let drawdown = peak.saturating_sub(equity);
                if drawdown > limit {
                    self.drawdown_tripped = true;
                    return Some(GuardBreach::MaxDrawdown {
                        peak,
                        equity,
                        drawdown,
                        limit,
                    });
                }
            }
        }

        None
    }

    /// Clear the tripped flags so both limits can breach again. Does
    /// not reset the peak or daily baseline — those reflect real
    /// history. Operator action only.
    pub fn reset(&mut self) {
        self.drawdown_tripped = false;
        self.daily_loss_tripped = false;
    }
}

impl Default for PnlGuard {
    fn default() -> Self {
        Self::new(PnlGuardConfig::permissive())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(max_dd: Option<i128>, max_daily: Option<i128>, day_ms: u64) -> PnlGuardConfig {
        PnlGuardConfig {
            max_drawdown: max_dd,
            max_daily_loss: max_daily,
            day_length: Duration::from_millis(day_ms),
        }
    }

    #[test]
    fn permissive_guard_never_breaches() {
        let mut g = PnlGuard::new(PnlGuardConfig::permissive());
        let now = Instant::now();
        assert!(g.observe(now, 0, 0).is_none());
        assert!(g.observe(now, -1_000_000, -500_000).is_none());
        assert!(g.observe(now, 1_000_000, 0).is_none());
    }

    #[test]
    fn peak_tracks_high_water_mark() {
        let mut g = PnlGuard::new(cfg_with(None, None, 60_000));
        let now = Instant::now();
        g.observe(now, 0, 100);
        g.observe(now, 50, 150); // equity = 200
        g.observe(now, 50, 20); // equity = 70 — peak stays 200
        assert_eq!(g.peak_equity(), Some(200));
    }

    #[test]
    fn drawdown_breach_fires_once_with_context() {
        let mut g = PnlGuard::new(cfg_with(Some(100), None, 60_000));
        let now = Instant::now();
        // Climb to peak 500.
        assert!(g.observe(now, 200, 300).is_none());
        // Drop to equity 390 — 110 below peak, exceeds 100.
        let breach = g.observe(now, 200, 190).expect("drawdown should breach");
        assert_eq!(
            breach,
            GuardBreach::MaxDrawdown {
                peak: 500,
                equity: 390,
                drawdown: 110,
                limit: 100,
            }
        );
        assert_eq!(breach.to_trip_reason(), TripReason::MaxDrawdown);
        // Further worsening does not re-fire.
        assert!(g.observe(now, 200, 0).is_none());
    }

    #[test]
    fn drawdown_exactly_equal_to_limit_does_not_trip() {
        let mut g = PnlGuard::new(cfg_with(Some(100), None, 60_000));
        let now = Instant::now();
        g.observe(now, 0, 500); // peak 500
                                // drop of exactly 100 — not > 100, so no breach.
        assert!(g.observe(now, 0, 400).is_none());
    }

    #[test]
    fn daily_loss_fires_on_realized_drop_only() {
        let mut g = PnlGuard::new(cfg_with(None, Some(50), 60_000));
        let now = Instant::now();
        // Seed day at realized=100.
        assert!(g.observe(now, 100, 0).is_none());
        // Realized drops to 40 — loss 60 > 50 → breach.
        let breach = g.observe(now, 40, 999).expect("daily loss should breach");
        assert_eq!(
            breach,
            GuardBreach::DailyLoss {
                day_start_realized: 100,
                realized: 40,
                loss: 60,
                limit: 50,
            }
        );
        assert_eq!(breach.to_trip_reason(), TripReason::DailyLoss);
    }

    #[test]
    fn daily_loss_does_not_fire_on_unrealized_only() {
        let mut g = PnlGuard::new(cfg_with(None, Some(50), 60_000));
        let now = Instant::now();
        g.observe(now, 100, 0);
        // Unrealized craters but realized is flat — daily is realized-only.
        assert!(g.observe(now, 100, -500).is_none());
    }

    #[test]
    fn day_rollover_reseats_baseline_and_rearms() {
        let mut g = PnlGuard::new(cfg_with(None, Some(50), 10));
        let t0 = Instant::now();
        g.observe(t0, 100, 0);
        // Trip daily loss.
        let breach = g.observe(t0, 40, 0).unwrap();
        assert!(matches!(breach, GuardBreach::DailyLoss { .. }));
        // After the day_length elapses, the baseline rolls to current
        // realized, and the limit can breach again.
        let t1 = t0 + Duration::from_millis(50);
        assert!(g.observe(t1, 40, 0).is_none());
        assert_eq!(g.day_start_realized(), Some(40));
        // Drop below rolled baseline by > 50.
        let breach = g.observe(t1, -20, 0).unwrap();
        assert_eq!(
            breach,
            GuardBreach::DailyLoss {
                day_start_realized: 40,
                realized: -20,
                loss: 60,
                limit: 50,
            }
        );
    }

    #[test]
    fn drawdown_does_not_reset_on_day_rollover() {
        let mut g = PnlGuard::new(cfg_with(Some(100), None, 10));
        let t0 = Instant::now();
        g.observe(t0, 0, 500); // peak 500
        let breach = g.observe(t0, 0, 390).unwrap();
        assert!(matches!(breach, GuardBreach::MaxDrawdown { .. }));
        // Day rollover — drawdown stays tripped.
        let t1 = t0 + Duration::from_millis(50);
        assert!(g.observe(t1, 0, 380).is_none());
    }

    #[test]
    fn both_limits_report_daily_first_on_combined_tick() {
        let mut g = PnlGuard::new(cfg_with(Some(10), Some(10), 60_000));
        let now = Instant::now();
        g.observe(now, 100, 0); // peak=100, baseline=100
                                // Realized drops -50, unrealized drops 0 — both would trip.
        let breach = g.observe(now, 50, 0).unwrap();
        assert!(
            matches!(breach, GuardBreach::DailyLoss { .. }),
            "daily loss reported first; drawdown follows on next tick"
        );
        // Drawdown still armed — the next observe surfaces it.
        let breach = g.observe(now, 50, 0).unwrap();
        assert!(matches!(breach, GuardBreach::MaxDrawdown { .. }));
    }

    #[test]
    fn reset_rearms_both_limits_but_keeps_history() {
        let mut g = PnlGuard::new(cfg_with(Some(50), Some(50), 60_000));
        let now = Instant::now();
        g.observe(now, 100, 100); // peak=200, baseline=100
        let _ = g.observe(now, 20, 0).unwrap(); // daily loss fires
        g.reset();
        // History retained.
        assert_eq!(g.peak_equity(), Some(200));
        assert_eq!(g.day_start_realized(), Some(100));
        // Still below baseline by 80 — breach fires again after reset.
        let breach = g.observe(now, 20, 0).unwrap();
        assert!(matches!(breach, GuardBreach::DailyLoss { .. }));
    }

    #[test]
    fn drawdown_from_negative_peak_still_works() {
        let mut g = PnlGuard::new(cfg_with(Some(100), None, 60_000));
        let now = Instant::now();
        // Peak equity below zero; drawdown is still measured as peak - equity,
        // which stays a positive mantissa even for negative carriers.
        g.observe(now, -500, 0); // peak = -500
        let breach = g
            .observe(now, -601, 0)
            .expect("drawdown of 101 should breach limit 100");
        assert_eq!(breach.to_trip_reason(), TripReason::MaxDrawdown);
    }
}
