# Cinder build notes

## Current phase (2026-04-19)
Max drawdown + daily loss guard — Section 7 P0 (#153).

## What shipped
- `ts_risk::pnl_guard::PnlGuard` — stateful i128 component with
  optional `max_drawdown` + `max_daily_loss` thresholds.
- `GuardBreach` enum with `MaxDrawdown { peak, equity, drawdown, limit }`
  and `DailyLoss { day_start_realized, realized, loss, limit }`.
- New `TripReason::MaxDrawdown` (=4) and `TripReason::DailyLoss` (=5),
  wired into `runner::metrics::observe_kill_switch`.
- `GuardBreach::to_trip_reason` maps breach → TripReason so runner
  wiring is a one-liner.

## Design calls
- Daily baseline is a sliding Instant-based window; production UTC
  midnight alignment is a runner concern, not this crate's. Pass
  `Duration::from_secs(86_400)` and seed on the first observe.
- Daily loss is realized-net only — unrealized drops shouldn't age
  the baseline, since realized is what operators must pay.
- Drawdown is all-time, does NOT reset on day rollover.
- One breach per call: if both limits would fire, `DailyLoss`
  reports first (louder alarm), `MaxDrawdown` surfaces on the
  next tick. Already-tripped limits are silent until `reset()`.

## Follow-ups
- Wire `PnlGuard` into `ts-runner`:
  * Feed an `Accountant` from the audit tap so realized-net/unrealized
    totals are live. Needs a mark source (best bid/ask) — maker strategy
    already has one.
  * On `observe()` return `Some(breach)`, call `kill_switch.trip(breach.to_trip_reason())`.
  * Plumb thresholds into `paper_cfg.rs` / `live_cfg.rs` as optional
    YAML fields (`risk.max_drawdown`, `risk.max_daily_loss`, `risk.day_length_secs`).
- "Auto-flatten" today = "kill switch trips, shutdown sweep cancels
  opens, strategy drains quotes". True auto-flatten (submit closing
  market orders) needs a position-reversal helper — park until we
  have the SOR scaffolding in place.
- Cross-asset commission accounting (from last phase) still open; the
  guard will under-count daily loss whenever BNB-discounted fees
  are material.
