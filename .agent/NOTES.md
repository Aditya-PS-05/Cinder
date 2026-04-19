# Cinder build notes

## Current phase (2026-04-19)
PnL-guard runner wiring — Section 7 P0 (#152).

## What shipped
- `LiveRunner` now owns a `ts_pnl::Accountant` (fills fold on every
  `observe_fill`) and an optional `PnlGuard`. `drain_reconcile` ends
  with `evaluate_pnl_guard()`; `run()` seeds the guard once at startup
  so the daily baseline is pinned to zero before any fills land.
- Builder surface: `LiveRunnerBuilder::pnl_guard(PnlGuard)`. On breach,
  the runner calls `kill_switch.trip(breach.to_trip_reason())` and
  re-observes the switch on the metrics snapshot.
- `LiveCfg.risk` + `RiskCfg { max_drawdown, max_daily_loss,
  day_length_secs }` with `serde(default)` on both limits.
- `ts-live-run` gains `--max-drawdown` / `--max-daily-loss` CLI flags
  plus a `build_pnl_guard` helper. Guard attaches only when at least
  one threshold is set; missing `KillSwitch` emits a warn log (guard
  can observe but not trip).
- Two new integration tests: a realized loss of 200 trips the switch
  on `DailyLoss`; a loss of 20 under a 50-limit stays armed.

## Design calls
- Guard re-evaluates at end of `drain_reconcile`, not per-fill. One
  call site per tick; fills inside a tick aggregate, then the guard
  sees the post-batch state. Mark-driven drawdown is caught on the
  next reconcile even without new fills.
- Startup seed is load-bearing: without it, a bursty first batch of
  fills would set the baseline to the already-lossy state and the
  loss would be invisible.
- `evaluate_pnl_guard` passes `self.book.mid()` as the mark for every
  symbol the accountant tracks. Correct today because `LiveRunner`
  is single-symbol; if we ever multiplex more symbols through one
  runner this closure needs a per-symbol book lookup.
- RiskCfg uses `Option<i64>` fields (YAML friendliness). The bin maps
  to `i128` before handing to `PnlGuardConfig`.

## Follow-ups
- Paper-runner (`EngineRunner`) wiring: `ReplaySummary` already has
  realized/unrealized, so the guard can observe from the periodic
  summary tap without a separate accountant. Deferred — paper is a
  lower-risk path and the shape differs from live.
- True auto-flatten (submit a closing market/aggressive-limit on
  breach) still needs a position-reversal helper. Park until SOR
  scaffolding exists.
- Tripped-switch semantics today drop ALL strategy actions including
  Cancels (`live.rs` `handle_market_event`). That leaves open orders
  alive until the runner exits and the shutdown sweep runs. Consider
  gating only `Place` on trip, letting `Cancel` through, once there's
  a clean way to distinguish intent.
- Cross-asset commission accounting (prior phase) still open; the
  guard under-counts daily loss whenever BNB-discounted fees are
  material.
