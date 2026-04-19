# Cinder build notes

## Current phase (2026-04-19)
Live-path PnL metrics — Section 7 P0 #150 (position tracker) +
Section 8 P0 #164 (real-time MTM) via observability.

## What shipped
- `ts_pnl::Accountant::position_total()` — signed-position sum across
  every symbol the accountant has seen.
- `RunnerMetrics::observe_pnl(realized_net, unrealized, position, mark)`
  — publishes `ts_position`, `ts_realized_pnl`, `ts_unrealized_pnl`,
  `ts_total_pnl`, `ts_mark_price`. i128 mantissas saturate to i64 for
  the gauge. `None` mark suppresses the mark-price series.
- `LiveRunner::evaluate_pnl_guard` now computes the snapshot once and
  publishes unconditionally (when metrics are attached), then hands
  the same values to the guard. Single call site; cadence matches the
  existing reconcile tick.
- Stale help text on `ts_kill_switch_reason` fixed: now enumerates
  codes 0–5 including `max-drawdown` and `daily-loss`.
- Two new metrics tests: happy-path gauge emission + i128 saturation
  with mark suppression.

## Design calls
- Live-path position is a scalar gauge (`ts_position`) summed across
  symbols. Matches the paper path's shape and is correct for the
  single-symbol runner today; if multi-symbol ever lands here it
  becomes semantically fuzzy and we'll want a per-symbol label.
- Publishing runs inside `evaluate_pnl_guard` to share the mark/
  realized/unrealized computation with guard evaluation — one round
  of accountant reads per reconcile tick, not two. Startup seed path
  (`run()` → `evaluate_pnl_guard()` before the select loop) now also
  seeds the metrics gauges to zero before the first event, so a
  scrape during warm-up isn't stale.
- `observe_live` still only touches counters. The prior comment
  claiming "live path doesn't maintain its own PnL" is now stale —
  updated to point at `observe_pnl`.

## Follow-ups
- Paper-runner (`EngineRunner`) wiring for the PnL guard: same
  snapshot shape is already available via `ReplaySummary`; deferred.
- Per-symbol `ts_position{symbol="..."}` labeled gauges once the
  runner multiplexes more than one instrument.
- True auto-flatten (submit closing orders on breach) still needs a
  position-reversal helper. Park until SOR scaffolding exists.
- Tripped-switch semantics drop ALL strategy actions including
  Cancels (`live.rs` `handle_market_event`). Leaves open orders
  alive until the runner exits and the shutdown sweep runs.
- Cross-asset commission accounting still open; guard under-counts
  daily loss whenever BNB-discounted fees are material.
