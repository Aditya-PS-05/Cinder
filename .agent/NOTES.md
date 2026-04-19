# Cinder build notes

## Current phase (2026-04-19)
Harden kill-switch trip semantics so open orders get canceled on
the venue within one reconcile tick of a trip, rather than
lingering until process exit. Closes the "Tripped-switch drops
ALL strategy actions including Cancels" follow-up from the
previous phase.

## What shipped this phase
- `LiveRunner::handle_market_event` now short-circuits before
  calling `strategy.on_book_update` when the kill switch is
  tripped. Prior behavior ticked the strategy, then dropped every
  emitted action — which silently advanced the maker's cid
  tracking for quotes we never actually placed, creating drift
  between the maker's internal view and the venue. Paper's
  `PaperEngine::apply_event` already honored this invariant via
  the `paused` flag; live now matches.
- New `LiveRunner::apply_trip_sweep` runs at the tail of every
  `drain_reconcile` tick. On the first tick where the switch is
  tripped, it calls `strategy.on_shutdown()` and forwards the
  resulting `Cancel` actions to the engine. Places that come
  back from `on_shutdown` (a strategy bug — the trait is
  cancel-only) are logged and dropped. Tracked via a new
  `swept_after_trip: bool` field; resets when the switch clears
  so operator reset + re-trip fires another sweep.
- New `LiveRunner::last_event_ts: Timestamp` field replaces the
  prior `run`-local `last_ts` so the sweep path can stamp
  cancels with a consistent timestamp without threading the
  value through three callers. `drain_strategy_shutdown` reads
  the same field on graceful exit.
- `EngineRunner::apply_trip_sweep` is the paper analog: runs on
  each event-processed + summary tick, calls
  `replay.drain_shutdown(last_ts)` (which bypasses the engine's
  `paused` flag), forwards the resulting reports to the audit
  sink, and refreshes metrics. Same `swept_after_trip` latch.
- Tests: `kill_switch_trip_cancels_open_quotes_via_reconcile_sweep`,
  `trip_sweep_fires_once_per_halted_epoch`, and
  `halted_runner_skips_strategy_tick` on the live side;
  `kill_switch_trip_triggers_cancel_sweep` and
  `trip_sweep_runs_once_per_halted_epoch` on the paper side.

## Design calls
- **Edge-triggered, latched**: one sweep per halted epoch, not
  per reconcile tick. Re-entering `on_shutdown` after the
  strategy has already cleared its tracked cids would flood the
  cancel path with `unknown cid` rejects, which `record_reject`
  would count against the kill-switch's own reject-rate
  trigger. The `swept_after_trip` latch resets on clear so
  operator reset → re-trip still gets a fresh sweep.
- **Skip strategy tick while halted, not just actions**:
  emitting and then ignoring a Place would keep the maker's
  `open_bid`/`open_ask` populated with cids that never existed
  on the venue; the next cancel for those would fail as
  `unknown cid`. Skipping the tick entirely keeps the maker's
  state consistent with reality.
- **Drop Places from `on_shutdown`, log once**: the trait
  contract is cancel-only, but a defensive filter avoids
  re-entering risk / venue with a new order right after we
  tripped to stop exposure.
- **Run sweep in `drain_reconcile`, not in the trip path
  itself**: the kill switch is a shared `Arc<KillSwitch>`
  tripped from many places (halt-file watcher, PnL guard,
  reject-rate). Tying the sweep to the runner's own reconcile
  tick keeps the cleanup single-threaded with the engine's
  submit/cancel machinery — no async re-entry into the same
  `OrderEngine`.

## Follow-ups
- Trip sweep currently fires once per runner even when multiple
  trip reasons stack. If a drawdown trip clears and a
  reject-rate trip fires, the latch re-arms on clear — no
  action needed. But an operator who trips manually mid-sweep
  sees only one sweep for the whole halted epoch, not one per
  reason. Probably fine; re-visit if we ever want per-reason
  audit tagging.
- True auto-flatten (submit closing market orders to zero out
  position) still needs a position-reversal helper. Parked
  until SOR scaffolding exists — see section 5 in `todo.md`.
- Per-symbol illegal-transition labels once the runner
  multiplexes more than one instrument. Still process-global.
- Cross-asset commission accounting; guard under-counts daily
  loss whenever BNB-discounted fees are material.
- Manual-intervention audit log (section 13 P0) — halt-file
  trips and operator resets are logged, but free-form
  `/admin/halt` style RPCs don't have a dedicated append-only
  log yet.
