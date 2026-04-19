# Cinder build notes

## Current phase (2026-04-19)
Pre-trade risk wiring for the LIVE runner — finishes the task
started last phase (Section 7 P0 "Pre-trade checks"), so the
gate sits in front of `BinanceLiveEngine::submit` as well as
`PaperEngine`.

## What shipped this phase
- `LiveRunner` owns a `RiskEngine` (default `RiskConfig::permissive()`)
  plus a `HashSet<ClientOrderId>` of cids it has reserved a slot for.
  New builder method `risk_config(RiskConfig)` seeds it.
- New helper `LiveRunner::submit_with_risk` runs the gate before every
  strategy-emitted `Place` (both hot path and shutdown sweep):
  - Computes ref-price from the local book (Limit → `order.price`,
    Market → opposite side, no fallback) — mirrors
    `PaperEngine::reference_price`.
  - Risk rejection → synthesized `ExecReport::rejected` routed through
    `observe_report`, so the audit tap, strategy callback, metrics,
    and kill-switch reject-rate all see it. `engine.submit` never
    fires, so no transport cost and no venue rejection.
  - Risk pass → `record_submit` + `live_cids.insert` before calling
    the engine. Transport error rolls back the reservation.
- `observe_fill` calls `risk.record_fill`; `observe_report` calls
  `risk.record_complete` when the status is terminal AND the cid is
  in `live_cids`. Externally-injected cids (arriving via the
  user-data-stream through `engine.inbound_sender`) never reserved a
  slot, so they won't spuriously decrement `open_orders`.
- `ts-live-run` now accepts the same pre-trade surface as
  `ts-paper-run`: CLI flags `--max-position-qty`, `--max-order-notional`,
  `--max-open-orders`, `--whitelist` (comma-separated) layered on top
  of the shared `risk.pre_trade.*` YAML section.
- Extracted `build_risk_config` from `ts_paper_run.rs` to
  `ts_runner::lib.rs` so paper and live share one folding helper.
  Divergence here has historically been a silent source of paper-vs-live
  drift — single source of truth now.
- Two new tests in `crates/runner/src/live.rs`:
  - `pre_trade_whitelist_rejects_orders_before_reaching_venue` — seeds
    a whitelist that excludes BTCUSDT with an empty `QueuedApi` (whose
    `pop()` panics on call); the runner drives the maker on a snapshot,
    both quotes surface as Rejected, `orders_submitted == 0`,
    `reconcile_errors == 0` proving the transport was never touched.
  - `pre_trade_notional_cap_blocks_oversized_order` — tightens
    `max_order_notional` to 1 and asserts the same gate behavior.

## Design calls
- `live_cids` is a set, not a counter: the `RiskEngine` open-order
  counter is a single integer, so without a set we'd have no way to
  tell "cid we tracked" from "cid we never saw". External pushes
  (user-data-stream) are explicit first-class inputs here and the
  counter must stay monotonic for them.
- `orders_submitted` is incremented only AFTER `risk.check` passes,
  matching the paper runner's replay summary which counts
  risk-rejected orders as `orders_rejected` (not submitted).
- `reference_price` has no fallback — the live runner never had a
  `notional_fallback_price` config and adding one felt like scope
  creep. The maker only emits Limit orders today, so the Market
  branch is theoretical.
- `submit_with_risk` takes a `&str err_ctx` so the existing "submit
  failed" vs "shutdown submit failed" log strings survive the
  refactor. Keeps log-grep contracts intact.

## Follow-ups
- Leverage + cross-venue exposure still unshipped (need perps/margin
  data and a cross-venue exposure tracker).
- Per-symbol `ts_position{symbol="..."}` labeled gauges once the
  runner multiplexes more than one instrument.
- True auto-flatten (submit closing orders on breach) still needs a
  position-reversal helper. Park until SOR scaffolding exists.
- Tripped-switch semantics drop ALL strategy actions including
  Cancels (`live.rs` `handle_market_event`). Leaves open orders
  alive until the runner exits and the shutdown sweep runs.
- Cross-asset commission accounting still open; guard under-counts
  daily loss whenever BNB-discounted fees are material.
- `RiskEngine::check` takes `&self` but `record_submit` takes
  `&mut self`. The submit-then-check ordering inside
  `submit_with_risk` is single-threaded today, but if we ever
  parallelize quote emission per symbol, expect contention here.
