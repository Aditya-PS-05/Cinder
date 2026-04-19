# Cinder build notes

## Current phase (2026-04-19)
Section 4 P0 "Strategy SDK: pluggable interface with `OnTick`,
`OnBook`, `OnFill`, `OnTimer`" — wired the two hooks that were
declared but not yet reached by any engine path: `on_trade` (for
public trade prints) and `on_timer` (for runner-driven wall-clock
cadence). Both now fan out end-to-end through paper and live, honour
the kill-switch gate symmetrically, and are exposed to operators via
YAML + CLI on both binaries.

## What shipped this phase
- `crates/strategy/src/lib.rs`: `Strategy` trait gains `on_trade` and
  `on_timer` hooks with empty defaults (a quote-only strategy stays a
  one-liner; a trade-flow or TWAP strategy overrides the relevant
  hook). `Box<dyn Strategy>` forwarders extended to match.
- `crates/oms/src/lib.rs` (`PaperEngine`):
  * `apply_event` now routes `MarketPayload::Trade` through
    `on_trade` (was previously ignored). Skips Funding/Liquidation.
  * New `apply_timer(now)` drains `on_timer` through the engine; the
    `paused` gate short-circuits it exactly the way it does
    `on_book_update`.
- `crates/replay/src/lib.rs`: `Replay::tick_timer` wraps
  `apply_timer` + `absorb_step` so `Accountant` and `ReplayMetrics`
  see the timer-driven reports and fills identically to book-driven
  ones.
- `crates/runner/src/lib.rs` (`EngineRunner`):
  * `timer_interval: Duration` field + `with_timer_interval` builder.
  * New `select!` branch that fires on a `tokio::time::interval`
    (MissedTickBehavior::Delay, burns the immediate tick), re-asserts
    `replay.set_paused(kill_switch.tripped())` each tick, calls
    `tick_timer(last_ts)`, forwards reports/fills to `audit_tx`, and
    re-runs the PnL guard + trip-sweep rails.
- `crates/runner/src/live.rs` (`LiveRunner`):
  * `timer_interval` on runner + builder + `timer_interval()` builder
    method; same burn-first-tick pattern.
  * `handle_market_event` refactored from a bool `ticked` into a
    `TickKind` enum (`Book`, `Trade(&Trade)`, `Skip`) so the Trade
    branch calls `strategy.on_trade` instead of the book hook.
  * New `handle_timer_tick`: short-circuits under a tripped kill
    switch (dispatching then discarding would advance cid state for
    orders we won't place), else calls `on_timer(last_event_ts)` and
    hands the actions to `dispatch_actions`.
  * `dispatch_actions` extracted from the book branch so the book,
    trade, and timer paths share one routing function — identical
    pre-trade gate + engine call for every action source.
- `crates/runner/src/paper_cfg.rs`: `RunnerCfg.timer_ms:
  Option<u64>`. `None` or `0` disables the tick.
- Both binaries: `--timer-ms` CLI flag, resolved with
  `0 → None` so operators can disable from the command line even
  when YAML sets a cadence.
- Tests:
  * `ts-strategy`: `default_on_trade_and_on_timer_and_on_shutdown_return_empty`,
    `box_dyn_strategy_forwards_every_hook` (2 new).
  * `ts-oms`: `trade_payload_fans_out_to_on_trade_and_actions_reach_engine`,
    `paused_engine_drops_on_trade_actions`,
    `apply_timer_fans_out_to_on_timer_and_actions_reach_engine`,
    `paused_engine_drops_on_timer_actions`, plus the rename of the
    legacy `trade_payload_does_not_tick_strategy` → `_default_impl_emits_nothing`
    (5 new/amended).
  * `ts-replay`: `tick_timer_fans_out_to_on_timer_without_market_events`,
    `tick_timer_is_gated_by_pause` (2 new).
  * `ts-runner`: `timer_tick_drives_on_timer_and_reaches_engine`,
    `halted_runner_drops_timer_actions` (paper),
    `trade_payload_reaches_strategy_on_trade_and_actions_dispatch`,
    `timer_tick_invokes_on_timer_and_dispatches_actions`,
    `halted_live_runner_skips_timer_ticks` (live) — 5 new.
  * `ts-runner::paper_cfg`: `timer_ms_defaults_to_none_and_round_trips`.

## Design calls
- **`0 → None` on the CLI, not `0 → Some(0)`.** A `Duration::ZERO`
  ticker in Tokio is a tight infinite loop. Collapsing zero to
  `None` at the config boundary means operators can pass
  `--timer-ms 0` to disable an enabled-by-YAML tick without needing
  to know the ticker semantics, and the runner's own
  `is_zero() → None` check gives belt-and-braces protection if it
  ever reaches the construction site.
- **Burn the immediate tick.** `tokio::time::interval` fires at `t=0`
  by default. We eat that first tick before entering the select loop
  so the first `on_timer` call lands at `t = interval`, not
  simultaneously with the first book update. Matches operator intuition
  ("fire every 250ms" means "fire 250ms from now, then every 250ms").
- **Halted runner must not tick `on_timer`.** Symmetric with the
  existing rule on `on_book_update`: a tripped kill switch drops
  actions at the runner boundary, before the strategy sees the event.
  The alternative — let the strategy run, then throw actions away — is
  observably worse: the strategy advances its internal cid counters,
  state machines, and inventory tracking for orders that were never
  placed. Live runner short-circuits in `handle_timer_tick` *before*
  calling `on_timer`. Paper runner re-asserts `set_paused(tripped())`
  each tick and lets `PaperEngine::apply_timer` short-circuit (same
  pattern as the event branch).
- **`dispatch_actions` as a named helper in `LiveRunner`.** With three
  sources now feeding actions (book, trade, timer), the inline
  duplication was not paying for itself. The Place branch still
  threads through `submit_with_risk` so the pre-trade gate fires
  identically for every source — the extraction is literal, no
  behavior change.
- **`TickKind` enum in `handle_market_event`.** A bool `ticked` flag
  made sense when Trade was a no-op; now that Trade has its own
  strategy call, an enum makes the branching explicit and the match
  at the strategy call site exhaustive.
- **Gate `on_trade` on `paused` too.** Mirrors `on_book_update` —
  a halted paper engine consumes book deltas to keep its local book
  correct but never ticks the strategy. Same rule for trades: book is
  read, strategy is not called. Live runner already guards actions in
  the pre-trade gate so the symmetry exists there too.

## Follow-ups
- A timer cadence in the paper Replay summary branch: right now the
  `EngineRunner` branch calls `self.replay.summary()` to feed metrics
  after each tick. If a strategy emits many timer actions between book
  updates, summary emission to the broadcast channel is still on the
  `summary_interval` cadence — that's probably fine, but worth
  revisiting if we see consumers lag.
- A `Strategy::on_reconcile` hook so live strategies can react to
  positive-ack fills arriving out of band from the WS stream instead
  of through `on_fill`/`on_exec_report`. Currently they can't see a
  reconciled fill differently from a push-stream one.
- Move `last_event_ts` into `LiveRunner` properly as a wallclock
  timestamp when feeds stall — `handle_timer_tick` currently stamps
  `last_event_ts` which, on a genuinely silent feed, is frozen. For
  heartbeat-cancel strategies that's the *right* semantics (they care
  about venue time), but a TWAP slice strategy probably wants wall
  time. We'll revisit when the first such strategy lands.
- InventorySkewMaker doesn't override `on_trade` or `on_timer` yet.
  Follow-up: feed trade imbalance into the maker's skew computation.

## Prior phase (2026-04-19)
Section 4 P0 "Feature pipeline (rolling windows, EMAs, imbalance,
microprice, VPIN)" — shipped as a new `features` module inside
`ts-book` alongside the existing `vol::EwmaVol`. microprice and
imbalance were already served off `OrderBook`, so this fills in the
missing three: generic EMA, rolling window with O(1) moments, and a
bulk-volume VPIN estimator.

## What shipped this phase
- `crates/book/src/features.rs` with three primitives:
  * `Ewma { alpha, value, samples }` — generic f64 EMA, α ∈ (0, 1],
    first observation seeds directly (so cold-start isn't biased
    down by a factor of α), subsequent observations blend via the
    standard recursion. α=1 is accepted and tracks the last obs
    exactly, handy as a degenerate test baseline.
  * `RollingWindow { cap, buf, sum, sum_sq }` — fixed-capacity FIFO
    over f64, O(1) sum / sum_sq updates with correction on eviction.
    `mean` returns `Some` from the first sample; `variance` /
    `stddev` gate on `len() ≥ 2` so the caller must explicitly
    handle cold-start. Variance is population (divide by n), and
    clamps at `0.0` via `max` to absorb the FP cancellation noise
    that shows up after long eviction churn on large common means.
  * `Vpin { bucket_vol, window, cur_buy, cur_sell, imbalances }` —
    signed trade flow packed into equal-volume buckets, bucket
    imbalance `|buy − sell| / bucket_vol` published into a rolling
    `window`-length deque, `value()` = mean across the deque.
    Oversized trades are split across bucket boundaries via a while-
    loop that takes `min(remaining, capacity)` so no volume is lost.
    `Side::Unknown` and non-positive qty are ignored — the estimator
    needs a real side label and falls back to BVC if we want to
    handle anonymous trades later.
- `crates/book/src/lib.rs` re-exports `Ewma`, `RollingWindow`, `Vpin`
  alongside the existing `EwmaVol` surface.
- 20 new unit tests in `features::tests` covering cold-start, the
  recursion math, eviction semantics, constant-stream convergence,
  FP-churn non-negativity, VPIN's bucket splitting (incl. single-
  trade → multi-bucket completion), rolling-window eviction, every
  constructor panic, and the exact-boundary case where a trade
  lands on a bucket boundary without spinning the split loop.
- `ts-book` test count: 37 → 57. Workspace clippy + fmt clean.

## Design calls
- **Three separate types over a single `Feature` trait.** No common
  trait yet — `Ewma::update(x: f64)` and `Vpin::ingest(side, qty)`
  have different shapes, and inventing a lowest-common-denominator
  trait here would be premature. The Strategy SDK task in section 4
  (`OnTick`/`OnBook`/`OnFill`/`OnTimer`) is where a formal feature
  trait belongs, once there's a second caller that needs it.
- **VPIN uses taker_side, not BVC.** We already have authoritative
  `taker_side` on every `ts_core::Trade`, so the estimator takes it
  directly rather than classifying by mid-price change. BVC is a
  fallback for venues that don't publish initiator side; we can add
  it behind a classifier trait when that case actually appears.
- **Variance is population, not sample.** Feature pipelines feed
  normalization layers that are equally happy with either; using `n`
  (not `n − 1`) makes the algebra simpler and keeps the single-sample
  case unambiguous (→ `None`).
- **`max(0.0)` clamp on variance.** The incremental
  `sum_sq/n − mean^2` identity is O(1) but numerically unstable for
  large common means and long churn. A test with 10k evictions
  around 1e6 ± 1e-3 reliably goes negative without the clamp; with
  it, we return `0.0` which is the correct population variance for
  a near-constant stream. Welford-style online variance would be
  more accurate but requires non-trivial bookkeeping on eviction
  — parking this until someone hits a real FP bug.
- **Drop `Side::Unknown` rather than classify.** Matches how both
  `Accountant`s in `ts-pnl` treat `Unknown` (sentinel, dropped on
  ingestion).

## Follow-ups
- Hook `Ewma` / `RollingWindow` into `InventorySkewMaker` — the
  maker currently only consumes `EwmaVol::sigma()`; smoothing
  microprice or imbalance through `Ewma` would likely calm the
  quote ledger around noisy books.
- BVC fallback for `Vpin` when a venue produces anonymous trades.
- Formal `Feature` trait once the Strategy SDK lands.
- Property tests on `Vpin` (bucket-volume conservation, symmetry
  under side-flip, monotone `completed`) — analogous to the
  proptests already shipped for `Accountant`/`LotAccountant`.

## Prior phase (2026-04-19 earlier)
Property-based tests on the OrderStatus state machine and both PnL
accountants. See commit `68cefb8`.

## Prior phase (2026-04-19)
FIFO tax-lot accounting alongside WAC in `ts-pnl`. See commit
`fcdd3d7`.

## Prior phase (2026-04-18)
Edge-triggered kill-switch cancel sweep on both EngineRunner and
LiveRunner. See commit `80377ff`.
