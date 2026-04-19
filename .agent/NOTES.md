# Cinder build notes

## Current phase (2026-04-19)
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
