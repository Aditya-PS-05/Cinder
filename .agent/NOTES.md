# Cinder build notes

## Current phase (2026-04-19)
Add FIFO tax-lot accounting to ts-pnl alongside the existing WAC
`Accountant`. Closes section 8's P0 "Realized vs unrealized
split, FIFO tax lots" — the split itself already shipped with the
WAC accountant; FIFO lots were the missing half.

## What shipped this phase
- New `ts-pnl::tax_lots` module exporting `LotAccountant`,
  `LotBook`, `TaxLot`, `ClosedLot`. Public API surface mirrors
  the WAC `Accountant` (`on_fill`, `position`, `realized`,
  `realized_net`, `fees`, `unrealized`, `iter`) so a runner can
  swap accounting models without touching anything else.
- FIFO semantics: every opening fill enqueues a lot;
  opposing fills consume the oldest lot first, producing one
  `ClosedLot { opened_side, qty, entry, exit, opened, closed,
  realized }` per matched slice. Overshoot on an opposing fill
  drains the queue, flips the side, and enqueues the remainder
  as a fresh lot at the fill price.
- `LotAccountant::on_fill` returns the `Vec<ClosedLot>` realized
  by that fill so the runner can stream tax-lot events without a
  second diff against `closed_lots()`.
- Quote-denominated commissions accumulate on `LotBook::fees`
  the same way the WAC accountant tracks them. Cross-asset
  commissions (fee=0, non-quote label) are preserved on the
  `Fill` but skipped by both accountants — converting them to
  quote is out of scope for pnl.
- 16 unit tests: opens, extensions (separate lots, no blending),
  partial close, multi-lot close via overflow, exact close,
  flip-with-remainder, short-side realize on both lower and
  higher buybacks, per-lot unrealized for long and short queues,
  per-symbol unrealized_total, closed-lot timestamps preserved,
  fee accumulation, cross-asset fee ignored, zero-qty +
  Unknown-side noops, FIFO-vs-WAC identity on single-cycle
  round trips. All pass; full workspace test + clippy green.

## Design calls
- **Additive, not replacement.** The runner, risk guard, report,
  and metrics all hinge on the WAC `Accountant`. Keeping FIFO
  side-by-side means nothing upstream has to change this phase;
  a later phase can add a `--pnl-mode=fifo` runtime switch if
  the research/tax desk actually needs FIFO gauges live.
- **No automatic averaging on extension.** Same-side fills
  always enqueue as distinct lots so the closed-lot history is
  tax-lot-grade (one buy = one acquired lot). WAC is the
  alternative for anyone who wants blended cost basis.
- **Closed-lot emission on realization, not on close-all.** Every
  opposing slice is its own `ClosedLot`. A Sell of 12 against
  two Buy lots of 10+10 emits two ClosedLots (10 @ lot1, 2 @
  lot2). This matches 8949's "acquired/disposed per lot" model.
- **Position + side derivation from queue, not a separate
  field.** `LotBook::position()` sums `qty` across open lots and
  signs by front lot's side. Invariant: queue is homogeneous in
  side or empty; `apply_fill` enforces it by draining before
  flipping. A `debug_assert!` catches accidental interleave.
- **Cross-asset fees: skip silently.** Matches the WAC
  accountant's contract. A future conversion pass can walk
  `Fill.fee_asset` against a rate oracle — parked until
  someone actually needs cross-asset PnL accuracy.

## Follow-ups
- Surface FIFO metrics through the runner: `ts_fifo_realized`,
  `ts_fifo_unrealized`, `ts_fifo_open_lots`. Needs a config
  switch + a second Accountant instance in EngineRunner /
  LiveRunner. Kept out of this phase to stay tight.
- Report crate: add a `--pnl-mode=fifo` pass that re-runs the
  fill stream through `LotAccountant` and emits a closed-lot CSV
  alongside the existing PnL curve. Deferred.
- Lot-level serde for closed-lot persistence / export. The types
  are already `Clone + Copy + Debug + Eq`; serde derives can
  land in a follow-up once the audit sink grows a tax-lot event.
- Gas + bridge fees (section 8's DEX side) still need a dedicated
  capture path. Neither accountant touches them yet.
- Cross-asset commission conversion (BNB discount in particular).
  Both accountants under-count fees whenever BNB-denominated
  commissions are material on Binance.

## Prior phase (2026-04-18)
Edge-triggered kill-switch cancel sweep on both EngineRunner and
LiveRunner. See commit `80377ff` and preceding notes for full
rationale; summary: one sweep per halted epoch via
`swept_after_trip` latch, strategy tick skipped while halted,
`Place`s emitted from `on_shutdown` dropped with a log.
