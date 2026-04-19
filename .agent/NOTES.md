# Cinder build notes

## Current phase (2026-04-19)
Property-based tests on the OrderStatus state machine and both PnL
accountants (`ts_pnl::Accountant` WAC + `ts_pnl::LotAccountant` FIFO).
Closes section 11's P0 "Property tests (`rapid` / `proptest`) on
state machines and math".

## What shipped this phase
- `proptest = "1"` added to `[workspace.dependencies]` (std only, no
  default features) and threaded into `ts-core` + `ts-pnl` as a
  `[dev-dependencies]` entry. Kept opt-in so non-test crates stay
  lean.
- `crates/core/tests/proptest_order_status.rs` (7 properties) nails
  the `OrderStatus` transition graph from every angle — pair-level
  agreement between `can_transition_to` and `try_transition_to`,
  conformance with a hand-rolled spec, self-edges, terminal
  absorption, `New` unreachability, `Rejected` only reachable from
  `New`, and a random-legal-walk that gets trapped at the first
  terminal it hits.
- `crates/pnl/tests/proptest_accountant.rs` (9 properties) on both
  the WAC `Accountant` and the FIFO `LotAccountant`:
  * `realized_net = realized − fees` per symbol and in total.
  * `realized_total == Σ realized(sym)`, `fees_total == Σ fees(sym)`.
  * FIFO queue stays homogeneous-by-side after any stream.
  * `|position|` in FIFO equals Σ lot qty, signed by front lot.
  * WAC and FIFO agree on signed position on every stream.
  * Σ `closed_lot.realized` == `LotBook::realized` at every step.
  * Extension-only streams keep realized at 0; position == Σ
    signed qty.
  * FIFO realizes the closed-form `Σ (exit − entry_i) · qty_i`
    exactly on a full close of a multi-lot same-side stack.
  * WAC matches FIFO bit-for-bit only when every open lot shares
    a single price (i.e. no `avg_entry` truncation).
  * FIFO `unrealized(mark) = 0` for any mark once flat.

## Design calls
- **Restrict to Buy/Sell in generators.** `Side::Unknown` is a
  sentinel the accountants intentionally drop, so properties over
  it degenerate to "nothing happens" and add no coverage. The
  property-test `arb_side` excludes it outright.
- **Bound mantissa ranges in the generator.** Prices `1..10_000`,
  qtys `1..1000`, up to 32 fills — stays well inside i128 for any
  product `price * qty`. Keeps shrinkers producing readable minimal
  cases rather than diagnosing overflow panics.
- **Don't claim WAC == FIFO in general.** The *first* version of
  the multi-lot full-close property did claim that, and proptest
  immediately shrank to Buy (1,1) + Buy (2,1) + Sell (2) @ 1,
  exposing the ½-unit truncation on `avg_entry = (1+2)/2 = 1`.
  That's real behavior, not a regression — WAC trades absolute
  correctness for O(1) state. The property now pins FIFO to the
  closed-form exact value and keeps a separate same-price property
  for the regime where WAC must match.
- **Tight case counts (96–128).** Sample space is small (36 pairs
  for OrderStatus, ≤ 32-step streams for accountants) and the
  properties are deterministic, so 256 (proptest default) buys
  nothing. 96 runs the whole suite in ~60 ms.
- **External `tests/` dir, not `#[cfg(test)] mod`.** Keeps the
  proptest dep out of the regular compile graph — it only gets
  pulled in when actually running tests.

## Follow-ups
- Accountant fuzzing on *partial* multi-lot closes. The WAC/FIFO
  divergence there is expected per design, but there's no direct
  proptest of FIFO's invariants mid-stream (only at the end of the
  fold). A step-level invariant checker would close that gap.
- Property tests on the `ts-core` decimal / scale math once we grow
  past simple `i64` mantissa (current arithmetic is trivially safe;
  properties become load-bearing when we add fee-asset conversion
  or cross-asset mark-to-market).
- Extend proptest coverage to the order-book matcher in `ts-paper`
  (queue invariant + monotone `seq`) — currently only unit-tested.

## Prior phase (2026-04-19 earlier)
FIFO tax-lot accounting alongside WAC in `ts-pnl`. See commit
`fcdd3d7` and earlier NOTES versions for the full design trace.

## Prior phase (2026-04-18)
Edge-triggered kill-switch cancel sweep on both EngineRunner and
LiveRunner. See commit `80377ff`; summary: one sweep per halted
epoch via `swept_after_trip` latch, strategy tick skipped while
halted, `Place`s emitted from `on_shutdown` dropped with a log.
