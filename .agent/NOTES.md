# Cinder build notes

## Current phase (2026-04-19)
Section 1.1 P0 "Sequence-number verification and auto-resubscribe on
desync" — the gap → reconnect path was already in place
(`SymbolResync` + `BinanceBookSync` raise `PushOutcome::Resync`,
`SpotStreamClient::route_event` returns `BinanceError::Align`, and
the outer `run` loop reconnects with bounded exponential backoff).
What was missing was operator observability: a way to count desync
events without parsing logs. This phase added that counter and
factored the dispatch path into a directly-testable helper so the
cumulative count can be asserted in unit tests.

## What shipped this phase
- `crates/binance/src/spot.rs`:
  * New field `resync_counter: Arc<AtomicU64>` on `SpotStreamClient`,
    initialised to 0 in `new`.
  * `pub fn resync_count(&self) -> u64` snapshot accessor and
    `pub fn resync_counter(&self) -> Arc<AtomicU64>` clone of the
    handle for downstream wiring (Prometheus / Grafana export).
  * `route_event` refactored to delegate `PushOutcome` handling to a
    new private `dispatch_book_outcome(outcome, sym)` helper. The
    helper returns `Ok(())` for `Buffered`, publishes on `Ready`, and
    on `Resync` does three things atomically: bump the counter, emit
    `warn!(symbol = %sym, count, "depth stream gapped; forcing reconnect")`,
    and return `BinanceError::Align` whose detail string carries the
    cumulative count (`"... (cumulative=N)"`).
- 4 new unit tests in `spot::tests`:
  * `dispatch_book_outcome_does_not_bump_counter_on_buffered_or_ready`
    — asserts the counter is sticky on the happy path so we don't
    inflate it via re-entry.
  * `dispatch_book_outcome_bumps_counter_and_errors_on_resync` —
    asserts the single-shot semantics + `BinanceError::Align` shape.
  * `dispatch_book_outcome_counter_is_cumulative` — asserts three
    successive Resyncs land 3 in the counter (no reset between
    reconnect attempts within the same client lifetime).
  * `route_event_increments_counter_on_lost_state_delta` — proves
    the *real* dispatch path (event → `SymbolResync` → outcome →
    counter bump) reaches the counter, not just the helper unit.
- `ts-binance` test count: 60 → 64. Workspace `cargo fmt --check`,
  `cargo clippy --workspace --all-targets -D warnings`, and
  `cargo test --workspace` all green.

## Design calls
- **Counter is per-stream, not per-symbol.** A `SpotStreamClient`
  typically owns 1–3 symbols and a single underlying socket. A gap
  on any symbol forces the *whole* socket to reconnect, so the
  cardinality that matters operationally is "how many times has
  this connector restarted because of an alignment gap" — that maps
  cleanly to one counter per client. Per-symbol breakdown can be
  added later (a `HashMap<Symbol, AtomicU64>`) if a multi-symbol
  deployment shows asymmetry, but it's premature now.
- **`Arc<AtomicU64>` over a `metrics` macro call.** Matches the
  precedent set by `RunnerMetrics` (`illegal_transitions_engine`,
  `illegal_transitions_stream`) and `LiveRunner::stream_illegal_counter`
  — the connector exposes the raw atomic, the runner-or-binary scope
  decides whether to forward it to a Prometheus surface or just log
  it. Keeps the connector free of metric-backend coupling.
- **Embed the cumulative count in the error string, not just the
  log.** Two reasons: (a) the standalone `ts_md_binance` binary
  doesn't run a Prometheus endpoint yet, so the error string is the
  only structured channel for downstream consumers of the connector
  during a reconnect cascade; (b) test assertions on the error
  shape are stable and don't require log capture.
- **Extract `dispatch_book_outcome` instead of testing `route_event`
  directly.** `route_event` matches against `MarketEvent` payloads
  and would force tests to construct full WS-decoded events; the
  outcome-level helper is the smallest unit that exercises the
  counter-bump invariant. The 4th test covers the full path so the
  refactor is proved equivalent end-to-end.

## Follow-ups
- Wire `resync_counter()` into `RunnerMetrics` once a `LiveRunner`
  is constructed with a `SpotStreamClient` (today the connector
  binary is standalone; the live trading runner consumes events via
  the bus and doesn't own the connector). When that wiring lands,
  add a `binance_resync_total{symbol="..."}` counter alongside the
  existing illegal-transition gauges.
- Per-symbol counter: only worth doing if a deployment runs >1
  symbol per stream and we see asymmetric desync patterns (e.g.
  one symbol's snapshot endpoint flapping while the others are
  fine). The `HashMap<Symbol, Arc<AtomicU64>>` shape is obvious;
  defer the work until the signal exists.
- Backoff-budget alarm: the outer reconnect loop already caps
  backoff at 30s, but a runaway Resync (counter incrementing every
  few seconds) deserves a separate operator alarm (e.g. "more than
  N resyncs in M seconds"). That's a runner-side concern, not a
  connector-side one — park until metrics are wired.

## Prior phase (2026-04-19)
Strategy SDK on_trade + on_timer hooks wired end-to-end through
paper and live, with kill-switch symmetry and YAML/CLI exposure.
See commit `7a81ab2`.

## Prior phase (2026-04-19)
Feature pipeline (Ewma, RollingWindow, Vpin) shipped in `ts-book` as
`features` module alongside `vol::EwmaVol`. See commit `7f78b45`.

## Prior phase (2026-04-19 earlier)
Property-based tests on the OrderStatus state machine and both PnL
accountants. See commit `68cefb8`.

## Prior phase (2026-04-19)
FIFO tax-lot accounting alongside WAC in `ts-pnl`. See commit
`fcdd3d7`.

## Prior phase (2026-04-18)
Edge-triggered kill-switch cancel sweep on both EngineRunner and
LiveRunner. See commit `80377ff`.
