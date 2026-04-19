# Cinder build notes

## Current phase (2026-04-19)
Wire the user-data-stream listener's illegal-transition counter
into `RunnerMetrics` so operators can see stream-vs-REST provenance
side by side. Finishes the follow-up parked in the last session.

## What shipped this phase
- `UserDataStreamClient::illegal_transitions_counter() -> Arc<AtomicU64>`
  exposes the listener's internal counter as a shared handle.
  `TransitionGuard::illegal` flipped from `AtomicU64` to
  `Arc<AtomicU64>` so cloning the handle is cheap and every
  publish stays lock-free (single load).
- `RunnerMetrics` now keeps two counters side by side:
  `illegal_transitions_engine` and `illegal_transitions_stream`.
  Encoding moved from the unlabeled `ts_illegal_transitions_total`
  to a labeled series:
  - `ts_illegal_transitions_total{source="engine"}` — REST reconcile
    path, sourced from `OrderEngine::illegal_transitions`.
  - `ts_illegal_transitions_total{source="stream"}` — user-data-stream
    listener, sourced from the shared `Arc<AtomicU64>`.
  Both series are emitted unconditionally (default 0) so scrapes don't
  race on first-seen-label behaviour.
- `LiveRunnerBuilder::stream_illegal_counter(Arc<AtomicU64>)` wires
  the listener counter into the runner. `LiveRunner::drain_reconcile`
  publishes both counters on every tick; when no stream counter is
  attached the `stream` series stays pegged at 0.
- `ts-live-run`: reordered the binary so `UserDataStreamClient::new`
  runs before `LiveRunner::builder` finalizes — the builder needs
  the counter handle before `build()`. The WS-run task still owns
  the client by value.

## Design calls
- Labels over multiple metric names: `ts_illegal_transitions_total`
  with a `source=` label keeps cardinality bounded (exactly 2 today)
  and lets a single Grafana panel sum or break it out. Adding
  a second unlabeled counter would force duplicate HELP/TYPE lines
  and split dashboards.
- `Arc<AtomicU64>` over a callback: `fn() -> u64` would work but
  closes over state that the runner doesn't need to own. The `Arc`
  clone is the lightest coupling that still keeps the publish path
  lock-free.
- Always emit both labels at 0 (rather than lazy-emit when observed):
  Prometheus treats missing series as "missing", not "zero", which
  would hide a healthy stream and break `rate()` on the series at
  startup. The cost of the extra line is two bytes.
- `LiveRunner` takes `Option<Arc<AtomicU64>>` — tests that don't
  attach a stream counter still want the engine counter to publish.

## Follow-ups
- Per-symbol label on the illegal-transition counter once the runner
  multiplexes more than one instrument. Currently both sources are
  process-global counts.
- Tie `illegal_transitions_stream > 0` into an alert — a stream
  reorder once an hour is noise, a sustained climb means the listener
  is falling behind its reconcile peer and fills might be getting
  suppressed.
- Leverage + cross-venue exposure still unshipped (needs perps/margin
  data and a cross-venue exposure tracker).
- Per-symbol `ts_position{symbol="..."}` labelled gauges once the
  runner multiplexes more than one instrument.
- True auto-flatten (submit closing orders on breach) still needs a
  position-reversal helper. Park until SOR scaffolding exists.
- Tripped-switch semantics drop ALL strategy actions including
  Cancels (`live.rs` `handle_market_event`). Leaves open orders
  alive until the runner exits and the shutdown sweep runs.
- Cross-asset commission accounting still open; guard under-counts
  daily loss whenever BNB-discounted fees are material.
