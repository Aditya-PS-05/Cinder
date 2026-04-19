# Cinder build notes

## Current phase (2026-04-19)
Pre-trade risk wiring — Section 7 P0 "Pre-trade checks".

## What shipped
- `paper_cfg::PreTradeCfg` — `max_position_qty`, `max_order_notional`,
  `max_open_orders`, `whitelist`. Nested under `risk.pre_trade` so the
  existing PnL-guard section isn't disturbed. Re-exported via
  `live_cfg` since it reuses `RiskCfg` verbatim.
- `ts-paper-run` now calls `build_risk_config(risk.pre_trade)` instead
  of the hardcoded `RiskConfig::permissive()`. Whitelist entries are
  normalized to upper-case before hashing into the set so YAML like
  `btcusdt` matches the traded symbol.
- Matching CLI flags on `ts-paper-run`: `--max-position-qty`,
  `--max-order-notional`, `--max-open-orders`, `--whitelist` (comma-
  separated). They layer on top of YAML like the other flags.
- `paper_cfg::tests::pre_trade_round_trips` asserts the full nested
  shape parses from YAML.

## Design calls
- Fields live at `risk.pre_trade.*` rather than flat on `risk.*` so the
  existing drawdown/daily-loss knobs keep their stable names and
  operators can grep "pre_trade" to find all pre-submission gates.
- Each field is `Option<T>`; missing fields inherit the permissive
  baseline. Lets operators tighten one knob at a time without having
  to restate the others.
- Whitelist uppercasing happens at config-build time, not at
  `RiskEngine::check` time — cheaper (done once) and matches how
  `ts-paper-run` upper-cases `cfg.market.symbol` everywhere else.

## Follow-ups
- `ts-live-run` accepts the same YAML (via the shared `RiskCfg`) but
  does **not** wire a `RiskEngine` into the submit path —
  `BinanceLiveEngine` takes strategy actions directly. Next phase:
  insert a pre-trade gate in `LiveRunner::handle_market_event` (or
  inside `BinanceLiveEngine::submit`) that consults a `RiskEngine`
  seeded from `cfg.risk.pre_trade`. Also add the matching CLI flags
  to `ts_live_run.rs`.
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
