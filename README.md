<!-- <CENTERED SECTION FOR GITHUB DISPLAY> -->

<div align="center">
<h1>Cinder</h1>
</div>

> A production-grade crypto trading system in Rust — from market data to signal to paper execution to PnL, in a single replayable loop.

>
> Cargo workspace of focused crates: a fixed-point hot path, a lock-free typed bus, a Binance spot WS connector, an order book reconstructor, a strategy SDK, an OMS with paper execution, layered YAML config, NDJSON tape record/replay, an audit log, and a Prometheus `/metrics` endpoint. <br />
> Built as a reference for what a low-latency, multi-venue trading stack looks like when each layer is a small, typed, testable crate instead of a framework.
>
> | [<img alt="GitHub Follow" src="https://img.shields.io/github/followers/Aditya-PS-05?style=flat-square&logo=github&labelColor=black&color=24292f" width="156px" />](https://github.com/Aditya-PS-05) | Follow [@Aditya-PS-05](https://github.com/Aditya-PS-05) on GitHub for more projects. Hacking on trading infrastructure, low-latency systems, and developer tooling. |
> | :-----| :----- |

<div align="center">

[![Rust](https://img.shields.io/badge/Rust-1.94-CE412B?labelColor=black&logo=rust&style=flat-square)](https://www.rust-lang.org/)
[![Tokio](https://img.shields.io/badge/Tokio-1-1A6FAF?labelColor=black&style=flat-square)](https://tokio.rs/)
[![Cargo](https://img.shields.io/badge/Cargo-workspace-DEA584?labelColor=black&logo=rust&style=flat-square)](https://doc.rust-lang.org/cargo/)
[![Postgres](https://img.shields.io/badge/Postgres-16-336791?labelColor=black&logo=postgresql&style=flat-square)](https://www.postgresql.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24-FFCC01?labelColor=black&logo=clickhouse&style=flat-square)](https://clickhouse.com/)
[![Prometheus](https://img.shields.io/badge/Prometheus-/metrics-E6522C?labelColor=black&logo=prometheus&style=flat-square)](https://prometheus.io/)
[![GitHub Stars](https://img.shields.io/github/stars/Aditya-PS-05/Cinder?color=0073FF&labelColor=black&style=flat-square)](https://github.com/Aditya-PS-05/Cinder/stargazers)
[![GitHub Forks](https://img.shields.io/github/forks/Aditya-PS-05/Cinder?color=0073FF&labelColor=black&style=flat-square)](https://github.com/Aditya-PS-05/Cinder/network/members)
[![GitHub Issues](https://img.shields.io/github/issues/Aditya-PS-05/Cinder?color=0073FF&labelColor=black&style=flat-square)](https://github.com/Aditya-PS-05/Cinder/issues)
[![License](https://img.shields.io/badge/license-MIT-white?labelColor=black&style=flat-square)](https://github.com/Aditya-PS-05/Cinder/blob/master/LICENSE)

</div>

<!-- </CENTERED SECTION FOR GITHUB DISPLAY> -->

> **Try it locally → `cargo run -p ts-runner --bin ts-paper-run -- --config-dir configs/paper_run --env dev`** — opens a Binance spot WS, runs an inventory-skew maker against paper execution, prints periodic PnL summaries, and writes an NDJSON audit log. Start from a public WS feed, reconstruct the book, run a maker against a deterministic paper matcher, and watch position / realized / unrealized PnL update every few seconds — all in one binary, no exchange keys required.

## Overview

**Cinder** is an opinionated crypto trading system built to explore what a low-latency, multi-venue stack looks like when each layer is a small, typed Rust crate instead of a monolith. Instead of bolting a strategy onto an exchange SDK, Cinder separates concerns into a Cargo workspace where the hot path, the OMS, the strategy SDK, the venue adapters, the storage layer, and the runtime are independently testable:

| Layer | Crate | What it owns |
|-------|-------|--------------|
| **Hot path primitives** | `ts-core` | Fixed-point `Price`/`Qty`, `MarketEvent`, lock-free typed `Bus` |
| **Order book** | `ts-book` | Snapshot + diff reconstruction with sequence verification |
| **Venue adapter** | `ts-binance` | Binance spot WS + REST, decoded into `MarketEvent` |
| **Strategy SDK** | `ts-strategy` | Object-safe `Strategy` trait; reference inventory-skew maker |
| **OMS** | `ts-oms` | `PaperEngine`, order state machine, pre-trade risk |
| **Risk / PnL** | `ts-risk`, `ts-pnl` | Pre-trade checks, mark-to-market, realized vs unrealized |
| **Replay** | `ts-replay`, `ts-backtest` | Drive the same strategy code path from live or historical ticks |
| **Storage** | `ts-storage` | Postgres + ClickHouse migrations, `ts-migrate` binary |
| **Config** | `ts-config` | Layered YAML (`base.yaml` + `<env>.yaml`) with `TS_*` env overrides |
| **Runner** | `ts-runner` | `ts-paper-run` and `ts-tape` binaries, `/metrics`, NDJSON audit |

The result is one workspace where you can rewire any layer — swap the maker for a different strategy, point the bus at a tape instead of a WS, or replace `PaperEngine` with a live executor — without touching the rest of the stack.

## Contents

- [Overview](#overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Quick Start](#quick-start)
  - [Configuration](#configuration)
- [Usage](#usage)
- [Development](#development)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

## Features

- **End-to-end paper trading loop** — `ts-paper-run` wires Binance WS → bus → bridge → `EngineRunner` → `Replay` → `PaperEngine` → `InventorySkewMaker` and prints periodic PnL summaries.
- **Lock-free typed bus** — `ts_core::bus::Bus<MarketEvent>` never blocks producers; consumers subscribe with bounded channels and slow consumers can be detected and dropped.
- **Fixed-point hot path** — `Price` and `Qty` are integer mantissas with explicit scales; no float math on order sizes, prices, or PnL accumulation.
- **Order book reconstruction** — snapshot + diff stream with sequence-number verification and gap detection in `ts-book`.
- **Strategy SDK** — object-safe `Strategy` trait with `on_book_update` / `on_fill` / `on_exec_report`; the same strategy code runs live, paper, and replay.
- **Layered YAML config** — `base.yaml` + `{dev,staging,prod}.yaml` overlay, `TS_*` env-var overrides, CLI flags win last.
- **Tape record + replay** — `ts-tape` writes NDJSON market-data tapes; `ts-replay` streams them back through the same strategy path for deterministic backtests.
- **NDJSON audit log** — every `ExecReport` and `Fill` flushed to disk for post-mortem and reconciliation.
- **Prometheus `/metrics`** — `ts-paper-run` exposes a scrape endpoint for events ingested, orders submitted, fills, position, PnL.
- **Schema-managed storage** — `ts-migrate` applies forward-only Postgres + ClickHouse migrations with on-disk checksums.
- **Local infra in one command** — `make up` brings Postgres, Redis, ClickHouse, and NATS up via Docker Compose with healthchecks.
- **Strict toolchain** — `forbid(unsafe_code)` across crates, pinned `rust-toolchain.toml`, `make ci` gates fmt + clippy `-D warnings` + tests.

## Tech Stack

**Language & Runtime**
- Rust 1.94 (pinned via `rust-toolchain.toml`)
- Tokio (multi-thread runtime, signal, fs, net)
- Cargo workspace (resolver = 2)

**Networking & Serialization**
- `tokio-tungstenite` (WS + rustls)
- `reqwest` (HTTP/2 + rustls)
- `serde` + `serde_yaml_ng` + `serde_json`

**Storage**
- Postgres 16 (`tokio-postgres` + `deadpool-postgres`) — orders, fills, audit
- ClickHouse 24 — tick data + PnL time series
- Redis 7 — hot state, locks, rate-limit counters
- NATS 2.10 — cross-service fan-out

**Observability**
- `tracing` + `tracing-subscriber` (structured logs, env-filtered)
- Prometheus `/metrics` scrape endpoint
- NDJSON audit log of every `ExecReport` and `Fill`

**Tooling**
- `clap` (derive CLI)
- `thiserror` + `anyhow`
- `chrono` (timestamps)
- `sha2` + `hex` (migration checksums)

## Installation

### Prerequisites

- [Rust 1.94+](https://rustup.rs/) — `rustup` will pick up the pinned channel from `rust-toolchain.toml` automatically
- [Docker](https://docs.docker.com/get-docker/) + Docker Compose v2 — for local Postgres / Redis / ClickHouse / NATS
- `make` and a POSIX shell

### Quick Start

```bash
# Clone the repo
git clone https://github.com/Aditya-PS-05/Cinder
cd Cinder

# Build the workspace
make build

# Bring up local Postgres / Redis / ClickHouse / NATS
make up

# Apply Postgres + ClickHouse migrations
make migrate

# Run the live paper-trading harness against Binance spot WS
cargo run -p ts-runner --bin ts-paper-run -- \
  --config-dir configs/paper_run --env dev
```

Watch the logs — every two seconds you'll see a `summary` line with events ingested, book updates, orders submitted, fills, position, and total PnL. `Ctrl-C` shuts down cleanly and prints a final summary.

### Configuration

`ts-paper-run` resolves config in three layers (later wins):

1. `configs/paper_run/base.yaml` — defaults shared by every overlay
2. `configs/paper_run/<env>.yaml` — overlay selected by `--env` (default `dev`)
3. `TS_*` environment variables — override individual scalars
4. CLI flags — win over everything else

Example `base.yaml`:

```yaml
market:
  symbol: BTCUSDT
  price_scale: 2
  qty_scale: 8
  ws_url: wss://stream.binance.com:9443/ws

maker:
  quote_qty: 2
  half_spread_ticks: 5
  inventory_skew_ticks: 1
  max_inventory: 20
  cid_prefix: pr

runner:
  summary_secs: 5
  channel: 4096
```

Optional sections:

```yaml
audit:
  path: ./var/audit/paper_run.ndjson

metrics:
  listen: 127.0.0.1:9898
```

Without `--config-dir`, the binary falls back to the built-in defaults encoded in `PaperCfg::default`.

## Usage

| Action | How |
|--------|-----|
| **Run paper trading** | `cargo run -p ts-runner --bin ts-paper-run -- --config-dir configs/paper_run --env dev` |
| **Override a flag** | `... --symbol ETHUSDT --quote_qty 5 --half_spread_ticks 8` |
| **Record a tape** | `cargo run -p ts-runner --bin ts-tape -- record --symbol BTCUSDT --out tape.ndjson` |
| **Replay a tape** | `cargo run -p ts-runner --bin ts-tape -- replay --tape tape.ndjson` |
| **Enable audit log** | `... --audit ./var/audit/run.ndjson` |
| **Enable `/metrics`** | `... --metrics-addr 127.0.0.1:9898` then `curl localhost:9898/metrics` |
| **Apply migrations** | `make migrate` (or `TS_ENV=staging make migrate`) |
| **Check migration status** | `make migrate-status` |
| **Bring infra up / down** | `make up` / `make down` (use `make nuke` to wipe volumes) |
| **Run the CI gate locally** | `make ci` (fmt-check + clippy `-D warnings` + tests) |

## Development

```bash
# Build the workspace
make build

# Build release artifacts
make release

# Run all tests
make test

# Lint with warnings as errors
make clippy

# Format
make fmt

# Full local CI gate
make ci
```

The workspace pins to Rust 1.94.1 via `rust-toolchain.toml` — `rustup` will fetch the right toolchain on first `cargo` invocation. All crates are `forbid(unsafe_code)`. CI runs `make ci`, so reproducing failures locally is a one-liner.

When iterating on the runner, keep `make up` running so Postgres / ClickHouse / Redis / NATS stay available for integration tests.

## Deployment

Cinder is currently a single-node paper-trading system intended to run on a VM near a venue (e.g. AWS Tokyo for Binance). To deploy:

1. Build a release binary: `make release`
2. Ship `target/release/ts-paper-run` and the relevant `configs/paper_run/<env>.yaml`
3. Provision Postgres + ClickHouse + Redis + NATS (the `infra/compose` file documents the expected versions and ports — production deployments should use managed equivalents)
4. Apply migrations: `TS_ENV=prod make migrate`
5. Run `ts-paper-run --config-dir configs/paper_run --env prod --metrics-addr 0.0.0.0:9898 --audit /var/log/tickforge/audit.ndjson`
6. Scrape `/metrics` from Prometheus and ship the audit NDJSON to long-term storage

The runner installs a `ctrl-c` handler that drains the bus, flushes the audit log, and prints a final summary — `SIGTERM` from systemd or Kubernetes triggers the same shutdown path.

## Contributing

Issues and PRs are welcome. If you want to add a feature:

1. Fork the repo and create a feature branch (`feat/my-thing`).
2. Run `make ci` and make sure fmt, clippy, and tests are green.
3. New crates go under `crates/` and must be added to the workspace `members` list and `[workspace.dependencies]` table.
4. Open a PR describing the *why*, not just the *what*. Include a `summary` log snippet for runner-affecting changes.

Please don't open PRs that bump dependencies without a reason tied to a bug, CVE, or feature.

## License

<p align="center">
  <strong>MIT &copy; <a href="https://github.com/Aditya-PS-05">Aditya Pratap Singh</a></strong>
</p>

If you find this project useful, **please consider starring it** or [follow me on GitHub](https://github.com/Aditya-PS-05) for more work on trading infrastructure and low-latency systems. Issues, PRs, and ideas all welcome.
