//! `ts-paper-run` — live paper-trading over Binance spot.
//!
//! Wires the whole stack end to end: Binance WS → `ts_core::bus::Bus`
//! → `bridge_bus` → `EngineRunner` → `Replay` owning a `PaperEngine`
//! with an `InventorySkewMaker`. Periodic summaries are broadcast on
//! a tap and printed; a final summary lands on graceful shutdown.
//!
//! The binary is a smoke test and demo harness — it runs against the
//! public mainnet WS endpoint by default, uses paper execution only,
//! and never sends orders to an exchange.
//!
//! Configuration is layered. A YAML directory via `--config-dir` loads
//! `base.yaml` + `<env>.yaml` (`--env`, default `dev`) and `TS_*` env
//! vars override scalars; any explicit CLI flag wins over the loaded
//! value. Without `--config-dir` the binary falls back to the built-in
//! defaults encoded in [`PaperCfg::default`].

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use ts_binance::{SpotStreamClient, SpotStreamConfig};
use ts_config::Env;
use ts_core::{bus::Bus, InstrumentSpec, MarketEvent, Qty, Symbol, Venue};
use ts_oms::{EngineConfig, PaperEngine, RiskConfig};
use ts_replay::{Replay, ReplaySummary};
use ts_runner::{
    audit::{spawn_audit_writer, AuditWriter},
    bridge_bus,
    metrics::{spawn_metrics_server, RunnerMetrics},
    paper_cfg::{AuditCfg, MetricsCfg, PaperCfg},
    EngineRunner,
};
use ts_strategy::{InventorySkewMaker, MakerConfig};

#[derive(Parser, Debug)]
#[command(
    name = "ts-paper-run",
    about = "Live paper-trading harness over Binance spot"
)]
struct Cli {
    /// Config directory (reads base.yaml + <env>.yaml). When omitted,
    /// the binary runs with built-in defaults.
    #[arg(long)]
    config_dir: Option<PathBuf>,

    /// Deployment overlay name. Ignored unless `--config-dir` is set.
    #[arg(long, default_value = "dev")]
    env: String,

    /// Symbol to trade (uppercase, e.g. BTCUSDT).
    #[arg(long)]
    symbol: Option<String>,

    /// Price scale (decimal places).
    #[arg(long)]
    price_scale: Option<u8>,

    /// Quantity scale (decimal places).
    #[arg(long)]
    qty_scale: Option<u8>,

    /// WebSocket endpoint.
    #[arg(long)]
    ws_url: Option<String>,

    /// Maker quote size (mantissa at qty_scale).
    #[arg(long)]
    quote_qty: Option<i64>,

    /// Half-spread ticks applied around mid.
    #[arg(long)]
    half_spread_ticks: Option<i64>,

    /// Inventory skew in ticks per unit of inventory.
    #[arg(long)]
    inventory_skew_ticks: Option<i64>,

    /// Absolute inventory cap before the maker suppresses a side.
    #[arg(long)]
    max_inventory: Option<i64>,

    /// Client-order-id prefix.
    #[arg(long)]
    cid_prefix: Option<String>,

    /// Summary broadcast interval in seconds. Zero disables the tap.
    #[arg(long)]
    summary_secs: Option<u64>,

    /// Bus subscription + mpsc channel capacity.
    #[arg(long)]
    channel: Option<usize>,

    /// Append every ExecReport and Fill to this NDJSON file. Overrides
    /// the YAML `audit.path` when set.
    #[arg(long)]
    audit: Option<PathBuf>,

    /// Listen address for the Prometheus `/metrics` scrape endpoint
    /// (e.g. `127.0.0.1:9898`). Overrides the YAML `metrics.listen`
    /// when set. Omit to disable.
    #[arg(long)]
    metrics_addr: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let cfg = resolve_config(&cli).context("resolve config")?;

    run(cfg).await
}

fn resolve_config(cli: &Cli) -> Result<PaperCfg> {
    let mut cfg = match cli.config_dir.as_ref() {
        Some(dir) => {
            let env = Env::parse(&cli.env).with_context(|| format!("--env {}", cli.env))?;
            PaperCfg::load(dir, env)
                .with_context(|| format!("load config from {}", dir.display()))?
        }
        None => PaperCfg::default(),
    };

    if let Some(v) = cli.symbol.clone() {
        cfg.market.symbol = v;
    }
    if let Some(v) = cli.price_scale {
        cfg.market.price_scale = v;
    }
    if let Some(v) = cli.qty_scale {
        cfg.market.qty_scale = v;
    }
    if let Some(v) = cli.ws_url.clone() {
        cfg.market.ws_url = v;
    }
    if let Some(v) = cli.quote_qty {
        cfg.maker.quote_qty = v;
    }
    if let Some(v) = cli.half_spread_ticks {
        cfg.maker.half_spread_ticks = v;
    }
    if let Some(v) = cli.inventory_skew_ticks {
        cfg.maker.inventory_skew_ticks = v;
    }
    if let Some(v) = cli.max_inventory {
        cfg.maker.max_inventory = v;
    }
    if let Some(v) = cli.cid_prefix.clone() {
        cfg.maker.cid_prefix = v;
    }
    if let Some(v) = cli.summary_secs {
        cfg.runner.summary_secs = v;
    }
    if let Some(v) = cli.channel {
        cfg.runner.channel = v;
    }
    if let Some(path) = cli.audit.clone() {
        cfg.audit = Some(AuditCfg { path });
    }
    if let Some(listen) = cli.metrics_addr.clone() {
        cfg.metrics = Some(MetricsCfg { listen });
    }

    Ok(cfg)
}

async fn run(cfg: PaperCfg) -> Result<()> {
    let venue = Venue::BINANCE;
    let symbol_str = cfg.market.symbol.to_uppercase();
    let symbol = Symbol::new(symbol_str.clone());

    let mut specs: HashMap<Symbol, InstrumentSpec> = HashMap::new();
    specs.insert(
        symbol.clone(),
        InstrumentSpec {
            venue: venue.clone(),
            symbol: symbol.clone(),
            base: String::new(),
            quote: String::new(),
            price_scale: cfg.market.price_scale,
            qty_scale: cfg.market.qty_scale,
            min_qty: Qty(0),
            min_notional: 0,
        },
    );

    let engine = PaperEngine::new(
        EngineConfig {
            venue: venue.clone(),
            symbol: symbol.clone(),
            notional_fallback_price: None,
        },
        RiskConfig::permissive(),
        InventorySkewMaker::new(MakerConfig {
            venue: venue.clone(),
            symbol: symbol.clone(),
            quote_qty: Qty(cfg.maker.quote_qty),
            half_spread_ticks: cfg.maker.half_spread_ticks,
            imbalance_widen_ticks: cfg.maker.imbalance_widen_ticks,
            inventory_skew_ticks: cfg.maker.inventory_skew_ticks,
            max_inventory: cfg.maker.max_inventory,
            cid_prefix: cfg.maker.cid_prefix.clone(),
        }),
    );
    let replay = Replay::new(engine);

    let bus: Arc<Bus<MarketEvent>> = Bus::new();
    let sub = bus.subscribe(cfg.runner.channel);
    let (tx, rx) = mpsc::channel::<MarketEvent>(cfg.runner.channel);
    let bridge = bridge_bus(sub, tx);

    let summary_interval = Duration::from_secs(cfg.runner.summary_secs);
    let (mut runner, handle) = EngineRunner::with_summary_tap(replay, rx, summary_interval, 8);

    let metrics_server = if let Some(mcfg) = cfg.metrics.as_ref() {
        let addr: std::net::SocketAddr = mcfg
            .listen
            .parse()
            .with_context(|| format!("parse metrics.listen `{}`", mcfg.listen))?;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("bind metrics listener on {addr}"))?;
        let actual = listener.local_addr().context("metrics local_addr")?;
        let metrics = RunnerMetrics::new();
        runner = runner.with_metrics(Arc::clone(&metrics));
        tracing::info!(addr = %actual, "metrics endpoint /metrics ready");
        Some(spawn_metrics_server(listener, metrics))
    } else {
        None
    };

    let audit_task = if let Some(audit_cfg) = cfg.audit.as_ref() {
        let writer = AuditWriter::create(&audit_cfg.path)
            .await
            .context("open audit log")?;
        let (audit_tx, audit_rx) = mpsc::channel(cfg.runner.channel);
        runner = runner.with_audit(audit_tx);
        Some(spawn_audit_writer(writer, audit_rx))
    } else {
        None
    };

    let printer = handle.subscribe_summaries().map(|mut sub| {
        let sym = symbol_str.clone();
        tokio::spawn(async move {
            while let Ok(summary) = sub.recv().await {
                log_summary(&sym, "live", &summary);
            }
        })
    });

    let mut ws_cfg = SpotStreamConfig::new(vec![symbol_str.clone()], specs);
    ws_cfg.ws_url = cfg.market.ws_url.clone();
    let client = Arc::new(SpotStreamClient::new(ws_cfg, Arc::clone(&bus)));
    let client_for_task = Arc::clone(&client);
    let client_task = tokio::spawn(async move { client_for_task.run().await });

    let runner_task = tokio::spawn(runner.run());

    tokio::signal::ctrl_c()
        .await
        .context("install ctrl_c handler")?;
    tracing::info!("ctrl-c, shutting down");

    // Stop the WS reconnect loop; close the bus so the bridge drains.
    client_task.abort();
    bus.close();
    let _ = bridge.await;
    handle.shutdown();
    if let Some(p) = printer {
        p.abort();
    }
    if let Some(srv) = metrics_server.as_ref() {
        srv.abort();
    }

    let summary = runner_task.await.context("runner task")?;
    if let Some(task) = audit_task {
        match task.await {
            Ok(written) => tracing::info!(events = written, "audit log flushed"),
            Err(err) => tracing::error!(error = %err, "audit task join failed"),
        }
    }
    if let Some(task) = metrics_server {
        let _ = task.await;
    }
    log_summary(&symbol_str, "final", &summary);
    Ok(())
}

fn log_summary(symbol: &str, tag: &str, s: &ReplaySummary) {
    let m = &s.metrics;
    tracing::info!(
        %symbol,
        %tag,
        events = m.events_ingested,
        books = m.book_updates,
        orders = m.orders_submitted,
        new = m.orders_new,
        filled = m.orders_filled,
        canceled = m.orders_canceled,
        rejected = m.orders_rejected,
        fills = m.fills,
        position = s.position,
        mark = s.mark.map(|p| p.0).unwrap_or_default(),
        realized = s.realized as i64,
        unrealized = s.unrealized as i64,
        total_pnl = s.total_pnl as i64,
        "summary",
    );
}
