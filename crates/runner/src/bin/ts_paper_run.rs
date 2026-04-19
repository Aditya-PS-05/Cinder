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
use ts_risk::{KillSwitch, KillSwitchConfig, PnlGuard, PnlGuardConfig};
use ts_runner::{
    audit::{spawn_audit_writer, AuditWriter},
    bridge_bus,
    kill_switch_watch::spawn_halt_file_watcher,
    metrics::{spawn_metrics_server, RunnerMetrics},
    paper_cfg::{AuditCfg, KillSwitchCfg, MetricsCfg, PaperCfg, PreTradeCfg, RiskCfg},
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

    /// Filesystem path whose appearance trips the kill switch
    /// (`touch <path>` to halt trading). Implies kill-switch attached.
    #[arg(long)]
    halt_file: Option<PathBuf>,

    /// Max drawdown (realized-net + unrealized, `price_scale * qty_scale`
    /// mantissa) before the PnL guard trips the kill switch.
    #[arg(long)]
    max_drawdown: Option<i64>,

    /// Max realized-net daily loss (same mantissa) before the PnL guard
    /// trips the kill switch.
    #[arg(long)]
    max_daily_loss: Option<i64>,

    /// Per-symbol absolute position cap (mantissa at `qty_scale`). Any
    /// submit whose signed fill would exceed this is rejected.
    #[arg(long)]
    max_position_qty: Option<i64>,

    /// Per-order notional cap (mantissa at `price_scale * qty_scale`).
    #[arg(long)]
    max_order_notional: Option<i64>,

    /// Global cap on concurrently-open orders.
    #[arg(long)]
    max_open_orders: Option<usize>,

    /// Symbol whitelist (comma-separated, e.g. `BTCUSDT,ETHUSDT`).
    /// Any symbol not in the list is rejected before submission.
    #[arg(long, value_delimiter = ',')]
    whitelist: Option<Vec<String>>,
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
    if let Some(path) = cli.halt_file.clone() {
        let mut ks = cfg.kill_switch.clone().unwrap_or_default();
        ks.halt_file = Some(path);
        cfg.kill_switch = Some(ks);
    }
    if cli.max_drawdown.is_some() || cli.max_daily_loss.is_some() {
        let mut r = cfg.risk.clone().unwrap_or_default();
        if let Some(v) = cli.max_drawdown {
            r.max_drawdown = Some(v);
        }
        if let Some(v) = cli.max_daily_loss {
            r.max_daily_loss = Some(v);
        }
        cfg.risk = Some(r);
    }
    if cli.max_position_qty.is_some()
        || cli.max_order_notional.is_some()
        || cli.max_open_orders.is_some()
        || cli.whitelist.is_some()
    {
        let mut r = cfg.risk.clone().unwrap_or_default();
        let mut pt = r.pre_trade.clone().unwrap_or_default();
        if let Some(v) = cli.max_position_qty {
            pt.max_position_qty = Some(v);
        }
        if let Some(v) = cli.max_order_notional {
            pt.max_order_notional = Some(v);
        }
        if let Some(v) = cli.max_open_orders {
            pt.max_open_orders = Some(v);
        }
        if let Some(v) = cli.whitelist.clone() {
            pt.whitelist = Some(v);
        }
        r.pre_trade = Some(pt);
        cfg.risk = Some(r);
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
        build_risk_config(cfg.risk.as_ref().and_then(|r| r.pre_trade.as_ref())),
        InventorySkewMaker::new(MakerConfig {
            venue: venue.clone(),
            symbol: symbol.clone(),
            quote_qty: Qty(cfg.maker.quote_qty),
            half_spread_ticks: cfg.maker.half_spread_ticks,
            imbalance_widen_ticks: cfg.maker.imbalance_widen_ticks,
            vol_lambda: cfg.maker.vol_lambda,
            vol_widen_coeff: cfg.maker.vol_widen_coeff,
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

    let (kill_switch, halt_watcher) = wire_kill_switch(cfg.kill_switch.as_ref());
    if let Some(ks) = kill_switch.as_ref() {
        runner = runner.with_kill_switch(Arc::clone(ks));
    }
    if let Some(guard) = build_pnl_guard(cfg.risk.as_ref()) {
        if kill_switch.is_none() {
            tracing::warn!(
                "pnl guard configured without kill_switch; guard will observe but cannot trip"
            );
        }
        runner = runner.with_pnl_guard(guard);
    }

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
        if let Some(ks) = kill_switch.as_ref() {
            metrics.observe_kill_switch(ks);
        }
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
    if let Some(w) = halt_watcher.as_ref() {
        w.abort();
    }
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

/// Fold an optional pre-trade config into a [`RiskConfig`]. Missing
/// fields inherit the permissive baseline, so operators can tighten
/// one knob at a time. Whitelist entries are normalized to upper-case
/// so YAML like `btcusdt` matches the symbol the runner trades.
fn build_risk_config(cfg: Option<&PreTradeCfg>) -> RiskConfig {
    let mut rc = RiskConfig::permissive();
    let Some(pt) = cfg else {
        return rc;
    };
    if let Some(v) = pt.max_position_qty {
        rc.max_position_qty = Qty(v);
    }
    if let Some(v) = pt.max_order_notional {
        rc.max_order_notional = v;
    }
    if let Some(v) = pt.max_open_orders {
        rc.max_open_orders = v;
    }
    if let Some(wl) = pt.whitelist.as_ref() {
        rc.whitelist = wl.iter().map(|s| Symbol::new(s.to_uppercase())).collect();
    }
    rc
}

/// Build a [`PnlGuard`] from `cfg`. Returns `None` when no risk section
/// is configured or both thresholds are unset — a no-op guard would
/// silently mask an obvious misconfiguration.
fn build_pnl_guard(cfg: Option<&RiskCfg>) -> Option<PnlGuard> {
    let cfg = cfg?;
    if cfg.max_drawdown.is_none() && cfg.max_daily_loss.is_none() {
        return None;
    }
    Some(PnlGuard::new(PnlGuardConfig {
        max_drawdown: cfg.max_drawdown.map(i128::from),
        max_daily_loss: cfg.max_daily_loss.map(i128::from),
        day_length: Duration::from_secs(cfg.day_length_secs),
    }))
}

/// Build the optional shared [`KillSwitch`] from config and, if a
/// `halt_file` path is set, spawn the filesystem watcher that trips it
/// when the file appears. Returns the switch and the watcher's join
/// handle so the caller can abort during shutdown. Missing
/// `kill_switch` section ⇒ no switch attached.
fn wire_kill_switch(
    cfg: Option<&KillSwitchCfg>,
) -> (Option<Arc<KillSwitch>>, Option<tokio::task::JoinHandle<()>>) {
    let Some(cfg) = cfg else {
        return (None, None);
    };
    let ks = Arc::new(KillSwitch::new(KillSwitchConfig {
        reject_threshold: cfg.reject_threshold,
        window: Duration::from_millis(cfg.window_ms),
    }));
    let watcher = cfg.halt_file.as_ref().map(|path| {
        spawn_halt_file_watcher(
            Arc::clone(&ks),
            path.clone(),
            Duration::from_millis(cfg.poll_ms),
        )
    });
    (Some(ks), watcher)
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
