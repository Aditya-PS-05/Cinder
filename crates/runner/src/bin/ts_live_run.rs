//! `ts-live-run` — live Binance spot trading harness.
//!
//! Wires the full live stack: Binance market-data WS → local book →
//! `InventorySkewMaker` → `BinanceLiveEngine` (REST orders) +
//! `UserDataStreamClient` (venue fills back into the engine) →
//! `LiveRunner` (orchestrator). Optional NDJSON audit log and
//! Prometheus `/metrics` endpoint.
//!
//! Credentials are loaded from the environment variables named in the
//! `binance.api_key_env` / `binance.api_secret_env` config fields.
//! Layered config: `base.yaml` + `<env>.yaml` via `--config-dir`,
//! overridden by `TS_*` env vars, overridden in turn by explicit CLI
//! flags. Without `--config-dir` the binary falls back to the
//! testnet-safe defaults in [`LiveCfg::default`].

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use ts_binance::{
    BinanceLiveEngine, SignedClient, SpotStreamClient, SpotStreamConfig, UserDataStreamClient,
    UserDataStreamConfig,
};
use ts_config::Env;
use ts_core::{bus::Bus, InstrumentSpec, MarketEvent, Qty, Symbol, Venue};
use ts_runner::{
    audit::{spawn_audit_writer, AuditWriter},
    bridge_bus,
    live::{LiveRunner, LiveSummary},
    live_cfg::LiveCfg,
    metrics::{spawn_metrics_server, RunnerMetrics},
    paper_cfg::{AuditCfg, MetricsCfg},
};
use ts_strategy::{InventorySkewMaker, MakerConfig, Strategy};

#[derive(Parser, Debug)]
#[command(name = "ts-live-run", about = "Live Binance-spot trading harness")]
struct Cli {
    #[arg(long)]
    config_dir: Option<PathBuf>,

    #[arg(long, default_value = "dev")]
    env: String,

    #[arg(long)]
    symbol: Option<String>,

    #[arg(long)]
    price_scale: Option<u8>,

    #[arg(long)]
    qty_scale: Option<u8>,

    #[arg(long)]
    ws_url: Option<String>,

    #[arg(long)]
    quote_qty: Option<i64>,

    #[arg(long)]
    half_spread_ticks: Option<i64>,

    #[arg(long)]
    inventory_skew_ticks: Option<i64>,

    #[arg(long)]
    max_inventory: Option<i64>,

    #[arg(long)]
    cid_prefix: Option<String>,

    #[arg(long)]
    summary_secs: Option<u64>,

    #[arg(long)]
    channel: Option<usize>,

    #[arg(long)]
    rest_base: Option<String>,

    #[arg(long)]
    user_ws_base: Option<String>,

    #[arg(long)]
    reconcile_ms: Option<u64>,

    /// Append every ExecReport and Fill to this NDJSON file.
    #[arg(long)]
    audit: Option<PathBuf>,

    /// Listen address for `/metrics`, e.g. `127.0.0.1:9899`.
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

fn resolve_config(cli: &Cli) -> Result<LiveCfg> {
    let mut cfg = match cli.config_dir.as_ref() {
        Some(dir) => {
            let env = Env::parse(&cli.env).with_context(|| format!("--env {}", cli.env))?;
            LiveCfg::load(dir, env)
                .with_context(|| format!("load config from {}", dir.display()))?
        }
        None => LiveCfg::default(),
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
    if let Some(v) = cli.rest_base.clone() {
        cfg.binance.rest_base = v;
    }
    if let Some(v) = cli.user_ws_base.clone() {
        cfg.binance.user_ws_base = v;
    }
    if let Some(v) = cli.reconcile_ms {
        cfg.binance.reconcile_ms = v;
    }
    if let Some(path) = cli.audit.clone() {
        cfg.audit = Some(AuditCfg { path });
    }
    if let Some(listen) = cli.metrics_addr.clone() {
        cfg.metrics = Some(MetricsCfg { listen });
    }

    Ok(cfg)
}

async fn run(cfg: LiveCfg) -> Result<()> {
    let venue = Venue::BINANCE;
    let symbol_str = cfg.market.symbol.to_uppercase();
    let symbol = Symbol::new(symbol_str.clone());

    let api_key = std::env::var(&cfg.binance.api_key_env)
        .with_context(|| format!("read {} env var", cfg.binance.api_key_env))?;
    let api_secret = std::env::var(&cfg.binance.api_secret_env)
        .with_context(|| format!("read {} env var", cfg.binance.api_secret_env))?;
    if api_key.is_empty() || api_secret.is_empty() {
        bail!(
            "credentials from {} / {} are empty",
            cfg.binance.api_key_env,
            cfg.binance.api_secret_env
        );
    }

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

    let rest = Arc::new(SignedClient::new(
        cfg.binance.rest_base.clone(),
        api_key,
        api_secret,
    ));

    let engine = BinanceLiveEngine::new(
        Arc::clone(&rest),
        specs.clone(),
        venue.clone(),
        cfg.runner.channel,
    );
    let inbound = engine.inbound_sender();

    let strategy: Box<dyn Strategy> = Box::new(InventorySkewMaker::new(MakerConfig {
        venue: venue.clone(),
        symbol: symbol.clone(),
        quote_qty: Qty(cfg.maker.quote_qty),
        half_spread_ticks: cfg.maker.half_spread_ticks,
        inventory_skew_ticks: cfg.maker.inventory_skew_ticks,
        max_inventory: cfg.maker.max_inventory,
        cid_prefix: cfg.maker.cid_prefix.clone(),
    }));

    let bus: Arc<Bus<MarketEvent>> = Bus::new();
    let sub = bus.subscribe(cfg.runner.channel);
    let (tx, rx) = mpsc::channel::<MarketEvent>(cfg.runner.channel);
    let bridge = bridge_bus(sub, tx);

    let mut builder = LiveRunner::builder(engine, strategy, rx)
        .reconcile_interval(Duration::from_millis(cfg.binance.reconcile_ms));

    let summary_interval = Duration::from_secs(cfg.runner.summary_secs);
    if !summary_interval.is_zero() {
        builder = builder.summary_tap(summary_interval, 8);
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
        builder = builder.metrics(Arc::clone(&metrics));
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
        builder = builder.audit(audit_tx);
        Some(spawn_audit_writer(writer, audit_rx))
    } else {
        None
    };

    let (runner, handle) = builder.build();

    let printer = handle.subscribe_summaries().map(|mut sub| {
        let sym = symbol_str.clone();
        tokio::spawn(async move {
            while let Ok(summary) = sub.recv().await {
                log_summary(&sym, "live", &summary);
            }
        })
    });

    let mut ws_cfg = SpotStreamConfig::new(vec![symbol_str.clone()], specs.clone());
    ws_cfg.ws_url = cfg.market.ws_url.clone();
    let md_client = Arc::new(SpotStreamClient::new(ws_cfg, Arc::clone(&bus)));
    let md_for_task = Arc::clone(&md_client);
    let md_task = tokio::spawn(async move { md_for_task.run().await });

    // User-data-stream: venue-side exec reports flow from the WS into
    // the engine's inbound sender so `reconcile` surfaces them.
    let user_cfg =
        UserDataStreamConfig::new(cfg.binance.user_ws_base.clone(), venue.clone(), specs);
    let user_client = UserDataStreamClient::new(user_cfg, Arc::clone(&rest));
    let user_task = tokio::spawn(async move {
        if let Err(err) = user_client.run(inbound).await {
            tracing::error!(error = %err, "user-data-stream session ended with error");
        }
    });

    let runner_task = tokio::spawn(runner.run());

    tokio::signal::ctrl_c()
        .await
        .context("install ctrl_c handler")?;
    tracing::info!("ctrl-c, shutting down");

    md_task.abort();
    user_task.abort();
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

fn log_summary(symbol: &str, tag: &str, s: &LiveSummary) {
    tracing::info!(
        %symbol,
        %tag,
        events = s.events_ingested,
        books = s.book_updates,
        orders = s.orders_submitted,
        new = s.orders_new,
        filled = s.orders_filled,
        canceled = s.orders_canceled,
        rejected = s.orders_rejected,
        expired = s.orders_expired,
        fills = s.fills,
        reconcile_errors = s.reconcile_errors,
        "summary",
    );
}
