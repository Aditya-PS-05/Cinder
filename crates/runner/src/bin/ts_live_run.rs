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
use ts_risk::{KillSwitch, KillSwitchConfig, PnlGuard, PnlGuardConfig};
use ts_runner::{
    audit::{spawn_audit_writer, AuditWriter},
    bridge_bus, build_risk_config,
    intent_log::{replay_open_intents, IntentLogWriter},
    kill_switch_watch::spawn_halt_file_watcher,
    live::{LiveRunner, LiveSummary},
    live_cfg::{IntentLogCfg, KillSwitchCfg, LiveCfg, RiskCfg},
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

    /// Cadence at which the runner fires `Strategy::on_timer`, in
    /// milliseconds. Omit (or pass zero) to disable the timer tick.
    #[arg(long)]
    timer_ms: Option<u64>,

    #[arg(long)]
    rest_base: Option<String>,

    #[arg(long)]
    user_ws_base: Option<String>,

    #[arg(long)]
    reconcile_ms: Option<u64>,

    /// Append every ExecReport and Fill to this NDJSON file.
    #[arg(long)]
    audit: Option<PathBuf>,

    /// Crash-safety intent WAL. Every pre-trade-passed submit is
    /// fsynced here before the engine's HTTP call fires; terminal
    /// reports write a completion tombstone. On startup, orphaned
    /// intents are logged so operators can reconcile against venue
    /// state manually.
    #[arg(long)]
    intent_log: Option<PathBuf>,

    /// Listen address for `/metrics`, e.g. `127.0.0.1:9899`.
    #[arg(long)]
    metrics_addr: Option<String>,

    /// Filesystem path whose appearance trips the kill switch
    /// (`touch <path>` to halt trading).
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
    /// submit whose signed fill would exceed this is rejected before it
    /// reaches the venue.
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
    if let Some(v) = cli.timer_ms {
        cfg.runner.timer_ms = if v == 0 { None } else { Some(v) };
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
        // Preserve any YAML-set fsync_every — the CLI flag overrides
        // only the path, not the durability cadence.
        match cfg.audit.as_mut() {
            Some(existing) => existing.path = path,
            None => {
                cfg.audit = Some(AuditCfg {
                    path,
                    ..Default::default()
                })
            }
        }
    }
    if let Some(path) = cli.intent_log.clone() {
        cfg.intent_log = Some(IntentLogCfg { path });
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
        imbalance_widen_ticks: cfg.maker.imbalance_widen_ticks,
        vol_lambda: cfg.maker.vol_lambda,
        vol_widen_coeff: cfg.maker.vol_widen_coeff,
        inventory_skew_ticks: cfg.maker.inventory_skew_ticks,
        max_inventory: cfg.maker.max_inventory,
        cid_prefix: cfg.maker.cid_prefix.clone(),
    }));

    let bus: Arc<Bus<MarketEvent>> = Bus::new();
    let sub = bus.subscribe(cfg.runner.channel);
    let (tx, rx) = mpsc::channel::<MarketEvent>(cfg.runner.channel);
    let bridge = bridge_bus(sub, tx);

    // Construct the user-data-stream client up front so its
    // illegal-transition counter can be wired into the runner's
    // Prometheus metrics before `build()` finalises the runner.
    let user_cfg = UserDataStreamConfig::new(
        cfg.binance.user_ws_base.clone(),
        venue.clone(),
        specs.clone(),
    );
    let user_client = UserDataStreamClient::new(user_cfg, Arc::clone(&rest));
    let stream_illegal_counter = user_client.illegal_transitions_counter();

    let mut builder = LiveRunner::builder(engine, strategy, rx)
        .reconcile_interval(Duration::from_millis(cfg.binance.reconcile_ms))
        .risk_config(build_risk_config(
            cfg.risk.as_ref().and_then(|r| r.pre_trade.as_ref()),
        ))
        .stream_illegal_counter(stream_illegal_counter);

    let summary_interval = Duration::from_secs(cfg.runner.summary_secs);
    if !summary_interval.is_zero() {
        builder = builder.summary_tap(summary_interval, 8);
    }
    if let Some(ms) = cfg.runner.timer_ms {
        builder = builder.timer_interval(Duration::from_millis(ms));
    }

    let (kill_switch, halt_watcher) = wire_kill_switch(cfg.kill_switch.as_ref());
    if let Some(ks) = kill_switch.as_ref() {
        builder = builder.kill_switch(Arc::clone(ks));
    }
    if let Some(guard) = build_pnl_guard(cfg.risk.as_ref()) {
        if kill_switch.is_none() {
            tracing::warn!(
                "pnl guard configured without kill_switch; guard will observe but cannot trip"
            );
        }
        builder = builder.pnl_guard(guard);
    }

    let mut metrics_handle: Option<Arc<RunnerMetrics>> = None;
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
        builder = builder.metrics(Arc::clone(&metrics));
        metrics_handle = Some(Arc::clone(&metrics));
        tracing::info!(addr = %actual, "metrics endpoint /metrics ready");
        Some(spawn_metrics_server(listener, metrics))
    } else {
        None
    };

    // Wire the intent WAL (Section 5 crash-safety). Replay-on-startup
    // surfaces any orphaned intents from a prior session; operators
    // reconcile them against venue state manually (the runner never
    // auto-resubmits). The writer is then attached to the runner so
    // the current session's submits + terminals extend the log.
    if let Some(wal_cfg) = cfg.intent_log.as_ref() {
        let open = replay_open_intents(&wal_cfg.path)
            .await
            .with_context(|| format!("replay intent log {}", wal_cfg.path.display()))?;
        if open.is_empty() {
            tracing::info!(path = %wal_cfg.path.display(), "intent-log replay: no orphaned intents");
        } else {
            tracing::warn!(
                path = %wal_cfg.path.display(),
                count = open.len(),
                "intent-log replay: orphaned intents detected from prior session; reconcile manually",
            );
            for order in &open {
                tracing::warn!(
                    cid = %order.cid.as_str(),
                    symbol = %order.symbol.as_str(),
                    side = ?order.side,
                    qty = order.qty.0,
                    "orphan intent",
                );
            }
        }
        let writer = IntentLogWriter::open(&wal_cfg.path)
            .await
            .with_context(|| format!("open intent log {}", wal_cfg.path.display()))?;
        builder = builder.intent_log(writer);
    }

    let audit_task = if let Some(audit_cfg) = cfg.audit.as_ref() {
        let writer = AuditWriter::create(&audit_cfg.path)
            .await
            .context("open audit log")?
            .with_fsync_every(audit_cfg.fsync_every);
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
    ws_cfg.rest_base = cfg.binance.rest_base.clone();
    let md_client = Arc::new(SpotStreamClient::new(ws_cfg, Arc::clone(&bus)));
    if let Some(m) = metrics_handle.as_ref() {
        m.attach_ws_ping_rtt(md_client.last_ping_rtt_handle());
        m.attach_ws_resync_counter(md_client.resync_counter());
    }
    let md_for_task = Arc::clone(&md_client);
    let md_task = tokio::spawn(async move { md_for_task.run().await });

    // User-data-stream: venue-side exec reports flow from the WS into
    // the engine's inbound sender so `reconcile` surfaces them. The
    // client was constructed above so its illegal-transition counter
    // could be wired into metrics.
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

/// Build the optional shared [`KillSwitch`] from config and, if a
/// `halt_file` path is set, spawn the filesystem watcher that trips it
/// when the file appears. Returns the switch handle and the watcher's
/// join-handle (so `main` can abort it during shutdown). A missing
/// `kill_switch` section leaves the runner without a switch attached.
/// Build a [`PnlGuard`] from `cfg`. Returns `None` when no risk section
/// is configured or when both thresholds are unset — a no-op guard is
/// pointless overhead, and silently attaching one would hide an obvious
/// misconfiguration.
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
