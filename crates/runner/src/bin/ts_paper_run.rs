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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use ts_binance::{SpotStreamClient, SpotStreamConfig};
use ts_core::{bus::Bus, InstrumentSpec, MarketEvent, Qty, Symbol, Venue};
use ts_oms::{EngineConfig, PaperEngine, RiskConfig};
use ts_replay::{Replay, ReplaySummary};
use ts_runner::{bridge_bus, EngineRunner};
use ts_strategy::{InventorySkewMaker, MakerConfig};

#[derive(Parser, Debug)]
#[command(
    name = "ts-paper-run",
    about = "Live paper-trading harness over Binance spot"
)]
struct Cli {
    /// Symbol to trade (uppercase, e.g. BTCUSDT).
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

    /// Price scale (decimal places).
    #[arg(long, default_value_t = 2)]
    price_scale: u8,

    /// Quantity scale (decimal places).
    #[arg(long, default_value_t = 8)]
    qty_scale: u8,

    /// WebSocket endpoint.
    #[arg(long, default_value = "wss://stream.binance.com:9443/ws")]
    ws_url: String,

    /// Maker quote size (mantissa at qty_scale).
    #[arg(long, default_value_t = 2)]
    quote_qty: i64,

    /// Half-spread ticks applied around mid.
    #[arg(long, default_value_t = 5)]
    half_spread_ticks: i64,

    /// Inventory skew in ticks per unit of inventory.
    #[arg(long, default_value_t = 1)]
    inventory_skew_ticks: i64,

    /// Absolute inventory cap before the maker suppresses a side.
    #[arg(long, default_value_t = 20)]
    max_inventory: i64,

    /// Client-order-id prefix.
    #[arg(long, default_value = "pr")]
    cid_prefix: String,

    /// Summary broadcast interval in seconds. Zero disables the tap.
    #[arg(long, default_value_t = 5)]
    summary_secs: u64,

    /// Bus subscription + mpsc channel capacity.
    #[arg(long, default_value_t = 4096)]
    channel: usize,
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
    let venue = Venue::BINANCE;
    let symbol_str = cli.symbol.to_uppercase();
    let symbol = Symbol::new(symbol_str.clone());

    let mut specs: HashMap<Symbol, InstrumentSpec> = HashMap::new();
    specs.insert(
        symbol.clone(),
        InstrumentSpec {
            venue: venue.clone(),
            symbol: symbol.clone(),
            base: String::new(),
            quote: String::new(),
            price_scale: cli.price_scale,
            qty_scale: cli.qty_scale,
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
            quote_qty: Qty(cli.quote_qty),
            half_spread_ticks: cli.half_spread_ticks,
            inventory_skew_ticks: cli.inventory_skew_ticks,
            max_inventory: cli.max_inventory,
            cid_prefix: cli.cid_prefix,
        }),
    );
    let replay = Replay::new(engine);

    let bus: Arc<Bus<MarketEvent>> = Bus::new();
    let sub = bus.subscribe(cli.channel);
    let (tx, rx) = mpsc::channel::<MarketEvent>(cli.channel);
    let bridge = bridge_bus(sub, tx);

    let summary_interval = Duration::from_secs(cli.summary_secs);
    let (runner, handle) = EngineRunner::with_summary_tap(replay, rx, summary_interval, 8);

    let printer = handle.subscribe_summaries().map(|mut sub| {
        let sym = symbol_str.clone();
        tokio::spawn(async move {
            while let Ok(summary) = sub.recv().await {
                log_summary(&sym, "live", &summary);
            }
        })
    });

    let mut ws_cfg = SpotStreamConfig::new(vec![symbol_str.clone()], specs);
    ws_cfg.ws_url = cli.ws_url;
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

    let summary = runner_task.await.context("runner task")?;
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
