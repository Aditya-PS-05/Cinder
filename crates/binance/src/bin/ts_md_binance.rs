//! `ts-md-binance` — minimal CLI that drives the spot connector end to
//! end. Useful as a smoke test: subscribes to a symbol list, spawns a
//! blocking printer on the bus, and runs until Ctrl-C.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use ts_binance::{SpotStreamClient, SpotStreamConfig};
use ts_core::{bus::Bus, InstrumentSpec, MarketEvent, MarketPayload, Qty, Symbol, Venue};

#[derive(Debug, Parser)]
#[command(name = "ts-md-binance", about = "Binance spot market data tap")]
struct Cli {
    /// Comma-separated uppercase symbols, e.g. `BTCUSDT,ETHUSDT`.
    #[arg(long, default_value = "BTCUSDT")]
    symbols: String,

    /// Price scale (decimal places) applied to every symbol.
    #[arg(long, default_value_t = 2)]
    price_scale: u8,

    /// Quantity scale (decimal places) applied to every symbol.
    #[arg(long, default_value_t = 8)]
    qty_scale: u8,

    /// Override the WebSocket URL (handy for testnet / local replay).
    #[arg(long, default_value = "wss://stream.binance.com:9443/ws")]
    ws_url: String,
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
    let symbols: Vec<String> = cli
        .symbols
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    anyhow::ensure!(!symbols.is_empty(), "at least one symbol is required");

    let mut specs: HashMap<Symbol, InstrumentSpec> = HashMap::new();
    for sym in &symbols {
        let s = Symbol::new(sym.clone());
        specs.insert(
            s.clone(),
            InstrumentSpec {
                venue: Venue::BINANCE,
                symbol: s,
                base: String::new(),
                quote: String::new(),
                price_scale: cli.price_scale,
                qty_scale: cli.qty_scale,
                min_qty: Qty(0),
                min_notional: 0,
            },
        );
    }

    let bus: Arc<Bus<MarketEvent>> = Bus::new();
    let sub = bus.subscribe(4096);
    let price_scale = cli.price_scale;
    let qty_scale = cli.qty_scale;

    let printer = tokio::task::spawn_blocking(move || {
        for evt in sub.iter() {
            let lat_us = evt.latency_nanos() / 1_000;
            match evt.payload {
                MarketPayload::Trade(t) => {
                    println!(
                        "TRADE {:8} {:>14} x {:<14} {:>4} seq={} lat={}us",
                        evt.symbol,
                        t.price.to_string(price_scale),
                        t.qty.to_string(qty_scale),
                        t.taker_side,
                        evt.seq,
                        lat_us,
                    );
                }
                MarketPayload::BookDelta(d) => {
                    println!(
                        "DEPTH {:8} bids={:>3} asks={:>3} seq={} lat={}us",
                        evt.symbol,
                        d.bids.len(),
                        d.asks.len(),
                        evt.seq,
                        lat_us,
                    );
                }
                other => {
                    println!(
                        "OTHER {:8} {:?}",
                        evt.symbol,
                        std::mem::discriminant(&other)
                    );
                }
            }
        }
    });

    let mut cfg = SpotStreamConfig::new(symbols, specs);
    cfg.ws_url = cli.ws_url;
    let client = SpotStreamClient::new(cfg, Arc::clone(&bus));

    tokio::select! {
        _ = client.run() => {}
        r = tokio::signal::ctrl_c() => {
            r.context("install ctrl_c handler")?;
            tracing::info!("ctrl-c, shutting down");
        }
    }

    bus.close();
    let _ = printer.await;
    Ok(())
}
