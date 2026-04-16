//! `ts-tape` — record a Binance spot feed to disk, or replay a tape
//! through the engine. Two subcommands, one binary.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use ts_binance::{SpotStreamClient, SpotStreamConfig};
use ts_core::{bus::Bus, InstrumentSpec, MarketEvent, Qty, Symbol, Venue};
use ts_oms::{EngineConfig, PaperEngine, RiskConfig};
use ts_replay::{Replay, ReplaySummary};
use ts_runner::{
    audit::{spawn_audit_writer, AuditWriter},
    tape::{pump_tape, TapeReader, TapeWriter},
    EngineRunner,
};
use ts_strategy::{InventorySkewMaker, MakerConfig};

#[derive(Parser, Debug)]
#[command(
    name = "ts-tape",
    about = "Record and replay Binance market-data tapes"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Subscribe to a Binance symbol and append every event to a
    /// newline-delimited JSON file until Ctrl-C.
    Record {
        /// Output tape path (appended to if present).
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value = "BTCUSDT")]
        symbol: String,
        #[arg(long, default_value_t = 2)]
        price_scale: u8,
        #[arg(long, default_value_t = 8)]
        qty_scale: u8,
        #[arg(long, default_value = "wss://stream.binance.com:9443/ws")]
        ws_url: String,
        /// Flush the file every N events (0 disables periodic flushes).
        #[arg(long, default_value_t = 256)]
        flush_every: u64,
    },
    /// Stream a tape through the paper engine and print the final summary.
    Replay {
        /// Input tape path.
        #[arg(long)]
        tape: PathBuf,
        #[arg(long, default_value = "BTCUSDT")]
        symbol: String,
        #[arg(long, default_value_t = 2)]
        quote_qty: i64,
        #[arg(long, default_value_t = 5)]
        half_spread_ticks: i64,
        #[arg(long, default_value_t = 1)]
        inventory_skew_ticks: i64,
        #[arg(long, default_value_t = 20)]
        max_inventory: i64,
        #[arg(long, default_value = "tp")]
        cid_prefix: String,
        /// mpsc channel capacity between reader and runner.
        #[arg(long, default_value_t = 4096)]
        channel: usize,
        /// Append every ExecReport and Fill to this NDJSON file.
        #[arg(long)]
        audit: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    match Cli::parse().cmd {
        Cmd::Record {
            out,
            symbol,
            price_scale,
            qty_scale,
            ws_url,
            flush_every,
        } => run_record(out, symbol, price_scale, qty_scale, ws_url, flush_every).await,
        Cmd::Replay {
            tape,
            symbol,
            quote_qty,
            half_spread_ticks,
            inventory_skew_ticks,
            max_inventory,
            cid_prefix,
            channel,
            audit,
        } => {
            run_replay(
                tape,
                symbol,
                quote_qty,
                half_spread_ticks,
                inventory_skew_ticks,
                max_inventory,
                cid_prefix,
                channel,
                audit,
            )
            .await
        }
    }
}

async fn run_record(
    out: PathBuf,
    symbol: String,
    price_scale: u8,
    qty_scale: u8,
    ws_url: String,
    flush_every: u64,
) -> Result<()> {
    let symbol_str = symbol.to_uppercase();
    let sym = Symbol::new(symbol_str.clone());
    let mut specs: HashMap<Symbol, InstrumentSpec> = HashMap::new();
    specs.insert(
        sym.clone(),
        InstrumentSpec {
            venue: Venue::BINANCE,
            symbol: sym.clone(),
            base: String::new(),
            quote: String::new(),
            price_scale,
            qty_scale,
            min_qty: Qty(0),
            min_notional: 0,
        },
    );

    let bus: Arc<Bus<MarketEvent>> = Bus::new();
    let sub = bus.subscribe(8192);

    let mut cfg = SpotStreamConfig::new(vec![symbol_str.clone()], specs);
    cfg.ws_url = ws_url;
    let client = Arc::new(SpotStreamClient::new(cfg, Arc::clone(&bus)));
    let client_for_task = Arc::clone(&client);
    let client_task = tokio::spawn(async move { client_for_task.run().await });

    // Drain the sync bus iterator on a blocking thread and forward to the
    // async writer via an mpsc channel.
    let (tx, mut rx) = mpsc::channel::<MarketEvent>(4096);
    let bridge = tokio::task::spawn_blocking(move || {
        for e in sub.iter() {
            if tx.blocking_send(e).is_err() {
                break;
            }
        }
    });

    let mut writer = TapeWriter::create(&out).await.context("open tape file")?;
    let writer_handle = tokio::spawn(async move {
        while let Some(e) = rx.recv().await {
            if let Err(err) = writer.write_event(&e).await {
                tracing::error!(error = %err, "tape write failed");
                break;
            }
            if flush_every > 0 && writer.written() % flush_every == 0 {
                let _ = writer.flush().await;
            }
        }
        let _ = writer.flush().await;
        writer.written()
    });

    tokio::signal::ctrl_c()
        .await
        .context("install ctrl_c handler")?;
    tracing::info!(path = %out.display(), "ctrl-c, closing tape");
    client_task.abort();
    bus.close();
    let _ = bridge.await;
    let written = writer_handle.await.context("writer task")?;
    tracing::info!(events = written, "tape recorded");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_replay(
    tape: PathBuf,
    symbol: String,
    quote_qty: i64,
    half_spread_ticks: i64,
    inventory_skew_ticks: i64,
    max_inventory: i64,
    cid_prefix: String,
    channel: usize,
    audit: Option<PathBuf>,
) -> Result<()> {
    let symbol_str = symbol.to_uppercase();
    let sym = Symbol::new(symbol_str.clone());
    let venue = Venue::BINANCE;

    let engine = PaperEngine::new(
        EngineConfig {
            venue: venue.clone(),
            symbol: sym.clone(),
            notional_fallback_price: None,
        },
        RiskConfig::permissive(),
        InventorySkewMaker::new(MakerConfig {
            venue: venue.clone(),
            symbol: sym.clone(),
            quote_qty: Qty(quote_qty),
            half_spread_ticks,
            inventory_skew_ticks,
            max_inventory,
            cid_prefix,
        }),
    );
    let replay = Replay::new(engine);

    let (tx, rx) = mpsc::channel::<MarketEvent>(channel);
    let reader = TapeReader::open(&tape).await.context("open tape")?;
    let reader_task = tokio::spawn(pump_tape(reader, tx));

    let (mut runner, _handle) = EngineRunner::with_summary_tap(replay, rx, Duration::ZERO, 0);
    let audit_task = if let Some(path) = audit.as_ref() {
        let writer = AuditWriter::create(path).await.context("open audit log")?;
        let (audit_tx, audit_rx) = mpsc::channel(channel);
        runner = runner.with_audit(audit_tx);
        Some(spawn_audit_writer(writer, audit_rx))
    } else {
        None
    };

    let summary = runner.run().await;
    let count = reader_task
        .await
        .context("reader task")?
        .context("tape read")?;
    if let Some(task) = audit_task {
        match task.await {
            Ok(written) => tracing::info!(events = written, "audit log flushed"),
            Err(err) => tracing::error!(error = %err, "audit task join failed"),
        }
    }
    tracing::info!(tape = %tape.display(), events_read = count, "tape drained");
    print_summary(&symbol_str, &summary);
    Ok(())
}

fn print_summary(symbol: &str, s: &ReplaySummary) {
    let m = &s.metrics;
    println!("symbol           : {symbol}");
    println!("events ingested  : {}", m.events_ingested);
    println!("book updates     : {}", m.book_updates);
    println!("orders submitted : {}", m.orders_submitted);
    println!("fills            : {}", m.fills);
    println!("gross filled qty : {}", m.gross_filled_qty);
    println!("final position   : {}", s.position);
    match s.mark {
        Some(p) => println!("final mark       : {}", p.0),
        None => println!("final mark       : (n/a)"),
    }
    println!("realized pnl     : {}", s.realized);
    println!("unrealized pnl   : {}", s.unrealized);
    println!("total pnl        : {}", s.total_pnl);
}
