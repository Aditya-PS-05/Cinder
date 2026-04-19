//! `ts-backtest` — run a scripted scenario against a flat-inventory
//! maker and print a summary. Purely synchronous; no I/O beyond stdout.

use anyhow::{bail, Result};
use clap::Parser;

use ts_backtest::{run_scenario, scenarios, MakerTuning};

#[derive(Parser, Debug)]
#[command(name = "ts-backtest", about = "Run a built-in backtest scenario")]
struct Cli {
    /// Scenario name: steady_maker_book | adverse_flow | trending_up.
    #[arg(long, default_value = "adverse_flow")]
    scenario: String,

    /// Number of market-event steps to generate.
    #[arg(long, default_value_t = 500)]
    steps: usize,

    /// PRNG seed for scenarios that use randomness.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Maker quote size.
    #[arg(long, default_value_t = 2)]
    quote_qty: i64,

    /// Half-spread ticks applied around mid.
    #[arg(long, default_value_t = 5)]
    half_spread_ticks: i64,

    /// Extra half-spread ticks per unit of |imbalance| (adverse-selection guard).
    #[arg(long, default_value_t = 0)]
    imbalance_widen_ticks: i64,

    /// Inventory skew in ticks per unit of inventory.
    #[arg(long, default_value_t = 1)]
    inventory_skew_ticks: i64,

    /// Absolute inventory cap before the maker suppresses a side.
    #[arg(long, default_value_t = 20)]
    max_inventory: i64,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let scenario = match scenarios::by_name(&cli.scenario, cli.seed, cli.steps) {
        Some(s) => s,
        None => bail!(
            "unknown scenario: '{}' (try steady_maker_book, adverse_flow, trending_up)",
            cli.scenario
        ),
    };

    let tuning = MakerTuning {
        quote_qty: ts_core::Qty(cli.quote_qty),
        half_spread_ticks: cli.half_spread_ticks,
        imbalance_widen_ticks: cli.imbalance_widen_ticks,
        inventory_skew_ticks: cli.inventory_skew_ticks,
        max_inventory: cli.max_inventory,
    };

    let summary = run_scenario(&scenario, &tuning);
    print_summary(&scenario.name, &summary);
    Ok(())
}

fn print_summary(scenario_name: &str, s: &ts_replay::ReplaySummary) {
    let m = &s.metrics;
    println!("scenario             : {scenario_name}");
    println!("events ingested      : {}", m.events_ingested);
    println!("book updates         : {}", m.book_updates);
    println!("orders submitted     : {}", m.orders_submitted);
    println!("  new                : {}", m.orders_new);
    println!("  partially_filled   : {}", m.orders_partially_filled);
    println!("  filled             : {}", m.orders_filled);
    println!("  canceled           : {}", m.orders_canceled);
    println!("  rejected           : {}", m.orders_rejected);
    println!("fills                : {}", m.fills);
    println!("gross filled qty     : {}", m.gross_filled_qty);
    println!("gross notional       : {}", m.gross_notional);
    println!("final position       : {}", s.position);
    match s.avg_entry {
        Some(p) => println!("final avg entry      : {}", p.0),
        None => println!("final avg entry      : (flat)"),
    }
    match s.mark {
        Some(p) => println!("final mark           : {}", p.0),
        None => println!("final mark           : (n/a)"),
    }
    println!("realized pnl         : {}", s.realized);
    println!("unrealized pnl       : {}", s.unrealized);
    println!("total pnl            : {}", s.total_pnl);
}
