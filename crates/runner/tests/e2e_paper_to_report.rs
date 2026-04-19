//! End-to-end integration test covering the paper-trading pipeline:
//!
//!     synthetic MarketEvents
//!       → TapeWriter → on-disk NDJSON tape
//!       → TapeReader → EngineRunner (PaperEngine + InventorySkewMaker)
//!       → audit mpsc → AuditWriter → on-disk NDJSON audit log
//!       → ts_report::read_audit_file → derived Report
//!
//! The whole path is exercised in one process; no crate is mocked.
//! The engine's strategy places both sides on each snapshot, so the
//! audit file accumulates `New` reports that the derived report must
//! account for. Fills are not generated because the paper engine
//! cannot match resting quotes against book-only market data — a
//! deliberate asymmetry that keeps this test deterministic.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;

use ts_core::{
    BookLevel, BookSnapshot, MarketEvent, MarketPayload, Price, Qty, Symbol, Timestamp, Venue,
};
use ts_oms::{EngineConfig, PaperEngine, RiskConfig};
use ts_replay::Replay;
use ts_runner::audit::{spawn_audit_writer, AuditWriter};
use ts_runner::tape::{pump_tape, TapeReader, TapeWriter};
use ts_runner::EngineRunner;
use ts_strategy::{InventorySkewMaker, MakerConfig};

fn temp_path(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ts-e2e-{}-{}-{}.ndjson",
        std::process::id(),
        tag,
        n
    ))
}

fn snapshot(bid: i64, ask: i64, seq: u64, ts_ms: i64) -> MarketEvent {
    MarketEvent {
        venue: Venue::BINANCE,
        symbol: Symbol::from_static("BTCUSDT"),
        exchange_ts: Timestamp::from_unix_millis(ts_ms),
        local_ts: Timestamp::from_unix_millis(ts_ms + 5),
        seq,
        payload: MarketPayload::BookSnapshot(BookSnapshot {
            bids: vec![BookLevel {
                price: Price(bid),
                qty: Qty(10),
            }],
            asks: vec![BookLevel {
                price: Price(ask),
                qty: Qty(10),
            }],
        }),
    }
}

fn build_replay() -> Replay<InventorySkewMaker> {
    let venue = Venue::BINANCE;
    let symbol = Symbol::from_static("BTCUSDT");
    let engine = PaperEngine::new(
        EngineConfig {
            venue: venue.clone(),
            symbol: symbol.clone(),
            notional_fallback_price: Some(Price(100)),
        },
        RiskConfig::permissive(),
        InventorySkewMaker::new(MakerConfig {
            venue,
            symbol,
            quote_qty: Qty(2),
            half_spread_ticks: 5,
            inventory_skew_ticks: 0,
            max_inventory: 20,
            cid_prefix: "e2e".into(),
        }),
    );
    Replay::new(engine)
}

#[tokio::test]
async fn paper_runner_pipeline_produces_audit_tape_that_ts_report_can_parse() {
    let tape_path = temp_path("tape");
    let audit_path = temp_path("audit");

    // Stage the tape: three snapshots walking the book up one tick at a time.
    {
        let mut w = TapeWriter::create(&tape_path).await.unwrap();
        w.write_event(&snapshot(100, 110, 1, 1_700_000_000_000))
            .await
            .unwrap();
        w.write_event(&snapshot(101, 111, 2, 1_700_000_000_050))
            .await
            .unwrap();
        w.write_event(&snapshot(102, 112, 3, 1_700_000_000_100))
            .await
            .unwrap();
        w.flush().await.unwrap();
    }

    // Wire the audit sink in front of the runner.
    let audit_writer = AuditWriter::create(&audit_path).await.unwrap();
    let (audit_tx, audit_rx) = mpsc::channel(64);
    let audit_task = spawn_audit_writer(audit_writer, audit_rx);

    // Wire the runner with a market-event mpsc and the audit tap.
    let (tx, rx) = mpsc::channel(64);
    let (runner, handle) = EngineRunner::new(build_replay(), rx);
    let runner = runner.with_audit(audit_tx);
    let runner_task = tokio::spawn(runner.run());

    // Replay the tape through the runner.
    let reader = TapeReader::open(&tape_path).await.unwrap();
    let pumped = pump_tape(reader, tx).await.unwrap();
    assert_eq!(pumped, 3, "tape reader should yield every staged event");

    // Give the runner a moment to drain the channel, then shut it down.
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.shutdown();

    let summary = runner_task.await.unwrap();
    assert_eq!(summary.metrics.events_ingested, 3);
    assert_eq!(summary.metrics.book_updates, 3);
    assert!(
        summary.metrics.orders_submitted >= 2,
        "maker must quote both sides on at least one tick (got {} orders)",
        summary.metrics.orders_submitted,
    );

    // Close the audit tap by dropping the runner's sender (already
    // dropped via the moved `runner`), then wait for the writer to
    // flush and report its line count.
    let audit_written = audit_task.await.unwrap();
    assert!(
        audit_written >= 2,
        "audit file should hold at least one report per quoted side, got {audit_written}"
    );

    // Parse the audit file back through ts_report and assert on the
    // derived aggregates — this is the contract the ts-report CLI
    // relies on when it ingests a tape after a run.
    let (report, stats) = ts_report::read_audit_file(&audit_path).unwrap();
    assert_eq!(
        stats.lines_read, audit_written,
        "every line in the file should parse cleanly"
    );
    assert_eq!(stats.lines_skipped, 0);

    assert_eq!(report.reports_seen, audit_written);
    assert_eq!(
        report.fills_seen, 0,
        "paper engine does not match resting limits on snapshots"
    );
    assert!(
        report.statuses.new >= 2,
        "every maker quote lands as OrderStatus::New"
    );

    // No fills means no quant series; the derived metrics must stay
    // unset rather than report synthetic zeroes.
    assert_eq!(report.quant.max_drawdown, 0);
    assert!(report.quant.fills_per_min.is_none());
    assert!(report.quant.sharpe_like.is_none());
    assert_eq!(report.liquidity.maker_fills, 0);
    assert_eq!(report.liquidity.taker_fills, 0);
    assert_eq!(report.liquidity.unknown_fills, 0);

    // format_text renders without panicking — smoke that the CLI path
    // from (Report, ReadStats) to a human-readable block works too.
    let text = ts_report::format_text(&report, &stats);
    assert!(text.contains("-- liquidity --"));
    assert!(text.contains("-- quant --"));

    let _ = tokio::fs::remove_file(&tape_path).await;
    let _ = tokio::fs::remove_file(&audit_path).await;
}
