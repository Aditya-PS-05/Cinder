//! Post-hoc performance reporter for NDJSON audit tapes.
//!
//! Reads an audit log produced by `ts-paper-run` or `ts-live-run` and
//! folds it into a structured [`Report`]: per-status counters,
//! per-symbol fill volume and inventory excursions, and realized pnl
//! via [`ts_pnl::Accountant`]. The companion `ts-report` binary
//! wraps this behind a CLI so humans can ask "how did last night's
//! session behave?" without writing code.
//!
//! This crate is stream-friendly — [`read_audit_ndjson`] consumes any
//! [`std::io::BufRead`], so it works equally on files, stdin, and
//! in-memory buffers. Errors during read propagate; malformed lines
//! (for example from a crashed writer leaving a half-flushed tail)
//! are counted in [`ReadStats`] and skipped rather than aborting the
//! whole report.
//!
//! Fills are attributed through [`AuditEvent::Fill`] only; a
//! `Report` carries its own `fills` vector (populated on the execute
//! path for Filled/PartiallyFilled statuses) but the runner writer
//! emits a separate `AuditEvent::Fill` for each one, so counting
//! both sides would double-count. The status counters here therefore
//! ignore the embedded fills entirely.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::{self, BufRead};
use std::path::Path;

use serde::Serialize;
use ts_core::{AuditEvent, OrderStatus, Side, Symbol};
use ts_pnl::Accountant;

/// Counts of terminal and progression statuses seen on the tape.
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub struct StatusCounters {
    pub new: u64,
    pub partially_filled: u64,
    pub filled: u64,
    pub canceled: u64,
    pub rejected: u64,
    pub expired: u64,
}

/// Per-symbol fill aggregates and end-of-tape inventory state.
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub struct SymbolReport {
    pub symbol: String,
    pub fills: u64,
    pub buy_fills: u64,
    pub sell_fills: u64,
    /// Sum of filled qty across all fills in qty-scale mantissa.
    pub gross_filled_qty: i128,
    /// Sum of `price * qty` across all fills in `price_scale * qty_scale`
    /// mantissa.
    pub gross_notional: i128,
    /// Signed position at end-of-tape in qty-scale mantissa.
    pub end_position: i64,
    /// Maximum long excursion reached while streaming.
    pub max_long: i64,
    /// Maximum short excursion reached while streaming (negative).
    pub max_short: i64,
    /// Weighted-average entry price at end-of-tape in price-scale
    /// mantissa. `None` when the position is flat.
    pub avg_entry: Option<i64>,
    /// Realized pnl at end-of-tape in `price_scale * qty_scale` mantissa.
    pub realized: i128,
}

/// Aggregated report over a stream of [`AuditEvent`]s.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Report {
    pub reports_seen: u64,
    pub fills_seen: u64,
    pub statuses: StatusCounters,
    pub symbols: BTreeMap<String, SymbolReport>,
    /// Sum of realized pnl across every symbol.
    pub realized_total: i128,
}

/// Counters describing what the reader saw on the wire.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadStats {
    pub lines_read: u64,
    pub lines_skipped: u64,
}

impl Report {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one audit event into the report. `accountant` accumulates
    /// position and realized pnl across fills so finalize() can pull
    /// the end-of-tape values.
    pub fn ingest(&mut self, event: &AuditEvent, accountant: &mut Accountant) {
        match event {
            AuditEvent::Report(r) => {
                self.reports_seen += 1;
                match r.status {
                    OrderStatus::New => self.statuses.new += 1,
                    OrderStatus::PartiallyFilled => self.statuses.partially_filled += 1,
                    OrderStatus::Filled => self.statuses.filled += 1,
                    OrderStatus::Canceled => self.statuses.canceled += 1,
                    OrderStatus::Rejected => self.statuses.rejected += 1,
                    OrderStatus::Expired => self.statuses.expired += 1,
                }
            }
            AuditEvent::Fill(f) => {
                self.fills_seen += 1;
                accountant.on_fill(f);

                let key = f.symbol.as_str().to_string();
                let entry = self
                    .symbols
                    .entry(key.clone())
                    .or_insert_with(|| SymbolReport {
                        symbol: key,
                        ..Default::default()
                    });
                entry.fills += 1;
                match f.side {
                    Side::Buy => entry.buy_fills += 1,
                    Side::Sell => entry.sell_fills += 1,
                    Side::Unknown => {}
                }
                entry.gross_filled_qty += f.qty.0 as i128;
                entry.gross_notional += (f.price.0 as i128) * (f.qty.0 as i128);

                let pos = accountant.position(&f.symbol);
                if pos > entry.max_long {
                    entry.max_long = pos;
                }
                if pos < entry.max_short {
                    entry.max_short = pos;
                }
            }
        }
    }

    /// Snapshot end-of-tape position, avg entry, and realized pnl from
    /// the accountant onto each per-symbol entry. Also totals realized
    /// across symbols. Call after the last `ingest`.
    pub fn finalize(&mut self, accountant: &Accountant) {
        let mut total: i128 = 0;
        for (sym_str, entry) in self.symbols.iter_mut() {
            let sym = Symbol::new(sym_str.clone());
            if let Some(book) = accountant.book(&sym) {
                entry.end_position = book.position;
                entry.realized = book.realized;
                entry.avg_entry = if book.position == 0 {
                    None
                } else {
                    Some(book.avg_entry)
                };
            }
            total += entry.realized;
        }
        self.realized_total = total;
    }
}

/// Read an NDJSON audit tape from any `BufRead`. Blank lines are
/// tolerated; lines that fail to decode are skipped and counted in
/// `ReadStats::lines_skipped` so a partial trailing line from a
/// crashed writer does not poison the report.
pub fn read_audit_ndjson<R: BufRead>(reader: R) -> io::Result<(Report, ReadStats)> {
    let mut report = Report::new();
    let mut accountant = Accountant::new();
    let mut lines_read = 0u64;
    let mut lines_skipped = 0u64;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        lines_read += 1;
        match serde_json::from_str::<AuditEvent>(&line) {
            Ok(ev) => report.ingest(&ev, &mut accountant),
            Err(_) => lines_skipped += 1,
        }
    }

    report.finalize(&accountant);
    Ok((
        report,
        ReadStats {
            lines_read,
            lines_skipped,
        },
    ))
}

/// Convenience wrapper: open `path`, read the whole tape.
pub fn read_audit_file<P: AsRef<Path>>(path: P) -> io::Result<(Report, ReadStats)> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    read_audit_ndjson(reader)
}

/// Plain-text summary for humans. Deterministic symbol ordering so
/// snapshot tests stay stable.
pub fn format_text(report: &Report, stats: &ReadStats) -> String {
    let mut out = String::new();
    out.push_str("=== audit report ===\n");
    out.push_str(&format!(
        "lines read: {} (skipped {})\n",
        stats.lines_read, stats.lines_skipped
    ));
    out.push_str(&format!("reports:    {}\n", report.reports_seen));
    out.push_str(&format!("fills:      {}\n", report.fills_seen));

    out.push_str("\n-- status counts --\n");
    out.push_str(&format!("new:              {}\n", report.statuses.new));
    out.push_str(&format!(
        "partially_filled: {}\n",
        report.statuses.partially_filled
    ));
    out.push_str(&format!("filled:           {}\n", report.statuses.filled));
    out.push_str(&format!("canceled:         {}\n", report.statuses.canceled));
    out.push_str(&format!("rejected:         {}\n", report.statuses.rejected));
    out.push_str(&format!("expired:          {}\n", report.statuses.expired));

    out.push_str("\n-- per-symbol --\n");
    for s in report.symbols.values() {
        out.push_str(&format!(
            "{:<10} fills={:>5} (buy={} sell={}) notional={} end_pos={} max_long={} max_short={} realized={}\n",
            s.symbol,
            s.fills,
            s.buy_fills,
            s.sell_fills,
            s.gross_notional,
            s.end_position,
            s.max_long,
            s.max_short,
            s.realized,
        ));
    }

    out.push_str(&format!(
        "\nrealized pnl (sum across symbols): {}\n",
        report.realized_total
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use ts_core::{ClientOrderId, ExecReport, Fill, Price, Qty, Timestamp, Venue};

    fn report_ev(cid: &str, status: OrderStatus) -> AuditEvent {
        AuditEvent::Report(ExecReport {
            cid: ClientOrderId::new(cid),
            status,
            filled_qty: Qty(0),
            avg_price: None,
            reason: None,
            fills: vec![],
        })
    }

    fn fill_ev(cid: &str, sym: &str, side: Side, price: i64, qty: i64) -> AuditEvent {
        AuditEvent::Fill(Fill {
            cid: ClientOrderId::new(cid),
            venue: Venue::BINANCE,
            symbol: Symbol::new(sym.to_string()),
            side,
            price: Price(price),
            qty: Qty(qty),
            ts: Timestamp::from_unix_millis(1_700_000_000_000),
        })
    }

    fn ingest_all(events: Vec<AuditEvent>) -> Report {
        let mut r = Report::new();
        let mut a = Accountant::new();
        for ev in &events {
            r.ingest(ev, &mut a);
        }
        r.finalize(&a);
        r
    }

    #[test]
    fn status_counters_track_each_variant() {
        let rep = ingest_all(vec![
            report_ev("a", OrderStatus::New),
            report_ev("b", OrderStatus::New),
            report_ev("c", OrderStatus::PartiallyFilled),
            report_ev("d", OrderStatus::Filled),
            report_ev("e", OrderStatus::Canceled),
            report_ev("f", OrderStatus::Rejected),
            report_ev("g", OrderStatus::Expired),
        ]);
        assert_eq!(rep.reports_seen, 7);
        assert_eq!(rep.statuses.new, 2);
        assert_eq!(rep.statuses.partially_filled, 1);
        assert_eq!(rep.statuses.filled, 1);
        assert_eq!(rep.statuses.canceled, 1);
        assert_eq!(rep.statuses.rejected, 1);
        assert_eq!(rep.statuses.expired, 1);
    }

    #[test]
    fn fills_aggregate_per_symbol_and_track_position_extremes() {
        let rep = ingest_all(vec![
            fill_ev("a", "BTCUSDT", Side::Buy, 100, 10),
            fill_ev("b", "BTCUSDT", Side::Buy, 105, 5),
            fill_ev("c", "BTCUSDT", Side::Sell, 115, 20),
            fill_ev("d", "ETHUSDT", Side::Sell, 3_000, 2),
        ]);
        assert_eq!(rep.fills_seen, 4);

        let btc = rep.symbols.get("BTCUSDT").expect("btc row");
        assert_eq!(btc.fills, 3);
        assert_eq!(btc.buy_fills, 2);
        assert_eq!(btc.sell_fills, 1);
        assert_eq!(btc.gross_filled_qty, 35);
        assert_eq!(btc.gross_notional, 100 * 10 + 105 * 5 + 115 * 20);
        // Long 15 at most, then flipped short on 20-sell → -5 at end.
        assert_eq!(btc.max_long, 15);
        assert_eq!(btc.max_short, -5);
        assert_eq!(btc.end_position, -5);

        let eth = rep.symbols.get("ETHUSDT").expect("eth row");
        assert_eq!(eth.fills, 1);
        assert_eq!(eth.sell_fills, 1);
        assert_eq!(eth.end_position, -2);
        assert_eq!(eth.max_short, -2);
    }

    #[test]
    fn realized_pnl_matches_accountant_rules() {
        // Buy 10 @ 100, sell 10 @ 120 → realized = 200 on BTC.
        // Sell 5 @ 3000, buy 5 @ 2900 on ETH → realized = 500.
        let rep = ingest_all(vec![
            fill_ev("a", "BTCUSDT", Side::Buy, 100, 10),
            fill_ev("b", "BTCUSDT", Side::Sell, 120, 10),
            fill_ev("c", "ETHUSDT", Side::Sell, 3_000, 5),
            fill_ev("d", "ETHUSDT", Side::Buy, 2_900, 5),
        ]);
        assert_eq!(rep.symbols["BTCUSDT"].realized, 200);
        assert_eq!(rep.symbols["ETHUSDT"].realized, 500);
        assert_eq!(rep.realized_total, 700);
        // Both flat at end.
        assert_eq!(rep.symbols["BTCUSDT"].end_position, 0);
        assert_eq!(rep.symbols["BTCUSDT"].avg_entry, None);
    }

    #[test]
    fn read_ndjson_roundtrips_writer_shape() {
        let events = vec![
            report_ev("a", OrderStatus::New),
            fill_ev("a", "BTCUSDT", Side::Buy, 100, 10),
            report_ev("a", OrderStatus::Filled),
            fill_ev("b", "BTCUSDT", Side::Sell, 110, 10),
        ];
        let mut buf = Vec::new();
        for ev in &events {
            buf.extend_from_slice(serde_json::to_string(ev).unwrap().as_bytes());
            buf.push(b'\n');
        }
        let (rep, stats) = read_audit_ndjson(Cursor::new(&buf)).unwrap();
        assert_eq!(stats.lines_read, 4);
        assert_eq!(stats.lines_skipped, 0);
        assert_eq!(rep.reports_seen, 2);
        assert_eq!(rep.fills_seen, 2);
        assert_eq!(rep.symbols["BTCUSDT"].realized, (110 - 100) * 10);
    }

    #[test]
    fn malformed_lines_are_counted_and_skipped() {
        let body = b"{\"kind\":\"report\",\"cid\":\"a\",\"status\":\"New\",\"filled_qty\":0,\"avg_price\":null,\"reason\":null,\"fills\":[]}\n\
                     not json at all\n\
                     \n\
                     {\"kind\":\"garbage\"}\n";
        let (rep, stats) = read_audit_ndjson(Cursor::new(body.as_slice())).unwrap();
        assert_eq!(stats.lines_read, 3, "blank lines skipped before counting");
        assert_eq!(stats.lines_skipped, 2);
        assert_eq!(rep.reports_seen, 1);
        assert_eq!(rep.statuses.new, 1);
    }

    #[test]
    fn format_text_renders_key_fields() {
        let rep = ingest_all(vec![
            report_ev("a", OrderStatus::New),
            fill_ev("a", "BTCUSDT", Side::Buy, 100, 5),
            fill_ev("b", "BTCUSDT", Side::Sell, 110, 5),
        ]);
        let stats = ReadStats {
            lines_read: 3,
            lines_skipped: 0,
        };
        let text = format_text(&rep, &stats);
        assert!(text.contains("=== audit report ==="));
        assert!(text.contains("reports:    1"));
        assert!(text.contains("fills:      2"));
        assert!(text.contains("BTCUSDT"));
        assert!(text.contains("realized=50"));
        assert!(text.contains("realized pnl (sum across symbols): 50"));
    }
}
