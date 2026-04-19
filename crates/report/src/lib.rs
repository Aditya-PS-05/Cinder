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

/// Breakdown of fill liquidity based on the `is_maker` flag carried on
/// each fill. `unknown` counts fills from sources that don't report it
/// (paper engines, historical tapes predating the field).
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub struct LiquidityBreakdown {
    pub maker_fills: u64,
    pub taker_fills: u64,
    pub unknown_fills: u64,
    /// Maker volume in qty-scale mantissa.
    pub maker_qty: i128,
    /// Taker volume in qty-scale mantissa.
    pub taker_qty: i128,
}

/// Quant-flavoured aggregates derived from the realized-equity curve
/// (one sample per fill). `None` when not enough data exists.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
pub struct QuantMetrics {
    /// Fills per minute across the full tape span. `None` for a tape
    /// with fewer than two timestamped fills or a zero-length span.
    pub fills_per_min: Option<f64>,
    /// Max peak-to-trough drop on the realized pnl curve (always ≥ 0).
    /// Expressed in `price_scale * qty_scale` mantissa to match
    /// [`SymbolReport::realized`] / [`Report::realized_total`].
    pub max_drawdown: i128,
    /// Sharpe-like ratio computed over per-fill realized-pnl deltas:
    /// `mean(delta) / stddev(delta)`. Dimensionless, not annualized —
    /// a rough profitability-to-noise read, not a proper return series.
    /// `None` when fewer than two samples or stddev is zero.
    pub sharpe_like: Option<f64>,
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
    pub liquidity: LiquidityBreakdown,
    pub quant: QuantMetrics,
    /// First and last timestamped fill seen on the tape. Drives
    /// [`QuantMetrics::fills_per_min`]; exposed on the report so
    /// consumers can render spans without re-walking the tape.
    pub first_fill_ts: Option<i64>,
    pub last_fill_ts: Option<i64>,

    /// Equity samples — one per fill after accountant update. Kept for
    /// drawdown / sharpe derivation at `finalize` time, not part of the
    /// wire format.
    #[serde(skip)]
    equity_curve: Vec<i128>,
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

                match f.is_maker {
                    Some(true) => {
                        self.liquidity.maker_fills += 1;
                        self.liquidity.maker_qty += f.qty.0 as i128;
                    }
                    Some(false) => {
                        self.liquidity.taker_fills += 1;
                        self.liquidity.taker_qty += f.qty.0 as i128;
                    }
                    None => self.liquidity.unknown_fills += 1,
                }

                // Unstamped fills are treated as "time unknown" and left
                // out of the span so fills/min stays meaningful on tapes
                // that mix stamped and unstamped sources.
                if !f.ts.is_unset() {
                    let ns = f.ts.0;
                    self.first_fill_ts = Some(self.first_fill_ts.map_or(ns, |t| t.min(ns)));
                    self.last_fill_ts = Some(self.last_fill_ts.map_or(ns, |t| t.max(ns)));
                }

                self.equity_curve.push(accountant.realized_total());
            }
        }
    }

    /// Snapshot end-of-tape position, avg entry, and realized pnl from
    /// the accountant onto each per-symbol entry. Also totals realized
    /// across symbols and derives [`QuantMetrics`] from the per-fill
    /// equity curve. Call after the last `ingest`.
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
        self.quant = derive_quant(
            &self.equity_curve,
            self.fills_seen,
            self.first_fill_ts,
            self.last_fill_ts,
        );
    }
}

fn derive_quant(
    equity: &[i128],
    fills: u64,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
) -> QuantMetrics {
    let max_drawdown = max_drawdown(equity);

    // Deltas: per-fill change in realized pnl. equity[0] is the total
    // after the first fill, so the first delta is equity[0] - 0.
    let sharpe_like = if equity.len() < 2 {
        None
    } else {
        let mut deltas: Vec<f64> = Vec::with_capacity(equity.len());
        let mut prev: i128 = 0;
        for &v in equity {
            deltas.push((v - prev) as f64);
            prev = v;
        }
        let n = deltas.len() as f64;
        let mean = deltas.iter().sum::<f64>() / n;
        let var = deltas.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / n;
        let std = var.sqrt();
        if std > 0.0 {
            Some(mean / std)
        } else {
            None
        }
    };

    let fills_per_min = match (first_ts, last_ts) {
        (Some(a), Some(b)) if b > a && fills >= 2 => {
            let span_nanos = (b - a) as f64;
            let minutes = span_nanos / 60_000_000_000.0;
            if minutes > 0.0 {
                Some(fills as f64 / minutes)
            } else {
                None
            }
        }
        _ => None,
    };

    QuantMetrics {
        fills_per_min,
        max_drawdown,
        sharpe_like,
    }
}

/// Peak-to-trough descent over the equity curve. Returns 0 when the
/// curve is monotonic or empty.
fn max_drawdown(equity: &[i128]) -> i128 {
    let mut peak = i128::MIN;
    let mut dd: i128 = 0;
    for &v in equity {
        if v > peak {
            peak = v;
        }
        let drop = peak - v;
        if drop > dd {
            dd = drop;
        }
    }
    dd.max(0)
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

    out.push_str("\n-- liquidity --\n");
    out.push_str(&format!(
        "maker fills: {} (qty {})\n",
        report.liquidity.maker_fills, report.liquidity.maker_qty
    ));
    out.push_str(&format!(
        "taker fills: {} (qty {})\n",
        report.liquidity.taker_fills, report.liquidity.taker_qty
    ));
    out.push_str(&format!(
        "unknown:     {}\n",
        report.liquidity.unknown_fills
    ));

    out.push_str("\n-- quant --\n");
    out.push_str(&format!(
        "fills/min:    {}\n",
        report
            .quant
            .fills_per_min
            .map(|v| format!("{v:.3}"))
            .unwrap_or_else(|| "n/a".into())
    ));
    out.push_str(&format!("max drawdown: {}\n", report.quant.max_drawdown));
    out.push_str(&format!(
        "sharpe-like:  {}\n",
        report
            .quant
            .sharpe_like
            .map(|v| format!("{v:.4}"))
            .unwrap_or_else(|| "n/a".into())
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
        fill_ev_at(
            cid,
            sym,
            side,
            price,
            qty,
            Timestamp::from_unix_millis(1_700_000_000_000),
            None,
        )
    }

    fn fill_ev_at(
        cid: &str,
        sym: &str,
        side: Side,
        price: i64,
        qty: i64,
        ts: Timestamp,
        is_maker: Option<bool>,
    ) -> AuditEvent {
        AuditEvent::Fill(Fill {
            cid: ClientOrderId::new(cid),
            venue: Venue::BINANCE,
            symbol: Symbol::new(sym.to_string()),
            side,
            price: Price(price),
            qty: Qty(qty),
            ts,
            is_maker,
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
        assert!(text.contains("-- quant --"));
        assert!(text.contains("-- liquidity --"));
    }

    #[test]
    fn liquidity_breakdown_counts_maker_taker_and_unknown() {
        let rep = ingest_all(vec![
            fill_ev_at(
                "a",
                "BTCUSDT",
                Side::Buy,
                100,
                5,
                Timestamp::from_unix_millis(1_700_000_000_000),
                Some(true),
            ),
            fill_ev_at(
                "b",
                "BTCUSDT",
                Side::Sell,
                105,
                2,
                Timestamp::from_unix_millis(1_700_000_000_100),
                Some(false),
            ),
            fill_ev_at(
                "c",
                "BTCUSDT",
                Side::Buy,
                104,
                1,
                Timestamp::from_unix_millis(1_700_000_000_200),
                None,
            ),
        ]);
        assert_eq!(rep.liquidity.maker_fills, 1);
        assert_eq!(rep.liquidity.maker_qty, 5);
        assert_eq!(rep.liquidity.taker_fills, 1);
        assert_eq!(rep.liquidity.taker_qty, 2);
        assert_eq!(rep.liquidity.unknown_fills, 1);
    }

    #[test]
    fn fills_per_min_uses_tape_span() {
        // Three fills spread across exactly 120 seconds (two minutes).
        // Span is t0 → t0 + 120s, so fills_per_min == 3 / 2 == 1.5.
        let t0 = 1_700_000_000_000i64;
        let rep = ingest_all(vec![
            fill_ev_at(
                "a",
                "BTCUSDT",
                Side::Buy,
                100,
                1,
                Timestamp::from_unix_millis(t0),
                Some(false),
            ),
            fill_ev_at(
                "b",
                "BTCUSDT",
                Side::Sell,
                100,
                1,
                Timestamp::from_unix_millis(t0 + 60_000),
                Some(false),
            ),
            fill_ev_at(
                "c",
                "BTCUSDT",
                Side::Buy,
                100,
                1,
                Timestamp::from_unix_millis(t0 + 120_000),
                Some(false),
            ),
        ]);
        let per_min = rep.quant.fills_per_min.expect("need a rate");
        assert!(
            (per_min - 1.5).abs() < 1e-6,
            "expected 1.5 fills/min, got {per_min}"
        );
    }

    #[test]
    fn fills_per_min_is_none_on_single_instant() {
        // All fills share the same timestamp; span is zero so the
        // denominator is undefined and we must return None rather than
        // Infinity.
        let rep = ingest_all(vec![
            fill_ev("a", "BTCUSDT", Side::Buy, 100, 1),
            fill_ev("b", "BTCUSDT", Side::Sell, 100, 1),
        ]);
        assert!(rep.quant.fills_per_min.is_none());
    }

    #[test]
    fn drawdown_captures_peak_to_trough_on_realized_curve() {
        // Realized equity walks 0 → +100 (winner) → 80 (loss of 20) →
        // 90 (small winner). Peak is 100, trough after peak is 80,
        // so max drawdown is 20. Final total is 90.
        let rep = ingest_all(vec![
            fill_ev("a", "BTCUSDT", Side::Buy, 100, 10),
            fill_ev("b", "BTCUSDT", Side::Sell, 110, 10), // realized +100
            fill_ev("c", "BTCUSDT", Side::Buy, 100, 10),
            fill_ev("d", "BTCUSDT", Side::Sell, 98, 10), // realized -20 → total 80
            fill_ev("e", "BTCUSDT", Side::Buy, 100, 10),
            fill_ev("f", "BTCUSDT", Side::Sell, 101, 10), // realized +10 → total 90
        ]);
        assert_eq!(rep.realized_total, 90);
        assert_eq!(rep.quant.max_drawdown, 20);
    }

    #[test]
    fn sharpe_like_is_none_for_zero_variance_curve() {
        // A single-fill tape: only one equity delta, so the sample
        // stddev is zero and we report None rather than Infinity.
        let rep = ingest_all(vec![fill_ev("a", "BTCUSDT", Side::Buy, 100, 1)]);
        assert!(rep.quant.sharpe_like.is_none());
    }

    #[test]
    fn sharpe_like_is_signed_and_finite_for_mixed_returns() {
        // Winners then a loser: mean delta > 0, stddev > 0 → positive.
        let rep = ingest_all(vec![
            fill_ev("a", "BTCUSDT", Side::Buy, 100, 1),
            fill_ev("b", "BTCUSDT", Side::Sell, 110, 1), // +10
            fill_ev("c", "BTCUSDT", Side::Buy, 100, 1),
            fill_ev("d", "BTCUSDT", Side::Sell, 108, 1), // +8
            fill_ev("e", "BTCUSDT", Side::Buy, 100, 1),
            fill_ev("f", "BTCUSDT", Side::Sell, 95, 1), // -5
        ]);
        let s = rep.quant.sharpe_like.expect("expected a ratio");
        assert!(s.is_finite());
        assert!(s > 0.0, "overall positive pnl should yield positive ratio");
    }
}
