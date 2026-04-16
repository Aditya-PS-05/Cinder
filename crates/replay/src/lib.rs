//! Deterministic replay harness.
//!
//! [`Replay`] drives a [`PaperEngine`] from any source of market
//! events (live feed, recorded file, synthetic fixture) and folds
//! the resulting fills into a [`ts_pnl::Accountant`]. Between market
//! events the caller can inject taker orders via
//! [`Replay::submit_taker`] to simulate adverse flow, one-off
//! admin submissions, or cross-strategy interaction.
//!
//! The harness is synchronous and owns no I/O. A runner binary or a
//! backtest harness feeds events in order and reads a
//! [`ReplaySummary`] at the end. There is no hidden randomness —
//! identical event streams always produce identical summaries.

#![forbid(unsafe_code)]

use ts_book::BookError;
use ts_core::{
    ClientOrderId, ExecReport, Fill, MarketEvent, NewOrder, OrderStatus, Price, Timestamp,
};
use ts_oms::{EngineStep, OrderEngine, PaperEngine};
use ts_pnl::Accountant;
use ts_strategy::Strategy;

#[derive(Default, Debug, Clone)]
pub struct ReplayMetrics {
    pub events_ingested: u64,
    pub book_updates: u64,
    pub orders_submitted: u64,
    pub orders_filled: u64,
    pub orders_partially_filled: u64,
    pub orders_rejected: u64,
    pub orders_canceled: u64,
    pub orders_new: u64,
    pub fills: u64,
    /// Sum of filled qty across every fill, in qty-scale mantissa.
    pub gross_filled_qty: i128,
    /// Sum of |price * qty| across every fill, in price*qty mantissa.
    pub gross_notional: i128,
}

#[derive(Debug, Clone)]
pub struct ReplaySummary {
    pub metrics: ReplayMetrics,
    /// Final signed position for the engine's symbol.
    pub position: i64,
    /// Final weighted-average entry price; `None` when flat.
    pub avg_entry: Option<Price>,
    /// Realized pnl in price*qty mantissa.
    pub realized: i128,
    /// Unrealized pnl marked to [`Self::mark`]; 0 when no mark is
    /// available (book one-sided at end-of-replay).
    pub unrealized: i128,
    /// Sum of realized + unrealized.
    pub total_pnl: i128,
    /// Final mark price used for `unrealized` — the book mid. `None`
    /// when the book is one-sided or uninitialized.
    pub mark: Option<Price>,
}

pub struct Replay<S: Strategy> {
    engine: PaperEngine<S>,
    accountant: Accountant,
    metrics: ReplayMetrics,
}

impl<S: Strategy> Replay<S> {
    pub fn new(engine: PaperEngine<S>) -> Self {
        Self {
            engine,
            accountant: Accountant::new(),
            metrics: ReplayMetrics::default(),
        }
    }

    pub fn engine(&self) -> &PaperEngine<S> {
        &self.engine
    }
    pub fn accountant(&self) -> &Accountant {
        &self.accountant
    }
    pub fn metrics(&self) -> &ReplayMetrics {
        &self.metrics
    }

    /// Feed a single market event through the engine and fold the
    /// resulting step into the accountant and metrics.
    pub fn step(&mut self, event: &MarketEvent) -> Result<EngineStep, BookError> {
        self.metrics.events_ingested += 1;
        let step = self.engine.apply_event(event)?;
        if !step.reports.is_empty() || !step.fills.is_empty() {
            self.metrics.book_updates += 1;
        } else if matches!(
            event.payload,
            ts_core::MarketPayload::BookSnapshot(_) | ts_core::MarketPayload::BookDelta(_)
        ) {
            // Book moved even though the strategy did not emit actions.
            self.metrics.book_updates += 1;
        }
        self.absorb_step(&step);
        Ok(step)
    }

    /// Drive the engine through an iterator of events. Returns the
    /// first `BookError` it encounters without consuming the rest.
    pub fn run<I>(&mut self, events: I) -> Result<(), BookError>
    where
        I: IntoIterator<Item = MarketEvent>,
    {
        for event in events {
            self.step(&event)?;
        }
        Ok(())
    }

    /// Inject an external (non-strategy) order between market events.
    /// The resulting report is folded into metrics and accountant the
    /// same way strategy-driven orders are.
    ///
    /// Goes through the [`OrderEngine`] trait; `PaperEngine` never
    /// errors here, so the inner result is `.unwrap()`ed.
    pub fn submit_taker(&mut self, order: NewOrder, now: Timestamp) -> ExecReport {
        let report =
            OrderEngine::submit(&mut self.engine, order, now).expect("PaperEngine is infallible");
        self.absorb_report(&report);
        for f in &report.fills {
            self.absorb_fill(f);
        }
        report
    }

    /// Externally cancel a live order and fold the resulting report.
    pub fn cancel_taker(&mut self, cid: &ClientOrderId) -> ExecReport {
        let report = OrderEngine::cancel(&mut self.engine, cid).expect("PaperEngine is infallible");
        self.absorb_report(&report);
        report
    }

    /// Build an immutable summary of the replay so far.
    pub fn summary(&self) -> ReplaySummary {
        let symbol = &self.engine.config().symbol;
        let position = self.accountant.position(symbol);
        let avg_entry = self.accountant.avg_entry(symbol);
        let realized = self.accountant.realized(symbol);
        let mark = self.engine.book().mid().map(Price);
        let unrealized = mark.map_or(0, |m| self.accountant.unrealized(symbol, m));
        ReplaySummary {
            metrics: self.metrics.clone(),
            position,
            avg_entry,
            realized,
            unrealized,
            total_pnl: realized + unrealized,
            mark,
        }
    }

    fn absorb_step(&mut self, step: &EngineStep) {
        for r in &step.reports {
            self.absorb_report(r);
        }
        for f in &step.fills {
            self.absorb_fill(f);
        }
    }

    fn absorb_report(&mut self, report: &ExecReport) {
        self.metrics.orders_submitted += 1;
        match report.status {
            OrderStatus::New => self.metrics.orders_new += 1,
            OrderStatus::PartiallyFilled => self.metrics.orders_partially_filled += 1,
            OrderStatus::Filled => self.metrics.orders_filled += 1,
            OrderStatus::Canceled => self.metrics.orders_canceled += 1,
            OrderStatus::Rejected => self.metrics.orders_rejected += 1,
            OrderStatus::Expired => self.metrics.orders_rejected += 1,
        }
    }

    fn absorb_fill(&mut self, fill: &Fill) {
        self.metrics.fills += 1;
        self.metrics.gross_filled_qty = self
            .metrics
            .gross_filled_qty
            .saturating_add(fill.qty.0 as i128);
        self.metrics.gross_notional = self
            .metrics
            .gross_notional
            .saturating_add((fill.price.0 as i128) * (fill.qty.0 as i128));
        self.accountant.on_fill(fill);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{
        BookLevel, BookSnapshot, ClientOrderId, MarketPayload, NewOrder, OrderKind, Qty, Side,
        Symbol, TimeInForce, Venue,
    };
    use ts_oms::EngineConfig;
    use ts_strategy::{InventorySkewMaker, MakerConfig};

    fn venue() -> Venue {
        Venue::BINANCE
    }
    fn sym() -> Symbol {
        Symbol::from_static("BTCUSDT")
    }

    fn engine() -> PaperEngine<InventorySkewMaker> {
        let cfg = EngineConfig {
            venue: venue(),
            symbol: sym(),
            notional_fallback_price: None,
        };
        let maker = InventorySkewMaker::new(MakerConfig {
            venue: venue(),
            symbol: sym(),
            quote_qty: Qty(2),
            half_spread_ticks: 5,
            inventory_skew_ticks: 0,
            max_inventory: 100,
            cid_prefix: "mk".into(),
        });
        PaperEngine::new(cfg, ts_oms::RiskConfig::permissive(), maker)
    }

    fn snapshot(bids: Vec<BookLevel>, asks: Vec<BookLevel>, seq: u64) -> MarketEvent {
        MarketEvent {
            venue: venue(),
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq,
            payload: MarketPayload::BookSnapshot(BookSnapshot { bids, asks }),
        }
    }

    fn lvl(p: i64, q: i64) -> BookLevel {
        BookLevel {
            price: Price(p),
            qty: Qty(q),
        }
    }

    fn market_order(cid: &str, side: Side, qty: i64) -> NewOrder {
        NewOrder {
            cid: ClientOrderId::new(cid),
            venue: venue(),
            symbol: sym(),
            side,
            kind: OrderKind::Market,
            tif: TimeInForce::Ioc,
            qty: Qty(qty),
            price: None,
            ts: Timestamp::default(),
        }
    }

    #[test]
    fn run_over_snapshots_counts_book_updates_and_places_quotes() {
        let mut r = Replay::new(engine());
        let events = vec![
            snapshot(vec![lvl(100, 10)], vec![lvl(110, 10)], 1),
            snapshot(vec![lvl(101, 10)], vec![lvl(109, 10)], 2),
        ];
        r.run(events).unwrap();

        let s = r.summary();
        assert_eq!(s.metrics.events_ingested, 2);
        assert_eq!(s.metrics.book_updates, 2);
        // First tick: 2 new places. Second tick: 2 cancels + 2 new places = 4 reports.
        // Total orders observed = 2 + 4 = 6.
        assert_eq!(s.metrics.orders_submitted, 6);
        assert_eq!(s.metrics.orders_new, 4);
        assert_eq!(s.metrics.orders_canceled, 2);
        assert_eq!(s.position, 0);
        assert_eq!(s.realized, 0);
    }

    #[test]
    fn taker_fill_updates_position_and_realized_pnl() {
        let mut r = Replay::new(engine());
        r.step(&snapshot(vec![lvl(100, 10)], vec![lvl(110, 10)], 1))
            .unwrap();

        // Buy 5 @ 110.
        let open = r.submit_taker(market_order("t1", Side::Buy, 5), Timestamp::default());
        assert_eq!(open.status, OrderStatus::Filled);
        assert_eq!(r.accountant().position(&sym()), 5);
        assert_eq!(r.accountant().avg_entry(&sym()), Some(Price(110)));

        // Snapshot where the ask has moved higher; strategy replaces quotes.
        r.step(&snapshot(vec![lvl(115, 10)], vec![lvl(125, 10)], 2))
            .unwrap();

        // Sell 5 at mid 120 to realize pnl (120 - 110) * 5 = 50.
        let close = r.submit_taker(market_order("t2", Side::Sell, 5), Timestamp::default());
        assert_eq!(close.status, OrderStatus::Filled);

        let s = r.summary();
        assert_eq!(s.position, 0);
        assert_eq!(s.realized, (115 - 110) * 5); // opposing best bid is 115
        assert!(s.metrics.fills >= 2);
        assert_eq!(s.avg_entry, None);
    }

    #[test]
    fn summary_reports_unrealized_from_book_mid() {
        let mut r = Replay::new(engine());
        r.step(&snapshot(vec![lvl(100, 10)], vec![lvl(110, 10)], 1))
            .unwrap();
        r.submit_taker(market_order("t1", Side::Buy, 5), Timestamp::default());

        // New mid: (120 + 130) / 2 = 125. Position entered at 110, so unrealized = (125 - 110) * 5 = 75.
        r.step(&snapshot(vec![lvl(120, 10)], vec![lvl(130, 10)], 2))
            .unwrap();
        let s = r.summary();
        assert_eq!(s.mark, Some(Price(125)));
        assert_eq!(s.unrealized, (125 - 110) * 5);
        assert_eq!(s.total_pnl, s.realized + s.unrealized);
    }

    #[test]
    fn unknown_cid_cancel_surfaces_rejected() {
        let mut r = Replay::new(engine());
        r.step(&snapshot(vec![lvl(100, 10)], vec![lvl(110, 10)], 1))
            .unwrap();
        let before = r.metrics().orders_rejected;
        let report = r.cancel_taker(&ClientOrderId::new("nope"));
        assert_eq!(report.status, OrderStatus::Rejected);
        assert_eq!(r.metrics().orders_rejected, before + 1);
    }

    #[test]
    fn run_propagates_book_error_on_bad_delta() {
        use ts_core::BookDelta;
        let mut r = Replay::new(engine());
        let bad_delta = MarketEvent {
            venue: venue(),
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq: 1,
            payload: MarketPayload::BookDelta(BookDelta {
                bids: vec![lvl(100, 1)],
                asks: vec![],
                prev_seq: 0,
            }),
        };
        let err = r.run(vec![bad_delta]).unwrap_err();
        assert_eq!(err, BookError::Uninitialized);
        // Nothing should have been admitted to the accountant.
        assert_eq!(r.summary().position, 0);
    }

    #[test]
    fn gross_notional_and_filled_qty_accumulate_across_fills() {
        let mut r = Replay::new(engine());
        r.step(&snapshot(vec![lvl(100, 10)], vec![lvl(110, 10)], 1))
            .unwrap();
        r.submit_taker(market_order("t1", Side::Buy, 3), Timestamp::default());
        r.submit_taker(market_order("t2", Side::Buy, 2), Timestamp::default());
        let m = r.metrics();
        assert!(m.fills >= 2);
        assert_eq!(m.gross_filled_qty, 5);
        assert_eq!(m.gross_notional, 110 * 3 + 110 * 2);
    }

    #[test]
    fn summary_mark_is_none_on_one_sided_book() {
        let mut r = Replay::new(engine());
        r.step(&snapshot(vec![lvl(100, 10)], vec![], 1)).unwrap();
        let s = r.summary();
        assert_eq!(s.mark, None);
        assert_eq!(s.unrealized, 0);
    }
}
