//! Inventory-skew market maker.
//!
//! [`InventorySkewMaker`] quotes a symmetric bid/ask pair around the
//! book mid and shifts both quotes against its current inventory to
//! encourage mean-reversion back to flat. Each tick cancels any still-
//! open quotes and reissues fresh ones; when the maker has saturated
//! its long/short bound it suppresses the corresponding side.

use ts_book::OrderBook;
use ts_core::{
    ClientOrderId, ExecReport, Fill, NewOrder, OrderKind, Price, Qty, Side, Symbol, TimeInForce,
    Timestamp, Venue,
};

use crate::{is_terminal, Strategy, StrategyAction};

#[derive(Clone, Debug)]
pub struct MakerConfig {
    pub venue: Venue,
    pub symbol: Symbol,
    /// Quote size in qty-scale mantissa.
    pub quote_qty: Qty,
    /// Half-spread added to/subtracted from mid, in price-scale ticks.
    pub half_spread_ticks: i64,
    /// Per-unit-inventory shift applied to both quotes, in price-scale
    /// ticks. Positive inventory pushes both quotes down (offering more
    /// aggressively, bidding less aggressively) to work back toward flat.
    pub inventory_skew_ticks: i64,
    /// Absolute inventory cap. While at or beyond this bound on either
    /// side the strategy suppresses the accumulating side's quote.
    pub max_inventory: i64,
    /// Prefix for generated client order ids. Kept short; the counter
    /// suffix makes each id unique within the process lifetime.
    pub cid_prefix: String,
}

pub struct InventorySkewMaker {
    cfg: MakerConfig,
    inventory: i64,
    open_bid: Option<ClientOrderId>,
    open_ask: Option<ClientOrderId>,
    cid_counter: u64,
}

impl InventorySkewMaker {
    pub fn new(cfg: MakerConfig) -> Self {
        Self {
            cfg,
            inventory: 0,
            open_bid: None,
            open_ask: None,
            cid_counter: 0,
        }
    }

    pub fn inventory(&self) -> i64 {
        self.inventory
    }

    pub fn open_bid(&self) -> Option<&ClientOrderId> {
        self.open_bid.as_ref()
    }

    pub fn open_ask(&self) -> Option<&ClientOrderId> {
        self.open_ask.as_ref()
    }

    fn next_cid(&mut self, tag: &str) -> ClientOrderId {
        self.cid_counter += 1;
        ClientOrderId::new(format!(
            "{}-{}-{}",
            self.cfg.cid_prefix, tag, self.cid_counter
        ))
    }

    fn build_order(
        &mut self,
        side: Side,
        price: Price,
        tag: &str,
        now: Timestamp,
    ) -> (ClientOrderId, NewOrder) {
        let cid = self.next_cid(tag);
        let order = NewOrder {
            cid: cid.clone(),
            venue: self.cfg.venue.clone(),
            symbol: self.cfg.symbol.clone(),
            side,
            kind: OrderKind::Limit,
            tif: TimeInForce::Gtc,
            qty: self.cfg.quote_qty,
            price: Some(price),
            ts: now,
        };
        (cid, order)
    }
}

impl Strategy for InventorySkewMaker {
    fn on_book_update(&mut self, now: Timestamp, book: &OrderBook) -> Vec<StrategyAction> {
        let mid = match book.mid() {
            Some(m) => m,
            None => return Vec::new(),
        };

        // Inventory skew shifts both quotes by the same amount, so the
        // spread is preserved but the center of mass walks away from the
        // side the maker is long on.
        let skew_shift = self.cfg.inventory_skew_ticks.saturating_mul(self.inventory);
        let bid_px = mid
            .saturating_sub(self.cfg.half_spread_ticks)
            .saturating_sub(skew_shift);
        let ask_px = mid
            .saturating_add(self.cfg.half_spread_ticks)
            .saturating_sub(skew_shift);

        let mut actions = Vec::new();

        if let Some(cid) = self.open_bid.take() {
            actions.push(StrategyAction::Cancel(cid));
        }
        if let Some(cid) = self.open_ask.take() {
            actions.push(StrategyAction::Cancel(cid));
        }

        let at_long_cap = self.inventory >= self.cfg.max_inventory;
        let at_short_cap = self.inventory <= -self.cfg.max_inventory;

        if !at_long_cap && bid_px > 0 {
            let (cid, order) = self.build_order(Side::Buy, Price(bid_px), "b", now);
            self.open_bid = Some(cid);
            actions.push(StrategyAction::Place(order));
        }

        if !at_short_cap && ask_px > 0 {
            let (cid, order) = self.build_order(Side::Sell, Price(ask_px), "a", now);
            self.open_ask = Some(cid);
            actions.push(StrategyAction::Place(order));
        }

        actions
    }

    fn on_fill(&mut self, fill: &Fill) {
        if fill.symbol != self.cfg.symbol {
            return;
        }
        let signed = match fill.side {
            Side::Buy => fill.qty.0,
            Side::Sell => -fill.qty.0,
            Side::Unknown => 0,
        };
        self.inventory = self.inventory.saturating_add(signed);
    }

    fn on_exec_report(&mut self, report: &ExecReport) {
        if !is_terminal(report.status) {
            return;
        }
        if self.open_bid.as_ref() == Some(&report.cid) {
            self.open_bid = None;
        }
        if self.open_ask.as_ref() == Some(&report.cid) {
            self.open_ask = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{BookLevel, BookSnapshot, OrderStatus};

    fn lvl(p: i64, q: i64) -> BookLevel {
        BookLevel {
            price: Price(p),
            qty: Qty(q),
        }
    }

    fn book_with(bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> OrderBook {
        let mut b = OrderBook::new();
        b.apply_snapshot(&BookSnapshot { bids, asks }, 1);
        b
    }

    fn cfg() -> MakerConfig {
        MakerConfig {
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            quote_qty: Qty(10),
            half_spread_ticks: 5,
            inventory_skew_ticks: 1,
            max_inventory: 20,
            cid_prefix: "mk".into(),
        }
    }

    fn fill(side: Side, qty: i64) -> Fill {
        Fill {
            cid: ClientOrderId::new("ignored"),
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            side,
            price: Price(100),
            qty: Qty(qty),
            ts: Timestamp::default(),
            is_maker: None,
        }
    }

    fn place_order(a: &StrategyAction) -> &NewOrder {
        match a {
            StrategyAction::Place(o) => o,
            _ => panic!("expected Place, got {a:?}"),
        }
    }

    #[test]
    fn empty_book_emits_no_actions() {
        let mut m = InventorySkewMaker::new(cfg());
        let book = OrderBook::new();
        assert!(m.on_book_update(Timestamp::default(), &book).is_empty());
    }

    #[test]
    fn one_sided_book_emits_no_actions() {
        let mut m = InventorySkewMaker::new(cfg());
        let book = book_with(vec![lvl(100, 1)], vec![]);
        assert!(m.on_book_update(Timestamp::default(), &book).is_empty());
    }

    #[test]
    fn flat_inventory_quotes_symmetric_around_mid() {
        let mut m = InventorySkewMaker::new(cfg());
        // mid = (100 + 110) / 2 = 105
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        // No prior orders, so only two Place actions.
        assert_eq!(actions.len(), 2);
        let bid = place_order(&actions[0]);
        let ask = place_order(&actions[1]);
        assert_eq!(bid.side, Side::Buy);
        assert_eq!(bid.price, Some(Price(100))); // 105 - 5
        assert_eq!(ask.side, Side::Sell);
        assert_eq!(ask.price, Some(Price(110))); // 105 + 5
        assert!(m.open_bid().is_some());
        assert!(m.open_ask().is_some());
    }

    #[test]
    fn second_tick_cancels_previous_quotes() {
        let mut m = InventorySkewMaker::new(cfg());
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        let first = m.on_book_update(Timestamp::default(), &book);
        assert_eq!(first.len(), 2);
        let second = m.on_book_update(Timestamp::default(), &book);
        // Two cancels followed by two places.
        assert_eq!(second.len(), 4);
        assert!(matches!(second[0], StrategyAction::Cancel(_)));
        assert!(matches!(second[1], StrategyAction::Cancel(_)));
        assert!(matches!(second[2], StrategyAction::Place(_)));
        assert!(matches!(second[3], StrategyAction::Place(_)));
    }

    #[test]
    fn long_inventory_skews_quotes_down() {
        let mut m = InventorySkewMaker::new(cfg());
        m.on_fill(&fill(Side::Buy, 5));
        assert_eq!(m.inventory(), 5);

        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        let bid = place_order(&actions[0]);
        let ask = place_order(&actions[1]);
        // mid=105, skew=5*1=5, half=5. bid = 105-5-5 = 95, ask = 105+5-5 = 105.
        assert_eq!(bid.price, Some(Price(95)));
        assert_eq!(ask.price, Some(Price(105)));
    }

    #[test]
    fn short_inventory_skews_quotes_up() {
        let mut m = InventorySkewMaker::new(cfg());
        m.on_fill(&fill(Side::Sell, 3));
        assert_eq!(m.inventory(), -3);

        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        let bid = place_order(&actions[0]);
        let ask = place_order(&actions[1]);
        // mid=105, skew=-3, half=5. bid = 105-5-(-3) = 103, ask = 105+5-(-3) = 113.
        assert_eq!(bid.price, Some(Price(103)));
        assert_eq!(ask.price, Some(Price(113)));
    }

    #[test]
    fn max_long_inventory_suppresses_bid() {
        let mut m = InventorySkewMaker::new(cfg());
        m.on_fill(&fill(Side::Buy, 20));
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        // Only one Place, and it must be the ask.
        let places: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                StrategyAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();
        assert_eq!(places.len(), 1);
        assert_eq!(places[0].side, Side::Sell);
        assert!(m.open_bid().is_none());
        assert!(m.open_ask().is_some());
    }

    #[test]
    fn max_short_inventory_suppresses_ask() {
        let mut m = InventorySkewMaker::new(cfg());
        m.on_fill(&fill(Side::Sell, 20));
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        let places: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                StrategyAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();
        assert_eq!(places.len(), 1);
        assert_eq!(places[0].side, Side::Buy);
        assert!(m.open_ask().is_none());
    }

    #[test]
    fn terminal_exec_report_clears_tracked_cid() {
        let mut m = InventorySkewMaker::new(cfg());
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        m.on_book_update(Timestamp::default(), &book);
        let bid_cid = m.open_bid().cloned().unwrap();

        let report = ExecReport {
            cid: bid_cid,
            status: OrderStatus::Filled,
            filled_qty: Qty(10),
            avg_price: Some(Price(100)),
            reason: None,
            fills: Vec::new(),
        };
        m.on_exec_report(&report);
        assert!(m.open_bid().is_none());
        assert!(m.open_ask().is_some());
    }

    #[test]
    fn non_terminal_exec_report_does_not_clear() {
        let mut m = InventorySkewMaker::new(cfg());
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        m.on_book_update(Timestamp::default(), &book);
        let bid_cid = m.open_bid().cloned().unwrap();

        let report = ExecReport {
            cid: bid_cid,
            status: OrderStatus::PartiallyFilled,
            filled_qty: Qty(1),
            avg_price: Some(Price(100)),
            reason: None,
            fills: Vec::new(),
        };
        m.on_exec_report(&report);
        assert!(m.open_bid().is_some());
    }

    #[test]
    fn fill_on_other_symbol_does_not_move_inventory() {
        let mut m = InventorySkewMaker::new(cfg());
        let mut f = fill(Side::Buy, 5);
        f.symbol = Symbol::from_static("ETHUSDT");
        m.on_fill(&f);
        assert_eq!(m.inventory(), 0);
    }
}
