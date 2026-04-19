//! FIFO tax-lot accounting.
//!
//! [`LotAccountant`] is an alternative to the weighted-average-cost
//! [`crate::Accountant`]. Instead of blending extending fills into a
//! single `avg_entry`, every open fill becomes a [`TaxLot`] queued
//! per symbol. Reducing fills close lots from the front of the queue
//! (oldest first, matching US 8949 default ordering), producing a
//! [`ClosedLot`] per matched pair.
//!
//! Rules:
//!
//! * Fills in the same direction as the existing queue enqueue a new
//!   lot. No averaging.
//! * Opposing fills pop the front lot(s) until the fill's quantity
//!   is exhausted. Each matched slice emits a [`ClosedLot`] with
//!   `realized = side_sign * (exit - entry) * overlap`, so long
//!   positions earn on higher exits and shorts earn on lower exits.
//! * If an opposing fill is larger than the total open qty on that
//!   side, the queue empties, the side flips, and the remainder
//!   enqueues as a fresh lot on the opposite side.
//! * Quote-denominated fees accumulate per-symbol the same way the
//!   WAC accountant tracks them. Cross-asset commissions (fee == 0
//!   with a non-quote asset label) are intentionally skipped.
//!
//! PnL and fees are denominated in `price_scale * qty_scale` mantissa
//! — identical to [`crate::Accountant`] so the two models can be
//! compared head-to-head on the same fill stream.

use std::collections::{HashMap, VecDeque};

use ts_core::{Fill, Price, Side, Symbol, Timestamp};

/// A single open tax lot: one unclosed buy or sell fill, waiting for
/// an opposing fill to close against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaxLot {
    /// Fill side that opened the lot. `Buy` = long exposure,
    /// `Sell` = short exposure.
    pub side: Side,
    /// Unsigned qty mantissa remaining on this lot.
    pub qty: i64,
    /// Entry price mantissa.
    pub entry: Price,
    /// Timestamp of the opening fill.
    pub opened: Timestamp,
}

/// A matched open/close pair. Emitted when an opposing fill consumes
/// some or all of an open lot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosedLot {
    /// Direction of the opening fill. For longs (`Buy`) a later `Sell`
    /// closes the lot; for shorts (`Sell`) a later `Buy` closes it.
    pub opened_side: Side,
    /// Unsigned qty mantissa realized in this slice.
    pub qty: i64,
    /// Entry price mantissa.
    pub entry: Price,
    /// Exit price mantissa.
    pub exit: Price,
    /// Timestamp of the opening fill.
    pub opened: Timestamp,
    /// Timestamp of the closing fill.
    pub closed: Timestamp,
    /// Realized pnl on this slice in `price_scale * qty_scale`
    /// mantissa (gross of commissions).
    pub realized: i128,
}

/// Per-symbol FIFO state.
#[derive(Clone, Debug, Default)]
pub struct LotBook {
    /// Open lots queued oldest-first. Invariant: all entries have the
    /// same [`Side`] (the current position's side) or the queue is
    /// empty. Opposing fills consume the head before any new lot may
    /// enqueue on the opposite side.
    open: VecDeque<TaxLot>,
    /// Closed slices in the order they were realized.
    closed: Vec<ClosedLot>,
    /// Cumulative realized pnl across [`Self::closed`], kept inline to
    /// avoid re-summing the history on every query.
    realized: i128,
    /// Cumulative quote-denominated commissions folded in per-fill.
    fees: i128,
}

impl LotBook {
    /// Signed open position mantissa. Positive = long, negative =
    /// short. Zero when flat.
    pub fn position(&self) -> i64 {
        let Some(first) = self.open.front() else {
            return 0;
        };
        let mut total: i64 = 0;
        for lot in &self.open {
            total = total
                .checked_add(lot.qty)
                .expect("open qty overflow in LotBook::position");
        }
        match first.side {
            Side::Buy => total,
            Side::Sell => -total,
            Side::Unknown => 0,
        }
    }

    /// Side of the currently open exposure, if any.
    pub fn side(&self) -> Option<Side> {
        self.open.front().map(|lot| lot.side)
    }

    /// Iterator over open lots in FIFO order (oldest first).
    pub fn open_lots(&self) -> impl Iterator<Item = &TaxLot> {
        self.open.iter()
    }

    /// Slice of closed-lot records in realization order. Intended for
    /// tax-lot export (US 8949 style: acquired/disposed with realized
    /// gain/loss).
    pub fn closed_lots(&self) -> &[ClosedLot] {
        &self.closed
    }

    /// Gross realized pnl (pre-fees) across every closed slice.
    pub fn realized(&self) -> i128 {
        self.realized
    }

    /// Cumulative quote-denominated commissions.
    pub fn fees(&self) -> i128 {
        self.fees
    }

    /// Realized pnl net of commissions on this symbol.
    pub fn realized_net(&self) -> i128 {
        self.realized - self.fees
    }

    /// Unrealized pnl of the queue at `mark`. Each lot contributes
    /// `(mark - entry) * qty` for longs and `(entry - mark) * qty` for
    /// shorts.
    pub fn unrealized(&self, mark: Price) -> i128 {
        let mut total: i128 = 0;
        for lot in &self.open {
            let qty = lot.qty as i128;
            let diff = (mark.0 as i128) - (lot.entry.0 as i128);
            total += match lot.side {
                Side::Buy => diff * qty,
                Side::Sell => -diff * qty,
                Side::Unknown => 0,
            };
        }
        total
    }
}

/// Multi-symbol FIFO accountant. Mirrors [`crate::Accountant`]'s
/// surface — `on_fill`, `position`, `realized`, `unrealized`, `fees`
/// — so callers can swap accounting models without touching the rest
/// of the runner.
#[derive(Clone, Debug, Default)]
pub struct LotAccountant {
    books: HashMap<Symbol, LotBook>,
}

impl LotAccountant {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn book(&self, symbol: &Symbol) -> Option<&LotBook> {
        self.books.get(symbol)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Symbol, &LotBook)> {
        self.books.iter()
    }

    pub fn position(&self, symbol: &Symbol) -> i64 {
        self.books.get(symbol).map_or(0, LotBook::position)
    }

    pub fn realized(&self, symbol: &Symbol) -> i128 {
        self.books.get(symbol).map_or(0, LotBook::realized)
    }

    pub fn realized_total(&self) -> i128 {
        self.books.values().map(LotBook::realized).sum()
    }

    pub fn fees(&self, symbol: &Symbol) -> i128 {
        self.books.get(symbol).map_or(0, LotBook::fees)
    }

    pub fn fees_total(&self) -> i128 {
        self.books.values().map(LotBook::fees).sum()
    }

    pub fn realized_net(&self, symbol: &Symbol) -> i128 {
        self.books.get(symbol).map_or(0, LotBook::realized_net)
    }

    pub fn realized_net_total(&self) -> i128 {
        self.realized_total() - self.fees_total()
    }

    pub fn unrealized(&self, symbol: &Symbol, mark: Price) -> i128 {
        self.books
            .get(symbol)
            .map_or(0, |book| book.unrealized(mark))
    }

    pub fn unrealized_total<F>(&self, mark_fn: F) -> i128
    where
        F: Fn(&Symbol) -> Option<Price>,
    {
        self.books
            .iter()
            .map(|(sym, book)| mark_fn(sym).map_or(0, |m| book.unrealized(m)))
            .sum()
    }

    /// Fold one fill into the per-symbol FIFO queue. Returns the
    /// closed-lot slices realized by this fill (empty when the fill
    /// only extends an existing queue or opens a fresh one).
    pub fn on_fill(&mut self, fill: &Fill) -> Vec<ClosedLot> {
        if fill.qty.0 <= 0 {
            return Vec::new();
        }
        if matches!(fill.side, Side::Unknown) {
            return Vec::new();
        }
        let book = self.books.entry(fill.symbol.clone()).or_default();
        let closed_now = apply_fill(book, fill.side, fill.qty.0, fill.price, fill.ts);
        if fill.fee != 0 {
            book.fees += fill.fee as i128;
        }
        closed_now
    }
}

fn apply_fill(
    book: &mut LotBook,
    side: Side,
    mut remaining: i64,
    price: Price,
    ts: Timestamp,
) -> Vec<ClosedLot> {
    let mut closed_now: Vec<ClosedLot> = Vec::new();
    // Flat or same-direction: enqueue.
    let same_or_empty = match book.open.front() {
        None => true,
        Some(l) => l.side == side,
    };
    if same_or_empty {
        book.open.push_back(TaxLot {
            side,
            qty: remaining,
            entry: price,
            opened: ts,
        });
        return closed_now;
    }

    // Opposing fill: consume from the front.
    while remaining > 0 {
        let Some(front) = book.open.front_mut() else {
            break;
        };
        let overlap = front.qty.min(remaining);
        let diff = (price.0 as i128) - (front.entry.0 as i128);
        let realized_delta = match front.side {
            Side::Buy => diff * (overlap as i128),
            Side::Sell => -diff * (overlap as i128),
            Side::Unknown => 0,
        };
        book.realized += realized_delta;
        let closed = ClosedLot {
            opened_side: front.side,
            qty: overlap,
            entry: front.entry,
            exit: price,
            opened: front.opened,
            closed: ts,
            realized: realized_delta,
        };
        closed_now.push(closed);
        book.closed.push(closed);
        front.qty -= overlap;
        remaining -= overlap;
        if front.qty == 0 {
            book.open.pop_front();
        }
    }

    // Leftover quantity flips side: enqueue as a new lot on `side`.
    if remaining > 0 {
        debug_assert!(book.open.is_empty(), "flip remainder with non-empty queue");
        book.open.push_back(TaxLot {
            side,
            qty: remaining,
            entry: price,
            opened: ts,
        });
    }

    closed_now
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{ClientOrderId, Qty, Symbol, Venue};

    fn sym() -> Symbol {
        Symbol::from_static("BTCUSDT")
    }

    fn fill(side: Side, price: i64, qty: i64) -> Fill {
        fill_at(side, price, qty, Timestamp::default())
    }

    fn fill_at(side: Side, price: i64, qty: i64, ts: Timestamp) -> Fill {
        Fill {
            cid: ClientOrderId::new("c"),
            venue: Venue::BINANCE,
            symbol: sym(),
            side,
            price: Price(price),
            qty: Qty(qty),
            ts,
            is_maker: None,
            fee: 0,
            fee_asset: None,
        }
    }

    fn fill_with_fee(side: Side, price: i64, qty: i64, fee: i64, asset: &str) -> Fill {
        let mut f = fill(side, price, qty);
        f.fee = fee;
        f.fee_asset = Some(asset.to_string());
        f
    }

    #[test]
    fn opens_long_from_flat() {
        let mut a = LotAccountant::new();
        let closed = a.on_fill(&fill(Side::Buy, 100, 10));
        assert!(closed.is_empty());
        let book = a.book(&sym()).unwrap();
        assert_eq!(book.position(), 10);
        assert_eq!(book.side(), Some(Side::Buy));
        assert_eq!(book.open_lots().count(), 1);
    }

    #[test]
    fn extending_fills_queue_as_separate_lots_not_averaged() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        a.on_fill(&fill(Side::Buy, 110, 5));
        let book = a.book(&sym()).unwrap();
        assert_eq!(book.position(), 15);
        let lots: Vec<_> = book.open_lots().copied().collect();
        assert_eq!(lots.len(), 2);
        assert_eq!(lots[0].entry, Price(100));
        assert_eq!(lots[0].qty, 10);
        assert_eq!(lots[1].entry, Price(110));
        assert_eq!(lots[1].qty, 5);
    }

    #[test]
    fn partial_close_consumes_oldest_lot_first() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        a.on_fill(&fill(Side::Buy, 120, 10));
        // Close 4: all from the first lot at entry=100, exit=115.
        let closed = a.on_fill(&fill(Side::Sell, 115, 4));
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].entry, Price(100));
        assert_eq!(closed[0].qty, 4);
        assert_eq!(closed[0].realized, (115 - 100) * 4);
        let book = a.book(&sym()).unwrap();
        assert_eq!(book.position(), 16);
        let lots: Vec<_> = book.open_lots().copied().collect();
        assert_eq!(lots[0].qty, 6, "first lot remainder after partial close");
        assert_eq!(lots[1].qty, 10);
    }

    #[test]
    fn close_larger_than_front_lot_spills_to_next_lot() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        a.on_fill(&fill(Side::Buy, 120, 10));
        // Close 12: all 10 from lot #1, 2 from lot #2.
        let closed = a.on_fill(&fill(Side::Sell, 130, 12));
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[0].qty, 10);
        assert_eq!(closed[0].entry, Price(100));
        assert_eq!(closed[0].realized, (130 - 100) * 10);
        assert_eq!(closed[1].qty, 2);
        assert_eq!(closed[1].entry, Price(120));
        assert_eq!(closed[1].realized, (130 - 120) * 2);
        assert_eq!(a.realized(&sym()), (130 - 100) * 10 + (130 - 120) * 2);
    }

    #[test]
    fn exact_close_empties_queue() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        a.on_fill(&fill(Side::Sell, 120, 10));
        let book = a.book(&sym()).unwrap();
        assert_eq!(book.position(), 0);
        assert_eq!(book.side(), None);
        assert_eq!(book.realized(), (120 - 100) * 10);
        assert_eq!(book.unrealized(Price(500)), 0);
    }

    #[test]
    fn overshoot_flips_side_and_opens_fresh_lot() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        let closed = a.on_fill(&fill(Side::Sell, 110, 15));
        // 10 of the 15 close the long lot; realized = (110-100)*10 = 100.
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].qty, 10);
        assert_eq!(closed[0].realized, 100);
        let book = a.book(&sym()).unwrap();
        assert_eq!(book.position(), -5);
        assert_eq!(book.side(), Some(Side::Sell));
        let lots: Vec<_> = book.open_lots().copied().collect();
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].qty, 5);
        assert_eq!(lots[0].entry, Price(110));
    }

    #[test]
    fn short_close_realizes_on_lower_buyback() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill(Side::Sell, 100, 10));
        let closed = a.on_fill(&fill(Side::Buy, 90, 6));
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].realized, (100 - 90) * 6);
        assert_eq!(a.position(&sym()), -4);
    }

    #[test]
    fn short_close_realizes_loss_on_higher_buyback() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill(Side::Sell, 100, 10));
        a.on_fill(&fill(Side::Buy, 115, 4));
        assert_eq!(a.realized(&sym()), (100 - 115) * 4);
    }

    #[test]
    fn unrealized_sums_per_lot_for_long_queue() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        a.on_fill(&fill(Side::Buy, 120, 5));
        // Mark 130: (130-100)*10 + (130-120)*5 = 300 + 50 = 350.
        assert_eq!(a.unrealized(&sym(), Price(130)), 350);
    }

    #[test]
    fn unrealized_sums_per_lot_for_short_queue() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill(Side::Sell, 100, 10));
        a.on_fill(&fill(Side::Sell, 90, 5));
        // Mark 80: -(80-100)*10 + -(80-90)*5 = 200 + 50 = 250.
        assert_eq!(a.unrealized(&sym(), Price(80)), 250);
    }

    #[test]
    fn unrealized_total_applies_per_symbol_mark() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill(Side::Buy, 100, 5));
        let mut eth = fill(Side::Buy, 3_000, 1);
        eth.symbol = Symbol::from_static("ETHUSDT");
        a.on_fill(&eth);
        let total = a.unrealized_total(|s| match s.as_str() {
            "BTCUSDT" => Some(Price(110)),
            "ETHUSDT" => Some(Price(3_200)),
            _ => None,
        });
        assert_eq!(total, (110 - 100) * 5 + (3_200 - 3_000));
    }

    #[test]
    fn closed_lots_preserve_open_and_close_timestamps() {
        let mut a = LotAccountant::new();
        let t0 = Timestamp::from_unix_nanos(100);
        let t1 = Timestamp::from_unix_nanos(200);
        a.on_fill(&fill_at(Side::Buy, 100, 10, t0));
        a.on_fill(&fill_at(Side::Sell, 120, 10, t1));
        let book = a.book(&sym()).unwrap();
        let closed = book.closed_lots();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].opened, t0);
        assert_eq!(closed[0].closed, t1);
        assert_eq!(closed[0].opened_side, Side::Buy);
        assert_eq!(closed[0].entry, Price(100));
        assert_eq!(closed[0].exit, Price(120));
    }

    #[test]
    fn fees_accumulate_on_quote_commissions_only() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill_with_fee(Side::Buy, 100, 10, 2, "USDT"));
        a.on_fill(&fill_with_fee(Side::Sell, 120, 10, 3, "USDT"));
        assert_eq!(a.fees(&sym()), 5);
        assert_eq!(a.realized(&sym()), (120 - 100) * 10);
        assert_eq!(a.realized_net(&sym()), 200 - 5);
    }

    #[test]
    fn cross_asset_fee_is_ignored_even_when_labeled() {
        let mut a = LotAccountant::new();
        a.on_fill(&fill_with_fee(Side::Buy, 100, 10, 0, "BNB"));
        a.on_fill(&fill_with_fee(Side::Sell, 120, 10, 0, "BNB"));
        assert_eq!(a.fees(&sym()), 0);
        assert_eq!(a.realized_net_total(), 200);
    }

    #[test]
    fn unknown_side_and_zero_qty_are_noops() {
        let mut a = LotAccountant::new();
        assert!(a.on_fill(&fill(Side::Unknown, 100, 5)).is_empty());
        assert!(a.on_fill(&fill(Side::Buy, 100, 0)).is_empty());
        assert!(a.book(&sym()).is_none());
    }

    #[test]
    fn fifo_matches_wac_totals_on_single_cycle() {
        // A single open/close round gives identical realized totals
        // between WAC and FIFO (they only diverge across multi-lot
        // partial closes, which the test above checks explicitly).
        let mut fifo = LotAccountant::new();
        let mut wac = crate::Accountant::new();
        for f in [
            fill(Side::Buy, 100, 10),
            fill(Side::Buy, 110, 10),
            fill(Side::Sell, 130, 20),
        ] {
            fifo.on_fill(&f);
            wac.on_fill(&f);
        }
        assert_eq!(fifo.realized(&sym()), wac.realized(&sym()));
    }
}
