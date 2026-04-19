//! Per-symbol position and realized/unrealized PnL accounting.
//!
//! [`Accountant`] consumes a stream of [`Fill`]s and maintains,
//! per [`Symbol`], a signed position, a weighted-average entry
//! price, and a running realized-pnl accumulator. Unrealized pnl
//! is computed on demand given a mark price.
//!
//! Accounting rules:
//!
//! * Fills that extend the current position (or open from flat)
//!   blend into the average entry price using a size-weighted
//!   mean: `avg' = (avg * |pos| + price * qty) / (|pos| + qty)`.
//! * Fills that reduce the current position realize pnl on the
//!   overlap `min(|pos|, qty)`:
//!   * long closing: `(fill_price - avg) * overlap`
//!   * short closing: `(avg - fill_price) * overlap`
//! * If a reducing fill is larger than the open position, the
//!   remainder flips the side and becomes the new position at
//!   `avg = fill_price`.
//!
//! All arithmetic uses `i128` internally. Mantissa units match the
//! venue's fixed-point scale: PnL is denominated in
//! `price_scale * qty_scale` units and callers rescale it for
//! display.
//!
//! The accountant is intentionally stateless with respect to orders
//! and statuses — it only needs executed fills, so it is reusable
//! from live execution, replay, and post-hoc analysis without
//! change.

#![forbid(unsafe_code)]

use std::collections::HashMap;

use ts_core::{Fill, Price, Side, Symbol};

pub mod tax_lots;

pub use tax_lots::{ClosedLot, LotAccountant, LotBook, TaxLot};

/// Per-symbol accounting state.
#[derive(Clone, Debug, Default)]
pub struct SymbolBook {
    /// Signed open position in qty-scale mantissa. Positive = long.
    pub position: i64,
    /// Weighted-average entry price in price-scale mantissa. Zero
    /// when `position == 0`.
    pub avg_entry: i64,
    /// Cumulative realized pnl *before* fees in `price_scale * qty_scale`
    /// units. Stays at gross for reconciliation; net figures come from
    /// subtracting [`Self::fees`].
    pub realized: i128,
    /// Cumulative commissions paid on this symbol's fills, at the same
    /// `price_scale + qty_scale` mantissa as [`Self::realized`]. Only
    /// fees denominated in the instrument's quote currency fold in here;
    /// cross-asset commissions are captured on the originating [`Fill`]
    /// with a zero mantissa so the asset label survives but the value
    /// does not skew PnL under an unknown conversion rate.
    pub fees: i128,
}

#[derive(Default, Debug, Clone)]
pub struct Accountant {
    books: HashMap<Symbol, SymbolBook>,
}

impl Accountant {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn book(&self, symbol: &Symbol) -> Option<&SymbolBook> {
        self.books.get(symbol)
    }

    /// Iterate over every `(symbol, book)` pair seen so far. Exposed so
    /// runners can publish per-symbol PnL/position gauges without a
    /// parallel bookkeeping structure.
    pub fn iter(&self) -> impl Iterator<Item = (&Symbol, &SymbolBook)> {
        self.books.iter()
    }

    pub fn position(&self, symbol: &Symbol) -> i64 {
        self.books.get(symbol).map_or(0, |b| b.position)
    }

    pub fn avg_entry(&self, symbol: &Symbol) -> Option<Price> {
        self.books.get(symbol).and_then(|b| {
            if b.position == 0 {
                None
            } else {
                Some(Price(b.avg_entry))
            }
        })
    }

    pub fn realized(&self, symbol: &Symbol) -> i128 {
        self.books.get(symbol).map_or(0, |b| b.realized)
    }

    /// Sum of realized pnl across every symbol seen. Gross — see
    /// [`Self::realized_net_total`] for the fee-adjusted figure.
    pub fn realized_total(&self) -> i128 {
        self.books.values().map(|b| b.realized).sum()
    }

    /// Sum of signed positions across every symbol seen. Meaningful as a
    /// scalar for single-symbol runners; multi-symbol callers should
    /// prefer per-symbol [`Self::position`] lookups.
    pub fn position_total(&self) -> i64 {
        self.books.values().map(|b| b.position).sum()
    }

    /// Cumulative fees paid on `symbol` (quote-currency only).
    pub fn fees(&self, symbol: &Symbol) -> i128 {
        self.books.get(symbol).map_or(0, |b| b.fees)
    }

    /// Sum of quote-denominated commissions across every symbol seen.
    pub fn fees_total(&self) -> i128 {
        self.books.values().map(|b| b.fees).sum()
    }

    /// Realized pnl on `symbol` net of cumulative commissions.
    pub fn realized_net(&self, symbol: &Symbol) -> i128 {
        self.books.get(symbol).map_or(0, |b| b.realized - b.fees)
    }

    /// Portfolio-wide realized pnl net of cumulative commissions.
    pub fn realized_net_total(&self) -> i128 {
        self.realized_total() - self.fees_total()
    }

    /// Unrealized pnl for a symbol given the current mark price.
    /// Returns 0 when the position is flat.
    pub fn unrealized(&self, symbol: &Symbol, mark: Price) -> i128 {
        let Some(b) = self.books.get(symbol) else {
            return 0;
        };
        if b.position == 0 {
            return 0;
        }
        let diff = (mark.0 as i128) - (b.avg_entry as i128);
        // position is signed; (mark - avg) * position gives the correct
        // sign for both long and short exposure.
        diff * (b.position as i128)
    }

    /// Sum unrealized pnl across every symbol in `marks`. Symbols
    /// missing a mark contribute zero — the caller is expected to
    /// provide marks for every symbol it cares about.
    pub fn unrealized_total<F>(&self, mark_fn: F) -> i128
    where
        F: Fn(&Symbol) -> Option<Price>,
    {
        self.books
            .keys()
            .map(|sym| mark_fn(sym).map_or(0, |m| self.unrealized(sym, m)))
            .sum()
    }

    /// Fold a single fill into the book for its symbol. Quote-denominated
    /// commissions on the fill accumulate into [`SymbolBook::fees`];
    /// cross-asset commissions (conveyed with `fill.fee == 0` and a
    /// non-quote label) are intentionally skipped here — converting
    /// them to quote is out of scope for the accountant.
    pub fn on_fill(&mut self, fill: &Fill) {
        if fill.qty.0 <= 0 {
            return;
        }
        let signed_qty = match fill.side {
            Side::Buy => fill.qty.0,
            Side::Sell => -fill.qty.0,
            Side::Unknown => return,
        };
        let book = self.books.entry(fill.symbol.clone()).or_default();
        apply_fill(book, signed_qty, fill.price.0);
        if fill.fee != 0 {
            book.fees += fill.fee as i128;
        }
    }
}

fn apply_fill(book: &mut SymbolBook, signed_qty: i64, price: i64) {
    // Starting flat: open the position at `price`.
    if book.position == 0 {
        book.position = signed_qty;
        book.avg_entry = price;
        return;
    }

    let same_sign = book.position.signum() == signed_qty.signum();
    if same_sign {
        // Extend the position — blend the average entry.
        let pos_abs = (book.position as i128).abs();
        let qty_abs = (signed_qty as i128).abs();
        let new_abs = pos_abs + qty_abs;
        let weighted = (book.avg_entry as i128) * pos_abs + (price as i128) * qty_abs;
        book.avg_entry = (weighted / new_abs) as i64;
        book.position = book
            .position
            .checked_add(signed_qty)
            .expect("position overflow on extension");
        return;
    }

    // Opposing fill: realize pnl on the overlap, then possibly flip.
    let pos_abs = (book.position as i128).abs();
    let qty_abs = (signed_qty as i128).abs();
    let overlap = pos_abs.min(qty_abs);

    // For a long being reduced by a sell: (price - avg) * overlap.
    // For a short being reduced by a buy: (avg - price) * overlap.
    // Positive-position branch covers the long case; flip for short.
    let realized_delta = if book.position > 0 {
        ((price as i128) - (book.avg_entry as i128)) * overlap
    } else {
        ((book.avg_entry as i128) - (price as i128)) * overlap
    };
    book.realized += realized_delta;

    if qty_abs <= pos_abs {
        // Partial or exact close. Position shrinks toward zero but
        // keeps its sign; avg_entry is unchanged. When the overlap is
        // exact the position is flat and avg_entry is cleared so
        // subsequent queries treat it as unset.
        book.position = book
            .position
            .checked_add(signed_qty)
            .expect("position overflow on reduction");
        if book.position == 0 {
            book.avg_entry = 0;
        }
        return;
    }

    // Full close plus a remainder that flips the side. The leftover
    // opens a fresh position at the fill price.
    let remainder = qty_abs - pos_abs;
    book.position = if signed_qty > 0 {
        remainder as i64
    } else {
        -(remainder as i64)
    };
    book.avg_entry = price;
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{ClientOrderId, Qty, Symbol, Timestamp, Venue};

    fn sym() -> Symbol {
        Symbol::from_static("BTCUSDT")
    }

    fn fill(side: Side, price: i64, qty: i64) -> Fill {
        Fill {
            cid: ClientOrderId::new("c"),
            venue: Venue::BINANCE,
            symbol: sym(),
            side,
            price: Price(price),
            qty: Qty(qty),
            ts: Timestamp::default(),
            is_maker: None,
            fee: 0,
            fee_asset: None,
        }
    }

    #[test]
    fn opens_long_from_flat() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        assert_eq!(a.position(&sym()), 10);
        assert_eq!(a.avg_entry(&sym()), Some(Price(100)));
        assert_eq!(a.realized(&sym()), 0);
    }

    #[test]
    fn extends_long_with_weighted_avg() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        a.on_fill(&fill(Side::Buy, 110, 10));
        assert_eq!(a.position(&sym()), 20);
        assert_eq!(a.avg_entry(&sym()), Some(Price(105))); // (100*10 + 110*10)/20
    }

    #[test]
    fn partial_close_long_realizes_pnl() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        a.on_fill(&fill(Side::Sell, 120, 4));
        assert_eq!(a.position(&sym()), 6);
        assert_eq!(a.avg_entry(&sym()), Some(Price(100)));
        assert_eq!(a.realized(&sym()), (120 - 100) * 4);
    }

    #[test]
    fn exact_close_clears_avg() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        a.on_fill(&fill(Side::Sell, 120, 10));
        assert_eq!(a.position(&sym()), 0);
        assert_eq!(a.avg_entry(&sym()), None);
        assert_eq!(a.realized(&sym()), (120 - 100) * 10);
    }

    #[test]
    fn flip_long_to_short_realizes_and_opens() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        // Sell 15 at 110: closes 10 long for realized +100, flips to -5 at 110.
        a.on_fill(&fill(Side::Sell, 110, 15));
        assert_eq!(a.position(&sym()), -5);
        assert_eq!(a.avg_entry(&sym()), Some(Price(110)));
        assert_eq!(a.realized(&sym()), (110 - 100) * 10);
    }

    #[test]
    fn opens_short_from_flat() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Sell, 100, 5));
        assert_eq!(a.position(&sym()), -5);
        assert_eq!(a.avg_entry(&sym()), Some(Price(100)));
    }

    #[test]
    fn short_close_realizes_on_lower_buyback() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Sell, 100, 10));
        a.on_fill(&fill(Side::Buy, 90, 6));
        assert_eq!(a.position(&sym()), -4);
        assert_eq!(a.realized(&sym()), (100 - 90) * 6);
    }

    #[test]
    fn short_close_realizes_loss_on_higher_buyback() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Sell, 100, 10));
        a.on_fill(&fill(Side::Buy, 115, 4));
        assert_eq!(a.position(&sym()), -6);
        assert_eq!(a.realized(&sym()), (100 - 115) * 4);
    }

    #[test]
    fn unrealized_long_positive_when_mark_above_avg() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        assert_eq!(a.unrealized(&sym(), Price(110)), (110 - 100) * 10);
    }

    #[test]
    fn unrealized_short_positive_when_mark_below_avg() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Sell, 100, 10));
        // position is -10; diff = mark - avg = -10; product = +100
        assert_eq!(a.unrealized(&sym(), Price(90)), 100);
    }

    #[test]
    fn unrealized_flat_is_zero() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Buy, 100, 5));
        a.on_fill(&fill(Side::Sell, 120, 5));
        assert_eq!(a.unrealized(&sym(), Price(150)), 0);
    }

    #[test]
    fn realized_total_sums_across_symbols() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Buy, 100, 10));
        a.on_fill(&fill(Side::Sell, 120, 10));

        let mut eth_fill = fill(Side::Sell, 3_000, 2);
        eth_fill.symbol = Symbol::from_static("ETHUSDT");
        a.on_fill(&eth_fill);
        let mut eth_close = fill(Side::Buy, 2_800, 2);
        eth_close.symbol = Symbol::from_static("ETHUSDT");
        a.on_fill(&eth_close);

        assert_eq!(a.realized(&sym()), 200);
        assert_eq!(a.realized(&Symbol::from_static("ETHUSDT")), 400);
        assert_eq!(a.realized_total(), 600);
    }

    #[test]
    fn unrealized_total_applies_per_symbol_mark() {
        let mut a = Accountant::new();
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
    fn unknown_side_is_ignored() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Unknown, 100, 5));
        assert_eq!(a.position(&sym()), 0);
        assert!(a.book(&sym()).is_none());
    }

    #[test]
    fn zero_qty_is_ignored() {
        let mut a = Accountant::new();
        a.on_fill(&fill(Side::Buy, 100, 0));
        assert_eq!(a.position(&sym()), 0);
    }

    fn fill_with_fee(side: Side, price: i64, qty: i64, fee: i64, asset: &str) -> Fill {
        let mut f = fill(side, price, qty);
        f.fee = fee;
        f.fee_asset = Some(asset.to_string());
        f
    }

    #[test]
    fn quote_fee_reduces_realized_net_but_keeps_gross_intact() {
        let mut a = Accountant::new();
        // Open long 10 @ 100 costing 2 USDT in commission.
        a.on_fill(&fill_with_fee(Side::Buy, 100, 10, 2, "USDT"));
        // Close at 120 paying another 3 USDT in commission.
        a.on_fill(&fill_with_fee(Side::Sell, 120, 10, 3, "USDT"));

        assert_eq!(
            a.realized(&sym()),
            (120 - 100) * 10,
            "gross stays untouched"
        );
        assert_eq!(a.fees(&sym()), 5);
        assert_eq!(a.realized_net(&sym()), 200 - 5);
        assert_eq!(a.realized_net_total(), 195);
    }

    #[test]
    fn cross_asset_fee_is_captured_on_fill_but_skipped_by_accountant() {
        // BNB-discounted commission — the decoder emits fee=0 with the
        // asset label preserved. The accountant must not treat it as
        // a quote-denominated value.
        let mut a = Accountant::new();
        a.on_fill(&fill_with_fee(Side::Buy, 100, 10, 0, "BNB"));
        a.on_fill(&fill_with_fee(Side::Sell, 120, 10, 0, "BNB"));
        assert_eq!(a.realized(&sym()), 200);
        assert_eq!(a.fees(&sym()), 0);
        assert_eq!(a.realized_net_total(), 200);
    }

    #[test]
    fn fees_total_sums_across_symbols() {
        let mut a = Accountant::new();
        a.on_fill(&fill_with_fee(Side::Buy, 100, 10, 1, "USDT"));
        let mut eth = fill_with_fee(Side::Buy, 3_000, 1, 4, "USDT");
        eth.symbol = Symbol::from_static("ETHUSDT");
        a.on_fill(&eth);

        assert_eq!(a.fees(&sym()), 1);
        assert_eq!(a.fees(&Symbol::from_static("ETHUSDT")), 4);
        assert_eq!(a.fees_total(), 5);
    }
}
