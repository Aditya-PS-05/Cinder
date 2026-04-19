//! Inventory-skew market maker.
//!
//! [`InventorySkewMaker`] quotes a symmetric bid/ask pair around the
//! book microprice and shifts both quotes against its current inventory
//! to encourage mean-reversion back to flat. Centring on microprice
//! rather than mid leans the quotes toward the thin side of the book —
//! a queue-imbalance signal that tends to predict the next tick. The
//! maker falls back to mid whenever microprice is unavailable. Each
//! tick cancels any still-open quotes and reissues fresh ones; when
//! the maker has saturated its long/short bound it suppresses the
//! corresponding side.

use ts_book::{EwmaVol, OrderBook};
use ts_core::{
    ClientOrderId, ExecReport, Fill, NewOrder, OrderKind, Price, Qty, Side, Symbol, TimeInForce,
    Timestamp, Venue,
};

use crate::{is_terminal, Strategy, StrategyAction};

/// Running counts of why the maker quoted (or didn't) on each tick.
///
/// The maker has three gates that can silently drop a side:
///
///   * `suppressed_cap`   — inventory is at or beyond `max_inventory`
///     on the side we'd be accumulating.
///   * `suppressed_cross` — the computed price would cross the book
///     and execute as a taker (see the Phase 45 cross guard).
///   * `suppressed_price` — the computed price is non-positive (the
///     stacked widening / skew math underflowed past zero).
///
/// `quotes_posted` is the count of `Place` actions emitted — every
/// tick that places both sides bumps this by two. Suppression counts
/// are per-side as well, so the per-side numbers sum to
/// `2 * ticks_with_valid_book` minus one-sided empty-book ticks.
///
/// Public so operators can ask a running maker "what are you doing?"
/// without plumbing metrics through the Strategy trait; future
/// observability phases can surface these through Prometheus / the
/// ReplaySummary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QuoteGateCounters {
    pub quotes_posted: u64,
    pub suppressed_cap: u64,
    pub suppressed_cross: u64,
    pub suppressed_price: u64,
}

#[derive(Clone, Debug)]
pub struct MakerConfig {
    pub venue: Venue,
    pub symbol: Symbol,
    /// Quote size in qty-scale mantissa.
    pub quote_qty: Qty,
    /// Half-spread added to/subtracted from mid, in price-scale ticks.
    pub half_spread_ticks: i64,
    /// Extra half-spread applied proportional to |imbalance|, in price-
    /// scale ticks. Zero preserves legacy fixed-spread behaviour. When
    /// positive, the half-spread widens by up to this many ticks as the
    /// top-of-book queue imbalance approaches ±1 — an adverse-selection
    /// guard that demands more edge when directional flow is likely.
    pub imbalance_widen_ticks: i64,
    /// Decay factor for the EWMA mid-price volatility tracker. Only
    /// consulted when `vol_widen_coeff > 0.0`; must be in `(0, 1)` in
    /// that case (a standard RiskMetrics choice is 0.94). When the
    /// vol-aware widening is disabled this field is ignored.
    pub vol_lambda: f64,
    /// Multiplier applied to the EWMA mid-tick sigma to produce an
    /// extra half-spread in ticks. Zero disables the volatility path
    /// entirely (and skips allocating the tracker); positive values
    /// widen the spread as realised mid-price volatility rises — an
    /// adverse-selection guard complementary to `imbalance_widen_ticks`.
    pub vol_widen_coeff: f64,
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
    /// EWMA mid-price volatility tracker. `None` when the vol-aware
    /// widening path is disabled (`vol_widen_coeff == 0.0`) so we don't
    /// pay for state we never read.
    vol: Option<EwmaVol>,
    inventory: i64,
    open_bid: Option<ClientOrderId>,
    open_ask: Option<ClientOrderId>,
    cid_counter: u64,
    counters: QuoteGateCounters,
}

impl InventorySkewMaker {
    pub fn new(cfg: MakerConfig) -> Self {
        let vol = (cfg.vol_widen_coeff > 0.0).then(|| EwmaVol::new(cfg.vol_lambda));
        Self {
            cfg,
            vol,
            inventory: 0,
            open_bid: None,
            open_ask: None,
            cid_counter: 0,
            counters: QuoteGateCounters::default(),
        }
    }

    /// Snapshot of the per-reason gate counters since construction.
    pub fn counters(&self) -> QuoteGateCounters {
        self.counters
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
        // Feed the vol tracker on every tick we have a mid, even on
        // ticks where we end up not quoting. That keeps the EWMA
        // monotonic across inventory-cap suppressions and one-sided
        // books — the signal is about the market, not about us.
        if let (Some(vol), Some(mid)) = (self.vol.as_mut(), book.mid()) {
            vol.update(mid);
        }

        let centre = match book.microprice().or_else(|| book.mid()) {
            Some(m) => m,
            None => return Vec::new(),
        };

        // Imbalance widens the spread symmetrically: toxic directional
        // flow is equally dangerous on both sides because the thin side
        // gets picked off while the heavy side trades against the move.
        // floor() keeps widening conservative — no half-tick rounding up.
        let imb_widen = if self.cfg.imbalance_widen_ticks > 0 {
            let imb = book.imbalance().unwrap_or(0.0).abs();
            (self.cfg.imbalance_widen_ticks as f64 * imb).floor() as i64
        } else {
            0
        };
        // Vol widening: add `floor(coeff * sigma_ticks)` extra half-
        // spread. `sigma` is `None` until the tracker has seen at least
        // two mids, so the first quote on a fresh session falls through
        // to the base spread — which is the safe default.
        let vol_widen = self
            .vol
            .as_ref()
            .and_then(EwmaVol::sigma)
            .map(|s| (s * self.cfg.vol_widen_coeff).floor() as i64)
            .unwrap_or(0);
        let half = self
            .cfg
            .half_spread_ticks
            .saturating_add(imb_widen)
            .saturating_add(vol_widen);

        // Inventory skew shifts both quotes by the same amount, so the
        // spread is preserved but the center of mass walks away from the
        // side the maker is long on.
        let skew_shift = self.cfg.inventory_skew_ticks.saturating_mul(self.inventory);
        let bid_px = centre.saturating_sub(half).saturating_sub(skew_shift);
        let ask_px = centre.saturating_add(half).saturating_sub(skew_shift);

        let mut actions = Vec::new();

        if let Some(cid) = self.open_bid.take() {
            actions.push(StrategyAction::Cancel(cid));
        }
        if let Some(cid) = self.open_ask.take() {
            actions.push(StrategyAction::Cancel(cid));
        }

        let at_long_cap = self.inventory >= self.cfg.max_inventory;
        let at_short_cap = self.inventory <= -self.cfg.max_inventory;

        // Passive-only cross guard. A limit posted at or past the
        // opposite side of the book would be an immediate taker fill
        // at the venue, not a resting maker quote — which defeats the
        // whole strategy and pays the taker fee. When the stacked
        // widening plus skew math pushes a computed price across the
        // spread, drop that side rather than post it. centre was
        // derived from a two-sided book, so best_bid and best_ask are
        // guaranteed Some here.
        let best_bid_px = book.best_bid().expect("two-sided book").price.0;
        let best_ask_px = book.best_ask().expect("two-sided book").price.0;
        let bid_would_cross = bid_px >= best_ask_px;
        let ask_would_cross = ask_px <= best_bid_px;

        // Gate ordering matches the precedence we want the counters to
        // reflect: cap fires first (strategy decision), then cross
        // (market-structure safety), then price (numeric sanity). Each
        // suppressed side bumps exactly one counter so the totals
        // stay disjoint and easy to reason about.
        if at_long_cap {
            self.counters.suppressed_cap += 1;
        } else if bid_would_cross {
            self.counters.suppressed_cross += 1;
        } else if bid_px <= 0 {
            self.counters.suppressed_price += 1;
        } else {
            let (cid, order) = self.build_order(Side::Buy, Price(bid_px), "b", now);
            self.open_bid = Some(cid);
            actions.push(StrategyAction::Place(order));
            self.counters.quotes_posted += 1;
        }

        if at_short_cap {
            self.counters.suppressed_cap += 1;
        } else if ask_would_cross {
            self.counters.suppressed_cross += 1;
        } else if ask_px <= 0 {
            self.counters.suppressed_price += 1;
        } else {
            let (cid, order) = self.build_order(Side::Sell, Price(ask_px), "a", now);
            self.open_ask = Some(cid);
            actions.push(StrategyAction::Place(order));
            self.counters.quotes_posted += 1;
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

    fn on_shutdown(&mut self) -> Vec<StrategyAction> {
        let mut actions = Vec::new();
        if let Some(cid) = self.open_bid.take() {
            actions.push(StrategyAction::Cancel(cid));
        }
        if let Some(cid) = self.open_ask.take() {
            actions.push(StrategyAction::Cancel(cid));
        }
        actions
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
            imbalance_widen_ticks: 0,
            vol_lambda: 0.94,
            vol_widen_coeff: 0.0,
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
    fn imbalance_widen_zero_leaves_spread_untouched() {
        // Default test cfg has imbalance_widen_ticks = 0, so even on a
        // heavily imbalanced book the half-spread stays at 5 ticks.
        let mut m = InventorySkewMaker::new(cfg());
        let book = book_with(vec![lvl(100, 30)], vec![lvl(110, 10)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        let bid = place_order(&actions[0]);
        let ask = place_order(&actions[1]);
        // microprice = 107; no widening. bid=102, ask=112.
        assert_eq!(bid.price, Some(Price(102)));
        assert_eq!(ask.price, Some(Price(112)));
    }

    #[test]
    fn imbalance_widens_spread_when_configured() {
        let mut c = cfg();
        c.imbalance_widen_ticks = 10;
        let mut m = InventorySkewMaker::new(c);
        // bids=30, asks=10 → |imbalance| = 0.5. widen = floor(10*0.5) = 5.
        // microprice = 107, effective half = 5 + 5 = 10.
        let book = book_with(vec![lvl(100, 30)], vec![lvl(110, 10)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        let bid = place_order(&actions[0]);
        let ask = place_order(&actions[1]);
        assert_eq!(bid.price, Some(Price(97))); // 107 - 10
        assert_eq!(ask.price, Some(Price(117))); // 107 + 10
    }

    #[test]
    fn vol_widen_coeff_zero_keeps_spread_fixed_even_on_jittery_ticks() {
        // Default cfg has vol_widen_coeff = 0.0 — the tracker is not
        // even allocated, and a wildly jumpy mid must not change the
        // quoted spread.
        let mut m = InventorySkewMaker::new(cfg());
        let book1 = book_with(vec![lvl(100, 10)], vec![lvl(110, 10)]);
        let book2 = book_with(vec![lvl(500, 10)], vec![lvl(510, 10)]);
        m.on_book_update(Timestamp::default(), &book1);
        let actions = m.on_book_update(Timestamp::default(), &book2);
        let places: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                StrategyAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();
        assert_eq!(places.len(), 2);
        // mid=505, half_spread=5. Spread stays 10 ticks wide regardless
        // of the 400-tick mid jump.
        assert_eq!(places[0].price, Some(Price(500)));
        assert_eq!(places[1].price, Some(Price(510)));
    }

    #[test]
    fn vol_widening_kicks_in_on_seeded_sigma() {
        // Configure the vol widening: coeff=2.0, lambda=0.5. The first
        // tick primes the tracker; the second tick seeds variance from
        // the delta (no blend on cold start), so sigma equals the
        // absolute delta. A delta of 3 ticks → widen = floor(2.0*3) = 6.
        let mut c = cfg();
        c.vol_widen_coeff = 2.0;
        c.vol_lambda = 0.5;
        let mut m = InventorySkewMaker::new(c);

        // Tick 1: mid=105, no sigma yet — quotes at the base spread.
        let book1 = book_with(vec![lvl(100, 10)], vec![lvl(110, 10)]);
        let first = m.on_book_update(Timestamp::default(), &book1);
        let first_bid = place_order(&first[0]);
        assert_eq!(first_bid.price, Some(Price(100))); // 105 - 5

        // Tick 2: mid=108 (walk +3 ticks). sigma = 3 → widen = 6.
        // Effective half = 5 + 6 = 11. bid=97, ask=119.
        let book2 = book_with(vec![lvl(103, 10)], vec![lvl(113, 10)]);
        let second = m.on_book_update(Timestamp::default(), &book2);
        let places: Vec<_> = second
            .iter()
            .filter_map(|a| match a {
                StrategyAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();
        assert_eq!(places.len(), 2);
        assert_eq!(places[0].price, Some(Price(97))); // 108 - 11
        assert_eq!(places[1].price, Some(Price(119))); // 108 + 11
    }

    #[test]
    fn vol_widening_decays_on_calm_ticks_after_a_shock() {
        // lambda=0.5, coeff=1.0. Shock: tick 1 primes (mid=100), tick 2
        // jumps to mid=110 seeding variance=100 → sigma=10 → widen=10.
        // Tick 3: mid stays at 110 → variance = 0.5*100 + 0.5*0 = 50 →
        // sigma ≈ 7.07 → widen = floor(7.07) = 7. Widening must shrink.
        let mut c = cfg();
        c.vol_widen_coeff = 1.0;
        c.vol_lambda = 0.5;
        let mut m = InventorySkewMaker::new(c);

        let book_100 = book_with(vec![lvl(95, 10)], vec![lvl(105, 10)]);
        let book_110 = book_with(vec![lvl(105, 10)], vec![lvl(115, 10)]);
        m.on_book_update(Timestamp::default(), &book_100); // priming
        let shock = m.on_book_update(Timestamp::default(), &book_110);
        let ask_after_shock = shock
            .iter()
            .find_map(|a| match a {
                StrategyAction::Place(o) if o.side == Side::Sell => Some(o),
                _ => None,
            })
            .unwrap();
        // mid=110, half = 5 + 10 = 15. ask = 125.
        assert_eq!(ask_after_shock.price, Some(Price(125)));

        let calm = m.on_book_update(Timestamp::default(), &book_110);
        let ask_after_calm = calm
            .iter()
            .find_map(|a| match a {
                StrategyAction::Place(o) if o.side == Side::Sell => Some(o),
                _ => None,
            })
            .unwrap();
        // widen = floor(sqrt(50)) = 7. half = 12. ask = 122.
        assert_eq!(ask_after_calm.price, Some(Price(122)));
    }

    #[test]
    fn vol_widen_stacks_with_imbalance_widen() {
        // Both widening paths active. We verify they sum rather than
        // either taking precedence.
        let mut c = cfg();
        c.imbalance_widen_ticks = 10;
        c.vol_widen_coeff = 1.0;
        c.vol_lambda = 0.5;
        let mut m = InventorySkewMaker::new(c);

        // Prime with a symmetric book (imbalance 0, no vol yet).
        let prime = book_with(vec![lvl(100, 10)], vec![lvl(110, 10)]);
        m.on_book_update(Timestamp::default(), &prime);

        // Now jump by 4 ticks AND run a 30:10 imbalance — widen should
        // stack to floor(1*4) + floor(10*0.5) = 4 + 5 = 9 extra ticks.
        // microprice(100*10 + 114*30)/40... wait let's recompute:
        // book: bids=[104, 30], asks=[114, 10]. mid=(104+114)/2=109.
        // microprice = (104*10 + 114*30)/40 = (1040 + 3420)/40 = 111.
        // delta from prime (mid=105) to this mid (109) = 4, sigma seeded=4.
        // half = 5 + 4 + 5 = 14. bid=111-14=97, ask=111+14=125.
        let next = book_with(vec![lvl(104, 30)], vec![lvl(114, 10)]);
        let actions = m.on_book_update(Timestamp::default(), &next);
        // Tick 2 begins with two Cancels for the prime-tick quotes,
        // then the two new Places we want to inspect.
        let places: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                StrategyAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();
        assert_eq!(places.len(), 2);
        assert_eq!(places[0].price, Some(Price(97)));
        assert_eq!(places[1].price, Some(Price(125)));
    }

    #[test]
    fn imbalance_widen_balanced_book_does_nothing() {
        let mut c = cfg();
        c.imbalance_widen_ticks = 10;
        let mut m = InventorySkewMaker::new(c);
        // Symmetric queues: imbalance = 0, widen floor(10*0) = 0.
        let book = book_with(vec![lvl(100, 10)], vec![lvl(110, 10)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        let bid = place_order(&actions[0]);
        let ask = place_order(&actions[1]);
        assert_eq!(bid.price, Some(Price(100)));
        assert_eq!(ask.price, Some(Price(110)));
    }

    #[test]
    fn asymmetric_book_centres_quotes_on_microprice_not_mid() {
        // mid=105, microprice leans toward the thin side: a heavy bid
        // queue (qty=30 vs ask qty=10) pulls the fair price up toward
        // the ask. microprice = (100*10 + 110*30) / 40 = 107.
        let mut m = InventorySkewMaker::new(cfg());
        let book = book_with(vec![lvl(100, 30)], vec![lvl(110, 10)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        let bid = place_order(&actions[0]);
        let ask = place_order(&actions[1]);
        // half_spread=5, skew=0. bid = 107-5 = 102, ask = 107+5 = 112.
        // If the maker were still centring on mid=105, we'd see 100/110.
        assert_eq!(bid.price, Some(Price(102)));
        assert_eq!(ask.price, Some(Price(112)));
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
    fn bid_that_would_cross_best_ask_is_suppressed() {
        // Contrive a config where the base spread is tiny, then rack
        // up a hugely negative inventory so the skew pushes both
        // quotes UP. With inventory=-19 and skew_ticks=1, shift=-19
        // so bid = centre + 19 - half. On a tight book that lands
        // the bid above best_ask — the guard must drop it.
        let mut c = cfg();
        c.half_spread_ticks = 1;
        c.inventory_skew_ticks = 1;
        // max_inventory at 20 keeps us below the suppression cap so
        // the short cap isn't what's killing the bid in this test.
        let mut m = InventorySkewMaker::new(c);
        m.on_fill(&fill(Side::Sell, 19)); // inventory = -19
        assert_eq!(m.inventory(), -19);

        // mid = 105, half = 1, skew_shift = -19. bid_px = 105-1+19=123,
        // ask_px = 105+1+19=125. best_ask = 110 → bid(123) >= 110, cross.
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        let places: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                StrategyAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();
        // Only the ask should survive; the bid would have crossed.
        assert_eq!(places.len(), 1);
        assert_eq!(places[0].side, Side::Sell);
        assert!(m.open_bid().is_none());
        assert!(m.open_ask().is_some());
    }

    #[test]
    fn ask_that_would_cross_best_bid_is_suppressed() {
        // Mirror of the above: large long inventory pushes both
        // quotes down, and a tight base spread lands the ask below
        // best_bid. Skew must be strong enough to cross the book.
        let mut c = cfg();
        c.half_spread_ticks = 1;
        c.inventory_skew_ticks = 1;
        let mut m = InventorySkewMaker::new(c);
        m.on_fill(&fill(Side::Buy, 19)); // inventory = +19
        assert_eq!(m.inventory(), 19);

        // mid=105, half=1, skew_shift=19. bid_px=105-1-19=85,
        // ask_px=105+1-19=87. best_bid=100 → ask(87) <= 100, cross.
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
        assert!(m.open_bid().is_some());
        assert!(m.open_ask().is_none());
    }

    #[test]
    fn cross_guard_triggers_at_the_touch_not_past_it() {
        // Equality boundary: a bid price that exactly matches best_ask
        // would execute as a taker on most venues (price-time match),
        // so the guard must reject equality as well as strict crosses.
        // Setup: half=5, no skew, symmetric queues so microprice==mid==105.
        // best_ask=110. To force bid_px=110 we need 105-5+skew=110 → skew=-10.
        // Use inventory=-10 with skew_ticks=1 to get skew_shift=-10.
        let mut c = cfg();
        c.half_spread_ticks = 5;
        c.inventory_skew_ticks = 1;
        let mut m = InventorySkewMaker::new(c);
        m.on_fill(&fill(Side::Sell, 10)); // inventory = -10
        let book = book_with(vec![lvl(100, 5)], vec![lvl(110, 5)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        // bid_px = 105 - 5 - (-10) = 110; ask_px = 105 + 5 - (-10) = 120.
        // bid_px == best_ask → crossed by the guard's >= check.
        let places: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                StrategyAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();
        assert_eq!(places.len(), 1);
        assert_eq!(places[0].side, Side::Sell);
    }

    #[test]
    fn cross_guard_allows_quotes_exactly_one_tick_inside_the_touch() {
        // A bid one tick below best_ask and an ask one tick above
        // best_bid are valid resting quotes — the guard must let
        // them through. Construct a wide book where centred passive
        // quotes would naturally land there.
        let mut m = InventorySkewMaker::new(cfg());
        // Book has a 10-tick spread; half=5 puts bid=100 and ask=110
        // (from the default test cfg), which is one tick inside the
        // best_bid=99 and best_ask=111 respectively.
        let book = book_with(vec![lvl(99, 5)], vec![lvl(111, 5)]);
        let actions = m.on_book_update(Timestamp::default(), &book);
        let places: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                StrategyAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();
        assert_eq!(places.len(), 2);
        // bid=100 < best_ask=111; ask=110 > best_bid=99. Both safe.
        assert_eq!(places[0].price, Some(Price(100)));
        assert_eq!(places[1].price, Some(Price(110)));
    }

    #[test]
    fn max_long_inventory_suppresses_bid() {
        // Disable the skew so this test isolates the inventory-cap
        // suppression from the cross-guard — otherwise a large long
        // inventory also drags the ask below best_bid and the cross
        // guard suppresses the ask too, conflating two behaviours.
        let mut c = cfg();
        c.inventory_skew_ticks = 0;
        let mut m = InventorySkewMaker::new(c);
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
        // Same rationale as max_long_inventory_suppresses_bid: isolate
        // the cap from the cross-guard by zeroing the skew.
        let mut c = cfg();
        c.inventory_skew_ticks = 0;
        let mut m = InventorySkewMaker::new(c);
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
    fn on_shutdown_returns_cancels_for_both_open_quotes() {
        let mut m = InventorySkewMaker::new(cfg());
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        m.on_book_update(Timestamp::default(), &book);
        assert!(m.open_bid().is_some());
        assert!(m.open_ask().is_some());

        let actions = m.on_shutdown();
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], StrategyAction::Cancel(_)));
        assert!(matches!(actions[1], StrategyAction::Cancel(_)));
        // State cleared so a second sweep is a no-op — important for
        // runners that may wind down more than once in pathological
        // tests or if shutdown signals race.
        assert!(m.open_bid().is_none());
        assert!(m.open_ask().is_none());
        assert!(m.on_shutdown().is_empty());
    }

    #[test]
    fn on_shutdown_is_noop_when_no_open_quotes() {
        let mut m = InventorySkewMaker::new(cfg());
        assert!(m.on_shutdown().is_empty());
    }

    #[test]
    fn fill_on_other_symbol_does_not_move_inventory() {
        let mut m = InventorySkewMaker::new(cfg());
        let mut f = fill(Side::Buy, 5);
        f.symbol = Symbol::from_static("ETHUSDT");
        m.on_fill(&f);
        assert_eq!(m.inventory(), 0);
    }

    #[test]
    fn fresh_maker_reports_zeroed_counters() {
        let m = InventorySkewMaker::new(cfg());
        assert_eq!(m.counters(), QuoteGateCounters::default());
    }

    #[test]
    fn clean_two_sided_tick_bumps_quotes_posted_by_two() {
        let mut m = InventorySkewMaker::new(cfg());
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        m.on_book_update(Timestamp::default(), &book);
        let c = m.counters();
        assert_eq!(c.quotes_posted, 2);
        assert_eq!(c.suppressed_cap, 0);
        assert_eq!(c.suppressed_cross, 0);
        assert_eq!(c.suppressed_price, 0);
    }

    #[test]
    fn inventory_cap_bumps_suppressed_cap_counter() {
        let mut c = cfg();
        c.inventory_skew_ticks = 0; // isolate cap from cross guard
        let mut m = InventorySkewMaker::new(c);
        m.on_fill(&fill(Side::Buy, 20));
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        m.on_book_update(Timestamp::default(), &book);
        let gc = m.counters();
        // Long cap pins the bid; ask posts normally.
        assert_eq!(gc.suppressed_cap, 1);
        assert_eq!(gc.quotes_posted, 1);
        assert_eq!(gc.suppressed_cross, 0);
        assert_eq!(gc.suppressed_price, 0);
    }

    #[test]
    fn cross_guard_bumps_suppressed_cross_counter() {
        // Re-use the cross-guard fixture: tight base spread + large
        // short inventory drives the bid above best_ask.
        let mut c = cfg();
        c.half_spread_ticks = 1;
        c.inventory_skew_ticks = 1;
        let mut m = InventorySkewMaker::new(c);
        m.on_fill(&fill(Side::Sell, 19));
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        m.on_book_update(Timestamp::default(), &book);
        let gc = m.counters();
        // Bid would cross → suppressed_cross=1; ask posts normally.
        assert_eq!(gc.suppressed_cross, 1);
        assert_eq!(gc.quotes_posted, 1);
        assert_eq!(gc.suppressed_cap, 0);
        assert_eq!(gc.suppressed_price, 0);
    }

    #[test]
    fn non_positive_price_bumps_suppressed_price_counter() {
        // Force bid_px <= 0 without crossing (cross is checked first).
        // Use a low-price book so centre is small; a big negative skew
        // shift pulls bid below zero while the ask stays safely above
        // best_bid. With centre=3, half=1, skew_shift = -(-4) = +4 on
        // inventory=-4: bid = 3-1+4 = 6, ask = 3+1+4 = 8. That's not
        // what we need — we want bid_px <= 0. Instead: use large long
        // inventory + low centre so bid sags below zero.
        // centre=3, half=1, skew=1, inventory=10, skew_shift=10:
        // bid = 3-1-10 = -8 (<=0 ✓), ask = 3+1-10 = -6 (<=0 too).
        // We want ask to post normally so the test isolates the price
        // gate. Trickier: we need one side >0 and the other <=0.
        // Use large short inventory so skew pushes UP:
        // centre=3, half=1, inventory=-10, skew_shift=-10:
        // bid = 3-1+10 = 12, ask = 3+1+10 = 14. Both positive.
        // Neither arrangement hits price gate on one side only.
        //
        // So instead just verify the counter fires when BOTH sides go
        // non-positive: centre=1, half=5, inventory=0:
        // bid = 1-5-0 = -4, ask = 1+5-0 = 6. Best_bid=0 so ask(6) > 0
        // and ask(6) > best_bid(0) → ask posts. bid is non-positive.
        // But cross guard runs first: bid(-4) >= best_ask(2)? no.
        // And best_bid must come from the book — book_with expects
        // a valid bid level. Use lvl(0,...) for bid? Price(0) is legal.
        //
        // Simpler construction that hits the price gate on the bid:
        // bids=[lvl(0, 1)], asks=[lvl(10, 1)]. centre = mid = 5.
        // half_spread=10 → bid = 5-10 = -5 (<=0), ask = 5+10 = 15.
        // cross check: bid(-5) >= best_ask(10)? no. ask(15) <= best_bid(0)? no.
        // → bid hits price gate; ask posts. quote_qty=10, etc.
        let mut c = cfg();
        c.half_spread_ticks = 10;
        c.inventory_skew_ticks = 0;
        let mut m = InventorySkewMaker::new(c);
        let book = book_with(vec![lvl(0, 1)], vec![lvl(10, 1)]);
        m.on_book_update(Timestamp::default(), &book);
        let gc = m.counters();
        assert_eq!(gc.suppressed_price, 1);
        assert_eq!(gc.quotes_posted, 1);
        assert_eq!(gc.suppressed_cap, 0);
        assert_eq!(gc.suppressed_cross, 0);
    }

    #[test]
    fn counters_accumulate_across_ticks() {
        let mut m = InventorySkewMaker::new(cfg());
        let book = book_with(vec![lvl(100, 1)], vec![lvl(110, 1)]);
        m.on_book_update(Timestamp::default(), &book);
        m.on_book_update(Timestamp::default(), &book);
        m.on_book_update(Timestamp::default(), &book);
        assert_eq!(m.counters().quotes_posted, 6);
    }
}
