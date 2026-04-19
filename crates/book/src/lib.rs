//! Generic L2 order book engine.
//!
//! [`OrderBook`] maintains sorted bid and ask sides backed by
//! `BTreeMap<Price, Qty>`, applies [`BookSnapshot`] and [`BookDelta`]
//! updates from any venue, and enforces sequence-chaining when the
//! venue provides it. Gap detection is surfaced as [`BookError::Gap`]
//! so higher-level logic (the Binance resync state machine, for
//! example) can decide whether to resync or abort.
//!
//! The engine is venue-agnostic: it does not know about REST snapshots,
//! reconnect, or backoff. Those concerns live in the connector crates.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use thiserror::Error;

use ts_core::{BookDelta, BookLevel, BookSnapshot, Price, Qty};

pub mod vol;
pub use vol::EwmaVol;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BookError {
    #[error("order book has not been initialized with a snapshot")]
    Uninitialized,

    /// A chained delta did not line up with the book's current sequence.
    /// `expected` is the book's `last_seq`; `got_prev` is the delta's
    /// `prev_seq`. Callers typically respond by refetching a snapshot.
    #[error("sequence gap: book at {expected}, delta chains from {got_prev}")]
    Gap { expected: u64, got_prev: u64 },
}

#[derive(Debug, Default, Clone)]
pub struct OrderBook {
    bids: BTreeMap<Price, Qty>,
    asks: BTreeMap<Price, Qty>,
    last_seq: u64,
    initialized: bool,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the book with `snap` and mark it initialized at `seq`.
    /// Zero-quantity levels are treated as "not present" — that matches
    /// how deletes are spelled in [`BookDelta`].
    pub fn apply_snapshot(&mut self, snap: &BookSnapshot, seq: u64) {
        self.bids.clear();
        self.asks.clear();
        for lvl in &snap.bids {
            if lvl.qty.0 > 0 {
                self.bids.insert(lvl.price, lvl.qty);
            }
        }
        for lvl in &snap.asks {
            if lvl.qty.0 > 0 {
                self.asks.insert(lvl.price, lvl.qty);
            }
        }
        self.last_seq = seq;
        self.initialized = true;
    }

    /// Apply an incremental update. `seq` is the sequence the frame
    /// lands on (what the book advances to on success).
    ///
    /// When `delta.prev_seq > 0` the call asserts the book is already
    /// at `prev_seq` and returns [`BookError::Gap`] otherwise. A
    /// `prev_seq` of zero means "venue does not chain" — the check is
    /// skipped and the delta is applied unconditionally.
    pub fn apply_delta(&mut self, delta: &BookDelta, seq: u64) -> Result<(), BookError> {
        if !self.initialized {
            return Err(BookError::Uninitialized);
        }
        if delta.prev_seq != 0 && delta.prev_seq != self.last_seq {
            return Err(BookError::Gap {
                expected: self.last_seq,
                got_prev: delta.prev_seq,
            });
        }
        for lvl in &delta.bids {
            apply_level(&mut self.bids, *lvl);
        }
        for lvl in &delta.asks {
            apply_level(&mut self.asks, *lvl);
        }
        self.last_seq = seq;
        Ok(())
    }

    pub fn best_bid(&self) -> Option<BookLevel> {
        self.bids
            .iter()
            .next_back()
            .map(|(p, q)| BookLevel { price: *p, qty: *q })
    }

    pub fn best_ask(&self) -> Option<BookLevel> {
        self.asks
            .iter()
            .next()
            .map(|(p, q)| BookLevel { price: *p, qty: *q })
    }

    /// Integer mid-price in the same fixed-point scale as the underlying
    /// levels. Returns `None` when either side is empty.
    pub fn mid(&self) -> Option<i64> {
        Some((self.best_bid()?.price.0 + self.best_ask()?.price.0) / 2)
    }

    /// Integer spread (`best_ask - best_bid`). Returns `None` when either
    /// side is empty. A negative result means the book is crossed — that
    /// is the caller's problem to detect.
    pub fn spread(&self) -> Option<i64> {
        Some(self.best_ask()?.price.0 - self.best_bid()?.price.0)
    }

    /// Top-of-book microprice in the same fixed-point scale as the
    /// underlying levels:
    ///
    /// ```text
    ///   microprice = (bid_price * ask_qty + ask_price * bid_qty)
    ///                / (bid_qty + ask_qty)
    /// ```
    ///
    /// The weighting is inverted on purpose: a heavy bid queue and a
    /// thin ask queue pull the fair price toward the ask, matching the
    /// intuition that the next trade is more likely to lift the offer.
    /// Returns `None` when either side is empty or the combined top
    /// quantity is zero.
    ///
    /// Integer division rounds toward zero so the value stays on the
    /// same scale as `mid()` and `best_bid()` / `best_ask()`. Products
    /// and the numerator are computed in `i128` so realistic
    /// `price * qty` values cannot overflow.
    pub fn microprice(&self) -> Option<i64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        let bq = bid.qty.0 as i128;
        let aq = ask.qty.0 as i128;
        let denom = bq + aq;
        if denom == 0 {
            return None;
        }
        let num = (bid.price.0 as i128) * aq + (ask.price.0 as i128) * bq;
        Some((num / denom) as i64)
    }

    /// Top-of-book order-flow imbalance in `[-1.0, 1.0]`:
    ///
    /// ```text
    ///   imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty)
    /// ```
    ///
    /// Positive values mean bid-heavy (buying pressure), negative
    /// values mean ask-heavy (selling pressure). Returns `None` when
    /// either side is empty or the combined top quantity is zero —
    /// callers should treat `None` as "no signal" rather than defaulting
    /// to a numeric value.
    pub fn imbalance(&self) -> Option<f64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        let bq = bid.qty.0 as f64;
        let aq = ask.qty.0 as f64;
        let denom = bq + aq;
        if denom == 0.0 {
            return None;
        }
        Some((bq - aq) / denom)
    }

    /// Snapshot the top `n` levels of each side as a [`BookSnapshot`].
    /// Bids come out descending, asks ascending.
    pub fn top_n(&self, n: usize) -> BookSnapshot {
        BookSnapshot {
            bids: self
                .bids
                .iter()
                .rev()
                .take(n)
                .map(|(p, q)| BookLevel { price: *p, qty: *q })
                .collect(),
            asks: self
                .asks
                .iter()
                .take(n)
                .map(|(p, q)| BookLevel { price: *p, qty: *q })
                .collect(),
        }
    }

    pub fn bid_depth(&self) -> usize {
        self.bids.len()
    }

    pub fn ask_depth(&self) -> usize {
        self.asks.len()
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Rewrite `last_seq` without touching the level state. Used by
    /// venue-specific alignment logic that needs to park the book on
    /// an earlier sequence so the next chained delta lands on it.
    pub fn rebase_seq(&mut self, seq: u64) {
        self.last_seq = seq;
    }
}

fn apply_level(side: &mut BTreeMap<Price, Qty>, lvl: BookLevel) {
    if lvl.qty.0 == 0 {
        side.remove(&lvl.price);
    } else {
        side.insert(lvl.price, lvl.qty);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvl(p: i64, q: i64) -> BookLevel {
        BookLevel {
            price: Price(p),
            qty: Qty(q),
        }
    }

    fn snap(bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> BookSnapshot {
        BookSnapshot { bids, asks }
    }

    #[test]
    fn apply_snapshot_sets_state() {
        let mut b = OrderBook::new();
        b.apply_snapshot(
            &snap(
                vec![lvl(100, 5), lvl(99, 2)],
                vec![lvl(101, 3), lvl(102, 7)],
            ),
            10,
        );
        assert_eq!(b.last_seq(), 10);
        assert!(b.is_initialized());
        assert_eq!(b.best_bid(), Some(lvl(100, 5)));
        assert_eq!(b.best_ask(), Some(lvl(101, 3)));
        assert_eq!(b.bid_depth(), 2);
        assert_eq!(b.ask_depth(), 2);
    }

    #[test]
    fn snapshot_drops_zero_qty_levels() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 5), lvl(99, 0)], vec![lvl(101, 0)]), 1);
        assert_eq!(b.bid_depth(), 1);
        assert_eq!(b.ask_depth(), 0);
    }

    #[test]
    fn delta_chains_and_advances_seq() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 5)], vec![lvl(101, 3)]), 10);
        let d = BookDelta {
            bids: vec![lvl(100, 7), lvl(99, 4)],
            asks: vec![lvl(101, 0), lvl(102, 6)],
            prev_seq: 10,
        };
        b.apply_delta(&d, 11).unwrap();
        assert_eq!(b.last_seq(), 11);
        assert_eq!(b.best_bid(), Some(lvl(100, 7)));
        // 101 deleted, so best ask advances to 102.
        assert_eq!(b.best_ask(), Some(lvl(102, 6)));
        assert_eq!(b.bid_depth(), 2);
        assert_eq!(b.ask_depth(), 1);
    }

    #[test]
    fn delta_without_snapshot_errors() {
        let mut b = OrderBook::new();
        let d = BookDelta {
            bids: vec![lvl(100, 1)],
            asks: vec![],
            prev_seq: 0,
        };
        assert_eq!(b.apply_delta(&d, 1).unwrap_err(), BookError::Uninitialized);
    }

    #[test]
    fn detects_gap() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 1)], vec![lvl(101, 1)]), 10);
        let d = BookDelta {
            bids: vec![lvl(100, 2)],
            asks: vec![],
            prev_seq: 12,
        };
        assert_eq!(
            b.apply_delta(&d, 13).unwrap_err(),
            BookError::Gap {
                expected: 10,
                got_prev: 12,
            }
        );
    }

    #[test]
    fn unchained_delta_skips_gap_check() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 1)], vec![lvl(101, 1)]), 10);
        // prev_seq == 0 means "venue does not chain" — apply blindly.
        let d = BookDelta {
            bids: vec![lvl(100, 2)],
            asks: vec![],
            prev_seq: 0,
        };
        b.apply_delta(&d, 20).unwrap();
        assert_eq!(b.last_seq(), 20);
        assert_eq!(b.best_bid(), Some(lvl(100, 2)));
    }

    #[test]
    fn top_n_returns_sorted_levels() {
        let mut b = OrderBook::new();
        b.apply_snapshot(
            &snap(
                vec![lvl(100, 1), lvl(99, 1), lvl(98, 1), lvl(97, 1)],
                vec![lvl(101, 1), lvl(102, 1), lvl(103, 1)],
            ),
            1,
        );
        let top = b.top_n(2);
        assert_eq!(top.bids, vec![lvl(100, 1), lvl(99, 1)]);
        assert_eq!(top.asks, vec![lvl(101, 1), lvl(102, 1)]);
    }

    #[test]
    fn spread_and_mid_round_down() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 1)], vec![lvl(105, 1)]), 1);
        assert_eq!(b.spread(), Some(5));
        // (100 + 105) / 2 == 102 (integer division toward zero).
        assert_eq!(b.mid(), Some(102));
    }

    #[test]
    fn empty_side_leaves_mid_none() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 1)], vec![]), 1);
        assert_eq!(b.mid(), None);
        assert_eq!(b.spread(), None);
    }

    #[test]
    fn microprice_is_mid_when_top_quantities_match() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 5)], vec![lvl(110, 5)]), 1);
        // Equal queues: microprice collapses to the integer mid.
        assert_eq!(b.microprice(), Some(105));
        assert_eq!(b.mid(), Some(105));
    }

    #[test]
    fn microprice_leans_toward_ask_when_bid_queue_is_heavier() {
        let mut b = OrderBook::new();
        // Bid queue 10x the ask queue → microprice pushed toward ask.
        b.apply_snapshot(&snap(vec![lvl(100, 100)], vec![lvl(110, 10)]), 1);
        let mp = b.microprice().expect("book is two-sided");
        assert!(
            mp > b.mid().unwrap(),
            "microprice {mp} should sit above mid {} under buy pressure",
            b.mid().unwrap()
        );
        // (100*10 + 110*100) / 110 = 12000/110 = 109
        assert_eq!(mp, 109);
    }

    #[test]
    fn microprice_leans_toward_bid_when_ask_queue_is_heavier() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 10)], vec![lvl(110, 100)]), 1);
        let mp = b.microprice().expect("book is two-sided");
        assert!(mp < b.mid().unwrap());
        // (100*100 + 110*10) / 110 = 11100/110 = 100
        assert_eq!(mp, 100);
    }

    #[test]
    fn microprice_is_none_when_a_side_is_empty() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 1)], vec![]), 1);
        assert_eq!(b.microprice(), None);
    }

    #[test]
    fn imbalance_is_zero_for_symmetric_top_of_book() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 7)], vec![lvl(110, 7)]), 1);
        assert_eq!(b.imbalance(), Some(0.0));
    }

    #[test]
    fn imbalance_sign_tracks_dominant_side() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 30)], vec![lvl(110, 10)]), 1);
        let imb = b.imbalance().unwrap();
        assert!(
            imb > 0.0,
            "bid-heavy book should produce positive imbalance"
        );
        assert!((imb - 0.5).abs() < 1e-9, "got {imb}");

        b.apply_snapshot(&snap(vec![lvl(100, 10)], vec![lvl(110, 30)]), 2);
        let imb = b.imbalance().unwrap();
        assert!(imb < 0.0);
        assert!((imb + 0.5).abs() < 1e-9, "got {imb}");
    }

    #[test]
    fn imbalance_saturates_at_one_when_a_side_has_trivial_size() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 1_000_000)], vec![lvl(110, 1)]), 1);
        let imb = b.imbalance().unwrap();
        assert!((-1.0..=1.0).contains(&imb));
        assert!(imb > 0.999, "near-saturated bid queue got {imb}");
    }

    #[test]
    fn imbalance_is_none_when_a_side_is_empty() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![], vec![lvl(110, 10)]), 1);
        assert_eq!(b.imbalance(), None);
    }

    #[test]
    fn rebase_seq_enables_backdated_chain() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&snap(vec![lvl(100, 1)], vec![lvl(101, 1)]), 50);
        // Venue-specific alignment path: rewind the book to 40 so the
        // next chained delta (prev_seq=40) lines up.
        b.rebase_seq(40);
        let d = BookDelta {
            bids: vec![lvl(100, 2)],
            asks: vec![],
            prev_seq: 40,
        };
        b.apply_delta(&d, 41).unwrap();
        assert_eq!(b.last_seq(), 41);
        assert_eq!(b.best_bid(), Some(lvl(100, 2)));
    }
}
