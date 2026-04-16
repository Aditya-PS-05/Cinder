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
