//! Binance spot depth-stream resync state machine.
//!
//! Binance publishes the alignment protocol for its spot depth stream
//! as: buffer stream events, fetch `/api/v3/depth?limit=1000`, drop any
//! buffered event whose `u <= lastUpdateId`, then apply the first event
//! whose `U <= lastUpdateId + 1 <= u` as the bridge and chain every
//! subsequent event off `prev_u + 1`.
//!
//! [`BinanceBookSync`] implements exactly that on top of the generic
//! [`ts_book::OrderBook`]. It does not fetch anything itself — the
//! caller hands it the snapshot via [`Self::apply_snapshot`] whenever
//! it is ready. Gaps mid-stream transition the state machine to
//! [`SyncState::Lost`] and the caller is expected to [`Self::reset`]
//! and refetch.

use ts_book::{BookError, OrderBook};
use ts_core::{BookDelta, BookLevel, BookSnapshot};

use crate::error::BinanceError;
use crate::rest::DepthSnapshot;

pub struct BinanceBookSync {
    book: OrderBook,
    state: SyncState,
    pending: Vec<Staged>,
}

struct Staged {
    seq: u64,
    delta: BookDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// No snapshot yet. Incoming deltas are buffered.
    NeedsSnapshot,
    /// Snapshot installed and stream is chaining.
    Active,
    /// Chain broke. Caller must drain, [`BinanceBookSync::reset`], and
    /// refetch the snapshot.
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookUpdate {
    /// Stored in the pending buffer; waiting for a snapshot.
    Buffered,
    /// Applied to the live book.
    Applied,
    /// Chain broke (or caller pushed while [`SyncState::Lost`]); resync
    /// required.
    Resync,
}

impl BinanceBookSync {
    pub fn new() -> Self {
        Self {
            book: OrderBook::new(),
            state: SyncState::NeedsSnapshot,
            pending: Vec::new(),
        }
    }

    pub fn state(&self) -> SyncState {
        self.state
    }

    /// Drop the book and any pending deltas. Puts the sync back into
    /// [`SyncState::NeedsSnapshot`].
    pub fn reset(&mut self) {
        self.book = OrderBook::new();
        self.state = SyncState::NeedsSnapshot;
        self.pending.clear();
    }

    pub fn best_bid(&self) -> Option<BookLevel> {
        self.book.best_bid()
    }

    pub fn best_ask(&self) -> Option<BookLevel> {
        self.book.best_ask()
    }

    pub fn top_n(&self, n: usize) -> BookSnapshot {
        self.book.top_n(n)
    }

    pub fn last_seq(&self) -> u64 {
        self.book.last_seq()
    }

    /// Feed a freshly decoded depth delta into the state machine. `seq`
    /// is the frame's `last_update_id`; `delta.prev_seq` is
    /// `first_update_id - 1` so the chain check in [`OrderBook`] works
    /// once we are [`SyncState::Active`].
    pub fn push_delta(&mut self, seq: u64, delta: BookDelta) -> Result<BookUpdate, BinanceError> {
        match self.state {
            SyncState::NeedsSnapshot => {
                self.pending.push(Staged { seq, delta });
                Ok(BookUpdate::Buffered)
            }
            SyncState::Active => match self.book.apply_delta(&delta, seq) {
                Ok(()) => Ok(BookUpdate::Applied),
                Err(BookError::Gap { .. }) => {
                    self.state = SyncState::Lost;
                    Ok(BookUpdate::Resync)
                }
                Err(e) => Err(BinanceError::Book(e)),
            },
            SyncState::Lost => Ok(BookUpdate::Resync),
        }
    }

    /// Install a REST snapshot and replay any buffered deltas that land
    /// after it. Returns the number of buffered deltas that were
    /// successfully applied. On an alignment failure the state moves to
    /// [`SyncState::Lost`] and [`BinanceError::Align`] is returned.
    pub fn apply_snapshot(&mut self, snap: DepthSnapshot) -> Result<usize, BinanceError> {
        let DepthSnapshot {
            last_update_id,
            snapshot,
        } = snap;
        self.book.apply_snapshot(&snapshot, last_update_id);

        let pending = std::mem::take(&mut self.pending);
        let mut applied = 0usize;
        let mut started = false;

        for Staged { seq, delta } in pending {
            // Step 3: drop events entirely before the snapshot.
            if seq <= last_update_id {
                continue;
            }
            if !started {
                // Step 4: the bridge event must satisfy
                //   first_update_id <= lastUpdateId + 1
                // which in our parameterization is
                //   prev_seq + 1 <= lastUpdateId + 1
                //   prev_seq <= lastUpdateId
                if delta.prev_seq > last_update_id {
                    self.state = SyncState::Lost;
                    return Err(BinanceError::Align {
                        detail: format!(
                            "first buffered delta prev_seq {} > lastUpdateId {}",
                            delta.prev_seq, last_update_id,
                        ),
                    });
                }
                // Park the book on `prev_seq` so the first chained apply
                // matches exactly.
                self.book.rebase_seq(delta.prev_seq);
                started = true;
            }
            match self.book.apply_delta(&delta, seq) {
                Ok(()) => applied += 1,
                Err(BookError::Gap { expected, got_prev }) => {
                    self.state = SyncState::Lost;
                    return Err(BinanceError::Align {
                        detail: format!(
                            "chain broken at pending replay: book={expected}, delta prev_seq={got_prev}"
                        ),
                    });
                }
                Err(e) => return Err(BinanceError::Book(e)),
            }
        }

        self.state = SyncState::Active;
        Ok(applied)
    }
}

impl Default for BinanceBookSync {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{BookLevel, BookSnapshot, Price, Qty};

    fn lvl(p: i64, q: i64) -> BookLevel {
        BookLevel {
            price: Price(p),
            qty: Qty(q),
        }
    }

    fn base_snap(last_update_id: u64) -> DepthSnapshot {
        DepthSnapshot {
            last_update_id,
            snapshot: BookSnapshot {
                bids: vec![lvl(100, 5)],
                asks: vec![lvl(101, 3)],
            },
        }
    }

    /// Build a (seq, BookDelta) from Binance-style (U, u) update ids.
    fn stream(first_id: u64, last_id: u64, bids: Vec<BookLevel>) -> (u64, BookDelta) {
        (
            last_id,
            BookDelta {
                bids,
                asks: vec![],
                prev_seq: first_id.saturating_sub(1),
            },
        )
    }

    #[test]
    fn buffers_before_snapshot() {
        let mut s = BinanceBookSync::new();
        let (seq, d) = stream(5, 10, vec![lvl(100, 6)]);
        assert_eq!(s.push_delta(seq, d).unwrap(), BookUpdate::Buffered);
        assert_eq!(s.state(), SyncState::NeedsSnapshot);
    }

    #[test]
    fn aligns_snapshot_and_replays_pending() {
        let mut s = BinanceBookSync::new();
        // Entirely pre-snapshot (u=15 <= lastUpdateId=20). Should be dropped.
        let (seq1, d1) = stream(1, 15, vec![lvl(100, 9)]);
        // Bridge event: U=18, u=25 — crosses the snapshot at 20.
        let (seq2, d2) = stream(18, 25, vec![lvl(100, 7)]);
        // Chains off the bridge: U=26, u=30.
        let (seq3, d3) = stream(26, 30, vec![lvl(99, 4)]);
        s.push_delta(seq1, d1).unwrap();
        s.push_delta(seq2, d2).unwrap();
        s.push_delta(seq3, d3).unwrap();

        let applied = s.apply_snapshot(base_snap(20)).unwrap();
        assert_eq!(applied, 2); // pre-snapshot event skipped
        assert_eq!(s.state(), SyncState::Active);
        assert_eq!(s.best_bid(), Some(lvl(100, 7)));
        assert_eq!(s.last_seq(), 30);
    }

    #[test]
    fn live_delta_after_alignment_applies() {
        let mut s = BinanceBookSync::new();
        let (seq, d) = stream(18, 25, vec![lvl(100, 7)]);
        s.push_delta(seq, d).unwrap();
        s.apply_snapshot(base_snap(20)).unwrap();

        let (seq2, d2) = stream(26, 30, vec![lvl(100, 8)]);
        assert_eq!(s.push_delta(seq2, d2).unwrap(), BookUpdate::Applied);
        assert_eq!(s.best_bid(), Some(lvl(100, 8)));
    }

    #[test]
    fn alignment_gap_sets_lost() {
        let mut s = BinanceBookSync::new();
        // Only buffered delta starts at U=25, but lastUpdateId=20 means
        // the bridge event is missing.
        let (seq, d) = stream(25, 30, vec![lvl(100, 9)]);
        s.push_delta(seq, d).unwrap();
        let err = s.apply_snapshot(base_snap(20)).unwrap_err();
        assert!(matches!(err, BinanceError::Align { .. }));
        assert_eq!(s.state(), SyncState::Lost);
    }

    #[test]
    fn mid_stream_gap_transitions_to_lost() {
        let mut s = BinanceBookSync::new();
        let (seq, d) = stream(18, 25, vec![lvl(100, 7)]);
        s.push_delta(seq, d).unwrap();
        s.apply_snapshot(base_snap(20)).unwrap();
        assert_eq!(s.state(), SyncState::Active);

        // prev_seq = 98, but book is at 25 → gap.
        let (seq2, d2) = stream(99, 100, vec![lvl(100, 3)]);
        assert_eq!(s.push_delta(seq2, d2).unwrap(), BookUpdate::Resync);
        assert_eq!(s.state(), SyncState::Lost);
        // Further pushes while Lost keep asking for resync.
        let (seq3, d3) = stream(101, 102, vec![lvl(100, 1)]);
        assert_eq!(s.push_delta(seq3, d3).unwrap(), BookUpdate::Resync);
    }

    #[test]
    fn reset_returns_to_needs_snapshot() {
        let mut s = BinanceBookSync::new();
        let (seq, d) = stream(18, 25, vec![lvl(100, 7)]);
        s.push_delta(seq, d).unwrap();
        s.apply_snapshot(base_snap(20)).unwrap();
        s.reset();
        assert_eq!(s.state(), SyncState::NeedsSnapshot);
        assert_eq!(s.best_bid(), None);
    }
}
