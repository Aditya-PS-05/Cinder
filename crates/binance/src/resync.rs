//! Event-stream resync for the Binance spot depth channel.
//!
//! The WS session publishes [`MarketEvent`]s onto a shared bus. For
//! downstream consumers that maintain their own `OrderBook`s, the
//! stream must arrive *snapshot-first-then-chained-deltas* so applies
//! do not gap. [`SymbolResync`] is the thin state machine that turns
//! a raw WS delta stream + a REST depth snapshot into that shape,
//! operating at the `MarketEvent` level so the same machinery works
//! for any consumer on the bus.
//!
//! Related: [`crate::book::BinanceBookSync`] implements the exact
//! same alignment protocol but at the `OrderBook` level (it owns the
//! book). Use `BinanceBookSync` when the consumer is the resync
//! itself; use `SymbolResync` when the consumer is a broadcast bus
//! with many downstream book-holders.
//!
//! ```text
//!   ws.subscribe ──┐      ┌── push_delta ──► Buffered / Ready / Resync
//!   rest.depth ────┴► apply_snapshot ──► [Snapshot, aligned deltas...]
//! ```
//!
//! Protocol (Binance spot):
//!
//! 1. Open WS and start buffering `depthUpdate` frames.
//! 2. `GET /api/v3/depth?limit=1000` returns `lastUpdateId`.
//! 3. Drop buffered events with `u <= lastUpdateId`.
//! 4. The first kept event must satisfy
//!    `U <= lastUpdateId + 1  AND  u >= lastUpdateId + 1` (a bridge).
//! 5. Subsequent events must chain: `U == prev.u + 1`.

use ts_core::{MarketEvent, MarketPayload};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlignState {
    /// No snapshot yet. Deltas are buffered.
    NeedsSnapshot,
    /// Snapshot installed and the next delta is expected to bridge.
    /// `last_u` is the snapshot's `lastUpdateId` until the bridge
    /// event arrives.
    Bridging,
    /// Bridge consumed; deltas chain off `prev.u`.
    Active,
    /// Chain broken. Caller must drop the session and reconnect.
    Lost,
}

#[derive(Debug)]
pub enum PushOutcome {
    /// Event held in the internal buffer; will be replayed by
    /// [`SymbolResync::apply_snapshot`].
    Buffered,
    /// Event passed alignment; caller should publish it.
    Ready(MarketEvent),
    /// Alignment failed. Caller must abort the session and resync
    /// from scratch.
    Resync,
}

pub struct SymbolResync {
    state: AlignState,
    last_u: u64,
    pending: Vec<MarketEvent>,
}

impl SymbolResync {
    pub fn new() -> Self {
        Self {
            state: AlignState::NeedsSnapshot,
            last_u: 0,
            pending: Vec::new(),
        }
    }

    pub fn state(&self) -> AlignState {
        self.state
    }

    pub fn last_u(&self) -> u64 {
        self.last_u
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Reset back to [`AlignState::NeedsSnapshot`]. Used after a
    /// reconnect so the next snapshot starts from a blank buffer.
    pub fn reset(&mut self) {
        self.state = AlignState::NeedsSnapshot;
        self.last_u = 0;
        self.pending.clear();
    }

    /// Feed one decoded delta [`MarketEvent`] into the resync. Non-delta
    /// events (trades, etc.) are returned immediately as `Ready` since
    /// alignment does not apply to them.
    pub fn push_delta(&mut self, evt: MarketEvent) -> PushOutcome {
        let (prev_seq, u) = match &evt.payload {
            MarketPayload::BookDelta(d) => (d.prev_seq, evt.seq),
            _ => return PushOutcome::Ready(evt),
        };

        match self.state {
            AlignState::NeedsSnapshot => {
                self.pending.push(evt);
                PushOutcome::Buffered
            }
            AlignState::Bridging => {
                // Bridge rule: prev_seq <= last_u (the snapshot's
                // lastUpdateId) and u >= last_u + 1.
                if u <= self.last_u {
                    // Pre-snapshot leftover — drop silently, stay Bridging.
                    return PushOutcome::Buffered;
                }
                if prev_seq > self.last_u {
                    self.state = AlignState::Lost;
                    return PushOutcome::Resync;
                }
                self.last_u = u;
                self.state = AlignState::Active;
                PushOutcome::Ready(evt)
            }
            AlignState::Active => {
                // Chain rule: prev_seq must equal the previous u.
                if prev_seq != self.last_u {
                    self.state = AlignState::Lost;
                    return PushOutcome::Resync;
                }
                self.last_u = u;
                PushOutcome::Ready(evt)
            }
            AlignState::Lost => PushOutcome::Resync,
        }
    }

    /// Install the REST depth snapshot. Returns the events to publish
    /// in order: the snapshot itself, followed by every buffered delta
    /// that survives the alignment filter. On alignment failure the
    /// state moves to [`AlignState::Lost`] and `Err` is returned.
    pub fn apply_snapshot(
        &mut self,
        snap_event: MarketEvent,
        snap_last_update_id: u64,
    ) -> Result<Vec<MarketEvent>, AlignError> {
        let pending = std::mem::take(&mut self.pending);
        let mut out = Vec::with_capacity(pending.len() + 1);
        out.push(snap_event);

        let mut last_u = snap_last_update_id;
        let mut bridged = false;
        for evt in pending {
            let (prev_seq, u) = match &evt.payload {
                MarketPayload::BookDelta(d) => (d.prev_seq, evt.seq),
                _ => continue,
            };
            if u <= snap_last_update_id {
                continue;
            }
            if !bridged {
                if prev_seq > snap_last_update_id {
                    self.state = AlignState::Lost;
                    return Err(AlignError {
                        detail: format!(
                            "first buffered delta U={} > lastUpdateId+1={}",
                            prev_seq + 1,
                            snap_last_update_id + 1,
                        ),
                    });
                }
                bridged = true;
            } else if prev_seq != last_u {
                self.state = AlignState::Lost;
                return Err(AlignError {
                    detail: format!(
                        "chain broken in buffered replay: expected U={} got U={}",
                        last_u + 1,
                        prev_seq + 1,
                    ),
                });
            }
            last_u = u;
            out.push(evt);
        }

        self.last_u = last_u;
        self.state = if bridged {
            AlignState::Active
        } else {
            AlignState::Bridging
        };
        Ok(out)
    }
}

impl Default for SymbolResync {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct AlignError {
    pub detail: String,
}

impl std::fmt::Display for AlignError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "alignment failed: {}", self.detail)
    }
}

impl std::error::Error for AlignError {}

impl From<AlignError> for crate::error::BinanceError {
    fn from(e: AlignError) -> Self {
        crate::error::BinanceError::Align { detail: e.detail }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{
        BookDelta, BookLevel, BookSnapshot, MarketEvent, MarketPayload, Price, Qty, Symbol,
        Timestamp, Venue,
    };

    fn sym() -> Symbol {
        Symbol::from_static("BTCUSDT")
    }

    fn delta_event(first_id: u64, last_id: u64, bid_qty: i64) -> MarketEvent {
        MarketEvent {
            venue: Venue::BINANCE,
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq: last_id,
            payload: MarketPayload::BookDelta(BookDelta {
                bids: vec![BookLevel {
                    price: Price(100),
                    qty: Qty(bid_qty),
                }],
                asks: vec![],
                prev_seq: first_id.saturating_sub(1),
            }),
        }
    }

    fn snapshot_event(last_update_id: u64) -> MarketEvent {
        MarketEvent {
            venue: Venue::BINANCE,
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq: last_update_id,
            payload: MarketPayload::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(100),
                    qty: Qty(5),
                }],
                asks: vec![BookLevel {
                    price: Price(101),
                    qty: Qty(3),
                }],
            }),
        }
    }

    #[test]
    fn new_state_buffers_every_delta() {
        let mut s = SymbolResync::new();
        assert_eq!(s.state(), AlignState::NeedsSnapshot);
        match s.push_delta(delta_event(5, 10, 6)) {
            PushOutcome::Buffered => {}
            other => panic!("expected Buffered, got {:?}", other),
        }
        assert_eq!(s.pending_len(), 1);
    }

    #[test]
    fn trade_event_passes_through_even_while_needs_snapshot() {
        use ts_core::Trade;
        let mut s = SymbolResync::new();
        let trade = MarketEvent {
            venue: Venue::BINANCE,
            symbol: sym(),
            exchange_ts: Timestamp::default(),
            local_ts: Timestamp::default(),
            seq: 1,
            payload: MarketPayload::Trade(Trade {
                id: "1".into(),
                price: Price(100),
                qty: Qty(1),
                taker_side: ts_core::Side::Buy,
            }),
        };
        match s.push_delta(trade) {
            PushOutcome::Ready(_) => {}
            other => panic!("trades must bypass alignment, got {:?}", other),
        }
    }

    #[test]
    fn apply_snapshot_drops_stale_and_aligns_bridge() {
        let mut s = SymbolResync::new();
        s.push_delta(delta_event(1, 15, 9)); // u=15 <= snap(20) → dropped
        s.push_delta(delta_event(18, 25, 7)); // bridge: U=18 <= 21, u=25 >= 21
        s.push_delta(delta_event(26, 30, 4)); // chains off 25
        assert_eq!(s.pending_len(), 3);

        let out = s.apply_snapshot(snapshot_event(20), 20).unwrap();
        // snapshot + 2 aligned deltas (pre-snapshot one dropped).
        assert_eq!(out.len(), 3);
        match &out[0].payload {
            MarketPayload::BookSnapshot(_) => {}
            _ => panic!("first must be snapshot"),
        }
        assert_eq!(out[1].seq, 25);
        assert_eq!(out[2].seq, 30);
        assert_eq!(s.state(), AlignState::Active);
        assert_eq!(s.last_u(), 30);
    }

    #[test]
    fn apply_snapshot_with_empty_pending_enters_bridging() {
        let mut s = SymbolResync::new();
        let out = s.apply_snapshot(snapshot_event(20), 20).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(s.state(), AlignState::Bridging);
        assert_eq!(s.last_u(), 20);
    }

    #[test]
    fn bridging_accepts_bridge_event_and_transitions_active() {
        let mut s = SymbolResync::new();
        s.apply_snapshot(snapshot_event(20), 20).unwrap();
        // Bridge: U=18 (prev_seq=17), u=25.
        match s.push_delta(delta_event(18, 25, 7)) {
            PushOutcome::Ready(evt) => assert_eq!(evt.seq, 25),
            other => panic!("expected Ready bridge, got {:?}", other),
        }
        assert_eq!(s.state(), AlignState::Active);
        assert_eq!(s.last_u(), 25);
    }

    #[test]
    fn bridging_drops_strictly_pre_snapshot_events() {
        let mut s = SymbolResync::new();
        s.apply_snapshot(snapshot_event(20), 20).unwrap();
        // u=15 <= 20 → buffered (treated as drop; state stays Bridging).
        match s.push_delta(delta_event(10, 15, 1)) {
            PushOutcome::Buffered => {}
            other => panic!("expected Buffered (dropped), got {:?}", other),
        }
        assert_eq!(s.state(), AlignState::Bridging);
    }

    #[test]
    fn bridging_rejects_post_snapshot_gap() {
        let mut s = SymbolResync::new();
        s.apply_snapshot(snapshot_event(20), 20).unwrap();
        // U=30 (prev_seq=29) > lastUpdateId+1=21 → gap.
        match s.push_delta(delta_event(30, 35, 1)) {
            PushOutcome::Resync => {}
            other => panic!("expected Resync, got {:?}", other),
        }
        assert_eq!(s.state(), AlignState::Lost);
    }

    #[test]
    fn active_chains_then_flags_gap() {
        let mut s = SymbolResync::new();
        s.push_delta(delta_event(18, 25, 7));
        s.apply_snapshot(snapshot_event(20), 20).unwrap();
        // Active now, last_u=25. Next: U=26 OK.
        match s.push_delta(delta_event(26, 30, 2)) {
            PushOutcome::Ready(_) => {}
            other => panic!("expected Ready, got {:?}", other),
        }
        // Gap: U=40, last_u=30 → expected U=31.
        match s.push_delta(delta_event(40, 50, 2)) {
            PushOutcome::Resync => {}
            other => panic!("expected Resync, got {:?}", other),
        }
        assert_eq!(s.state(), AlignState::Lost);
    }

    #[test]
    fn alignment_fails_when_bridge_missing_in_buffer() {
        let mut s = SymbolResync::new();
        // Only buffered delta starts at U=25, but lastUpdateId=20 → no bridge.
        s.push_delta(delta_event(25, 30, 1));
        let err = s.apply_snapshot(snapshot_event(20), 20).unwrap_err();
        assert!(err.detail.contains("first buffered delta"));
        assert_eq!(s.state(), AlignState::Lost);
    }

    #[test]
    fn buffered_replay_chain_break_errors() {
        let mut s = SymbolResync::new();
        s.push_delta(delta_event(18, 25, 7)); // bridge
        s.push_delta(delta_event(40, 50, 9)); // gap after bridge
        let err = s.apply_snapshot(snapshot_event(20), 20).unwrap_err();
        assert!(err.detail.contains("chain broken"));
        assert_eq!(s.state(), AlignState::Lost);
    }

    #[test]
    fn lost_state_remains_lost_until_reset() {
        let mut s = SymbolResync::new();
        s.push_delta(delta_event(25, 30, 1));
        let _ = s.apply_snapshot(snapshot_event(20), 20).unwrap_err();
        assert_eq!(s.state(), AlignState::Lost);
        matches!(s.push_delta(delta_event(31, 40, 1)), PushOutcome::Resync);
        s.reset();
        assert_eq!(s.state(), AlignState::NeedsSnapshot);
        matches!(s.push_delta(delta_event(1, 5, 1)), PushOutcome::Buffered);
    }
}
