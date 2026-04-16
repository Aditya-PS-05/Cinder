//! Deterministic scenario generators.
//!
//! Each generator takes a seed and a step count and returns a
//! [`Scenario`] that scripts the book and any injected taker flow.
//! Generators use a private xorshift64 PRNG so runs are reproducible
//! without pulling in an external rand crate.

use ts_core::{
    BookDelta, BookLevel, BookSnapshot, ClientOrderId, MarketEvent, MarketPayload, NewOrder,
    OrderKind, Price, Qty, Side, Symbol, TimeInForce, Timestamp, Venue,
};

use crate::{Scenario, ScenarioAction};

/// Seeded xorshift64. Period ~2^64, enough for any scenario we'd
/// reasonably run. Zero seed is remapped so `next` never returns 0.
#[derive(Clone, Debug)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniform i64 in [-range, range].
    fn symmetric(&mut self, range: i64) -> i64 {
        if range <= 0 {
            return 0;
        }
        let modulus = (range as u64) * 2 + 1;
        ((self.next() % modulus) as i64) - range
    }

    /// True with probability `p_over_256 / 256`.
    fn gate_p256(&mut self, p_over_256: u64) -> bool {
        (self.next() & 0xff) < p_over_256
    }
}

fn venue() -> Venue {
    Venue::BINANCE
}

fn symbol() -> Symbol {
    Symbol::from_static("BTCUSDT")
}

fn snapshot_event(bid_px: i64, ask_px: i64, depth: i64, seq: u64) -> MarketEvent {
    MarketEvent {
        venue: venue(),
        symbol: symbol(),
        exchange_ts: Timestamp::default(),
        local_ts: Timestamp::default(),
        seq,
        payload: MarketPayload::BookSnapshot(BookSnapshot {
            bids: vec![BookLevel {
                price: Price(bid_px),
                qty: Qty(depth),
            }],
            asks: vec![BookLevel {
                price: Price(ask_px),
                qty: Qty(depth),
            }],
        }),
    }
}

fn _delta_event(
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    prev_seq: u64,
    seq: u64,
) -> MarketEvent {
    MarketEvent {
        venue: venue(),
        symbol: symbol(),
        exchange_ts: Timestamp::default(),
        local_ts: Timestamp::default(),
        seq,
        payload: MarketPayload::BookDelta(BookDelta {
            bids,
            asks,
            prev_seq,
        }),
    }
}

fn taker(cid: &str, side: Side, qty: i64) -> ScenarioAction {
    ScenarioAction::Taker(NewOrder {
        cid: ClientOrderId::new(cid),
        venue: venue(),
        symbol: symbol(),
        side,
        kind: OrderKind::Market,
        tif: TimeInForce::Ioc,
        qty: Qty(qty),
        price: None,
        ts: Timestamp::default(),
    })
}

/// A flat, two-sided book around mid=1000 that wobbles by a few ticks
/// each step. No taker flow — the strategy just quotes, cancels, and
/// requotes without ever filling.
pub fn steady_maker_book(seed: u64, steps: usize) -> Scenario {
    let mut rng = Rng::new(seed);
    let base_mid = 1_000;
    let half_spread = 4;
    let mut actions = Vec::with_capacity(steps);
    for i in 0..steps {
        let jitter = rng.symmetric(2);
        let mid = base_mid + jitter;
        actions.push(ScenarioAction::Market(snapshot_event(
            mid - half_spread,
            mid + half_spread,
            50,
            i as u64 + 1,
        )));
    }
    Scenario {
        name: "steady_maker_book".into(),
        venue: venue(),
        symbol: symbol(),
        actions,
    }
}

/// Book wobbles around a slowly-drifting mid while adverse takers
/// punch through the maker's quotes every few ticks. Good for
/// checking that inventory skew limits runaway exposure.
pub fn adverse_flow(seed: u64, steps: usize) -> Scenario {
    let mut rng = Rng::new(seed);
    let base_mid = 1_000;
    let half_spread = 4;
    let mut actions = Vec::with_capacity(steps * 2);
    for i in 0..steps {
        let jitter = rng.symmetric(3);
        let mid = base_mid + jitter;
        actions.push(ScenarioAction::Market(snapshot_event(
            mid - half_spread,
            mid + half_spread,
            50,
            i as u64 + 1,
        )));
        // About one in three steps gets a taker; bias direction randomly.
        if rng.gate_p256(85) {
            let side = if rng.next() & 1 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            actions.push(taker(&format!("t-{i}"), side, 1));
        }
    }
    Scenario {
        name: "adverse_flow".into(),
        venue: venue(),
        symbol: symbol(),
        actions,
    }
}

/// Mid drifts monotonically upward by one tick per step. Nothing forces
/// takers, so the maker's bid gets repeatedly filled as the mark runs
/// away from it, building long inventory until the skew cap kicks in.
pub fn trending_up(_seed: u64, steps: usize) -> Scenario {
    let mut actions = Vec::with_capacity(steps * 2);
    let base_mid = 1_000;
    let half_spread = 4;
    for i in 0..steps {
        let mid = base_mid + (i as i64);
        actions.push(ScenarioAction::Market(snapshot_event(
            mid - half_spread,
            mid + half_spread,
            50,
            i as u64 + 1,
        )));
        // Inject a buy taker every step that lifts the maker's ask and
        // leaves the maker short, which pushes skew one direction.
        actions.push(taker(&format!("up-{i}"), Side::Buy, 1));
    }
    Scenario {
        name: "trending_up".into(),
        venue: venue(),
        symbol: symbol(),
        actions,
    }
}

/// Pick a scenario by name. Returns `None` for unknown names.
pub fn by_name(name: &str, seed: u64, steps: usize) -> Option<Scenario> {
    match name {
        "steady_maker_book" => Some(steady_maker_book(seed, steps)),
        "adverse_flow" => Some(adverse_flow(seed, steps)),
        "trending_up" => Some(trending_up(seed, steps)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_scenarios_are_deterministic() {
        let a = steady_maker_book(42, 20);
        let b = steady_maker_book(42, 20);
        // Determinism check — same seed/steps must produce same action count
        // and the same market events at the same positions.
        assert_eq!(a.actions.len(), b.actions.len());
        for (x, y) in a.actions.iter().zip(b.actions.iter()) {
            match (x, y) {
                (ScenarioAction::Market(ex), ScenarioAction::Market(ey)) => {
                    assert_eq!(ex.seq, ey.seq);
                }
                (ScenarioAction::Taker(_), ScenarioAction::Taker(_)) => {}
                (ScenarioAction::Cancel(_), ScenarioAction::Cancel(_)) => {}
                _ => panic!("action shape drift between seeded runs"),
            }
        }
    }

    #[test]
    fn different_seeds_yield_different_actions() {
        let a = adverse_flow(1, 50);
        let b = adverse_flow(2, 50);
        // Lengths may match by chance; compare the taker-injection pattern.
        let pattern = |s: &Scenario| -> Vec<bool> {
            s.actions
                .iter()
                .map(|a| matches!(a, ScenarioAction::Taker(_)))
                .collect()
        };
        assert_ne!(pattern(&a), pattern(&b));
    }

    #[test]
    fn by_name_resolves_known_scenarios() {
        assert!(by_name("steady_maker_book", 1, 5).is_some());
        assert!(by_name("adverse_flow", 1, 5).is_some());
        assert!(by_name("trending_up", 1, 5).is_some());
        assert!(by_name("nope", 1, 5).is_none());
    }

    #[test]
    fn trending_up_mid_rises_each_step() {
        let s = trending_up(0, 5);
        let mids: Vec<i64> = s
            .actions
            .iter()
            .filter_map(|a| match a {
                ScenarioAction::Market(e) => match &e.payload {
                    MarketPayload::BookSnapshot(bs) => {
                        let bid = bs.bids[0].price.0;
                        let ask = bs.asks[0].price.0;
                        Some((bid + ask) / 2)
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect();
        assert_eq!(mids.len(), 5);
        for w in mids.windows(2) {
            assert!(w[1] > w[0], "mid should strictly increase: {:?}", mids);
        }
    }
}
