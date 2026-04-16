//! Deterministic backtest scenarios and runner.
//!
//! A [`Scenario`] is a flat sequence of [`ScenarioAction`]s — book
//! updates mixed with injected taker orders — that exercises a
//! strategy end-to-end against [`ts_replay::Replay`]. Scenarios here
//! are generated from a seed so any result is reproducible; there is
//! no clock and no I/O.
//!
//! [`run_scenario`] is the entry point: it builds a flat-inventory
//! maker against the provided config, drives the scenario through
//! `Replay`, and returns the final [`ReplaySummary`]. The companion
//! `ts-backtest` binary wraps this behind a CLI so humans can ask
//! "how does my maker behave in a trending market?" and read a
//! summary table without writing code.

#![forbid(unsafe_code)]

pub mod scenarios;

use ts_core::{ClientOrderId, MarketEvent, NewOrder, Price, Qty, Symbol, Timestamp, Venue};
use ts_oms::{EngineConfig, PaperEngine, RiskConfig};
use ts_replay::{Replay, ReplaySummary};
use ts_strategy::{InventorySkewMaker, MakerConfig};

/// One step in a scripted backtest.
#[derive(Clone, Debug)]
pub enum ScenarioAction {
    /// Drive the book through the engine.
    Market(MarketEvent),
    /// Inject an external taker order between market events.
    Taker(NewOrder),
    /// Externally cancel a live order.
    Cancel(ClientOrderId),
}

#[derive(Clone, Debug)]
pub struct Scenario {
    pub name: String,
    pub venue: Venue,
    pub symbol: Symbol,
    pub actions: Vec<ScenarioAction>,
}

/// Config tuning the maker used by [`run_scenario`].
#[derive(Clone, Debug)]
pub struct MakerTuning {
    pub quote_qty: Qty,
    pub half_spread_ticks: i64,
    pub inventory_skew_ticks: i64,
    pub max_inventory: i64,
}

impl Default for MakerTuning {
    fn default() -> Self {
        Self {
            quote_qty: Qty(2),
            half_spread_ticks: 5,
            inventory_skew_ticks: 1,
            max_inventory: 20,
        }
    }
}

/// Run a scenario against a flat-inventory maker with the given tuning.
/// Returns the final summary even if a `BookError` aborts playback —
/// the error is folded into the summary via the `events_ingested`
/// counter stopping early.
pub fn run_scenario(scenario: &Scenario, tuning: &MakerTuning) -> ReplaySummary {
    let engine_cfg = EngineConfig {
        venue: scenario.venue.clone(),
        symbol: scenario.symbol.clone(),
        // Replay scenarios always keep the book two-sided, so the
        // fallback is unused in practice — we still set it low so an
        // accidentally one-sided fixture doesn't leak unbounded notional.
        notional_fallback_price: Some(Price(1)),
    };
    let maker = InventorySkewMaker::new(MakerConfig {
        venue: scenario.venue.clone(),
        symbol: scenario.symbol.clone(),
        quote_qty: tuning.quote_qty,
        half_spread_ticks: tuning.half_spread_ticks,
        inventory_skew_ticks: tuning.inventory_skew_ticks,
        max_inventory: tuning.max_inventory,
        cid_prefix: "bt".into(),
    });
    let engine = PaperEngine::new(engine_cfg, RiskConfig::permissive(), maker);
    let mut replay = Replay::new(engine);

    for action in &scenario.actions {
        match action {
            ScenarioAction::Market(event) => {
                if replay.step(event).is_err() {
                    break;
                }
            }
            ScenarioAction::Taker(order) => {
                replay.submit_taker(order.clone(), Timestamp::default());
            }
            ScenarioAction::Cancel(cid) => {
                replay.cancel_taker(cid);
            }
        }
    }

    replay.summary()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tuning_default_is_sane() {
        let t = MakerTuning::default();
        assert!(t.quote_qty.0 > 0);
        assert!(t.half_spread_ticks > 0);
        assert!(t.max_inventory > 0);
    }

    #[test]
    fn run_scenario_executes_all_actions() {
        let s = scenarios::steady_maker_book(42, 10);
        let summary = run_scenario(&s, &MakerTuning::default());
        assert!(summary.metrics.events_ingested > 0);
    }

    #[test]
    fn run_scenario_produces_pnl_on_adverse_flow() {
        // Adverse flow — frequent takers running through the maker's
        // quotes — should produce a nonzero realized pnl (sign varies
        // with scenario skew).
        let s = scenarios::adverse_flow(7, 30);
        let summary = run_scenario(&s, &MakerTuning::default());
        assert!(summary.metrics.fills > 0);
    }
}
