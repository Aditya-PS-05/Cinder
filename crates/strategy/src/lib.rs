//! Strategy interface and reference implementations.
//!
//! A [`Strategy`] observes book updates and its own fills and emits
//! a stream of [`StrategyAction`]s (place/cancel). Strategies are
//! object-safe so an orchestrator can own a `Box<dyn Strategy>` and
//! swap implementations behind a config flag.
//!
//! Strategies do not submit orders themselves. They allocate client
//! order ids and describe the desired order flow; a surrounding
//! trading loop is responsible for running each action through risk
//! and the executor, then feeding fills and exec reports back in.

#![forbid(unsafe_code)]

pub mod maker;

pub use maker::{InventorySkewMaker, MakerConfig};

use ts_book::OrderBook;
use ts_core::{ClientOrderId, ExecReport, Fill, NewOrder, OrderStatus, Timestamp};

/// An action the strategy wants the orchestrator to perform.
#[derive(Clone, Debug)]
pub enum StrategyAction {
    Place(NewOrder),
    Cancel(ClientOrderId),
}

/// Object-safe strategy contract.
///
/// The orchestrator invokes [`Self::on_book_update`] every time the
/// local book changes and drains the returned actions. [`Self::on_fill`]
/// and [`Self::on_exec_report`] propagate execution feedback so the
/// strategy can track inventory and clear references to orders that
/// have reached a terminal state.
pub trait Strategy: Send {
    fn on_book_update(&mut self, now: Timestamp, book: &OrderBook) -> Vec<StrategyAction>;

    fn on_fill(&mut self, fill: &Fill);

    fn on_exec_report(&mut self, _report: &ExecReport) {}
}

/// `true` when an [`OrderStatus`] will not produce further fills.
pub fn is_terminal(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Expired
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_classification() {
        assert!(is_terminal(OrderStatus::Filled));
        assert!(is_terminal(OrderStatus::Canceled));
        assert!(is_terminal(OrderStatus::Rejected));
        assert!(is_terminal(OrderStatus::Expired));
        assert!(!is_terminal(OrderStatus::New));
        assert!(!is_terminal(OrderStatus::PartiallyFilled));
    }
}
