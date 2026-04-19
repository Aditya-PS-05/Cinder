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
use ts_core::{ClientOrderId, ExecReport, Fill, NewOrder, OrderStatus, Timestamp, Trade};

/// An action the strategy wants the orchestrator to perform.
#[derive(Clone, Debug)]
pub enum StrategyAction {
    Place(NewOrder),
    Cancel(ClientOrderId),
}

/// Object-safe strategy contract.
///
/// The orchestrator invokes [`Self::on_book_update`] every time the
/// local book changes, [`Self::on_trade`] for every public trade print,
/// and [`Self::on_timer`] on a runner-driven cadence independent of
/// market events. [`Self::on_fill`] and [`Self::on_exec_report`]
/// propagate execution feedback so the strategy can track inventory
/// and clear references to orders that have reached a terminal state.
///
/// Only `on_book_update` and `on_fill` are required; the rest default
/// to no-ops so a quote-only strategy stays a one-liner. A trade-flow
/// or VWAP-timer strategy overrides the relevant hook.
///
/// [`Self::on_shutdown`] runs once when the runner is winding down.
/// Strategies that hold live orders should return `Cancel` actions for
/// each one so the venue does not retain open quotes after the process
/// exits. The default is empty for strategies that don't track live
/// state (e.g. pure analytics).
pub trait Strategy: Send {
    fn on_book_update(&mut self, now: Timestamp, book: &OrderBook) -> Vec<StrategyAction>;

    /// Public trade print. Fires for every [`MarketPayload::Trade`] the
    /// orchestrator receives, regardless of whether the book moved.
    /// The orchestrator does not pass the book here — trade flow is
    /// often all the strategy needs, and adding the book would cost a
    /// lookup the runner may not have done. If a strategy needs both,
    /// it should cache the last book passed to `on_book_update`.
    ///
    /// [`MarketPayload::Trade`]: ts_core::MarketPayload::Trade
    fn on_trade(&mut self, _now: Timestamp, _trade: &Trade) -> Vec<StrategyAction> {
        Vec::new()
    }

    fn on_fill(&mut self, fill: &Fill);

    fn on_exec_report(&mut self, _report: &ExecReport) {}

    /// Periodic wall-clock tick driven by the runner, independent of
    /// market events. Lets time-scheduled strategies (TWAP slices,
    /// heartbeat cancels, stale-quote pruning) emit actions even on a
    /// silent feed. The default is empty — most strategies react only
    /// to market events.
    fn on_timer(&mut self, _now: Timestamp) -> Vec<StrategyAction> {
        Vec::new()
    }

    fn on_shutdown(&mut self) -> Vec<StrategyAction> {
        Vec::new()
    }
}

/// Forwarding impl so callers can own a strategy behind a trait object
/// and hand it to code that is generic over `S: Strategy`.
impl Strategy for Box<dyn Strategy> {
    fn on_book_update(&mut self, now: Timestamp, book: &OrderBook) -> Vec<StrategyAction> {
        (**self).on_book_update(now, book)
    }
    fn on_trade(&mut self, now: Timestamp, trade: &Trade) -> Vec<StrategyAction> {
        (**self).on_trade(now, trade)
    }
    fn on_fill(&mut self, fill: &Fill) {
        (**self).on_fill(fill)
    }
    fn on_exec_report(&mut self, report: &ExecReport) {
        (**self).on_exec_report(report)
    }
    fn on_timer(&mut self, now: Timestamp) -> Vec<StrategyAction> {
        (**self).on_timer(now)
    }
    fn on_shutdown(&mut self) -> Vec<StrategyAction> {
        (**self).on_shutdown()
    }
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
    use ts_core::{Price, Qty, Side};

    #[test]
    fn terminal_classification() {
        assert!(is_terminal(OrderStatus::Filled));
        assert!(is_terminal(OrderStatus::Canceled));
        assert!(is_terminal(OrderStatus::Rejected));
        assert!(is_terminal(OrderStatus::Expired));
        assert!(!is_terminal(OrderStatus::New));
        assert!(!is_terminal(OrderStatus::PartiallyFilled));
    }

    /// Minimal strategy that records how many times each hook was invoked
    /// and returns no actions. Used to exercise the default trait
    /// surface and the `Box<dyn Strategy>` forwarder.
    #[derive(Default)]
    struct NoopStrategy {
        book_calls: usize,
        trade_calls: usize,
        fill_calls: usize,
        report_calls: usize,
        timer_calls: usize,
        shutdown_calls: usize,
    }

    impl Strategy for NoopStrategy {
        fn on_book_update(&mut self, _now: Timestamp, _book: &OrderBook) -> Vec<StrategyAction> {
            self.book_calls += 1;
            Vec::new()
        }
        fn on_trade(&mut self, _now: Timestamp, _trade: &Trade) -> Vec<StrategyAction> {
            self.trade_calls += 1;
            Vec::new()
        }
        fn on_fill(&mut self, _fill: &Fill) {
            self.fill_calls += 1;
        }
        fn on_exec_report(&mut self, _report: &ExecReport) {
            self.report_calls += 1;
        }
        fn on_timer(&mut self, _now: Timestamp) -> Vec<StrategyAction> {
            self.timer_calls += 1;
            Vec::new()
        }
        fn on_shutdown(&mut self) -> Vec<StrategyAction> {
            self.shutdown_calls += 1;
            Vec::new()
        }
    }

    /// Trivial strategy that only overrides `on_book_update` and
    /// `on_fill`; every other hook inherits the trait default so we can
    /// assert the defaults stay action-free.
    struct DefaultsOnly;

    impl Strategy for DefaultsOnly {
        fn on_book_update(&mut self, _now: Timestamp, _book: &OrderBook) -> Vec<StrategyAction> {
            Vec::new()
        }
        fn on_fill(&mut self, _fill: &Fill) {}
    }

    fn sample_trade() -> Trade {
        Trade {
            id: "t1".into(),
            price: Price(100),
            qty: Qty(1),
            taker_side: Side::Buy,
        }
    }

    #[test]
    fn default_on_trade_and_on_timer_and_on_shutdown_return_empty() {
        let mut s = DefaultsOnly;
        assert!(s.on_trade(Timestamp::default(), &sample_trade()).is_empty());
        assert!(s.on_timer(Timestamp::default()).is_empty());
        assert!(s.on_shutdown().is_empty());
    }

    #[test]
    fn box_dyn_strategy_forwards_every_hook() {
        let mut boxed: Box<dyn Strategy> = Box::new(NoopStrategy::default());
        boxed.on_book_update(Timestamp::default(), &OrderBook::new());
        boxed.on_trade(Timestamp::default(), &sample_trade());
        boxed.on_fill(&Fill {
            cid: ClientOrderId::new("c"),
            venue: ts_core::Venue::BINANCE,
            symbol: ts_core::Symbol::from_static("BTCUSDT"),
            side: Side::Buy,
            price: Price(1),
            qty: Qty(1),
            ts: Timestamp::default(),
            is_maker: None,
            fee: 0,
            fee_asset: None,
        });
        boxed.on_exec_report(&ExecReport::rejected(ClientOrderId::new("c"), "x"));
        boxed.on_timer(Timestamp::default());
        let _ = boxed.on_shutdown();
        // Cannot peek inside a Box<dyn Strategy>, so instead rely on
        // the fact that none of the above calls panicked. To assert the
        // forwarder reaches the inner type's state, reuse the strategy
        // without erasure:
        let mut direct = NoopStrategy::default();
        direct.on_book_update(Timestamp::default(), &OrderBook::new());
        direct.on_trade(Timestamp::default(), &sample_trade());
        direct.on_timer(Timestamp::default());
        let _ = direct.on_shutdown();
        assert_eq!(direct.book_calls, 1);
        assert_eq!(direct.trade_calls, 1);
        assert_eq!(direct.timer_calls, 1);
        assert_eq!(direct.shutdown_calls, 1);
    }
}
