//! In-process paper executor.
//!
//! Given a [`NewOrder`] and a read-only [`OrderBook`] snapshot,
//! [`PaperExecutor::execute`] walks the opposing side and returns an
//! [`ExecReport`] with every simulated fill. No state is carried
//! between calls — that's the risk/OMS layer's job.
//!
//! Matching rules:
//!
//! * **Market** — walk the opposing side until qty is exhausted or
//!   depth runs out. Any unfilled remainder is always canceled;
//!   markets never rest.
//! * **Limit** — walk while the opposing level price still crosses
//!   the limit. Under `Gtc` the remainder would rest, so we report
//!   [`OrderStatus::PartiallyFilled`] (or `New` if nothing filled)
//!   and leave persistence to the caller. `Ioc` cancels the
//!   remainder. `Fok` is pre-checked and rejected if depth cannot
//!   cover the order without mutating the book.

#![forbid(unsafe_code)]

use ts_book::OrderBook;
use ts_core::{
    ExecReport, Fill, NewOrder, OrderKind, OrderStatus, Price, Qty, Side, TimeInForce, Timestamp,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct PaperExecutor;

impl PaperExecutor {
    pub fn new() -> Self {
        Self
    }

    pub fn execute(&self, order: &NewOrder, book: &OrderBook, now: Timestamp) -> ExecReport {
        if order.qty.0 <= 0 {
            return ExecReport::rejected(order.cid.clone(), "non-positive qty");
        }
        if matches!(order.side, Side::Unknown) {
            return ExecReport::rejected(order.cid.clone(), "unknown side");
        }
        if matches!(order.kind, OrderKind::Limit) && order.price.is_none() {
            return ExecReport::rejected(order.cid.clone(), "limit without price");
        }

        // Collect the opposing side once. top_n allocates proportional
        // to real depth, which is bounded by the underlying book.
        let levels = match order.side {
            Side::Buy => book.top_n(usize::MAX).asks,
            Side::Sell => book.top_n(usize::MAX).bids,
            Side::Unknown => Vec::new(),
        };

        if matches!(order.tif, TimeInForce::Fok) {
            let mut avail: i64 = 0;
            for lvl in &levels {
                if !crosses(order, lvl.price) {
                    break;
                }
                avail = avail.saturating_add(lvl.qty.0);
                if avail >= order.qty.0 {
                    break;
                }
            }
            if avail < order.qty.0 {
                return ExecReport::rejected(order.cid.clone(), "FOK: insufficient liquidity");
            }
        }

        let mut fills: Vec<Fill> = Vec::new();
        let mut remaining = order.qty.0;
        let mut filled: i64 = 0;
        let mut notional: i128 = 0;

        for lvl in &levels {
            if remaining == 0 {
                break;
            }
            if !crosses(order, lvl.price) {
                break;
            }
            let take = lvl.qty.0.min(remaining);
            fills.push(Fill {
                cid: order.cid.clone(),
                venue: order.venue.clone(),
                symbol: order.symbol.clone(),
                side: order.side,
                price: lvl.price,
                qty: Qty(take),
                ts: now,
                is_maker: Some(false),
                fee: 0,
                fee_asset: None,
            });
            notional += (lvl.price.0 as i128) * (take as i128);
            filled += take;
            remaining -= take;
        }

        let status = determine_status(order, filled, remaining);
        let avg_price = if filled > 0 {
            Some(Price((notional / filled as i128) as i64))
        } else {
            None
        };

        ExecReport {
            cid: order.cid.clone(),
            status,
            filled_qty: Qty(filled),
            avg_price,
            reason: None,
            fills,
        }
    }
}

fn crosses(order: &NewOrder, level_price: Price) -> bool {
    match order.kind {
        OrderKind::Market => true,
        OrderKind::Limit => {
            let limit = match order.price {
                Some(p) => p,
                None => return false,
            };
            match order.side {
                Side::Buy => level_price.0 <= limit.0,
                Side::Sell => level_price.0 >= limit.0,
                Side::Unknown => false,
            }
        }
    }
}

fn determine_status(order: &NewOrder, filled: i64, remaining: i64) -> OrderStatus {
    if remaining == 0 {
        return OrderStatus::Filled;
    }
    // Market orders never rest.
    if matches!(order.kind, OrderKind::Market) {
        return OrderStatus::Canceled;
    }
    match order.tif {
        TimeInForce::Gtc => {
            if filled > 0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::New
            }
        }
        TimeInForce::Ioc => OrderStatus::Canceled,
        // FOK should have been pre-checked; anything reaching here is a bug.
        TimeInForce::Fok => OrderStatus::Rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::{
        BookLevel, BookSnapshot, ClientOrderId, OrderKind, Symbol, TimeInForce, Timestamp, Venue,
    };

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

    fn order(
        side: Side,
        kind: OrderKind,
        tif: TimeInForce,
        qty: i64,
        price: Option<i64>,
    ) -> NewOrder {
        NewOrder {
            cid: ClientOrderId::new("c1"),
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            side,
            kind,
            tif,
            qty: Qty(qty),
            price: price.map(Price),
            ts: Timestamp::default(),
        }
    }

    #[test]
    fn market_buy_fills_across_asks() {
        let book = book_with(
            vec![lvl(100, 10)],
            vec![lvl(101, 3), lvl(102, 4), lvl(103, 10)],
        );
        let r = PaperExecutor::new().execute(
            &order(Side::Buy, OrderKind::Market, TimeInForce::Ioc, 8, None),
            &book,
            Timestamp::default(),
        );
        assert_eq!(r.status, OrderStatus::Filled);
        assert_eq!(r.filled_qty, Qty(8));
        assert_eq!(r.fills.len(), 3);
        assert_eq!(r.fills[0].price, Price(101));
        assert_eq!(r.fills[0].qty, Qty(3));
        assert_eq!(r.fills[2].qty, Qty(1));
        // (101*3 + 102*4 + 103*1) / 8 == 101 (integer division).
        assert_eq!(r.avg_price, Some(Price(101)));
    }

    #[test]
    fn market_sell_walks_bids_descending() {
        let book = book_with(
            vec![lvl(100, 2), lvl(99, 5), lvl(98, 10)],
            vec![lvl(101, 5)],
        );
        let r = PaperExecutor::new().execute(
            &order(Side::Sell, OrderKind::Market, TimeInForce::Ioc, 4, None),
            &book,
            Timestamp::default(),
        );
        assert_eq!(r.status, OrderStatus::Filled);
        assert_eq!(r.filled_qty, Qty(4));
        assert_eq!(r.fills[0].price, Price(100));
        assert_eq!(r.fills[1].price, Price(99));
    }

    #[test]
    fn market_buy_insufficient_depth_is_canceled() {
        let book = book_with(vec![lvl(100, 5)], vec![lvl(101, 2)]);
        let r = PaperExecutor::new().execute(
            &order(Side::Buy, OrderKind::Market, TimeInForce::Ioc, 10, None),
            &book,
            Timestamp::default(),
        );
        assert_eq!(r.status, OrderStatus::Canceled);
        assert_eq!(r.filled_qty, Qty(2));
    }

    #[test]
    fn limit_buy_only_crosses_up_to_limit() {
        let book = book_with(
            vec![lvl(99, 5)],
            vec![lvl(101, 2), lvl(102, 3), lvl(103, 5)],
        );
        let r = PaperExecutor::new().execute(
            &order(Side::Buy, OrderKind::Limit, TimeInForce::Gtc, 10, Some(102)),
            &book,
            Timestamp::default(),
        );
        assert_eq!(r.status, OrderStatus::PartiallyFilled);
        assert_eq!(r.filled_qty, Qty(5)); // 2 @ 101 + 3 @ 102; 103 is above limit
    }

    #[test]
    fn limit_buy_no_cross_reports_new() {
        let book = book_with(vec![lvl(99, 5)], vec![lvl(101, 2)]);
        let r = PaperExecutor::new().execute(
            &order(Side::Buy, OrderKind::Limit, TimeInForce::Gtc, 3, Some(100)),
            &book,
            Timestamp::default(),
        );
        assert_eq!(r.status, OrderStatus::New);
        assert_eq!(r.filled_qty, Qty(0));
        assert!(r.fills.is_empty());
    }

    #[test]
    fn limit_ioc_partial_is_canceled_with_fills() {
        let book = book_with(vec![lvl(99, 5)], vec![lvl(101, 2)]);
        let r = PaperExecutor::new().execute(
            &order(Side::Buy, OrderKind::Limit, TimeInForce::Ioc, 5, Some(101)),
            &book,
            Timestamp::default(),
        );
        assert_eq!(r.status, OrderStatus::Canceled);
        assert_eq!(r.filled_qty, Qty(2));
    }

    #[test]
    fn fok_rejects_on_insufficient_depth() {
        let book = book_with(vec![lvl(99, 5)], vec![lvl(101, 2)]);
        let r = PaperExecutor::new().execute(
            &order(Side::Buy, OrderKind::Limit, TimeInForce::Fok, 5, Some(105)),
            &book,
            Timestamp::default(),
        );
        assert_eq!(r.status, OrderStatus::Rejected);
        assert!(r.fills.is_empty());
    }

    #[test]
    fn fok_fills_when_depth_sufficient() {
        let book = book_with(vec![lvl(99, 5)], vec![lvl(101, 3), lvl(102, 3)]);
        let r = PaperExecutor::new().execute(
            &order(Side::Buy, OrderKind::Limit, TimeInForce::Fok, 5, Some(102)),
            &book,
            Timestamp::default(),
        );
        assert_eq!(r.status, OrderStatus::Filled);
        assert_eq!(r.filled_qty, Qty(5));
    }

    #[test]
    fn rejects_unknown_side() {
        let book = book_with(vec![lvl(99, 5)], vec![lvl(101, 5)]);
        let r = PaperExecutor::new().execute(
            &order(Side::Unknown, OrderKind::Market, TimeInForce::Ioc, 1, None),
            &book,
            Timestamp::default(),
        );
        assert_eq!(r.status, OrderStatus::Rejected);
    }

    #[test]
    fn rejects_zero_qty() {
        let book = book_with(vec![lvl(99, 5)], vec![lvl(101, 5)]);
        let r = PaperExecutor::new().execute(
            &order(Side::Buy, OrderKind::Market, TimeInForce::Ioc, 0, None),
            &book,
            Timestamp::default(),
        );
        assert_eq!(r.status, OrderStatus::Rejected);
    }
}
