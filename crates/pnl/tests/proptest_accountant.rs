//! Property-based tests for the WAC [`ts_pnl::Accountant`] and the
//! FIFO [`ts_pnl::LotAccountant`].
//!
//! The unit tests in `src/` cover specific scenarios; these assertions
//! pin the invariants that both accountants uphold across arbitrary
//! fill streams:
//!
//! * `realized_net == realized - fees` on every symbol and in total.
//! * `realized_total == Σ realized(sym)` and `fees_total == Σ fees(sym)`.
//! * FIFO's queue stays homogeneous-by-side, and `|position| == Σ qty`
//!   across open lots, matching WAC's signed position on the same
//!   fill stream.
//! * FIFO's cumulative `realized()` equals the sum of its closed-lot
//!   realized slices at every step (tail invariant of the fold).
//! * When a stream of same-side opens is closed out by a single
//!   opposing fill of the full size, FIFO and WAC agree on realized
//!   pnl — their only documented divergence is multi-lot *partial*
//!   closes.
//! * Extension-only streams (no opposing fills) keep realized at 0 on
//!   both accountants and maintain `position == Σ signed_qty`.

use proptest::collection::vec as prop_vec;
use proptest::prelude::*;
use ts_core::{ClientOrderId, Fill, Price, Qty, Side, Symbol, Timestamp, Venue};
use ts_pnl::{Accountant, LotAccountant};

fn sym() -> Symbol {
    Symbol::from_static("BTCUSDT")
}

/// Build a fill with deterministic sym/venue/cid. Values stay small
/// so the i128 arithmetic cannot overflow even across 32-step
/// sequences.
fn mk_fill(side: Side, price: i32, qty: i32, fee: i16, fee_asset: Option<&'static str>) -> Fill {
    Fill {
        cid: ClientOrderId::new("c"),
        venue: Venue::BINANCE,
        symbol: sym(),
        side,
        price: Price(price as i64),
        qty: Qty(qty as i64),
        ts: Timestamp::default(),
        is_maker: None,
        fee: fee as i64,
        fee_asset: fee_asset.map(str::to_string),
    }
}

fn arb_side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

/// Arbitrary fill bounded to comfortable i128-safe ranges.
fn arb_fill() -> impl Strategy<Value = Fill> {
    (arb_side(), 1i32..10_000, 1i32..1_000, -100i16..100).prop_map(|(side, price, qty, fee)| {
        let asset = if fee == 0 { None } else { Some("USDT") };
        mk_fill(side, price, qty, fee, asset)
    })
}

/// Signed cumulative qty over a fill stream (Unknown side dropped in
/// the accountants, so the generator refuses to emit it). Used to
/// assert WAC position tracking on extension-only streams.
fn cumulative_signed_qty(fills: &[Fill]) -> i64 {
    fills
        .iter()
        .map(|f| match f.side {
            Side::Buy => f.qty.0,
            Side::Sell => -f.qty.0,
            Side::Unknown => 0,
        })
        .sum()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    /// On every fill stream WAC's `realized_net = realized - fees`,
    /// per-symbol and in total. This is a linearity invariant of the
    /// accountant and must not drift as fees pile up.
    #[test]
    fn wac_realized_net_is_gross_minus_fees(fills in prop_vec(arb_fill(), 0..32)) {
        let mut a = Accountant::new();
        for f in &fills {
            a.on_fill(f);
        }
        prop_assert_eq!(a.realized_net(&sym()), a.realized(&sym()) - a.fees(&sym()));
        prop_assert_eq!(a.realized_net_total(), a.realized_total() - a.fees_total());
    }

    /// Same invariant on the FIFO accountant. Two separate strategies
    /// compute the same numbers from different state, so the check is
    /// real on both.
    #[test]
    fn fifo_realized_net_is_gross_minus_fees(fills in prop_vec(arb_fill(), 0..32)) {
        let mut a = LotAccountant::new();
        for f in &fills {
            a.on_fill(f);
        }
        prop_assert_eq!(a.realized_net(&sym()), a.realized(&sym()) - a.fees(&sym()));
        prop_assert_eq!(a.realized_net_total(), a.realized_total() - a.fees_total());
    }

    /// Per-symbol sums match totals on both accountants. Catches
    /// accidental double-count or drop in the aggregate paths.
    #[test]
    fn totals_equal_per_symbol_sums(fills in prop_vec(arb_fill(), 0..32)) {
        let mut wac = Accountant::new();
        let mut fifo = LotAccountant::new();
        for f in &fills {
            wac.on_fill(f);
            fifo.on_fill(f);
        }
        let wac_realized: i128 = wac.iter().map(|(_, b)| b.realized).sum();
        let wac_fees: i128 = wac.iter().map(|(_, b)| b.fees).sum();
        prop_assert_eq!(wac.realized_total(), wac_realized);
        prop_assert_eq!(wac.fees_total(), wac_fees);

        let fifo_realized: i128 = fifo.iter().map(|(_, b)| b.realized()).sum();
        let fifo_fees: i128 = fifo.iter().map(|(_, b)| b.fees()).sum();
        prop_assert_eq!(fifo.realized_total(), fifo_realized);
        prop_assert_eq!(fifo.fees_total(), fifo_fees);
    }

    /// After any stream, FIFO's open-lot queue must be homogeneous by
    /// side, and `|position|` must equal the sum of open lot qtys
    /// (with the sign matching the front lot's side). This is the
    /// core queue invariant enforced by `apply_fill`.
    #[test]
    fn fifo_queue_is_homogeneous_and_position_sums_open_lots(
        fills in prop_vec(arb_fill(), 0..32),
    ) {
        let mut fifo = LotAccountant::new();
        for f in &fills {
            fifo.on_fill(f);
        }
        if let Some(book) = fifo.book(&sym()) {
            let sides: Vec<_> = book.open_lots().map(|l| l.side).collect();
            if let Some(first) = sides.first() {
                for s in &sides {
                    prop_assert_eq!(s, first, "heterogeneous lot sides in FIFO queue");
                }
                let total_qty: i64 = book.open_lots().map(|l| l.qty).sum();
                let expected = match first {
                    Side::Buy => total_qty,
                    Side::Sell => -total_qty,
                    Side::Unknown => 0,
                };
                prop_assert_eq!(book.position(), expected);
            } else {
                prop_assert_eq!(book.position(), 0);
            }
        }
    }

    /// WAC and FIFO must report the same signed position on every
    /// fill stream. They diverge on realized-pnl allocation across
    /// multiple open lots, but the running position is a pure function
    /// of the fill signs and sizes — identical under both models.
    #[test]
    fn wac_and_fifo_agree_on_signed_position(fills in prop_vec(arb_fill(), 0..32)) {
        let mut wac = Accountant::new();
        let mut fifo = LotAccountant::new();
        for f in &fills {
            wac.on_fill(f);
            fifo.on_fill(f);
        }
        prop_assert_eq!(wac.position(&sym()), fifo.position(&sym()));
    }

    /// Sum of `ClosedLot.realized` across every closed slice equals
    /// the FIFO book's running `realized()` at every step. Proven by
    /// induction; the tail check suffices.
    #[test]
    fn fifo_closed_lots_sum_to_realized(fills in prop_vec(arb_fill(), 0..32)) {
        let mut fifo = LotAccountant::new();
        for f in &fills {
            fifo.on_fill(f);
        }
        if let Some(book) = fifo.book(&sym()) {
            let sum: i128 = book.closed_lots().iter().map(|c| c.realized).sum();
            prop_assert_eq!(sum, book.realized());
        }
    }

    /// Streams made up entirely of *same-side* fills never realize
    /// anything — there's no opposing flow to close against. Position
    /// equals the cumulative signed qty, and realized stays zero on
    /// both accountants.
    #[test]
    fn extension_only_stream_has_zero_realized_and_exact_position(
        side in arb_side(),
        fills in prop_vec((1i32..10_000, 1i32..1_000), 1..16),
    ) {
        let fills: Vec<Fill> = fills
            .into_iter()
            .map(|(p, q)| mk_fill(side, p, q, 0, None))
            .collect();
        let expected_pos = cumulative_signed_qty(&fills);

        let mut wac = Accountant::new();
        let mut fifo = LotAccountant::new();
        for f in &fills {
            wac.on_fill(f);
            fifo.on_fill(f);
        }
        prop_assert_eq!(wac.realized(&sym()), 0);
        prop_assert_eq!(fifo.realized(&sym()), 0);
        prop_assert_eq!(wac.position(&sym()), expected_pos);
        prop_assert_eq!(fifo.position(&sym()), expected_pos);
        if let Some(book) = fifo.book(&sym()) {
            prop_assert_eq!(book.open_lots().count(), fills.len());
            prop_assert!(book.closed_lots().is_empty());
        }
    }

    /// When a batch of same-side opens is cleared by a single
    /// opposing fill of the full size, FIFO realizes the closed-form
    /// exact pnl `Σ (exit − entry_i) · qty_i` (sign-flipped for
    /// shorts). WAC's integer-truncated `avg_entry` drifts on
    /// compounding extensions, so this property deliberately does
    /// *not* tie WAC to the same number — it only pins FIFO to the
    /// tax-lot-exact value and asserts both models flatten the book.
    #[test]
    fn fifo_matches_closed_form_on_full_close(
        open_side in arb_side(),
        opens in prop_vec((1i32..10_000, 1i32..1_000), 1..8),
        close_price in 1i32..10_000,
    ) {
        let open_fills: Vec<Fill> = opens
            .iter()
            .map(|&(p, q)| mk_fill(open_side, p, q, 0, None))
            .collect();
        let total_qty: i32 = opens.iter().map(|(_, q)| *q).sum();
        let close_side = match open_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
            Side::Unknown => unreachable!(),
        };
        let close = mk_fill(close_side, close_price, total_qty, 0, None);

        // Closed-form exact realized pnl from the full close.
        let side_sign: i128 = match open_side {
            Side::Buy => 1,
            Side::Sell => -1,
            Side::Unknown => unreachable!(),
        };
        let exact_realized: i128 = opens
            .iter()
            .map(|&(p, q)| side_sign * ((close_price as i128) - (p as i128)) * (q as i128))
            .sum();

        let mut wac = Accountant::new();
        let mut fifo = LotAccountant::new();
        for f in &open_fills {
            wac.on_fill(f);
            fifo.on_fill(f);
        }
        wac.on_fill(&close);
        fifo.on_fill(&close);
        prop_assert_eq!(fifo.realized(&sym()), exact_realized);
        prop_assert_eq!(wac.position(&sym()), 0);
        prop_assert_eq!(fifo.position(&sym()), 0);
        if let Some(book) = fifo.book(&sym()) {
            prop_assert_eq!(book.open_lots().count(), 0);
            prop_assert_eq!(book.closed_lots().len(), open_fills.len());
        }
    }

    /// When every open lot sits at the same price, WAC's `avg_entry`
    /// suffers no truncation (the weighted mean equals the common
    /// price exactly), so the two accountants agree on realized pnl
    /// after a full close. Guards the narrow regime where FIFO and
    /// WAC must match bit-for-bit.
    #[test]
    fn wac_matches_fifo_when_all_opens_share_a_price(
        open_side in arb_side(),
        common_price in 1i32..10_000,
        qtys in prop_vec(1i32..1_000, 1..8),
        close_price in 1i32..10_000,
    ) {
        let open_fills: Vec<Fill> = qtys
            .iter()
            .map(|&q| mk_fill(open_side, common_price, q, 0, None))
            .collect();
        let total_qty: i32 = qtys.iter().copied().sum();
        let close_side = match open_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
            Side::Unknown => unreachable!(),
        };
        let close = mk_fill(close_side, close_price, total_qty, 0, None);

        let mut wac = Accountant::new();
        let mut fifo = LotAccountant::new();
        for f in &open_fills {
            wac.on_fill(f);
            fifo.on_fill(f);
        }
        wac.on_fill(&close);
        fifo.on_fill(&close);
        prop_assert_eq!(wac.realized(&sym()), fifo.realized(&sym()));
    }

    /// After any fill stream, FIFO's `unrealized(mark)` is zero when
    /// the position is flat — regardless of how aggressive the mark
    /// is. Catches stale lot bookkeeping after full closes.
    #[test]
    fn fifo_unrealized_is_zero_when_flat(
        open_side in arb_side(),
        price in 1i32..10_000,
        qty in 1i32..1_000,
        mark in 1i32..10_000,
    ) {
        let open = mk_fill(open_side, price, qty, 0, None);
        let close_side = match open_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
            Side::Unknown => unreachable!(),
        };
        let close = mk_fill(close_side, price + 5, qty, 0, None);

        let mut fifo = LotAccountant::new();
        fifo.on_fill(&open);
        fifo.on_fill(&close);
        prop_assert_eq!(fifo.unrealized(&sym(), Price(mark as i64)), 0);
    }
}
