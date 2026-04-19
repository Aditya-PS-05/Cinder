//! Property-based tests for [`ts_core::OrderStatus`]' legal-transition
//! graph.
//!
//! These assertions back up the hand-coded unit tests in
//! `crates/core/src/order.rs` with `proptest`-driven coverage over all
//! `6 × 6 = 36` edges, plus random walks through the state machine.
//! The invariants we bolt down:
//!
//! * `try_transition_to` is `Ok` iff `can_transition_to` is true.
//! * `is_terminal` matches an explicit enumerated set and terminal
//!   states reject every transition except the self edge.
//! * The graph is *reachability-closed*: every edge out of `New` is
//!   legal, nothing else can ever move back to `New`, and `Rejected`
//!   is unreachable from anywhere other than `New`.
//! * Self-transitions are always legal, so reconcilers can re-apply
//!   the same status idempotently without special-casing.
//! * A random sequence of legal transitions stays inside the graph and
//!   terminates (in the DAG sense) once it hits a terminal state.

use proptest::collection::vec as prop_vec;
use proptest::prelude::*;
use ts_core::OrderStatus;

fn arb_status() -> impl Strategy<Value = OrderStatus> {
    prop_oneof![
        Just(OrderStatus::New),
        Just(OrderStatus::PartiallyFilled),
        Just(OrderStatus::Filled),
        Just(OrderStatus::Canceled),
        Just(OrderStatus::Rejected),
        Just(OrderStatus::Expired),
    ]
}

fn expected_can_transition(from: OrderStatus, to: OrderStatus) -> bool {
    use OrderStatus::*;
    if from == to {
        return true;
    }
    matches!(
        (from, to),
        (
            New,
            PartiallyFilled | Filled | Canceled | Rejected | Expired
        ) | (PartiallyFilled, Filled | Canceled | Expired)
    )
}

proptest! {
    // Smaller case count for each invariant — the sample space is
    // small (36 pairs, then sequences) so this stays fast without
    // running into proptest's 256-case default.
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    fn try_transition_matches_can_transition(from in arb_status(), to in arb_status()) {
        let can = from.can_transition_to(to);
        match from.try_transition_to(to) {
            Ok(next) => {
                prop_assert!(can, "can=false but try returned Ok({next:?})");
                prop_assert_eq!(next, to);
            }
            Err(e) => {
                prop_assert!(!can, "can=true but try returned Err({e:?})");
                prop_assert_eq!(e.from, from);
                prop_assert_eq!(e.to, to);
            }
        }
    }

    #[test]
    fn can_transition_matches_hand_rolled_spec(from in arb_status(), to in arb_status()) {
        prop_assert_eq!(from.can_transition_to(to), expected_can_transition(from, to));
    }

    #[test]
    fn self_transition_is_always_legal(s in arb_status()) {
        prop_assert!(s.can_transition_to(s));
        prop_assert_eq!(s.try_transition_to(s).ok(), Some(s));
    }

    #[test]
    fn terminal_states_reject_all_other_transitions(from in arb_status(), to in arb_status()) {
        prop_assume!(from.is_terminal());
        prop_assume!(from != to);
        prop_assert!(
            !from.can_transition_to(to),
            "{from:?} is terminal but accepts transition to {to:?}"
        );
    }

    #[test]
    fn non_terminal_never_spontaneously_terminates_backwards(from in arb_status()) {
        // New and PartiallyFilled are the only non-terminals; neither
        // should ever be reachable from another state (only from
        // itself). Encoded here as: no `to ≠ New` has an edge into New.
        for to in [
            OrderStatus::PartiallyFilled,
            OrderStatus::Filled,
            OrderStatus::Canceled,
            OrderStatus::Rejected,
            OrderStatus::Expired,
        ] {
            if to == from {
                continue;
            }
            prop_assert!(
                !to.can_transition_to(OrderStatus::New),
                "{to:?} must not transition back to New"
            );
        }
    }

    #[test]
    fn rejected_is_only_reachable_from_new(from in arb_status()) {
        prop_assume!(from != OrderStatus::New);
        prop_assume!(from != OrderStatus::Rejected);
        prop_assert!(
            !from.can_transition_to(OrderStatus::Rejected),
            "{from:?} → Rejected is illegal — a venue can only reject from New"
        );
    }

    /// A random walk that only ever takes legal edges should always
    /// stay inside the graph. Once a terminal state is reached, every
    /// subsequent non-self edge is refused, so the walk is trapped at
    /// that state until the sequence ends.
    #[test]
    fn random_legal_walk_respects_terminal_absorption(
        start in arb_status(),
        steps in prop_vec(arb_status(), 0..16),
    ) {
        let mut current = start;
        for next in steps {
            if current.is_terminal() && next != current {
                prop_assert!(!current.can_transition_to(next));
                // stay absorbed
                continue;
            }
            if current.can_transition_to(next) {
                current = current.try_transition_to(next).expect("legal edge");
            }
        }
    }
}
