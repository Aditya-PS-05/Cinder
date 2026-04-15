//! In-process typed publish/subscribe bus.
//!
//! Hot-path producers (market data connectors) hand off events with a
//! non-blocking [`Bus::publish`]. A subscriber that cannot keep up sees
//! its per-subscription drop counter rise instead of backpressuring the
//! producer. Slow-consumer detection is the caller's responsibility —
//! poll [`Subscription::dropped`] on a cadence and disconnect the laggard.
//!
//! The bus is intentionally synchronous (no tokio) so the hot path has
//! zero async overhead. Subscribers own a `std::sync::mpsc::Receiver`
//! and can block, try_recv, or iterate as they prefer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, RwLock, Weak};

/// Typed broadcast bus. Construct with [`Bus::new`].
pub struct Bus<T: Clone + Send + 'static> {
    state: RwLock<State<T>>,
}

struct State<T> {
    next_id: u64,
    subs: HashMap<u64, SubState<T>>,
    closed: bool,
}

struct SubState<T> {
    tx: SyncSender<T>,
    received: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
}

/// Single subscriber handle. Dropping the subscription automatically
/// removes it from the bus.
pub struct Subscription<T: Clone + Send + 'static> {
    id: u64,
    rx: Receiver<T>,
    received: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    bus: Weak<Bus<T>>,
}

impl<T: Clone + Send + 'static> Bus<T> {
    /// Construct an empty bus behind an `Arc` so [`Subscription`] can hold
    /// a `Weak` back-reference for drop-time cleanup.
    pub fn new() -> Arc<Self> {
        Arc::new(Bus {
            state: RwLock::new(State {
                next_id: 0,
                subs: HashMap::new(),
                closed: false,
            }),
        })
    }

    /// Register a new subscriber with a bounded channel of capacity `buffer`.
    /// A buffer of 0 behaves like a rendezvous channel and will drop every
    /// publish unless a thread is already blocked in `recv`; use >= 16 in
    /// practice.
    pub fn subscribe(self: &Arc<Self>, buffer: usize) -> Subscription<T> {
        let (tx, rx) = sync_channel(buffer);
        let received = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let mut st = self.state.write().expect("bus state poisoned");
        let id = st.next_id;
        st.next_id += 1;
        if !st.closed {
            st.subs.insert(
                id,
                SubState {
                    tx,
                    received: Arc::clone(&received),
                    dropped: Arc::clone(&dropped),
                },
            );
        }
        // If closed, we never insert, so the SyncSender drops at end of scope
        // and the Receiver observes a closed channel.
        Subscription {
            id,
            rx,
            received,
            dropped,
            bus: Arc::downgrade(self),
        }
    }

    /// Broadcast `v` to every current subscriber. Returns
    /// `(delivered, dropped)` where delivered is the count that accepted
    /// the value and dropped is the count whose buffer was full. Never
    /// blocks.
    pub fn publish(&self, v: T) -> (usize, usize) {
        let st = self.state.read().expect("bus state poisoned");
        if st.closed {
            return (0, 0);
        }
        let mut delivered = 0;
        let mut dropped = 0;
        for sub in st.subs.values() {
            match sub.tx.try_send(v.clone()) {
                Ok(()) => {
                    sub.received.fetch_add(1, Ordering::Relaxed);
                    delivered += 1;
                }
                Err(TrySendError::Full(_)) => {
                    sub.dropped.fetch_add(1, Ordering::Relaxed);
                    dropped += 1;
                }
                Err(TrySendError::Disconnected(_)) => {
                    // Receiver has already gone away; sweep will catch it on
                    // the next subscribe/close.
                }
            }
        }
        (delivered, dropped)
    }

    /// Current number of subscribers.
    pub fn len(&self) -> usize {
        self.state.read().map(|s| s.subs.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reject further publishes and close every subscriber's channel.
    /// Idempotent.
    pub fn close(&self) {
        let mut st = self.state.write().expect("bus state poisoned");
        if st.closed {
            return;
        }
        st.closed = true;
        st.subs.clear(); // dropping SyncSenders closes all receivers
    }

    fn unsubscribe(&self, id: u64) {
        if let Ok(mut st) = self.state.write() {
            st.subs.remove(&id);
        }
    }
}

impl<T: Clone + Send + 'static> Subscription<T> {
    /// Block until the next event or the bus closes.
    pub fn recv(&self) -> Result<T, std::sync::mpsc::RecvError> {
        self.rx.recv()
    }

    /// Non-blocking receive.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.rx.try_recv()
    }

    /// Iterate over events until the bus closes.
    pub fn iter(&self) -> std::sync::mpsc::Iter<'_, T> {
        self.rx.iter()
    }

    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl<T: Clone + Send + 'static> Drop for Subscription<T> {
    fn drop(&mut self) {
        if let Some(bus) = self.bus.upgrade() {
            bus.unsubscribe(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn basic_pub_sub() {
        let bus = Bus::<i32>::new();
        let s1 = bus.subscribe(16);
        let s2 = bus.subscribe(16);

        let (d, dr) = bus.publish(42);
        assert_eq!((d, dr), (2, 0));
        assert_eq!(s1.recv().unwrap(), 42);
        assert_eq!(s2.recv().unwrap(), 42);
        assert_eq!(s1.received(), 1);
        assert_eq!(s2.received(), 1);
    }

    #[test]
    fn slow_subscriber_drops_not_blocks() {
        let bus = Bus::<i32>::new();
        let s = bus.subscribe(2);

        assert_eq!(bus.publish(1), (1, 0));
        assert_eq!(bus.publish(2), (1, 0));
        // Buffer full now; next publish must drop.
        assert_eq!(bus.publish(3), (0, 1));
        assert_eq!(s.dropped(), 1);

        // First two events are still sitting in the channel.
        assert_eq!(s.recv().unwrap(), 1);
        assert_eq!(s.recv().unwrap(), 2);
    }

    #[test]
    fn drop_removes_from_bus() {
        let bus = Bus::<&'static str>::new();
        let s = bus.subscribe(4);
        assert_eq!(bus.len(), 1);
        drop(s);
        assert_eq!(bus.len(), 0);
    }

    #[test]
    fn close_is_idempotent_and_publish_becomes_noop() {
        let bus = Bus::<i32>::new();
        let _s = bus.subscribe(4);
        bus.close();
        bus.close(); // idempotent
        assert_eq!(bus.publish(1), (0, 0));
    }

    #[test]
    fn iter_terminates_after_close() {
        let bus = Bus::<i32>::new();
        let s = bus.subscribe(8);
        bus.publish(1);
        bus.publish(2);
        bus.publish(3);
        bus.close();

        let collected: Vec<i32> = s.iter().collect();
        assert_eq!(collected, vec![1, 2, 3]);
    }

    #[test]
    fn concurrent_multi_producer_multi_subscriber() {
        const SUBS: usize = 10;
        const PUBS: usize = 4;
        const MSGS_PER_PUB: usize = 500;

        let bus = Bus::<usize>::new();
        let mut handles = Vec::new();
        let mut subs = Vec::new();

        for _ in 0..SUBS {
            let s = bus.subscribe(PUBS * MSGS_PER_PUB);
            subs.push(s);
        }

        for p in 0..PUBS {
            let b = Arc::clone(&bus);
            handles.push(thread::spawn(move || {
                for i in 0..MSGS_PER_PUB {
                    b.publish(p * 10_000 + i);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Give any in-flight deliveries a moment to land.
        thread::sleep(Duration::from_millis(20));
        bus.close();

        for s in &subs {
            let count: usize = s.iter().count();
            assert_eq!(
                count,
                PUBS * MSGS_PER_PUB,
                "subscriber received {count}, expected {}",
                PUBS * MSGS_PER_PUB
            );
        }
    }
}
