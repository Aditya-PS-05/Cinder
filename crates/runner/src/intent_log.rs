//! Crash-safe intent WAL for the live runner.
//!
//! Before the runner hands a [`NewOrder`] to the engine, it records the
//! intent to an append-only NDJSON file and fsyncs to durable storage.
//! When the same cid reaches a terminal [`ts_core::OrderStatus`]
//! (`Filled` / `Canceled` / `Rejected` / `Expired`), the runner writes a
//! completion tombstone. If the process crashes between the intent
//! record and the terminal record, [`replay_open_intents`] returns the
//! orphan intents at next startup so operators can reconcile against
//! venue state.
//!
//! Each line is one [`IntentLogEntry`] JSON document. Lines that fail to
//! deserialize — e.g. a torn final line from a power loss mid-append —
//! are skipped with a warning rather than aborting the replay.
//!
//! `record_submit` / `record_complete` fsync before returning, so a
//! successful return is a durability guarantee: any subsequent
//! crash will leave the record on disk.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::warn;

use ts_core::{ClientOrderId, NewOrder};

/// One line of the intent log. Submit records the in-flight order;
/// Complete tombstones close it. Deserialization is tolerant of unknown
/// tags — if a future version adds a third variant, older replayers
/// skip it with a warning rather than panicking.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IntentLogEntry {
    Submit { order: NewOrder },
    Complete { cid: ClientOrderId },
}

/// Append-only writer for [`IntentLogEntry`] lines. Each `record_*` call
/// writes one NDJSON line and fsyncs to durable storage before
/// returning.
pub struct IntentLogWriter {
    path: PathBuf,
    file: File,
    written: u64,
}

impl IntentLogWriter {
    /// Open `path` for append, creating it if missing. Existing content
    /// is preserved so restarts continue the same log.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        Ok(Self {
            path,
            file,
            written: 0,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Count of entries written by this writer instance since construction.
    /// Does not include any entries that pre-existed in the file.
    pub fn written(&self) -> u64 {
        self.written
    }

    /// Durably record a submit intent. Fsyncs before returning.
    pub async fn record_submit(&mut self, order: &NewOrder) -> io::Result<()> {
        self.write_entry(&IntentLogEntry::Submit {
            order: order.clone(),
        })
        .await
    }

    /// Durably record a terminal completion. Fsyncs before returning.
    pub async fn record_complete(&mut self, cid: &ClientOrderId) -> io::Result<()> {
        self.write_entry(&IntentLogEntry::Complete { cid: cid.clone() })
            .await
    }

    async fn write_entry(&mut self, entry: &IntentLogEntry) -> io::Result<()> {
        let mut line = serde_json::to_vec(entry).map_err(io::Error::other)?;
        line.push(b'\n');
        self.file.write_all(&line).await?;
        self.file.sync_all().await?;
        self.written += 1;
        Ok(())
    }
}

/// Replay the intent log at `path` and return every `NewOrder` whose
/// submit was recorded but whose completion tombstone is missing.
/// Malformed lines (torn final writes from crashes) are skipped with a
/// warning. A missing file yields an empty vector.
pub async fn replay_open_intents(path: impl AsRef<Path>) -> io::Result<Vec<NewOrder>> {
    let path = path.as_ref();
    let file = match File::open(path).await {
        Ok(f) => f,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let mut lines = BufReader::new(file).lines();
    let mut open: HashMap<ClientOrderId, NewOrder> = HashMap::new();
    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<IntentLogEntry>(trimmed) {
            Ok(IntentLogEntry::Submit { order }) => {
                open.insert(order.cid.clone(), order);
            }
            Ok(IntentLogEntry::Complete { cid }) => {
                open.remove(&cid);
            }
            Err(err) => {
                warn!(error = %err, "intent_log: skipping malformed line during replay");
            }
        }
    }
    let mut out: Vec<_> = open.into_values().collect();
    out.sort_by(|a, b| a.cid.as_str().cmp(b.cid.as_str()));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use ts_core::{ClientOrderId, OrderKind, Qty, Side, Symbol, TimeInForce, Timestamp, Venue};

    fn unique_path(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ts-intent-log-{}-{}-{}.ndjson",
            std::process::id(),
            tag,
            n
        ))
    }

    fn order(cid: &str) -> NewOrder {
        NewOrder {
            cid: ClientOrderId::new(cid),
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            side: Side::Buy,
            kind: OrderKind::Limit,
            tif: TimeInForce::Gtc,
            qty: Qty(100_000),
            price: Some(ts_core::Price(10_000)),
            ts: Timestamp::default(),
        }
    }

    #[tokio::test]
    async fn replay_of_missing_file_yields_empty_vec() {
        let path = unique_path("missing");
        // No file on disk.
        let open = replay_open_intents(&path).await.unwrap();
        assert!(open.is_empty());
    }

    #[tokio::test]
    async fn submit_without_complete_surfaces_on_replay() {
        let path = unique_path("orphan");
        let mut w = IntentLogWriter::open(&path).await.unwrap();
        w.record_submit(&order("a1")).await.unwrap();
        w.record_submit(&order("a2")).await.unwrap();
        w.record_complete(&ClientOrderId::new("a1")).await.unwrap();
        assert_eq!(w.written(), 3);
        drop(w);

        let open = replay_open_intents(&path).await.unwrap();
        assert_eq!(open.len(), 1, "a2 should be orphaned");
        assert_eq!(open[0].cid.as_str(), "a2");
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn completion_after_submit_closes_the_intent() {
        let path = unique_path("closed");
        let mut w = IntentLogWriter::open(&path).await.unwrap();
        w.record_submit(&order("b1")).await.unwrap();
        w.record_complete(&ClientOrderId::new("b1")).await.unwrap();
        drop(w);

        let open = replay_open_intents(&path).await.unwrap();
        assert!(
            open.is_empty(),
            "terminal tombstone should close the intent"
        );
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn replay_is_deterministically_ordered_by_cid() {
        let path = unique_path("order");
        let mut w = IntentLogWriter::open(&path).await.unwrap();
        // Insert in anti-lexicographic order so the HashMap iteration
        // alone could not produce the assertion's output.
        for cid in ["z", "m", "a"] {
            w.record_submit(&order(cid)).await.unwrap();
        }
        drop(w);

        let open = replay_open_intents(&path).await.unwrap();
        let cids: Vec<_> = open.iter().map(|o| o.cid.as_str().to_string()).collect();
        assert_eq!(cids, vec!["a", "m", "z"]);
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn restart_replays_previously_written_entries() {
        // Simulate a crash: write, drop, reopen in append mode, write
        // more, drop, replay. The replay must see both writer sessions.
        let path = unique_path("restart");
        {
            let mut w = IntentLogWriter::open(&path).await.unwrap();
            w.record_submit(&order("r1")).await.unwrap();
        }
        {
            let mut w = IntentLogWriter::open(&path).await.unwrap();
            w.record_submit(&order("r2")).await.unwrap();
            w.record_complete(&ClientOrderId::new("r1")).await.unwrap();
        }
        let open = replay_open_intents(&path).await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].cid.as_str(), "r2");
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn malformed_line_is_skipped_not_fatal() {
        // Mimic a torn trailing line from a crash: the replay should
        // accept everything up to the break and only warn on the bad
        // tail.
        let path = unique_path("torn");
        {
            let mut w = IntentLogWriter::open(&path).await.unwrap();
            w.record_submit(&order("ok")).await.unwrap();
        }
        // Append a corrupt line without a trailing newline.
        let mut raw = OpenOptions::new().append(true).open(&path).await.unwrap();
        raw.write_all(b"{not-json\n").await.unwrap();
        raw.sync_all().await.unwrap();
        drop(raw);

        let open = replay_open_intents(&path).await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].cid.as_str(), "ok");
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn complete_before_submit_is_a_noop() {
        // Tombstone without a matching submit must not panic or
        // corrupt the open set — this can happen if the runner
        // observes a stream-pushed terminal report for a cid it never
        // submitted itself (e.g. a manually-placed order landing on the
        // user-data stream). The log just records the fact; replay
        // gracefully produces an empty open set.
        let path = unique_path("orphan-complete");
        let mut w = IntentLogWriter::open(&path).await.unwrap();
        w.record_complete(&ClientOrderId::new("never-seen"))
            .await
            .unwrap();
        drop(w);

        let open = replay_open_intents(&path).await.unwrap();
        assert!(open.is_empty());
        let _ = tokio::fs::remove_file(&path).await;
    }
}
