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
//!
//! ## Compaction
//!
//! Append-only means the file grows forever even though most entries
//! are superseded by their own completion tombstones. [`compact`]
//! rewrites the file to retain only the orphan submits (plus nothing
//! else), shrinking the WAL while preserving the replay semantics.
//! Compaction is atomic: the new content is written to a sibling
//! `*.compact` file, fsynced, then renamed onto the target path, so a
//! crash at any point leaves one intact file on disk — never a torn
//! mix. Callers must arrange exclusive access — compacting while a
//! writer holds the file open on another handle would either race the
//! rename or leave the writer appending into an unlinked inode.

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
    Ok(scan_log(path.as_ref()).await?.open)
}

/// Intermediate snapshot of a log scan. Used by both
/// [`replay_open_intents`] (which only needs `open`) and [`compact`]
/// (which additionally needs the total entry count for stats).
struct ScanResult {
    /// Every non-empty line that parsed, counted once. Includes
    /// tombstones so the before-compaction count is meaningful.
    total_entries: u64,
    /// Orphan submits, sorted by cid — same contract as
    /// [`replay_open_intents`].
    open: Vec<NewOrder>,
}

async fn scan_log(path: &Path) -> io::Result<ScanResult> {
    let file = match File::open(path).await {
        Ok(f) => f,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Ok(ScanResult {
                total_entries: 0,
                open: Vec::new(),
            });
        }
        Err(err) => return Err(err),
    };
    let mut lines = BufReader::new(file).lines();
    let mut open: HashMap<ClientOrderId, NewOrder> = HashMap::new();
    let mut total_entries = 0u64;
    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<IntentLogEntry>(trimmed) {
            Ok(IntentLogEntry::Submit { order }) => {
                total_entries += 1;
                open.insert(order.cid.clone(), order);
            }
            Ok(IntentLogEntry::Complete { cid }) => {
                total_entries += 1;
                open.remove(&cid);
            }
            Err(err) => {
                warn!(error = %err, "intent_log: skipping malformed line during scan");
            }
        }
    }
    let mut out: Vec<_> = open.into_values().collect();
    out.sort_by(|a, b| a.cid.as_str().cmp(b.cid.as_str()));
    Ok(ScanResult {
        total_entries,
        open: out,
    })
}

/// Byte and entry totals before/after a [`compact`] run. Surfaced to
/// operators so a maintenance job can log how much disk it reclaimed
/// and whether any orphan submits are still in flight.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub entries_before: u64,
    pub entries_after: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

/// Rewrite `path` so it holds only the orphan submits that a replay
/// would surface. Atomic: writes to a sibling `<path>.compact` file,
/// fsyncs it, then renames onto `path` — a crash at any point leaves
/// one intact log on disk (either the old one or the compacted one,
/// never a torn overlay).
///
/// Preconditions:
/// * No concurrent [`IntentLogWriter`] may hold `path` open: the
///   rename would leave the writer appending into an unlinked inode.
///   Callers should shut the writer down, compact, then reopen.
/// * A missing source file is not an error; the function returns a
///   zero-valued `CompactionStats` and does not create an empty log.
pub async fn compact(path: impl AsRef<Path>) -> io::Result<CompactionStats> {
    let path = path.as_ref();
    let bytes_before = match tokio::fs::metadata(path).await {
        Ok(m) => m.len(),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Ok(CompactionStats::default());
        }
        Err(err) => return Err(err),
    };

    let scan = scan_log(path).await?;
    let entries_after = scan.open.len() as u64;

    // Pick a sibling filename so the rename stays within one
    // directory (atomic on every Linux filesystem we care about).
    let mut tmp_name = path
        .file_name()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "intent log path has no filename",
            )
        })?
        .to_os_string();
    tmp_name.push(".compact");
    let tmp_path = path.with_file_name(tmp_name);

    // Clobber a stale `*.compact` from a previous aborted run before
    // we start writing. `create(true).truncate(true)` does this in one
    // syscall, which also defends against a leftover that is owned by
    // a different uid.
    {
        let mut tmp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;
        for order in &scan.open {
            let mut line = serde_json::to_vec(&IntentLogEntry::Submit {
                order: order.clone(),
            })
            .map_err(io::Error::other)?;
            line.push(b'\n');
            tmp.write_all(&line).await?;
        }
        tmp.sync_all().await?;
    }

    // Atomic replace. After rename the old inode is unlinked; any
    // concurrent reader with an open fd keeps seeing the old content
    // until they drop it — rename is POSIX-atomic on the target name.
    tokio::fs::rename(&tmp_path, path).await?;

    let bytes_after = tokio::fs::metadata(path).await?.len();
    Ok(CompactionStats {
        entries_before: scan.total_entries,
        entries_after,
        bytes_before,
        bytes_after,
    })
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
    async fn compact_is_noop_on_missing_file() {
        // A missing log should compact to the default stats without
        // creating an empty file — operators can call this on a fresh
        // install without worrying about stray files.
        let path = unique_path("compact-missing");
        let stats = compact(&path).await.unwrap();
        assert_eq!(stats, CompactionStats::default());
        assert!(!tokio::fs::try_exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn compact_preserves_orphan_set_and_shrinks_file() {
        // After compaction the on-disk log must replay to the exact
        // same open set as before — and be strictly smaller when any
        // completed intents were reclaimable.
        let path = unique_path("compact-shrink");
        let mut w = IntentLogWriter::open(&path).await.unwrap();
        w.record_submit(&order("keep-a")).await.unwrap();
        w.record_submit(&order("drop")).await.unwrap();
        w.record_submit(&order("keep-b")).await.unwrap();
        w.record_complete(&ClientOrderId::new("drop"))
            .await
            .unwrap();
        drop(w);

        let before = replay_open_intents(&path).await.unwrap();
        let cids_before: Vec<_> = before.iter().map(|o| o.cid.as_str().to_string()).collect();
        assert_eq!(cids_before, vec!["keep-a", "keep-b"]);

        let stats = compact(&path).await.unwrap();
        assert_eq!(stats.entries_before, 4);
        assert_eq!(stats.entries_after, 2);
        assert!(
            stats.bytes_after < stats.bytes_before,
            "compacted log must be smaller, got {stats:?}"
        );

        let after = replay_open_intents(&path).await.unwrap();
        let cids_after: Vec<_> = after.iter().map(|o| o.cid.as_str().to_string()).collect();
        assert_eq!(cids_after, cids_before);

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn compact_is_idempotent() {
        // Running compaction twice in a row must leave the file byte-
        // identical after the second run. A non-idempotent compactor
        // would mask bugs and make the stats output unreliable.
        let path = unique_path("compact-idem");
        let mut w = IntentLogWriter::open(&path).await.unwrap();
        w.record_submit(&order("a")).await.unwrap();
        w.record_submit(&order("b")).await.unwrap();
        w.record_complete(&ClientOrderId::new("a")).await.unwrap();
        drop(w);

        let first = compact(&path).await.unwrap();
        assert_eq!(first.entries_after, 1);
        let bytes_after_first = tokio::fs::read(&path).await.unwrap();

        let second = compact(&path).await.unwrap();
        // entries_before on the second call equals entries_after on
        // the first — the log already contains nothing but live
        // submits and there was nothing more to reclaim.
        assert_eq!(second.entries_before, first.entries_after);
        assert_eq!(second.entries_after, first.entries_after);
        let bytes_after_second = tokio::fs::read(&path).await.unwrap();
        assert_eq!(bytes_after_first, bytes_after_second);

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn compact_all_closed_yields_empty_but_valid_log() {
        // Every submit was matched by a tombstone, so post-compaction
        // the log must exist and be zero-length. An empty but present
        // file is important: the next writer opens it in append mode
        // and finds no stale bytes to confuse replay.
        let path = unique_path("compact-empty");
        let mut w = IntentLogWriter::open(&path).await.unwrap();
        for cid in ["x", "y"] {
            w.record_submit(&order(cid)).await.unwrap();
            w.record_complete(&ClientOrderId::new(cid)).await.unwrap();
        }
        drop(w);

        let stats = compact(&path).await.unwrap();
        assert_eq!(stats.entries_before, 4);
        assert_eq!(stats.entries_after, 0);
        assert_eq!(stats.bytes_after, 0);
        assert!(tokio::fs::try_exists(&path).await.unwrap());

        let open = replay_open_intents(&path).await.unwrap();
        assert!(open.is_empty());

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn compact_clobbers_stale_sibling_compact_file() {
        // Simulate a crash mid-compact: a previous run left a
        // `*.compact` file lying around. The next compaction must
        // clobber it rather than erroring out or appending to it, or
        // we'd leave stale garbage in the namespace forever.
        let path = unique_path("compact-stale");
        let mut w = IntentLogWriter::open(&path).await.unwrap();
        w.record_submit(&order("live")).await.unwrap();
        drop(w);

        let mut tmp_name = path.file_name().unwrap().to_os_string();
        tmp_name.push(".compact");
        let stale = path.with_file_name(tmp_name);
        tokio::fs::write(&stale, b"garbage-from-previous-crash\n")
            .await
            .unwrap();

        let stats = compact(&path).await.unwrap();
        assert_eq!(stats.entries_after, 1);
        // The rename moved tmp over `path`, so the stale sibling is
        // gone — it became `path`.
        assert!(!tokio::fs::try_exists(&stale).await.unwrap());

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn compact_followed_by_append_still_replays_correctly() {
        // End-to-end: compact, reopen the writer, append a new
        // submit, crash, replay. The orphan set must include both the
        // pre-compact survivors and the post-compact addition.
        let path = unique_path("compact-append");
        {
            let mut w = IntentLogWriter::open(&path).await.unwrap();
            w.record_submit(&order("survivor")).await.unwrap();
            w.record_submit(&order("evicted")).await.unwrap();
            w.record_complete(&ClientOrderId::new("evicted"))
                .await
                .unwrap();
        }
        let _ = compact(&path).await.unwrap();
        {
            let mut w = IntentLogWriter::open(&path).await.unwrap();
            w.record_submit(&order("post-compact")).await.unwrap();
        }

        let open = replay_open_intents(&path).await.unwrap();
        let cids: Vec<_> = open.iter().map(|o| o.cid.as_str().to_string()).collect();
        assert_eq!(cids, vec!["post-compact", "survivor"]);

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
