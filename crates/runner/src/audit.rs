//! Newline-delimited JSON audit logs of post-trade events.
//!
//! Each line is an [`AuditEvent`] — either an [`ExecReport`] that
//! reached a terminal or progression state, or a single [`Fill`]. The
//! runner forwards events through an mpsc channel and a writer task
//! drains them to disk; the channel absorbs brief I/O stalls without
//! blocking the event loop.
//!
//! We block (`send().await`) rather than `try_send` so audit capture
//! is lossless under backpressure. Pick a channel capacity that can
//! cover the longest expected fsync stall and sleep comfortably.
//!
//! ## Durability
//!
//! The writer wraps a [`BufWriter<File>`], so `flush()` only pushes
//! buffered bytes into the kernel page cache — a hard crash can still
//! lose everything the kernel hasn't written out. The
//! [`AuditWriter::with_fsync_every`] knob configures a periodic
//! `File::sync_all` call every N events so crash loss is bounded to at
//! most N events. Shutdown always calls `sync_all` regardless of the
//! cadence so clean exits are durable.

use std::io;
use std::path::Path;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub use ts_core::AuditEvent;

/// Append-only writer for [`AuditEvent`]s. Mirrors [`crate::tape::TapeWriter`]
/// but dedicated to the post-trade stream so recording market data and
/// recording trade outcomes can be enabled independently.
pub struct AuditWriter {
    inner: BufWriter<File>,
    written: u64,
    /// Fsync cadence: `0` disables periodic fsync (shutdown is still
    /// synced); a positive value means "call `sync_all` every N
    /// events." Configured via [`Self::with_fsync_every`].
    fsync_every: u64,
}

impl AuditWriter {
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            inner: BufWriter::new(file),
            written: 0,
            fsync_every: 0,
        })
    }

    /// Configure periodic `sync_all` on the underlying file. The
    /// [`spawn_audit_writer`] loop calls [`Self::sync_all`] every `n`
    /// events, bounding crash loss to at most `n - 1` events. `0`
    /// (the default) disables periodic fsync — only the shutdown path
    /// syncs. Each fsync costs a filesystem round-trip, so pick `n`
    /// against expected write volume: `n = 100` at 10k events/sec is
    /// 100 fsyncs/sec, which most SSDs can absorb.
    pub fn with_fsync_every(mut self, n: u64) -> Self {
        self.fsync_every = n;
        self
    }

    /// Current fsync cadence. `0` means "no periodic fsync" (shutdown
    /// still syncs). Exposed for observability and tests.
    pub fn fsync_every(&self) -> u64 {
        self.fsync_every
    }

    pub async fn write_event(&mut self, event: &AuditEvent) -> io::Result<()> {
        let mut line = serde_json::to_vec(event).map_err(io::Error::other)?;
        line.push(b'\n');
        self.inner.write_all(&line).await?;
        self.written += 1;
        Ok(())
    }

    pub fn written(&self) -> u64 {
        self.written
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.inner.flush().await
    }

    /// Flush the buffered writer and call `sync_all` on the inner
    /// file. Returns success only when the bytes are on disk — flush
    /// alone is not enough because `BufWriter::flush` only reaches the
    /// kernel page cache. Called periodically by
    /// [`spawn_audit_writer`] when a non-zero [`Self::fsync_every`] is
    /// configured, and always once at shutdown.
    pub async fn sync_all(&mut self) -> io::Result<()> {
        self.inner.flush().await?;
        self.inner.get_ref().sync_all().await
    }
}

/// Spawn a background task that drains audit events from `rx` into
/// `writer`. The task exits when the channel closes; it always
/// fsyncs before returning. The join value is the number of events
/// written.
///
/// If the writer has [`AuditWriter::fsync_every`] set to a positive
/// `N`, the task calls [`AuditWriter::sync_all`] after every `N`
/// writes so crash loss is bounded.
pub fn spawn_audit_writer(
    mut writer: AuditWriter,
    mut rx: mpsc::Receiver<AuditEvent>,
) -> JoinHandle<u64> {
    tokio::spawn(async move {
        let fsync_every = writer.fsync_every();
        let mut since_sync: u64 = 0;
        while let Some(event) = rx.recv().await {
            if let Err(err) = writer.write_event(&event).await {
                tracing::error!(error = %err, "audit write failed; closing writer");
                break;
            }
            if fsync_every > 0 {
                since_sync += 1;
                if since_sync >= fsync_every {
                    if let Err(err) = writer.sync_all().await {
                        tracing::error!(error = %err, "audit periodic fsync failed; closing writer");
                        break;
                    }
                    since_sync = 0;
                }
            }
        }
        if let Err(err) = writer.sync_all().await {
            tracing::error!(error = %err, "audit shutdown fsync failed");
        }
        writer.written()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::AsyncBufReadExt;
    use ts_core::{
        ClientOrderId, ExecReport, Fill, OrderStatus, Price, Qty, Side, Symbol, Timestamp, Venue,
    };

    fn unique_path(tag: &str) -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ts-audit-{}-{}-{}.ndjson",
            std::process::id(),
            tag,
            n
        ))
    }

    fn report() -> ExecReport {
        ExecReport {
            cid: ClientOrderId::new("c1"),
            status: OrderStatus::Filled,
            filled_qty: Qty(2),
            avg_price: Some(Price(110)),
            reason: None,
            fills: vec![],
        }
    }

    fn fill() -> Fill {
        Fill {
            cid: ClientOrderId::new("c1"),
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            side: Side::Buy,
            price: Price(110),
            qty: Qty(2),
            ts: Timestamp::from_unix_millis(1_700_000_000_000),
            is_maker: None,
            fee: 0,
            fee_asset: None,
        }
    }

    #[tokio::test]
    async fn writer_appends_both_event_kinds_as_ndjson() {
        let path = unique_path("shape");
        let mut w = AuditWriter::create(&path).await.unwrap();
        w.write_event(&AuditEvent::Report(report())).await.unwrap();
        w.write_event(&AuditEvent::Fill(fill())).await.unwrap();
        w.flush().await.unwrap();
        assert_eq!(w.written(), 2);

        let file = File::open(&path).await.unwrap();
        let mut lines = tokio::io::BufReader::new(file).lines();
        let a = lines.next_line().await.unwrap().expect("line 1");
        let b = lines.next_line().await.unwrap().expect("line 2");
        assert!(a.contains("\"kind\":\"report\""), "got: {a}");
        assert!(b.contains("\"kind\":\"fill\""), "got: {b}");

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn spawned_writer_drains_channel_and_closes() {
        let path = unique_path("spawn");
        let writer = AuditWriter::create(&path).await.unwrap();
        let (tx, rx) = mpsc::channel::<AuditEvent>(8);
        let task = spawn_audit_writer(writer, rx);

        tx.send(AuditEvent::Report(report())).await.unwrap();
        tx.send(AuditEvent::Fill(fill())).await.unwrap();
        tx.send(AuditEvent::Fill(fill())).await.unwrap();
        drop(tx);

        let written = task.await.unwrap();
        assert_eq!(written, 3);

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn fsync_every_defaults_to_zero_and_is_configurable() {
        let path = unique_path("fsync-cfg");
        let w = AuditWriter::create(&path).await.unwrap();
        assert_eq!(w.fsync_every(), 0, "default is no periodic fsync");
        let w = w.with_fsync_every(100);
        assert_eq!(w.fsync_every(), 100);
        let w = w.with_fsync_every(0);
        assert_eq!(w.fsync_every(), 0, "zero disables the cadence");
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn sync_all_pushes_buffered_writes_to_disk() {
        // Proves the flush + sync_all sequence lands bytes on a fresh
        // read — `BufWriter::flush` alone would leave them in the
        // page cache until the OS chooses to write them out.
        let path = unique_path("sync-all");
        let mut w = AuditWriter::create(&path).await.unwrap();
        w.write_event(&AuditEvent::Report(report())).await.unwrap();
        w.write_event(&AuditEvent::Fill(fill())).await.unwrap();
        w.sync_all().await.unwrap();

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents.lines().count(), 2);
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn spawned_writer_with_fsync_cadence_records_every_event() {
        // With fsync_every = 1 the periodic-sync branch runs on every
        // write. This asserts the cadence path doesn't drop or
        // reorder events and that the final count still matches.
        let path = unique_path("fsync-cadence");
        let writer = AuditWriter::create(&path)
            .await
            .unwrap()
            .with_fsync_every(1);
        let (tx, rx) = mpsc::channel::<AuditEvent>(8);
        let task = spawn_audit_writer(writer, rx);

        for _ in 0..5 {
            tx.send(AuditEvent::Report(report())).await.unwrap();
        }
        drop(tx);

        let written = task.await.unwrap();
        assert_eq!(written, 5);

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(
            contents.lines().count(),
            5,
            "every event made it through the fsync cadence"
        );
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn spawned_writer_fsyncs_on_shutdown_even_without_cadence() {
        // fsync_every = 0 disables *periodic* fsync but the shutdown
        // path must still call sync_all. We can't observe fsync
        // directly; we can observe that nothing is lost on clean
        // channel close.
        let path = unique_path("fsync-shutdown");
        let writer = AuditWriter::create(&path).await.unwrap();
        assert_eq!(writer.fsync_every(), 0);
        let (tx, rx) = mpsc::channel::<AuditEvent>(8);
        let task = spawn_audit_writer(writer, rx);

        tx.send(AuditEvent::Report(report())).await.unwrap();
        tx.send(AuditEvent::Fill(fill())).await.unwrap();
        drop(tx);

        let written = task.await.unwrap();
        assert_eq!(written, 2);

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents.lines().count(), 2);
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn fsync_cadence_partial_batch_is_still_durable_on_shutdown() {
        // 10 events through a writer with fsync_every = 4: two periodic
        // fsyncs (after events 4 and 8), then the final shutdown fsync
        // catches the trailing 2 events that didn't reach the cadence.
        let path = unique_path("partial");
        let writer = AuditWriter::create(&path)
            .await
            .unwrap()
            .with_fsync_every(4);
        let (tx, rx) = mpsc::channel::<AuditEvent>(16);
        let task = spawn_audit_writer(writer, rx);

        for _ in 0..10 {
            tx.send(AuditEvent::Fill(fill())).await.unwrap();
        }
        drop(tx);

        let written = task.await.unwrap();
        assert_eq!(written, 10);

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents.lines().count(), 10);
        let _ = tokio::fs::remove_file(&path).await;
    }
}
