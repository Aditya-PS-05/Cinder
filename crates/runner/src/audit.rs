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
        })
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
}

/// Spawn a background task that drains audit events from `rx` into
/// `writer`. The task exits when the channel closes; it always flushes
/// before returning. The join value is the number of events written.
pub fn spawn_audit_writer(
    mut writer: AuditWriter,
    mut rx: mpsc::Receiver<AuditEvent>,
) -> JoinHandle<u64> {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if let Err(err) = writer.write_event(&event).await {
                tracing::error!(error = %err, "audit write failed; closing writer");
                break;
            }
        }
        let _ = writer.flush().await;
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
}
