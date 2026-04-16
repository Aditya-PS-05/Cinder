//! Newline-delimited JSON tapes of [`MarketEvent`]s.
//!
//! A tape is a plain text file with one [`MarketEvent`] per line.
//! [`TapeWriter`] appends events as they arrive; [`TapeReader`]
//! streams them back in order. The format is intentionally boring
//! (`serde_json`) so the files are inspectable with `jq` and easy to
//! diff across runs.
//!
//! Tapes are used to capture real venue traffic for later replay
//! through the same [`crate::EngineRunner`] that drives live paper
//! trading — the engine cannot tell a replayed tape from a live
//! feed, which is what makes the harness trustworthy.

use std::io;
use std::path::Path;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc;

use ts_core::MarketEvent;

/// Appends [`MarketEvent`]s as newline-delimited JSON. Callers should
/// [`Self::flush`] before dropping to ensure the buffer reaches disk.
pub struct TapeWriter {
    inner: BufWriter<File>,
    written: u64,
}

impl TapeWriter {
    /// Open (or create) `path` for append. Existing content is preserved.
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

    /// Append one event. A trailing newline is added.
    pub async fn write_event(&mut self, event: &MarketEvent) -> io::Result<()> {
        let mut line = serde_json::to_vec(event).map_err(io::Error::other)?;
        line.push(b'\n');
        self.inner.write_all(&line).await?;
        self.written += 1;
        Ok(())
    }

    /// Number of events appended since the writer was created.
    pub fn written(&self) -> u64 {
        self.written
    }

    /// Flush buffered bytes to the kernel. Use before shutdown.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.inner.flush().await
    }
}

/// Streams [`MarketEvent`]s from a tape file.
pub struct TapeReader {
    inner: BufReader<File>,
    read: u64,
}

impl TapeReader {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path).await?;
        Ok(Self {
            inner: BufReader::new(file),
            read: 0,
        })
    }

    /// Read one event, or `Ok(None)` at end of file. Malformed lines
    /// bubble up as an I/O error with `InvalidData`.
    pub async fn read_event(&mut self) -> io::Result<Option<MarketEvent>> {
        let mut line = String::new();
        loop {
            line.clear();
            let n = self.inner.read_line(&mut line).await?;
            if n == 0 {
                return Ok(None);
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let event: MarketEvent = serde_json::from_str(trimmed)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.read += 1;
            return Ok(Some(event));
        }
    }

    /// Number of events yielded by this reader so far.
    pub fn read_count(&self) -> u64 {
        self.read
    }
}

/// Drain a tape into an mpsc channel. Closes the channel by dropping
/// `tx` when the file ends or a read fails. Returns the count of events
/// forwarded, or the I/O error from an unrecoverable read.
pub async fn pump_tape(mut reader: TapeReader, tx: mpsc::Sender<MarketEvent>) -> io::Result<u64> {
    while let Some(event) = reader.read_event().await? {
        if tx.send(event).await.is_err() {
            // Receiver dropped; stop draining.
            break;
        }
    }
    Ok(reader.read_count())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use ts_core::{BookLevel, BookSnapshot, MarketPayload, Price, Qty, Symbol, Timestamp, Venue};

    fn unique_path(tag: &str) -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ts-tape-{}-{}-{}.ndjson",
            std::process::id(),
            tag,
            n
        ))
    }

    fn snap(bid: i64, ask: i64, seq: u64) -> MarketEvent {
        MarketEvent {
            venue: Venue::BINANCE,
            symbol: Symbol::from_static("BTCUSDT"),
            exchange_ts: Timestamp::from_unix_millis(1_700_000_000_000),
            local_ts: Timestamp::from_unix_millis(1_700_000_000_020),
            seq,
            payload: MarketPayload::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(bid),
                    qty: Qty(10),
                }],
                asks: vec![BookLevel {
                    price: Price(ask),
                    qty: Qty(10),
                }],
            }),
        }
    }

    #[tokio::test]
    async fn writer_reader_roundtrip_preserves_events() {
        let path = unique_path("roundtrip");
        {
            let mut w = TapeWriter::create(&path).await.unwrap();
            w.write_event(&snap(100, 110, 1)).await.unwrap();
            w.write_event(&snap(101, 111, 2)).await.unwrap();
            w.flush().await.unwrap();
            assert_eq!(w.written(), 2);
        }

        let mut r = TapeReader::open(&path).await.unwrap();
        let a = r.read_event().await.unwrap().expect("event 1");
        let b = r.read_event().await.unwrap().expect("event 2");
        let end = r.read_event().await.unwrap();
        assert_eq!(a.seq, 1);
        assert_eq!(b.seq, 2);
        assert!(end.is_none());
        assert_eq!(r.read_count(), 2);

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn reader_skips_blank_lines_and_reports_corrupt_ones() {
        let path = unique_path("corrupt");
        let valid = serde_json::to_string(&snap(1, 2, 7)).unwrap();
        let body = format!("\n{valid}\nnot-json\n");
        tokio::fs::write(&path, body).await.unwrap();

        let mut r = TapeReader::open(&path).await.unwrap();
        let first = r.read_event().await.unwrap().expect("valid line");
        assert_eq!(first.seq, 7);
        let err = r.read_event().await.expect_err("bad json must error");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn pump_tape_forwards_all_events_then_closes_channel() {
        let path = unique_path("pump");
        {
            let mut w = TapeWriter::create(&path).await.unwrap();
            for seq in 1..=3 {
                w.write_event(&snap(100, 110, seq)).await.unwrap();
            }
            w.flush().await.unwrap();
        }

        let reader = TapeReader::open(&path).await.unwrap();
        let (tx, mut rx) = mpsc::channel::<MarketEvent>(4);
        let task = tokio::spawn(pump_tape(reader, tx));

        let mut seqs = Vec::new();
        while let Some(e) = rx.recv().await {
            seqs.push(e.seq);
        }
        let count = task.await.unwrap().unwrap();
        assert_eq!(seqs, vec![1, 2, 3]);
        assert_eq!(count, 3);

        let _ = tokio::fs::remove_file(&path).await;
    }
}
