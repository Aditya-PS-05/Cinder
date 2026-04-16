//! Prometheus-style runtime metrics for [`crate::EngineRunner`].
//!
//! `RunnerMetrics` holds a small set of atomic counters and gauges that
//! mirror a [`ts_replay::ReplaySummary`] — the runner calls
//! [`RunnerMetrics::observe`] after every processed event so scrapes
//! always see a recent view of cumulative event counts, order-status
//! totals, position, and PnL.
//!
//! A tiny hand-rolled HTTP server serves `/metrics` over plain TCP.
//! The server speaks only the fragment of HTTP/1.1 required for a
//! single-shot GET and closes the connection after each response; it
//! is intended for Prometheus-style scrape polling, not general
//! traffic.

use std::fmt::Write as _;
use std::io;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use ts_replay::ReplaySummary;

/// Lock-free view of the runner's cumulative state.
#[derive(Debug, Default)]
pub struct RunnerMetrics {
    events_ingested: AtomicU64,
    book_updates: AtomicU64,
    orders_submitted: AtomicU64,
    orders_new: AtomicU64,
    orders_filled: AtomicU64,
    orders_canceled: AtomicU64,
    orders_rejected: AtomicU64,
    fills: AtomicU64,

    position: AtomicI64,
    realized_pnl: AtomicI64,
    unrealized_pnl: AtomicI64,
    total_pnl: AtomicI64,
    mark_price: AtomicI64,
    mark_known: AtomicU64,
}

impl RunnerMetrics {
    /// Allocate a fresh, zeroed metrics handle.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Copy every field of `summary` into the atomics with `Relaxed`
    /// ordering. Readers see a possibly-inconsistent slice across
    /// fields; that's acceptable for a scrape endpoint and avoids the
    /// lock a snapshot guarantee would require.
    pub fn observe(&self, summary: &ReplaySummary) {
        let m = &summary.metrics;
        self.events_ingested
            .store(m.events_ingested, Ordering::Relaxed);
        self.book_updates.store(m.book_updates, Ordering::Relaxed);
        self.orders_submitted
            .store(m.orders_submitted, Ordering::Relaxed);
        self.orders_new.store(m.orders_new, Ordering::Relaxed);
        self.orders_filled.store(m.orders_filled, Ordering::Relaxed);
        self.orders_canceled
            .store(m.orders_canceled, Ordering::Relaxed);
        self.orders_rejected
            .store(m.orders_rejected, Ordering::Relaxed);
        self.fills.store(m.fills, Ordering::Relaxed);
        self.position.store(summary.position, Ordering::Relaxed);
        self.realized_pnl
            .store(summary.realized as i64, Ordering::Relaxed);
        self.unrealized_pnl
            .store(summary.unrealized as i64, Ordering::Relaxed);
        self.total_pnl
            .store(summary.total_pnl as i64, Ordering::Relaxed);
        match summary.mark {
            Some(p) => {
                self.mark_price.store(p.0, Ordering::Relaxed);
                self.mark_known.store(1, Ordering::Relaxed);
            }
            None => {
                self.mark_known.store(0, Ordering::Relaxed);
            }
        }
    }

    /// Render the current snapshot in Prometheus text exposition
    /// format (version 0.0.4). Each series carries a `# HELP` and a
    /// `# TYPE` line.
    pub fn encode_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);
        counter(
            &mut out,
            "ts_events_ingested_total",
            "Total market events processed by the runner.",
            self.events_ingested.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "ts_book_updates_total",
            "Total book updates observed.",
            self.book_updates.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "ts_orders_submitted_total",
            "Total orders submitted to the engine.",
            self.orders_submitted.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "ts_orders_new_total",
            "Orders acknowledged with status=New.",
            self.orders_new.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "ts_orders_filled_total",
            "Orders with terminal status=Filled.",
            self.orders_filled.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "ts_orders_canceled_total",
            "Orders canceled (strategy or risk).",
            self.orders_canceled.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "ts_orders_rejected_total",
            "Orders rejected by the risk engine.",
            self.orders_rejected.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "ts_fills_total",
            "Total fills observed.",
            self.fills.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "ts_position",
            "Current signed position (qty-scale mantissa).",
            self.position.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "ts_realized_pnl",
            "Realized PnL (price*qty mantissa).",
            self.realized_pnl.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "ts_unrealized_pnl",
            "Unrealized PnL marked to the latest mid.",
            self.unrealized_pnl.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "ts_total_pnl",
            "Realized + unrealized PnL.",
            self.total_pnl.load(Ordering::Relaxed),
        );
        if self.mark_known.load(Ordering::Relaxed) != 0 {
            gauge(
                &mut out,
                "ts_mark_price",
                "Last mark price used for PnL (price-scale mantissa).",
                self.mark_price.load(Ordering::Relaxed),
            );
        }
        out
    }
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

fn gauge(out: &mut String, name: &str, help: &str, value: i64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

/// Serve `/metrics` on `listener` using `metrics` as the data source.
/// Each accepted connection is handled on its own task and closed
/// after a single response. The task only exits on an `accept` error.
pub fn spawn_metrics_server(
    listener: TcpListener,
    metrics: Arc<RunnerMetrics>,
) -> JoinHandle<io::Result<()>> {
    tokio::spawn(async move {
        loop {
            let (mut sock, _peer) = listener.accept().await?;
            let m = Arc::clone(&metrics);
            tokio::spawn(async move {
                if let Err(err) = handle_conn(&mut sock, &m).await {
                    tracing::debug!(error = %err, "metrics conn closed with error");
                }
            });
        }
    })
}

async fn handle_conn(sock: &mut TcpStream, metrics: &RunnerMetrics) -> io::Result<()> {
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 512];

    // Bounded read: stop at end-of-headers or 8 KB, timing out after a
    // couple of seconds to keep stalled clients from pinning tasks.
    let read_deadline = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(read_deadline);
    loop {
        tokio::select! {
            biased;
            _ = &mut read_deadline => break,
            res = sock.read(&mut tmp) => {
                match res {
                    Ok(0) => break,
                    Ok(n) => {
                        buf.extend_from_slice(&tmp[..n]);
                        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 8192 {
                            break;
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }

    let req = std::str::from_utf8(&buf).unwrap_or("");
    let first = req.lines().next().unwrap_or("");
    let path = first.split_whitespace().nth(1).unwrap_or("/");

    let (status, ctype, body): (&str, &str, String) = match path {
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4",
            metrics.encode_prometheus(),
        ),
        "/" | "/health" => ("200 OK", "text/plain", "ts-runner ok\n".to_string()),
        _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
    };

    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(head.as_bytes()).await?;
    sock.write_all(body.as_bytes()).await?;
    sock.shutdown().await.ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ts_core::Price;
    use ts_replay::{ReplayMetrics, ReplaySummary};

    fn summary(events: u64, position: i64, mark: Option<i64>) -> ReplaySummary {
        ReplaySummary {
            metrics: ReplayMetrics {
                events_ingested: events,
                book_updates: 1,
                orders_submitted: 2,
                orders_filled: 1,
                orders_partially_filled: 0,
                orders_rejected: 0,
                orders_canceled: 0,
                orders_new: 2,
                fills: 1,
                gross_filled_qty: 5,
                gross_notional: 550,
            },
            position,
            avg_entry: None,
            realized: 100,
            unrealized: 50,
            total_pnl: 150,
            mark: mark.map(Price),
        }
    }

    #[test]
    fn encoder_emits_required_fields() {
        let m = RunnerMetrics::new();
        m.observe(&summary(7, 3, Some(11_000)));
        let body = m.encode_prometheus();

        for needle in [
            "# TYPE ts_events_ingested_total counter",
            "ts_events_ingested_total 7",
            "# TYPE ts_position gauge",
            "ts_position 3",
            "# TYPE ts_mark_price gauge",
            "ts_mark_price 11000",
            "ts_total_pnl 150",
        ] {
            assert!(
                body.contains(needle),
                "expected body to contain `{needle}`, got:\n{body}"
            );
        }
    }

    #[test]
    fn mark_price_omitted_when_unknown() {
        let m = RunnerMetrics::new();
        m.observe(&summary(1, 0, None));
        let body = m.encode_prometheus();
        assert!(
            !body.contains("ts_mark_price"),
            "mark should be hidden when None, got:\n{body}"
        );
    }

    #[tokio::test]
    async fn server_returns_metrics_body() {
        let metrics = RunnerMetrics::new();
        metrics.observe(&summary(42, -1, Some(9_000)));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = spawn_metrics_server(listener, Arc::clone(&metrics));

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        client.read_to_end(&mut resp).await.unwrap();
        let text = String::from_utf8_lossy(&resp);

        assert!(text.starts_with("HTTP/1.1 200 OK"), "got:\n{text}");
        assert!(
            text.contains("ts_events_ingested_total 42"),
            "body missing counter, got:\n{text}"
        );
        assert!(text.contains("ts_position -1"), "got:\n{text}");

        server.abort();
    }

    #[tokio::test]
    async fn server_404s_unknown_paths() {
        let metrics = RunnerMetrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = spawn_metrics_server(listener, metrics);

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /nope HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        client.read_to_end(&mut resp).await.unwrap();
        let text = String::from_utf8_lossy(&resp);
        assert!(text.starts_with("HTTP/1.1 404"), "got:\n{text}");

        server.abort();
    }
}
