//! Filesystem-based kill switch trigger.
//!
//! [`spawn_halt_file_watcher`] polls a configured path at a fixed
//! cadence. When the file appears, the task calls
//! [`KillSwitch::trip`] with [`TripReason::Manual`] and exits. Operators
//! halt trading by creating the file (`touch`), and rearm by deleting
//! the file and calling [`KillSwitch::reset`] through whatever
//! management channel the deployment exposes.
//!
//! Polling beats inotify here because the watcher's whole purpose is
//! to survive partial failures: a path on a mounted volume that
//! disappears or a filesystem that doesn't emit events is still
//! poll-visible.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{info, warn};

use ts_risk::{KillSwitch, TripReason};

/// Spawn a task that trips `ks` once `path` exists. Polls every
/// `period`; returns the join handle so the caller can abort on
/// shutdown. A zero period is clamped to 100 ms.
pub fn spawn_halt_file_watcher(
    ks: Arc<KillSwitch>,
    path: PathBuf,
    period: Duration,
) -> JoinHandle<()> {
    let period = if period.is_zero() {
        Duration::from_millis(100)
    } else {
        period
    };
    tokio::spawn(async move {
        info!(
            path = %path.display(),
            ?period,
            "halt-file watcher armed"
        );
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if ks.tripped() {
                // Someone else tripped; watcher's work is done.
                return;
            }
            match tokio::fs::try_exists(&path).await {
                Ok(true) => {
                    warn!(path = %path.display(), "halt file present; tripping kill switch");
                    ks.trip(TripReason::Manual);
                    return;
                }
                Ok(false) => {}
                Err(err) => {
                    // Surface the error once per scan but keep polling
                    // — a transient EACCES shouldn't knock the watcher
                    // offline permanently.
                    warn!(path = %path.display(), error = %err, "halt-file stat failed");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::time::{sleep, timeout};

    fn unique_path(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("ts-halt-{}-{}-{}.lock", std::process::id(), tag, n))
    }

    #[tokio::test]
    async fn trips_when_halt_file_appears() {
        let path = unique_path("appears");
        let ks = Arc::new(KillSwitch::default());
        let task =
            spawn_halt_file_watcher(Arc::clone(&ks), path.clone(), Duration::from_millis(20));

        // File absent → no trip for a beat.
        sleep(Duration::from_millis(60)).await;
        assert!(!ks.tripped());

        tokio::fs::write(&path, b"halt").await.unwrap();
        timeout(Duration::from_millis(500), async {
            while !ks.tripped() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("kill switch should trip once file is present");

        assert_eq!(ks.reason(), Some(TripReason::Manual));
        let _ = task.await;
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn exits_if_switch_is_already_tripped() {
        let path = unique_path("preempted");
        let ks = Arc::new(KillSwitch::default());
        ks.trip(TripReason::External);
        let task = spawn_halt_file_watcher(Arc::clone(&ks), path, Duration::from_millis(10));
        timeout(Duration::from_millis(200), task)
            .await
            .expect("watcher should exit promptly when switch is already tripped")
            .unwrap();
    }
}
