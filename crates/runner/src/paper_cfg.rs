//! Static configuration for the `ts-paper-run` binary.
//!
//! The struct layout mirrors the runtime knobs the binary exposes on the
//! command line: market identity, maker parameters, runner plumbing, and
//! an optional audit sink. Loading is delegated to [`ts_config::Loader`]
//! so the binary gets layered `base.yaml` + env overlay + `TS_*`
//! environment-variable overrides for free.
//!
//! CLI flags remain the outermost layer — any flag explicitly provided
//! on the command line wins over a value loaded from YAML.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use ts_config::{ConfigError, Env, Loader};

/// Root config tree.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct PaperCfg {
    pub market: MarketCfg,
    pub maker: MakerCfg,
    pub runner: RunnerCfg,
    #[serde(default)]
    pub audit: Option<AuditCfg>,
    #[serde(default)]
    pub metrics: Option<MetricsCfg>,
    #[serde(default)]
    pub kill_switch: Option<KillSwitchCfg>,
    #[serde(default)]
    pub risk: Option<RiskCfg>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MarketCfg {
    pub symbol: String,
    pub price_scale: u8,
    pub qty_scale: u8,
    pub ws_url: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MakerCfg {
    pub quote_qty: i64,
    pub half_spread_ticks: i64,
    /// Extra half-spread applied proportional to |book imbalance|.
    /// Zero disables the adverse-selection guard; older YAML configs
    /// that predate this field still load cleanly via the default.
    #[serde(default)]
    pub imbalance_widen_ticks: i64,
    /// EWMA decay factor for the mid-price volatility tracker. Must be
    /// in `(0, 1)` if `vol_widen_coeff > 0`, otherwise ignored.
    /// Defaults to 0.94 (RiskMetrics).
    #[serde(default = "default_vol_lambda")]
    pub vol_lambda: f64,
    /// Multiplier applied to EWMA sigma (in ticks) to compute extra
    /// half-spread. Zero disables the vol-aware widening path. Older
    /// YAML configs that predate this field load cleanly via the default.
    #[serde(default)]
    pub vol_widen_coeff: f64,
    pub inventory_skew_ticks: i64,
    pub max_inventory: i64,
    pub cid_prefix: String,
}

fn default_vol_lambda() -> f64 {
    0.94
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RunnerCfg {
    pub summary_secs: u64,
    pub channel: usize,
    /// Cadence at which the runner fires [`ts_strategy::Strategy::on_timer`],
    /// in milliseconds. `None` (the default) disables the timer tick
    /// entirely — strategies that don't override `on_timer` pay no
    /// overhead. Wired identically on `ts-paper-run` and `ts-live-run`.
    #[serde(default)]
    pub timer_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AuditCfg {
    pub path: PathBuf,
    /// `sync_all` cadence on the audit log, expressed in events. `0`
    /// (the default, or a missing key) disables periodic fsync —
    /// shutdown still syncs, but unclean exits may lose up to one
    /// BufWriter's worth of buffered writes. A positive value bounds
    /// crash loss to at most `fsync_every - 1` events at the cost of
    /// one fsync per batch. Sensible production values sit in the
    /// 50–500 range depending on event volume.
    #[serde(default)]
    pub fsync_every: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetricsCfg {
    /// "host:port" address the scrape endpoint listens on.
    pub listen: String,
}

/// Optional risk wiring. Holds both the PnL-guard thresholds (drawdown
/// / daily-loss) and the pre-trade gate (`pre_trade`). A missing section
/// leaves the runner with no guard and fully permissive pre-trade
/// checks. Mantissas match [`ts_pnl::Accountant`] — `price_scale *
/// qty_scale` for notional, `qty_scale` for position.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct RiskCfg {
    /// Drop from peak equity (realized-net + unrealized) that trips
    /// [`ts_risk::TripReason::MaxDrawdown`].
    #[serde(default)]
    pub max_drawdown: Option<i64>,
    /// Realized-net loss since day-start that trips
    /// [`ts_risk::TripReason::DailyLoss`].
    #[serde(default)]
    pub max_daily_loss: Option<i64>,
    /// Sliding-window length for the daily baseline, in seconds.
    /// Defaults to 24 h.
    #[serde(default = "default_day_length_secs")]
    pub day_length_secs: u64,
    /// Pre-trade gate knobs. `None` == permissive across the board.
    #[serde(default)]
    pub pre_trade: Option<PreTradeCfg>,
}

/// Pre-trade gate thresholds — consumed by `ts_oms::RiskConfig`.
/// Any field left unset inherits the permissive baseline so operators
/// can tighten one knob at a time without re-specifying the rest.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PreTradeCfg {
    /// Per-symbol absolute position cap (mantissa at `qty_scale`).
    #[serde(default)]
    pub max_position_qty: Option<i64>,
    /// Per-order notional cap (mantissa at `price_scale * qty_scale`).
    #[serde(default)]
    pub max_order_notional: Option<i64>,
    /// Global cap on concurrently-open orders.
    #[serde(default)]
    pub max_open_orders: Option<usize>,
    /// Allowed symbols. Omit or leave empty to allow all.
    #[serde(default)]
    pub whitelist: Option<Vec<String>>,
}

fn default_day_length_secs() -> u64 {
    24 * 60 * 60
}

/// Optional kill-switch wiring. A missing section leaves the runner
/// with no switch attached; an empty section enables defaults
/// (reject-rate trigger only, no halt-file watcher).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct KillSwitchCfg {
    /// Filesystem path polled by the halt-file watcher. Creating the
    /// file (e.g. `touch /tmp/ts-halt`) trips the switch. Omit to leave
    /// manual halt unmonitored.
    #[serde(default)]
    pub halt_file: Option<PathBuf>,
    /// Rejects inside `window_ms` that force the switch to trip.
    #[serde(default = "default_reject_threshold")]
    pub reject_threshold: u32,
    /// Sliding reject-window horizon, in milliseconds.
    #[serde(default = "default_window_ms")]
    pub window_ms: u64,
    /// Halt-file polling cadence, in milliseconds.
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
}

impl Default for KillSwitchCfg {
    fn default() -> Self {
        Self {
            halt_file: None,
            reject_threshold: default_reject_threshold(),
            window_ms: default_window_ms(),
            poll_ms: default_poll_ms(),
        }
    }
}

fn default_reject_threshold() -> u32 {
    10
}
fn default_window_ms() -> u64 {
    5_000
}
fn default_poll_ms() -> u64 {
    250
}

impl Default for MarketCfg {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".into(),
            price_scale: 2,
            qty_scale: 8,
            ws_url: "wss://stream.binance.com:9443/ws".into(),
        }
    }
}

impl Default for MakerCfg {
    fn default() -> Self {
        Self {
            quote_qty: 2,
            half_spread_ticks: 5,
            imbalance_widen_ticks: 0,
            vol_lambda: default_vol_lambda(),
            vol_widen_coeff: 0.0,
            inventory_skew_ticks: 1,
            max_inventory: 20,
            cid_prefix: "pr".into(),
        }
    }
}

impl Default for RunnerCfg {
    fn default() -> Self {
        Self {
            summary_secs: 5,
            channel: 4096,
            timer_ms: None,
        }
    }
}

impl PaperCfg {
    /// Load `<dir>/base.yaml` + `<dir>/<env>.yaml` through [`ts_config`].
    pub fn load(dir: impl Into<PathBuf>, env: Env) -> Result<Self, ConfigError> {
        Loader::new(dir, env).load::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmpdir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p =
            std::env::temp_dir().join(format!("ts-paper-cfg-{}-{}-{}", std::process::id(), tag, n));
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn write(path: &Path, body: &str) {
        fs::write(path, body).unwrap();
    }

    #[test]
    fn default_is_self_consistent() {
        let cfg = PaperCfg::default();
        assert_eq!(cfg.market.symbol, "BTCUSDT");
        assert_eq!(cfg.runner.channel, 4096);
        assert!(cfg.audit.is_none());
    }

    #[test]
    fn loads_full_base_and_env_overlay() {
        let dir = tmpdir("load");
        write(
            &dir.join("base.yaml"),
            r#"
market:
  symbol: BTCUSDT
  price_scale: 2
  qty_scale: 8
  ws_url: wss://stream.binance.com:9443/ws
maker:
  quote_qty: 2
  half_spread_ticks: 5
  inventory_skew_ticks: 1
  max_inventory: 20
  cid_prefix: pr
runner:
  summary_secs: 5
  channel: 4096
"#,
        );
        write(
            &dir.join("prod.yaml"),
            r#"
market:
  ws_url: wss://stream.binance.com:443/ws
maker:
  quote_qty: 3
  cid_prefix: live
"#,
        );

        let cfg = PaperCfg::load(&dir, Env::Prod).unwrap();
        assert_eq!(cfg.market.symbol, "BTCUSDT");
        assert_eq!(cfg.market.ws_url, "wss://stream.binance.com:443/ws");
        assert_eq!(cfg.maker.quote_qty, 3);
        assert_eq!(cfg.maker.cid_prefix, "live");
        assert_eq!(cfg.maker.half_spread_ticks, 5, "base preserved");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn pre_trade_round_trips() {
        let dir = tmpdir("pretrade");
        write(
            &dir.join("base.yaml"),
            r#"
market:
  symbol: BTCUSDT
  price_scale: 2
  qty_scale: 8
  ws_url: wss://x
maker:
  quote_qty: 1
  half_spread_ticks: 4
  inventory_skew_ticks: 0
  max_inventory: 10
  cid_prefix: t
runner:
  summary_secs: 0
  channel: 128
risk:
  max_drawdown: 5000
  pre_trade:
    max_position_qty: 100
    max_order_notional: 25000
    max_open_orders: 4
    whitelist: [BTCUSDT, ETHUSDT]
"#,
        );
        write(&dir.join("dev.yaml"), "{}\n");

        let cfg = PaperCfg::load(&dir, Env::Dev).unwrap();
        let risk = cfg.risk.as_ref().expect("risk section");
        assert_eq!(risk.max_drawdown, Some(5000));
        let pt = risk.pre_trade.as_ref().expect("pre_trade");
        assert_eq!(pt.max_position_qty, Some(100));
        assert_eq!(pt.max_order_notional, Some(25000));
        assert_eq!(pt.max_open_orders, Some(4));
        assert_eq!(
            pt.whitelist.as_deref(),
            Some(&["BTCUSDT".to_string(), "ETHUSDT".to_string()][..])
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn timer_ms_defaults_to_none_and_round_trips() {
        // Absent section → None (default impl + Deserialize default).
        assert!(RunnerCfg::default().timer_ms.is_none());

        let dir = tmpdir("timer-ms");
        write(
            &dir.join("base.yaml"),
            r#"
market:
  symbol: BTCUSDT
  price_scale: 2
  qty_scale: 8
  ws_url: wss://x
maker:
  quote_qty: 1
  half_spread_ticks: 4
  inventory_skew_ticks: 0
  max_inventory: 10
  cid_prefix: t
runner:
  summary_secs: 0
  channel: 128
  timer_ms: 250
"#,
        );
        write(&dir.join("dev.yaml"), "{}\n");
        let cfg = PaperCfg::load(&dir, Env::Dev).unwrap();
        assert_eq!(cfg.runner.timer_ms, Some(250));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn audit_sink_round_trips() {
        let dir = tmpdir("audit");
        write(
            &dir.join("base.yaml"),
            r#"
market:
  symbol: ETHUSDT
  price_scale: 2
  qty_scale: 8
  ws_url: wss://x
maker:
  quote_qty: 1
  half_spread_ticks: 4
  inventory_skew_ticks: 0
  max_inventory: 10
  cid_prefix: t
runner:
  summary_secs: 0
  channel: 128
audit:
  path: /tmp/out.ndjson
"#,
        );
        write(&dir.join("dev.yaml"), "{}\n");

        let cfg = PaperCfg::load(&dir, Env::Dev).unwrap();
        assert_eq!(
            cfg.audit.as_ref().map(|a| a.path.as_path()),
            Some(std::path::Path::new("/tmp/out.ndjson"))
        );
        fs::remove_dir_all(&dir).ok();
    }
}
