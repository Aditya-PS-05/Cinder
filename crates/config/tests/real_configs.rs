//! Integration test: load the actual `config/*.yaml` files in the repo so
//! we catch schema drift the moment it happens.

use std::path::PathBuf;

use serde::Deserialize;
use ts_config::{Env, Loader};

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct FullCfg {
    service: Service,
    postgres: Postgres,
    redis: Redis,
    clickhouse: ClickHouse,
    nats: Nats,
    observability: Observability,
    risk: Risk,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Service {
    name: String,
    log_level: String,
    log_format: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Postgres {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    max_open_conns: u32,
    max_idle_conns: u32,
    conn_max_lifetime: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Redis {
    addr: String,
    db: u8,
    pool_size: u32,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ClickHouse {
    addr: String,
    database: String,
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Nats {
    url: String,
    cluster_id: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Observability {
    metrics_addr: String,
    tracing_endpoint: String,
    tracing_sample_rate: f64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Risk {
    kill_switch_enabled: bool,
    max_daily_loss_usd: f64,
    max_position_notional_usd: f64,
}

fn repo_config_dir() -> PathBuf {
    // tests/real_configs.rs -> ../../config
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.join("..").join("..").join("config")
}

#[test]
fn loads_for_every_environment() {
    let dir = repo_config_dir();
    for env in [Env::Dev, Env::Staging, Env::Prod] {
        let cfg: FullCfg = Loader::new(&dir, env)
            .load()
            .unwrap_or_else(|e| panic!("load {}: {e}", env.as_str()));
        assert!(
            !cfg.service.name.is_empty(),
            "{}: service.name",
            env.as_str()
        );
        assert!(cfg.postgres.port != 0, "{}: postgres.port", env.as_str());
    }
}

#[test]
fn dev_overlay_lowers_risk_limits() {
    let cfg: FullCfg = Loader::new(repo_config_dir(), Env::Dev).load().unwrap();
    assert_eq!(
        cfg.risk.max_daily_loss_usd, 50.0,
        "dev overlay should cap daily loss at $50"
    );
    assert_eq!(
        cfg.risk.max_position_notional_usd, 200.0,
        "dev overlay should cap notional at $200"
    );
}

#[test]
fn prod_overlay_wins_over_base_for_pool_size() {
    let cfg: FullCfg = Loader::new(repo_config_dir(), Env::Prod).load().unwrap();
    assert_eq!(cfg.redis.pool_size, 200);
    assert_eq!(cfg.postgres.max_open_conns, 100);
}
