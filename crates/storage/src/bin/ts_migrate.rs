//! `ts-migrate` CLI: loads the layered app config, dials Postgres +
//! ClickHouse, and applies `infra/migrations/{postgres,clickhouse}`.
//!
//! Subcommands:
//!   up       apply pending migrations to both backends
//!   status   list applied migrations
//!   validate parse files on disk and print checksums (no network)

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

use ts_config::{Env as CfgEnv, Loader};
use ts_storage::{
    clickhouse::{migrate as ch_migrate, ClickHouse},
    config::{ClickHouseCfg, PostgresCfg},
    migrate::load_from_dir,
    postgres::{migrate as pg_migrate, Postgres},
};

#[derive(Debug, Parser)]
#[command(
    name = "ts-migrate",
    about = "Apply Postgres + ClickHouse schema migrations"
)]
struct Cli {
    /// Path to the config directory.
    #[arg(long, default_value = "./config")]
    config: PathBuf,

    /// Environment name: dev|staging|prod.
    #[arg(long, default_value = "dev")]
    env: String,

    /// Root directory holding postgres/ and clickhouse/ subdirs of .sql files.
    #[arg(long, default_value = "./infra/migrations")]
    migrations: PathBuf,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Apply every pending migration.
    Up,
    /// Print applied migrations per backend.
    Status,
    /// Parse the migration directory and print filenames + checksums.
    Validate,
}

#[derive(Debug, Deserialize)]
struct AppCfg {
    postgres: PostgresCfg,
    clickhouse: ClickHouseCfg,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let env = CfgEnv::parse(&cli.env).with_context(|| format!("bad env {}", cli.env))?;

    let pg_dir = cli.migrations.join("postgres");
    let ch_dir = cli.migrations.join("clickhouse");

    match cli.cmd {
        Cmd::Validate => {
            let pg_mig = load_from_dir(&pg_dir)
                .await
                .context("load postgres migrations")?;
            let ch_mig = load_from_dir(&ch_dir)
                .await
                .context("load clickhouse migrations")?;

            println!("postgres: {} migration file(s)", pg_mig.len());
            for m in &pg_mig {
                println!(
                    "  V{:03} {:<40} sha256={}",
                    m.version,
                    m.name,
                    &m.sha256[..12]
                );
            }
            println!("clickhouse: {} migration file(s)", ch_mig.len());
            for m in &ch_mig {
                println!(
                    "  V{:03} {:<40} sha256={}",
                    m.version,
                    m.name,
                    &m.sha256[..12]
                );
            }
            return Ok(());
        }
        Cmd::Up | Cmd::Status => {}
    }

    let cfg: AppCfg = Loader::new(&cli.config, env)
        .load()
        .context("load config")?;

    let pg = Postgres::connect(&cfg.postgres)
        .await
        .context("connect postgres")?;
    let ch = ClickHouse::connect(&cfg.clickhouse)
        .await
        .context("connect clickhouse")?;

    match cli.cmd {
        Cmd::Up => {
            let pg_mig = load_from_dir(&pg_dir)
                .await
                .context("load postgres migrations")?;
            let applied = pg_migrate::apply_all(&pg, &pg_mig)
                .await
                .context("apply postgres")?;
            println!(
                "postgres: applied {} new migration(s) {:?}",
                applied.len(),
                applied
            );

            let ch_mig = load_from_dir(&ch_dir)
                .await
                .context("load clickhouse migrations")?;
            let applied = ch_migrate::apply_all(&ch, &ch_mig)
                .await
                .context("apply clickhouse")?;
            println!(
                "clickhouse: applied {} new migration(s) {:?}",
                applied.len(),
                applied
            );
        }
        Cmd::Status => {
            let pg_rows = pg_migrate::status(&pg).await?;
            println!("postgres ({} applied):", pg_rows.len());
            for (v, n) in pg_rows {
                println!("  V{v:03} {n}");
            }

            let ch_rows = ch_migrate::status(&ch).await?;
            println!("clickhouse ({} applied):", ch_rows.len());
            for (v, n) in ch_rows {
                println!("  V{v:03} {n}");
            }
        }
        Cmd::Validate => unreachable!(),
    }

    Ok(())
}
