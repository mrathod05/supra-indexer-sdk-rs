//! Example indexer for `token_factory::TokenCreated` on Supra testnet.
//!
//! Configure via environment (see `.env.example`), then `cargo run -p sdk-example`.

mod schema;
mod token_created;

use std::env;
use std::net::SocketAddr;

use anyhow::{Context, Result};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use supra_indexer_sdk::prelude::*;
use tracing_subscriber::EnvFilter;

use crate::token_created::{EVENT_TYPE, TokenCreated, TokenCreatedStore};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let rpc_url = env_or("SUPRA_RPC_URL", "https://rpc-testnet.supra.com");
    let contract = env_or(
        "CONTRACT_ADDRESS",
        "0xa802ca2e7a6604e8a4cc3ef390bc137767b0cb04438525b0a3451e0bbd410ddf",
    );
    let start_block: u64 = env_or("START_BLOCK", "135000000")
        .parse()
        .context("START_BLOCK must be a number")?;
    let force_start = env_or("FORCE_START_BLOCK", "false") == "true";
    let http_addr: SocketAddr = env_or("HTTP_ADDR", "0.0.0.0:8080")
        .parse()
        .context("HTTP_ADDR must be host:port")?;

    // Indexer lock + HTTP readiness check + page writes.
    let pool = create_pool(&database_url, 5)?;

    // App tables are owned by the app; the SDK creates only its checkpoint table.
    {
        let mut conn = pool.get()?;
        conn.run_pending_migrations(MIGRATIONS)
            .map_err(|e| anyhow::anyhow!("migrations failed: {e}"))?;
    }

    let config = IndexerConfig::new(rpc_url, contract)
        .start_block(start_block)
        .force_start_block(force_start);

    let mut indexer = SupraIndexer::new(pool, config)?
        .serve(ServerConfig::new(http_addr).admin_token(env::var("ADMIN_TOKEN").ok()));

    // Register in dependency order: events others rely on go first.
    indexer.register_event::<TokenCreated, _>(EVENT_TYPE, TokenCreatedStore);

    indexer.run().await?;
    Ok(())
}
