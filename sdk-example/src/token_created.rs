//! `token_factory::TokenCreated`: payload type and its store.

use std::str::FromStr;

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use serde::Deserialize;
use supra_indexer_sdk::utils::normalize_hex_to_fixed;
use supra_indexer_sdk::{EventStore, SupraEvent};

use crate::schema::tokens_created;

pub const EVENT_TYPE: &str = "token_factory::TokenCreated";

/// Matches the event's `data` object. Move `u64`/`u128` values arrive as
/// JSON strings.
#[derive(Debug, Deserialize)]
pub struct TokenCreated {
    pub creator: String,
    pub token_address: String,
    pub initial_supply: String,
    pub timestamp: String,
}

#[derive(Insertable)]
#[diesel(table_name = tokens_created)]
struct NewTokenCreated {
    token_address: String,
    creator: String,
    initial_supply: BigDecimal,
    created_at_secs: i64,
    block_height: i64,
    transaction_hash: Option<String>,
}

impl TryFrom<&SupraEvent<TokenCreated>> for NewTokenCreated {
    type Error = anyhow::Error;

    fn try_from(e: &SupraEvent<TokenCreated>) -> Result<Self> {
        let d = e.data();
        Ok(Self {
            token_address: normalize_hex_to_fixed(&d.token_address)?,
            creator: normalize_hex_to_fixed(&d.creator)?,
            initial_supply: BigDecimal::from_str(&d.initial_supply)
                .with_context(|| format!("initial_supply `{}`", d.initial_supply))?,
            created_at_secs: d
                .timestamp
                .parse()
                .with_context(|| format!("timestamp `{}`", d.timestamp))?,
            block_height: i64::try_from(e.block_height)?,
            transaction_hash: e.transaction_hash.clone(),
        })
    }
}

pub struct TokenCreatedStore;

impl EventStore<SupraEvent<TokenCreated>> for TokenCreatedStore {
    fn store_events(
        &self,
        conn: &mut PgConnection,
        events: &[SupraEvent<TokenCreated>],
    ) -> Result<()> {
        let rows = events
            .iter()
            .map(NewTokenCreated::try_from)
            .collect::<Result<Vec<_>>>()?;

        // Idempotent: a page may be replayed after a crash before commit.
        let inserted = diesel::insert_into(tokens_created::table)
            .values(&rows)
            .on_conflict(tokens_created::token_address)
            .do_nothing()
            .execute(conn)?;

        tracing::info!(
            received = rows.len(),
            inserted,
            "stored TokenCreated events"
        );
        Ok(())
    }
}
