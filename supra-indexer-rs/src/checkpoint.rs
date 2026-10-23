//! Per-event progress stored in `supra_indexer_checkpoints`.
//!
//! A row tracks `next_block` (first block not yet fully indexed) and, while a
//! block range is being paged through, the RPC cursor for the next page plus
//! the range's exclusive end. Supra cursors are position-encoded, so a saved
//! cursor stays valid across restarts.
//!
//! All functions are blocking; call them from `spawn_blocking`.

use diesel::prelude::*;
use diesel::sql_types::{Bool, Text};

use crate::error::{IndexerError, Result};

diesel::table! {
    supra_indexer_checkpoints (event_key) {
        event_key -> Text,
        next_block -> BigInt,
        cursor -> Nullable<Text>,
        cursor_end -> Nullable<BigInt>,
        updated_at -> Timestamptz,
    }
}

use supra_indexer_checkpoints::dsl as cp;

const CREATE_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS supra_indexer_checkpoints (
    event_key   TEXT PRIMARY KEY,
    next_block  BIGINT NOT NULL CHECK (next_block >= 0),
    cursor      TEXT,
    cursor_end  BIGINT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((cursor IS NULL) = (cursor_end IS NULL))
)"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub next_block: u64,
    /// `(cursor, exclusive end)` of a range that is partially paged.
    pub in_flight: Option<(String, u64)>,
}

pub(crate) fn ensure_schema(conn: &mut PgConnection) -> Result<()> {
    conn.transaction(|conn| {
        // Serialise concurrent first-time DDL from several processes.
        diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext('supra_indexer_sdk:schema'))")
            .execute(conn)?;
        diesel::sql_query(CREATE_TABLE).execute(conn)?;
        Ok(())
    })
}

#[derive(QueryableByName)]
struct Locked {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

/// Session-level advisory lock; held for as long as `conn` stays open.
pub(crate) fn try_instance_lock(conn: &mut PgConnection, name: &str) -> Result<bool> {
    let row: Locked = diesel::sql_query("SELECT pg_try_advisory_lock(hashtext($1)) AS locked")
        .bind::<Text, _>(format!("supra_indexer_sdk:instance:{name}"))
        .get_result(conn)?;
    Ok(row.locked)
}

/// Create missing rows at `start_block`; with `force`, rewind existing rows
/// to `start_block` too.
pub(crate) fn seed(
    conn: &mut PgConnection,
    keys: &[String],
    start_block: u64,
    force: bool,
) -> Result<()> {
    let start = to_i64(start_block)?;
    conn.transaction(|conn| {
        for key in keys {
            let insert = diesel::insert_into(cp::supra_indexer_checkpoints)
                .values((cp::event_key.eq(key), cp::next_block.eq(start)));
            if force {
                insert
                    .on_conflict(cp::event_key)
                    .do_update()
                    .set((
                        cp::next_block.eq(start),
                        cp::cursor.eq(None::<String>),
                        cp::cursor_end.eq(None::<i64>),
                        cp::updated_at.eq(diesel::dsl::now),
                    ))
                    .execute(conn)?;
            } else {
                insert.on_conflict_do_nothing().execute(conn)?;
            }
        }
        Ok(())
    })
}

pub(crate) fn load(conn: &mut PgConnection, key: &str) -> Result<Checkpoint> {
    let (next, cursor, end): (i64, Option<String>, Option<i64>) = cp::supra_indexer_checkpoints
        .filter(cp::event_key.eq(key))
        .select((cp::next_block, cp::cursor, cp::cursor_end))
        .first(conn)?;
    let in_flight = match (cursor, end) {
        (Some(c), Some(e)) => Some((c, to_u64(e)?)),
        _ => None,
    };
    Ok(Checkpoint {
        next_block: to_u64(next)?,
        in_flight,
    })
}

/// Mid-range: remember where the next page starts.
pub(crate) fn save_cursor(
    conn: &mut PgConnection,
    key: &str,
    cursor: &str,
    end_exclusive: u64,
) -> Result<()> {
    let n = diesel::update(cp::supra_indexer_checkpoints.filter(cp::event_key.eq(key)))
        .set((
            cp::cursor.eq(Some(cursor)),
            cp::cursor_end.eq(Some(to_i64(end_exclusive)?)),
            cp::updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)?;
    expect_one(n, key)
}

/// Range finished: advance `next_block` and clear the cursor.
pub(crate) fn complete_range(conn: &mut PgConnection, key: &str, next_block: u64) -> Result<()> {
    let n = diesel::update(cp::supra_indexer_checkpoints.filter(cp::event_key.eq(key)))
        .set((
            cp::next_block.eq(to_i64(next_block)?),
            cp::cursor.eq(None::<String>),
            cp::cursor_end.eq(None::<i64>),
            cp::updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)?;
    expect_one(n, key)
}

fn expect_one(updated: usize, key: &str) -> Result<()> {
    if updated == 1 {
        Ok(())
    } else {
        Err(IndexerError::Checkpoint(format!(
            "checkpoint row for `{key}` is missing"
        )))
    }
}

pub(crate) fn to_i64(v: u64) -> Result<i64> {
    i64::try_from(v).map_err(|_| IndexerError::Checkpoint(format!("block {v} exceeds i64")))
}

fn to_u64(v: i64) -> Result<u64> {
    u64::try_from(v)
        .map_err(|_| IndexerError::Checkpoint(format!("negative block height {v} in checkpoint")))
}
