use anyhow::{anyhow, Context, Result};
use diesel::{Connection, ExpressionMethods, QueryDsl, RunQueryDsl};
use diesel::associations::HasTable;
use tracing::{error, info};

use crate::database::{DbConnection};
use crate::schema::block_heights::dsl::*;
use crate::types::{EventBlockHeightInfo};

pub fn create_block_heights_table(conn: &mut DbConnection) -> Result<()> {
    info!("Creating a block_heights table if it doesn't exist...");
    // Execute table creation in a transaction
    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::sql_query(
            r#"
            CREATE TABLE IF NOT EXISTS block_heights (
                id SERIAL PRIMARY KEY,
                event VARCHAR(255) NOT NULL UNIQUE,
                cursor TEXT,
                last_block_height BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
            .execute(conn)?;

        diesel::sql_query(
            "CREATE INDEX IF NOT EXISTS idx_block_heights_event ON block_heights(event)"
        )
            .execute(conn)?;

        diesel::sql_query(
            "CREATE INDEX IF NOT EXISTS idx_block_heights_height ON block_heights(last_block_height)"
        )
            .execute(conn)?;

        // Create trigger for updated_at if not exists
        diesel::sql_query(
            r#"
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ language 'plpgsql'
            "#,
        )
            .execute(conn)?;

        Ok(())
    })
        .map_err(|e| anyhow!("Failed to create a block_heights table: {}", e))?;

    info!("Block_heights table created successfully");
    Ok(())
}


/// Initialize events in the block_heights table (if not already present)
pub fn initialize_events(pool: &mut DbConnection, event_list: Vec<String>, initial_block: u64) -> Result<()> {
    info!("Initializing the block_heights table");

    // Check which events already exist in the database
    let existing_events: Vec<String> = block_heights
        .select(event)
        .filter(event.eq_any(&event_list))
        .load(pool)
        .context("Failed to fetch existing events")?;

    // Find events that need to be inserted
    let events_to_insert: Vec<String> = event_list
        .into_iter()
        .filter(|e| !existing_events.contains(e))
        .collect();

    // Bulk insert only new events
    if !events_to_insert.is_empty() {
        bulk_insert_events(pool, events_to_insert, initial_block)?;
    }

    Ok(())
}

pub fn bulk_insert_events(
    conn: &mut DbConnection,
    event_list: Vec<String>,
    initial_block: u64,
) -> Result<()> {
    info!("Bulk inserting events into the block_heights table");
    if event_list.is_empty() {
        return Ok(());
    }

    let values: Vec<_> = event_list
        .into_iter()
        .map(|evt| (event.eq(evt), last_block_height.eq(initial_block as i64)))
        .collect();

    diesel::insert_into(block_heights::table())
        .values(&values)
        .on_conflict(event)
        .do_nothing()
        .execute(conn)
        .map_err(|e| {
            error!("Failed to bulk insert block heights: {:?}", e);
            anyhow!("Database error: {}", e)
        })?;

    Ok(())
}

pub fn upsert_block_height(conn: &mut DbConnection, data: EventBlockHeightInfo) -> Result<usize> {
    diesel::insert_into(block_heights)
        .values((
            event.eq(&data.event),
            cursor.eq(&data.cursor),
            last_block_height.eq(data.last_block_height as i64),
        ))
        .on_conflict(event)
        .do_update()
        .set((
            cursor.eq(&data.cursor),
            last_block_height.eq(data.last_block_height as i64),
            updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .map_err(|e| {
            error!("Failed to upsert block height for event '{}': {:?}", data.event, e);
            anyhow!("Database error: {}", e)
        })
}

pub fn get_last_block_by_event(
    conn: &mut DbConnection,
    event_str: &str,
) -> Result<Option<u64>> {
    let result: Result<i64, diesel::result::Error> = block_heights
        .select(last_block_height)
        .filter(event.eq(event_str))
        .get_result(conn);

    match result {
        Ok(height) => Ok(Some(height as u64)),
        Err(diesel::result::Error::NotFound) => Ok(None),
        Err(e) => Err(anyhow::anyhow!("Failed to fetch the last block height: {}", e)),
    }
}