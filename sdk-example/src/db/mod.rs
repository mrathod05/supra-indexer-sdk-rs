use std::env;
use anyhow::{Context, Result};
use diesel::{r2d2, PgConnection};
use diesel::r2d2::ConnectionManager;
use dotenvy::dotenv;
use supra_indexer_sdk::database::DbPool;

pub fn establish_connection() -> Result<DbPool> {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // Create a connection manager
    let manager = ConnectionManager::<PgConnection>::new(database_url);

    // Create Pool
    let pool = r2d2::Pool::builder()
        .build(manager)
        .context("Failed to create a database connection pool")?;

    Ok(pool)
}
