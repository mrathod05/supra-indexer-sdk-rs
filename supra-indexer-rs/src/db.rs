use diesel::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};

use crate::error::{IndexerError, Result};

pub type DbPool = Pool<ConnectionManager<PgConnection>>;

/// Build an r2d2 pool. With `instance_lock` enabled the indexer keeps one
/// connection for the lock, so use `max_size >= 2` (3+ with the HTTP server).
pub fn create_pool(database_url: &str, max_size: u32) -> Result<DbPool> {
    Pool::builder()
        .max_size(max_size)
        .build(ConnectionManager::new(database_url))
        .map_err(|e| IndexerError::Config(format!("database pool: {e}")))
}

/// Run blocking diesel work on the blocking thread pool.
pub(crate) async fn with_conn<T, F>(pool: &DbPool, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut PgConnection) -> Result<T> + Send + 'static,
{
    let pool = pool.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = pool.get()?;
        f(&mut conn)
    })
    .await?
}
