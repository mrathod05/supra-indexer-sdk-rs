//! Build crash-safe Supra event indexers backed by PostgreSQL (diesel).
//!
//! ```no_run
//! use diesel::PgConnection;
//! use serde::Deserialize;
//! use supra_indexer_sdk::prelude::*;
//!
//! #[derive(Debug, Deserialize)]
//! struct TokenCreated {
//!     creator: String,
//!     token_address: String,
//! }
//!
//! struct TokenStore;
//!
//! impl EventStore<SupraEvent<TokenCreated>> for TokenStore {
//!     fn store_events(
//!         &self,
//!         conn: &mut PgConnection,
//!         events: &[SupraEvent<TokenCreated>],
//!     ) -> anyhow::Result<()> {
//!         // Idempotent insert using `conn` (same transaction as the checkpoint).
//!         Ok(())
//!     }
//! }
//!
//! # async fn run() -> Result<(), IndexerError> {
//! let pool = create_pool("postgres://localhost/indexer", 5)?;
//! let config = IndexerConfig::new("https://rpc-testnet.supra.com", "0xa802...")
//!     .start_block(135_000_000);
//!
//! let mut indexer = SupraIndexer::new(pool, config)?;
//! indexer.register_event::<TokenCreated, _>("token_factory::TokenCreated", TokenStore);
//! indexer.run().await
//! # }
//! ```
//!
//! # How it works
//!
//! Each cycle reads the chain tip, picks a target of `tip - finality_lag`
//! (bounded by [`IndexerConfig::max_blocks_per_cycle`] during catch-up), and
//! pages every registered event type up to it through
//! `GET /rpc/v3/events/{address}::{module}::{Event}`, following the
//! `x-supra-cursor` header. Progress lives in the
//! `supra_indexer_checkpoints` table, which is created automatically.
//!
//! # Guarantees
//!
//! * Each page of events is stored in the same transaction as its checkpoint.
//!   After a crash the indexer resumes from the saved cursor, so nothing is
//!   skipped.
//! * A page can be delivered again if the process dies before commit, so
//!   [`EventStore`] implementations must be idempotent.
//! * Within one event type events arrive in block order. Across event types,
//!   all types are indexed to the same target block each cycle, in
//!   registration order.
//! * One process per contract and database, enforced by a PostgreSQL
//!   advisory lock ([`IndexerError::AlreadyRunning`]).
//!
//! # Control
//!
//! [`SupraIndexer::handle`] returns an [`IndexerHandle`] to pause, resume,
//! shut down and observe [`IndexerStatus`] (mode, lag, events, pages, cycles,
//! RPC requests). [`SupraIndexer::run`] stops cleanly on SIGINT/SIGTERM.
//!
//! # Feature flags
//!
//! * `server`: `/health`, `/ready`, `/status` and bearer-protected
//!   `/admin/{pause,resume,shutdown}` via `SupraIndexer::serve` and
//!   `server::ServerConfig`.
//!
//! # Performance
//!
//! Measured on an i5-13400 with PostgreSQL 14 on the same host (single runs;
//! see the [performance report] for method and raw data):
//!
//! | | |
//! |---|---|
//! | Full Supra testnet history (14.88 M blocks), real binary | 37 s, 0.011 CPU cores, 17.8 MiB RSS |
//! | Max ingest (100 events/page, batch insert) | ≈ 27 000 events/s |
//! | At real RPC latency (135 / 300 ms) | 643 / 304 events/s |
//! | Crash mid-range, then restart | 0 lost, 0 duplicated |
//!
//! [performance report]: https://github.com/mrathod05/supra-indexer-sdk-rs/blob/main/docs/perf/PERFORMANCE_REPORT.md

mod checkpoint;
mod collector;
mod config;
mod db;
mod error;
mod handler;
mod indexer;
pub mod rpc;
#[cfg(feature = "server")]
pub mod server;
mod status;
pub mod utils;

pub use config::{IndexerConfig, RetryPolicy};
pub use db::{DbPool, create_pool};
pub use error::{IndexerError, Result};
pub use handler::{
    EventEnvelope, EventStore, EventsResponse, ExtractEventData, HasBlockHeight, SupraEvent,
};
pub use indexer::SupraIndexer;
pub use status::{IndexerHandle, IndexerStatus, Mode};

/// Re-exported so users get the same version the SDK is built against.
pub use diesel;
pub use tokio_util::sync::CancellationToken;

pub mod prelude {
    #[cfg(feature = "server")]
    pub use crate::server::ServerConfig;
    pub use crate::{
        DbPool, EventStore, EventsResponse, ExtractEventData, HasBlockHeight, IndexerConfig,
        IndexerError, IndexerHandle, IndexerStatus, Mode, SupraEvent, SupraIndexer, create_pool,
    };
}
