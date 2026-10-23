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
//! # Guarantees
//!
//! * Each page of events is stored in the same transaction as its checkpoint.
//! * A page can be delivered again if the process dies before commit, so
//!   [`EventStore`] implementations must be idempotent.
//! * Within one event type events arrive in block order. Across event types,
//!   all types are indexed to the same target block each cycle, in
//!   registration order.

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
