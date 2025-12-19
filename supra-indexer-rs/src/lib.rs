pub mod database;
pub mod processor;
pub mod types;
pub mod events;
pub mod schema;

use anyhow::{Context, Result};
use processor::{EventCollector, GenericEventCollector, EventStore, HasBlockHeight};
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::RwLock;

pub use types::{IndexerConfig, ExtractEventData};

use crate::database::DbPool;

/// Main Indexer where events process
pub struct SupraIndexer {
    config: Arc<RwLock<IndexerConfig>>,
    db_pool: DbPool,
    http_client: Client,
}

impl SupraIndexer {
    /// Create a new indexer instance
    pub async fn new(db_pool: DbPool, config: IndexerConfig) -> Result<Self> {
        let http_client = Client::new();

        {
            let mut conn = db_pool
                .get()
                .context("Failed to get a database connection")?;

            database::block_heights::create_block_heights_table(&mut conn)?;

            let config = config.clone();
            database::block_heights::bulk_insert_events(&mut conn, config.events, config.last_block_height)?;
        }
            Ok(Self {
                config: Arc::new(RwLock::new(config)),
                db_pool,
                http_client,
            })
    }

    /// Register and start an event collector
    pub async fn register_collector<E, R, S>(
        &self,
        event_type: String,
        event_store: S,
    ) -> Result<Box<dyn EventCollector>>
    where
        E: HasBlockHeight + Send + Sync + 'static,
        R: ExtractEventData<E> + serde::de::DeserializeOwned + Send + Sync + 'static,
        S: EventStore<E> + Send + Sync + 'static,
    {
        Ok(Box::new(
            GenericEventCollector::<E, R, S>::new(
                event_type,
                self.config.clone(),
                self.http_client.clone(),
                self.db_pool.clone(),
                event_store,
            )
                .await?
        ))
    }

    /// Start multiple collectors concurrently
    pub async fn start_all(&self, collectors: Vec<Box<dyn EventCollector>>) -> Result<()> {
        let mut handles = vec![];

        for mut collector in collectors {
            let config = self.config.clone();
            let handle = tokio::spawn(async move {
                loop {
                    if let Err(e) = collector.process_event_batch().await {
                        tracing::error!(
                        "Error processing {} events: {:?}",
                        collector.event_type(),
                        e
                    );
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }

                    let cfg = config.read().await;
                    tokio::time::sleep(tokio::time::Duration::from_millis(cfg.fetch_interval_ms))
                        .await;
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }
}
