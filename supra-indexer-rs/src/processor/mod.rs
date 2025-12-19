use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::{database, database::DbPool, events};
use crate::types::{ExtractEventData, IndexerConfig, EventBlockHeightInfo};

/// Trait that users implement to define how to store their events
#[async_trait]
pub trait EventStore<E> {
    async fn store_events(&self, db_pool: &mut DbPool, events: Vec<E>) -> Result<()>;
}

/// Core trait for event collectors
#[async_trait]
pub trait EventCollector: Send + Sync {
    fn event_type(&self) -> &str;
    async fn process_event_batch(&mut self) -> Result<()>;
}

/// Trait to extract block height from events
pub trait HasBlockHeight {
    fn block_height(&self) -> u64;
}

/// A generic collector that handles fetching and storing events
pub struct GenericEventCollector<E, R, S>
where
    R: ExtractEventData<E>,
    S: EventStore<E>,
{
    event_type: String,
    http_client: Client,
    db_pool: DbPool,
    config: Arc<RwLock<IndexerConfig>>,
    event_store: S,
    _phantom: std::marker::PhantomData<(E, R)>,
}

impl<E, R, S> GenericEventCollector<E, R, S>
where
    E: HasBlockHeight,
    R: ExtractEventData<E> + serde::de::DeserializeOwned,
    S: EventStore<E>,
{
    pub async fn new(
        event_type: String,
        config: Arc<RwLock<IndexerConfig>>,
        http_client: Client,
        db_pool: DbPool,
        event_store: S,
    ) -> Result<Self> {
        Ok(Self {
            event_type,
            config,
            http_client,
            db_pool,
            event_store,
            _phantom: std::marker::PhantomData,
        })
    }

    async fn parse_and_store_events(&mut self, response: reqwest::Response) -> Result<Option<u64>> {
        let api_response: R = response
            .json()
            .await
            .context("Failed to parse API response")?;

        let events = api_response.get_data();

        if events.is_empty() {
            return Ok(None);
        }

        let last_block_height = events.last().map(|event| event.block_height());

        self.event_store
            .store_events(&mut self.db_pool, events)
            .await
            .context("Failed to store events in a database")?;

        Ok(last_block_height)
    }

    async fn get_fetch_config(&self) -> (String, String) {
        let config = self.config.read().await;
        (config.node_url.clone(), config.contract_address.clone())
    }
}

#[async_trait]
impl<E, R, S> EventCollector for GenericEventCollector<E, R, S>
where
    E: HasBlockHeight + Send + Sync + 'static,
    R: ExtractEventData<E> + serde::de::DeserializeOwned + Send + Sync + 'static,
    S: EventStore<E> + Send + Sync + 'static,
{
    fn event_type(&self) -> &str {
        &self.event_type
    }

    async fn process_event_batch(&mut self) -> Result<()> {
        const MAX_BATCH_LIMIT: u8 = 100;

        // Get initial block height
        let mut current_start_block = {
            let mut conn = self.db_pool.get().context("Failed to get DB connection")?;
            match database::block_heights::get_last_block_by_event(&mut conn, self.event_type())? {
                Some(block_height) => block_height,
                None => {
                    error!("No, the last block found for an event: {}", self.event_type());
                    return Ok(());
                }
            }
        };

        let mut _current_end_block = current_start_block;
        let mut pagination_cursor = None;

        loop {
            let (base_url, contract_address) = self.get_fetch_config().await;

            let response = match events::fetch_events(
                &self.http_client,
                &base_url,
                &contract_address,
                self.event_type(),
                current_start_block,
                pagination_cursor.clone(),
                MAX_BATCH_LIMIT,
                None,
            )
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    error!("Failed to fetch events from a block {}: {}", current_start_block, error);
                    break;
                }
            };

            let response_headers = response.headers().clone();

            let last_processed_block = self.parse_and_store_events(response).await.map_err(|e| {
                error!("Failed to parse and store events: {:}", e);
                e
            })?;

            match last_processed_block {
                Some(last_block) => {
                    _current_end_block = last_block;
                }
                None => {
                    info!("No events found for a block {}", current_start_block);
                    break;
                }
            }

            // Update block height
            {
                let mut conn = self.db_pool.get().context("Failed to get DB connection")?;
                database::block_heights::upsert_block_height(
                    &mut conn,
                    EventBlockHeightInfo {
                        event: self.event_type().to_string(),
                        cursor: pagination_cursor.clone(),
                        last_block_height: _current_end_block + 1,
                    },
                )?;
            }

            info!(
                "Processed {} events from block {} to {}",
                self.event_type(),
                current_start_block,
                _current_end_block
            );

            // Handle pagination
            if let Some(next_cursor) = response_headers
                .get("x-supra-cursor")
                .and_then(|h| h.to_str().ok())
            {
                pagination_cursor = Some(next_cursor.to_string());
            } else {
                info!("No more {} events to fetch", self.event_type());
                break;
            }

            current_start_block = _current_end_block + 1;
        }

        Ok(())
    }
}