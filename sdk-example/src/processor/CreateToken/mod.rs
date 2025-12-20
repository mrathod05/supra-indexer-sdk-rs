pub mod types;

use anyhow::{Result};
use async_trait::async_trait;
use supra_indexer_sdk::{ExtractEventData, processor::{EventStore, HasBlockHeight}};
use supra_indexer_sdk::database::DbPool;
use crate::processor::CreateToken::types::{CreateTokenEvent, CreateTokenEventApiResponse, CreateTokenStore};

// Implement HasBlockHeight
impl HasBlockHeight for CreateTokenEvent {
    fn block_height(&self) -> u64 {
        self.block_height
    }
}

// Implement ExtractEventData
impl ExtractEventData<CreateTokenEvent> for CreateTokenEventApiResponse {
    fn get_data(self) -> Vec<CreateTokenEvent> {
        self.data
    }
}

// Implement EventStore - This is where users define their storage logic
#[async_trait]
impl EventStore<CreateTokenEvent> for CreateTokenStore {
    async fn store_events(
        &self,
        _conn: &mut DbPool,
        _events: Vec<CreateTokenEvent>,
    ) -> Result<()> {
        // "TODO: implement your storage logic here";
        // let mut pool = conn.get().context("Failed to get pool")?;
        Ok(())
    }
}
