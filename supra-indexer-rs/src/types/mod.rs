use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub node_url: String,
    pub contract_address: String,
    pub last_block_height: u64,
    pub fetch_interval_ms: u64,
    pub events: Vec<String>,
}

/// Trait for extracting event data from API responses
pub trait ExtractEventData<E> {
    fn get_data(self) -> Vec<E>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBlockHeightInfo {
    pub event: String,
    pub cursor: Option<String>,
    pub last_block_height: u64,
}