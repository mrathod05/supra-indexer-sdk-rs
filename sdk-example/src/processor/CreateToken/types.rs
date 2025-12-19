use serde::Deserialize;
use supra_indexer_sdk::processor::GenericEventCollector;

#[derive(Debug, Deserialize)]
pub struct CreateTokenEventPayload {
    pub creator: String,
    pub timestamp: String,
    pub token_address: String,
    pub initial_supply: String,
}
#[derive(Debug, Deserialize)]
pub struct CreateTokenEventData {
    pub data: CreateTokenEventPayload,
}

#[derive(Debug, Deserialize)]
pub struct CreateTokenEvent {
    pub event: CreateTokenEventData,
    pub block_height: u64,
}

#[derive(Debug, Deserialize)]
pub struct CreateTokenEventApiResponse {
    pub data: Vec<CreateTokenEvent>,
}

pub struct CreateTokenStore;

pub type CreateTokenCollector =
GenericEventCollector<CreateTokenEvent, CreateTokenEventApiResponse, CreateTokenStore>;
