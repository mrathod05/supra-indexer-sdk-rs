use anyhow::{Context, Result};
use reqwest::{Client, Response};
use tracing::warn;

pub async fn fetch_events(
    client: &Client,
    base_url: &str,
    contract_address: &str,
    event_type: &str,
    start_block: u64,
    cursor: Option<String>,
    limit: u8,
    end_block: Option<u64>,
) -> Result<Response> {
    // Build the base URL with common parameters
    let mut url = format!(
        "{}/rpc/v3/events/{}::{}?start_height={}&limit={}",
        base_url, contract_address, event_type, start_block, limit
    );

    // Add cursor if provided
    if let Some(cursor_value) = cursor {
        url.push_str(&format!("&start={}", cursor_value));
    }

    // Add end_block if provided
    if let Some(end_block_value) = end_block {
        url.push_str(&format!("&end_height={}", end_block_value));
    }

    warn!("API -> {}", url);

    // Get call
    let response = client
        .get(&url)
        .send()
        .await
        .context("Failed to fetch events from API")?;

    // On unsuccess returns error
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Api returned error status: {}",
            response.status()
        ));
    }

    Ok(response)
}
