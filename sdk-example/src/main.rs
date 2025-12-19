// Example: Supra Indexer SDK Usage
// This example demonstrates how to use the Supra Indexer SDK to monitor
// and process blockchain events from a Supra smart contract.
// Note: Set the .env variable and run `diesel setup` to creat the database

// Import necessary modules
mod processor;
mod db;

use anyhow::Result;
use tracing::info;
use supra_indexer_sdk::{IndexerConfig, SupraIndexer};

// Import specific event types and handlers for the CreateToken event
// Note: You'll need to create these types in your processor module
use crate::processor::CreateToken::types::{
    CreateTokenEvent,
    CreateTokenEventApiResponse,
    CreateTokenStore
};

#[tokio::main]
async fn main() -> Result<()> {
    // =========================================================================
    // CONFIGURATION SECTION
    // =========================================================================

    // Set up Supra RPC endpoint
    // For testnet (use this for development and testing)
    let rpc_url = "https://rpc-testnet.supra.com".to_string();

    // For mainnet (uncomment and use for production)
    // let rpc_url = "https://rpc-mainnet.supra.com".to_string();

    // Contract address to monitor
    // This is the address of the smart contract emitting the events
    let contract_address = "0xa802ca2e7a6604e8a4cc3ef390bc137767b0cb04438525b0a3451e0bbd410ddf".to_string();

    // Event types to monitor (format: "module::EventName")
    let create_token_event = "token_factory::TokenCreated".to_string();

    // Example: Additional event type (uncomment to enable)
    // let transfer_token_event = "token_factory::Transfer".to_string();

    // =========================================================================
    // INITIALIZATION SECTION
    // =========================================================================

    // Initialize tracing for structured logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Starting Supra Indexer Example...");
    info!("Monitoring contract: {}", contract_address);

    // =========================================================================
    // INDEXER CONFIGURATION
    // =========================================================================

    let config = IndexerConfig {
        node_url: rpc_url,
        contract_address,
        last_block_height: 51891731,  // Start from this block
        fetch_interval_ms: 3000,      // Poll every 3 seconds
        events: vec![
            create_token_event.clone(),
            // Add more events here as needed:
            // transfer_token_event.clone(),
        ],
    };

    info!("Indexer configured to start from a block: {}", config.last_block_height);

    // =========================================================================
    // DATABASE SETUP
    // =========================================================================

    // Establish database connection pool
    // Note: The `establish_connection()` function should be defined in your db module
    let db_pool = db::establish_connection()
        .map_err(|e| anyhow::anyhow!("Failed to establish database connection: {}", e))?;

    info!("Database connection established successfully");

    // =========================================================================
    // INDEXER INSTANCE CREATION
    // =========================================================================

    // Create the main indexer instance
    let indexer = SupraIndexer::new(db_pool.clone(), config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create an indexer: {}", e))?;

    info!("Indexer initialized successfully");

    // =========================================================================
    // EVENT COLLECTOR REGISTRATION
    // =========================================================================

    // Register collectors for each event type
    info!("Registering event collectors...");

    // Register CreateToken event collector
    let create_token_collector = indexer
        .register_collector::<CreateTokenEvent, CreateTokenEventApiResponse, CreateTokenStore>(
            create_token_event.clone(),
            CreateTokenStore,  // Your custom event storage implementation
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register CreateToken collector: {}", e))?;

    info!("A registered collector for: {}", create_token_event);

    /*
    // Example: Register TransferToken event collector (uncomment to enable)
    let transfer_token_collector = indexer
        .register_collector::<TransferTokenEvent, TransferTokenEventApiResponse, TransferTokenStore>(
            transfer_token_event.clone(),
            TransferTokenStore,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register TransferToken collector: {}", e))?;
    
    info!("Registered collector for: {}", transfer_token_event);
    */

    // =========================================================================
    // START INDEXING
    // =========================================================================

    // Create a vector of all collectors to start
    let collectors = vec![
        create_token_collector,
        // transfer_token_collector, // Add more collectors here
    ];

    info!("Starting {} event collector(s)...", collectors.len());

    // Start all collectors (this will run indefinitely)
    indexer.start_all(collectors)
        .await
        .map_err(|e| anyhow::anyhow!("Indexer failed: {}", e))?;

    // Note: The indexer runs indefinitely until interrupted
    // Use Ctrl+C to stop the indexer gracefully

    Ok(())
}