# Supra Indexer SDK

A Rust SDK for efficiently indexing and processing events from the Supra blockchain.

## Overview

The Supra Indexer SDK provides a structured framework for building custom indexers that fetch, process, and store blockchain events from Supra nodes. It handles database management, pagination, error handling, and event processing out of the box.

## Getting Started

For a basic, straightforward example of how to use the SDK, check out the [examples folder](./sdk-example).

## Features

- **Automated Event Fetching**: Efficiently retrieves blockchain events with built-in pagination
- **Database Management**: Handles PostgresSQL connections and state persistence
- **Flexible Processing**: Customizable event processing pipeline
- **Type Safety**: Leverages Rust's type system for reliable indexing

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
supra-indexer-sdk = "0.1.0"
```
## Examples

Visit the [examples directory](./sdk-example) to see complete, working implementations including:

- Basic event indexer
- Custom event filtering
- Database integration patterns

## Documentation

For detailed API documentation, run:

```bash
cargo doc --open
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.