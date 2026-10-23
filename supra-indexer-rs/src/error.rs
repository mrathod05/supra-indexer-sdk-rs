use thiserror::Error;

pub type Result<T, E = IndexerError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("RPC request to {url} failed with HTTP {status}: {message}")]
    Rpc {
        url: String,
        status: u16,
        message: String,
    },

    #[error("HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("unexpected RPC response: {0}")]
    InvalidResponse(String),

    #[error("failed to decode `{event}` response: {source}")]
    Decode {
        event: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("event store for `{event}` failed: {source:#}")]
    Store {
        event: String,
        #[source]
        source: anyhow::Error,
    },

    #[error("database error: {0}")]
    Db(#[from] diesel::result::Error),

    #[error("checkpoint error: {0}")]
    Checkpoint(String),

    #[error("database pool error: {0}")]
    Pool(#[from] diesel::r2d2::PoolError),

    #[error("another indexer instance holds the lock for `{0}`")]
    AlreadyRunning(String),

    #[error("background task failed: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("indexer was cancelled")]
    Cancelled,
}
