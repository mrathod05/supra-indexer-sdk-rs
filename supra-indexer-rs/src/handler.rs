//! Traits you implement per event type, plus ready-made types matching the
//! Supra RPC v3 event response shape.

use diesel::PgConnection;
use serde::Deserialize;

/// Block height an event was emitted at.
pub trait HasBlockHeight {
    fn block_height(&self) -> u64;
}

/// Turns a decoded RPC response body into its events.
pub trait ExtractEventData<E> {
    fn get_data(self) -> Vec<E>;
}

/// Persists a page of events.
///
/// Called inside a database transaction opened by the SDK; the checkpoint for
/// this page is written in the same transaction, so the page and its progress
/// commit (or roll back) together. Use only `conn` for writes you want to be
/// atomic with the checkpoint.
///
/// A page may be delivered again after a failed commit, so writes must be
/// idempotent (e.g. `ON CONFLICT DO NOTHING` on a natural key).
pub trait EventStore<E>: Send + Sync + 'static {
    fn store_events(&self, conn: &mut PgConnection, events: &[E]) -> anyhow::Result<()>;
}

/// One event as returned by `/rpc/v3/events`, with a typed payload `T`.
///
/// ```json
/// {
///   "event": { "type": "0x..::module::Event", "sequence_number": "0", "data": { .. } },
///   "block_height": 135980545,
///   "transaction_hash": "0xcc48.."
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct SupraEvent<T> {
    pub event: EventEnvelope<T>,
    pub block_height: u64,
    #[serde(default)]
    pub transaction_hash: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventEnvelope<T> {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub sequence_number: Option<String>,
    pub data: T,
}

impl<T> SupraEvent<T> {
    pub fn data(&self) -> &T {
        &self.event.data
    }
}

impl<T> HasBlockHeight for SupraEvent<T> {
    fn block_height(&self) -> u64 {
        self.block_height
    }
}

/// The `{"data": [...]}` wrapper around a page of events.
#[derive(Debug, Clone, Deserialize)]
pub struct EventsResponse<E> {
    pub data: Vec<E>,
}

impl<E> ExtractEventData<E> for EventsResponse<E> {
    fn get_data(self) -> Vec<E> {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize)]
    struct TokenCreated {
        creator: String,
        initial_supply: String,
    }

    #[test]
    fn decodes_rpc_shape() {
        let body = r#"{"data":[{"event":{"guid":{"creation_number":"0","account_address":"0x0"},
            "sequence_number":"0","type":"0xa8::token_factory::TokenCreated",
            "data":{"creator":"0x60","initial_supply":"100","timestamp":"1","token_address":"0xbf"}},
            "block_height":135980545,"transaction_hash":"0xcc"}]}"#;
        let resp: EventsResponse<SupraEvent<TokenCreated>> = serde_json::from_str(body).unwrap();
        let events = resp.get_data();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_height(), 135980545);
        assert_eq!(events[0].data().creator, "0x60");
        assert_eq!(events[0].data().initial_supply, "100");
        assert_eq!(events[0].transaction_hash.as_deref(), Some("0xcc"));
    }
}
