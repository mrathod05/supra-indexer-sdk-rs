//! Pages one event type through a block range, committing each page together
//! with its checkpoint.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use diesel::{Connection, PgConnection};
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::checkpoint::{self, Checkpoint};
use crate::db::{DbPool, with_conn};
use crate::error::{IndexerError, Result};
use crate::handler::{EventStore, ExtractEventData, HasBlockHeight};
use crate::rpc::{EventQuery, PageStart, SupraClient};

pub(crate) struct PagePlan {
    pub next_cursor: Option<String>,
    pub end_exclusive: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PageOutcome {
    pub events: usize,
    /// The range is exhausted; `next_block` advanced to its end.
    pub done: bool,
}

/// Type-erased event handler, so collectors for different event types can
/// live in one list.
pub(crate) trait ErasedHandler: Send + Sync {
    fn key(&self) -> &str;
    fn apply_page(
        &self,
        conn: &mut PgConnection,
        body: &[u8],
        plan: &PagePlan,
    ) -> Result<PageOutcome>;
}

pub(crate) struct TypedHandler<E, R, S> {
    key: String,
    store: S,
    _marker: PhantomData<fn() -> (E, R)>,
}

impl<E, R, S> TypedHandler<E, R, S> {
    pub fn new(key: String, store: S) -> Self {
        Self {
            key,
            store,
            _marker: PhantomData,
        }
    }
}

impl<E, R, S> ErasedHandler for TypedHandler<E, R, S>
where
    E: HasBlockHeight,
    R: DeserializeOwned + ExtractEventData<E>,
    S: EventStore<E>,
{
    fn key(&self) -> &str {
        &self.key
    }

    fn apply_page(
        &self,
        conn: &mut PgConnection,
        body: &[u8],
        plan: &PagePlan,
    ) -> Result<PageOutcome> {
        let response: R = serde_json::from_slice(body).map_err(|source| IndexerError::Decode {
            event: self.key.clone(),
            source,
        })?;
        let events = response.get_data();
        validate_heights(&self.key, &events, plan.end_exclusive)?;

        // The node caps pages below `limit` (100 on Supra RPC v3), so a short
        // page says nothing. The range ends only on an empty page or a
        // missing cursor; this costs one extra request per non-empty range.
        let more = !events.is_empty() && plan.next_cursor.is_some();

        conn.transaction(|conn| {
            if !events.is_empty() {
                self.store
                    .store_events(conn, &events)
                    .map_err(|source| IndexerError::Store {
                        event: self.key.clone(),
                        source,
                    })?;
            }
            match (&plan.next_cursor, more) {
                (Some(cursor), true) => {
                    checkpoint::save_cursor(conn, &self.key, cursor, plan.end_exclusive)
                }
                _ => checkpoint::complete_range(conn, &self.key, plan.end_exclusive),
            }
        })?;

        Ok(PageOutcome {
            events: events.len(),
            done: !more,
        })
    }
}

fn validate_heights<E: HasBlockHeight>(key: &str, events: &[E], end_exclusive: u64) -> Result<()> {
    let mut prev = 0;
    for event in events {
        let h = event.block_height();
        if h < prev || h >= end_exclusive {
            return Err(IndexerError::InvalidResponse(format!(
                "`{key}`: event at block {h} out of order or beyond range end {end_exclusive}"
            )));
        }
        prev = h;
    }
    Ok(())
}

/// Result of draining one event type to a target.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Drained {
    pub next_block: u64,
    pub events: usize,
    pub pages: usize,
}

pub(crate) struct Collector {
    pub handler: Arc<dyn ErasedHandler>,
    warned_pruned: AtomicBool,
}

impl Collector {
    pub fn new(handler: Arc<dyn ErasedHandler>) -> Self {
        Self {
            handler,
            warned_pruned: AtomicBool::new(false),
        }
    }

    pub fn key(&self) -> &str {
        self.handler.key()
    }

    /// Index this event type through `target` (inclusive), resuming any
    /// in-flight range first.
    pub async fn drain(
        &self,
        pool: &DbPool,
        client: &SupraClient,
        cancel: &CancellationToken,
        limit: u16,
        mut cp: Checkpoint,
        target: u64,
    ) -> Result<Drained> {
        let key = self.key();
        let mut total = 0;
        let mut pages = 0;

        loop {
            let (start, end_exclusive) = match cp.in_flight.take() {
                Some((cursor, end)) => (PageStart::Cursor(cursor), end),
                None if cp.next_block > target => {
                    return Ok(Drained {
                        next_block: cp.next_block,
                        events: total,
                        pages,
                    });
                }
                None => (PageStart::Height(cp.next_block), target + 1),
            };

            let query = EventQuery {
                event_type: key,
                start: start.clone(),
                end_height_exclusive: end_exclusive,
                limit,
            };
            let page = tokio::select! {
                _ = cancel.cancelled() => return Err(IndexerError::Cancelled),
                page = client.fetch_events(&query) => page?,
            };

            if let (PageStart::Height(h), Some(oldest)) = (&start, page.oldest_block) {
                if *h < oldest && !self.warned_pruned.swap(true, Ordering::Relaxed) {
                    warn!(
                        event = key,
                        start = h,
                        oldest_available = oldest,
                        "start block is older than the node's history; earlier events may be missing"
                    );
                }
            }

            let plan = PagePlan {
                next_cursor: page.next_cursor,
                end_exclusive,
            };
            let handler = Arc::clone(&self.handler);
            let next_cursor = plan.next_cursor.clone();
            let body = page.body;
            // Not raced against cancellation: an in-progress commit finishes.
            let outcome =
                with_conn(pool, move |conn| handler.apply_page(conn, &body, &plan)).await?;
            total += outcome.events;
            pages += 1;

            if outcome.done {
                if total > 0 {
                    info!(
                        event = key,
                        through = end_exclusive - 1,
                        events = total,
                        "range indexed"
                    );
                } else {
                    debug!(
                        event = key,
                        through = end_exclusive - 1,
                        "range indexed, no events"
                    );
                }
                cp.next_block = end_exclusive;
            } else {
                debug!(
                    event = key,
                    events = outcome.events,
                    "page stored, more pending"
                );
                // `done == false` implies a cursor was present.
                cp.in_flight = next_cursor.map(|c| (c, end_exclusive));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct H(u64);
    impl HasBlockHeight for H {
        fn block_height(&self) -> u64 {
            self.0
        }
    }

    #[test]
    fn rejects_out_of_order_or_out_of_range() {
        assert!(validate_heights("e", &[H(1), H(1), H(5)], 6).is_ok());
        assert!(validate_heights("e", &[H(5), H(4)], 10).is_err());
        assert!(validate_heights("e", &[H(10)], 10).is_err());
    }
}
