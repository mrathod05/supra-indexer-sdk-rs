use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Serialize;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Not yet caught up with `chain tip - finality_lag`.
    Reindexing,
    /// Has reached the chain tip target at least once.
    Live,
}

/// Snapshot of indexer progress, published after every cycle.
#[derive(Debug, Clone, Serialize)]
pub struct IndexerStatus {
    pub mode: Mode,
    pub paused: bool,
    /// Latest chain height seen.
    pub chain_height: Option<u64>,
    /// Highest block every registered event has been indexed through.
    pub indexed_height: Option<u64>,
    /// `next_block` per event type.
    pub events: BTreeMap<String, u64>,
    /// Events stored since this process started.
    pub events_indexed: u64,
    /// Pages committed (one transaction each) since this process started.
    pub pages_committed: u64,
    /// Indexing cycles completed since this process started.
    pub cycles_completed: u64,
    /// RPC requests sent since this process started, retries included.
    pub rpc_requests: u64,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
}

impl IndexerStatus {
    pub(crate) fn new() -> Self {
        Self {
            mode: Mode::Reindexing,
            paused: false,
            chain_height: None,
            indexed_height: None,
            events: BTreeMap::new(),
            events_indexed: 0,
            pages_committed: 0,
            cycles_completed: 0,
            rpc_requests: 0,
            consecutive_failures: 0,
            last_error: None,
        }
    }

    /// Blocks between the chain tip and `indexed_height`.
    pub fn lag(&self) -> Option<u64> {
        Some(self.chain_height?.saturating_sub(self.indexed_height?))
    }
}

/// Cloneable control handle: pause, resume, shut down and observe the
/// indexer from other tasks.
#[derive(Debug, Clone)]
pub struct IndexerHandle {
    pub(crate) cancel: CancellationToken,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) status: watch::Receiver<IndexerStatus>,
}

impl IndexerHandle {
    /// Stop after the page currently being written commits.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }

    pub fn is_shutting_down(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Stop starting new cycles until [`resume`](Self::resume).
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    pub fn status(&self) -> IndexerStatus {
        let mut status = self.status.borrow().clone();
        status.paused = self.is_paused();
        status
    }

    /// Receiver notified whenever the status changes.
    pub fn subscribe(&self) -> watch::Receiver<IndexerStatus> {
        self.status.clone()
    }
}
