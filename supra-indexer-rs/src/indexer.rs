use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::checkpoint::{self, Checkpoint};
use crate::collector::{Collector, TypedHandler};
use crate::config::IndexerConfig;
use crate::db::{DbPool, with_conn};
use crate::error::{IndexerError, Result};
use crate::handler::{EventStore, EventsResponse, ExtractEventData, HasBlockHeight, SupraEvent};
use crate::rpc::SupraClient;
use crate::status::{IndexerHandle, IndexerStatus, Mode};

/// Polls Supra RPC for registered event types and hands each page to its
/// [`EventStore`], tracking progress per event type in PostgreSQL.
///
/// Every cycle picks one target block (`chain tip - finality_lag`, bounded by
/// `max_blocks_per_cycle`) and indexes each event type up to it, in
/// registration order. Register events that others depend on first.
pub struct SupraIndexer {
    config: IndexerConfig,
    pool: DbPool,
    client: SupraClient,
    collectors: Vec<Collector>,
    cancel: CancellationToken,
    paused: Arc<AtomicBool>,
    status: watch::Sender<IndexerStatus>,
    handle_signals: bool,
    #[cfg(feature = "server")]
    server: Option<crate::server::ServerConfig>,
}

impl SupraIndexer {
    pub fn new(pool: DbPool, config: IndexerConfig) -> Result<Self> {
        config.validate()?;
        let client = SupraClient::new(&config)?;
        Ok(Self {
            config,
            pool,
            client,
            collectors: Vec::new(),
            cancel: CancellationToken::new(),
            paused: Arc::new(AtomicBool::new(false)),
            status: watch::Sender::new(IndexerStatus::new()),
            handle_signals: true,
            #[cfg(feature = "server")]
            server: None,
        })
    }

    /// Register an event type with explicit event (`E`) and response (`R`)
    /// types.
    ///
    /// `event_type` is `module::EventName` (resolved against
    /// `contract_address`) or a fully qualified `0x..::module::EventName`.
    pub fn register<E, R, S>(&mut self, event_type: &str, store: S) -> &mut Self
    where
        E: HasBlockHeight + 'static,
        R: DeserializeOwned + ExtractEventData<E> + 'static,
        S: EventStore<E>,
    {
        let key = self.qualify(event_type);
        let handler = TypedHandler::<E, R, S>::new(key, store);
        self.collectors.push(Collector::new(Arc::new(handler)));
        self
    }

    /// Register an event type using the standard RPC shape: the store receives
    /// [`SupraEvent<T>`] where `T` is your payload struct.
    pub fn register_event<T, S>(&mut self, event_type: &str, store: S) -> &mut Self
    where
        T: DeserializeOwned + 'static,
        S: EventStore<SupraEvent<T>>,
    {
        self.register::<SupraEvent<T>, EventsResponse<SupraEvent<T>>, S>(event_type, store)
    }

    /// Use an external token to stop the indexer.
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel = token;
        self
    }

    /// Whether [`run`](Self::run) stops on SIGINT/SIGTERM (default: true).
    pub fn handle_signals(mut self, enabled: bool) -> Self {
        self.handle_signals = enabled;
        self
    }

    /// Serve `/health`, `/ready`, `/status` and the admin routes while running.
    #[cfg(feature = "server")]
    pub fn serve(mut self, config: crate::server::ServerConfig) -> Self {
        self.server = Some(config);
        self
    }

    pub fn handle(&self) -> IndexerHandle {
        IndexerHandle {
            cancel: self.cancel.clone(),
            paused: Arc::clone(&self.paused),
            status: self.status.subscribe(),
        }
    }

    fn qualify(&self, event_type: &str) -> String {
        let event_type = event_type.trim();
        if event_type.starts_with("0x") {
            event_type.to_owned()
        } else {
            format!("{}::{}", self.config.contract_address, event_type)
        }
    }

    /// Run until shutdown (signal, [`IndexerHandle::shutdown`] or the
    /// cancellation token). Returns `Ok(())` on a clean stop.
    pub async fn run(self) -> Result<()> {
        let keys = self.validate_registrations()?;

        let (start, force) = (self.config.start_block, self.config.force_start_block);
        let seed_keys = keys.clone();
        with_conn(&self.pool, move |conn| {
            checkpoint::ensure_schema(conn)?;
            checkpoint::seed(conn, &seed_keys, start, force)
        })
        .await?;
        if force {
            warn!(
                start_block = start,
                "force_start_block set: all events rewound"
            );
        }

        let lock = if self.config.instance_lock {
            Some(self.acquire_instance_lock().await?)
        } else {
            None
        };

        #[cfg(feature = "server")]
        let server = match &self.server {
            Some(cfg) => {
                match crate::server::spawn(cfg.clone(), self.handle(), self.pool.clone()).await {
                    Ok(task) => Some(task),
                    Err(e) => {
                        if let Some(lock) = lock {
                            release_instance_lock(lock).await;
                        }
                        return Err(e);
                    }
                }
            }
            None => None,
        };

        if self.handle_signals {
            tokio::spawn(shutdown_on_signal(self.cancel.clone()));
        }

        info!(
            contract = %self.config.contract_address,
            events = keys.len(),
            "indexer started"
        );
        let result = self.main_loop().await;

        if let Some(lock) = lock {
            release_instance_lock(lock).await;
        }
        #[cfg(feature = "server")]
        if let Some(server) = server {
            self.cancel.cancel();
            if let Err(e) = server.await {
                warn!(error = %e, "HTTP server task failed");
            }
        }

        info!("indexer stopped");
        result
    }

    fn validate_registrations(&self) -> Result<Vec<String>> {
        if self.collectors.is_empty() {
            return Err(IndexerError::Config("no event types registered".into()));
        }
        let mut seen = HashSet::new();
        let mut keys = Vec::with_capacity(self.collectors.len());
        for c in &self.collectors {
            if !seen.insert(c.key()) {
                return Err(IndexerError::Config(format!(
                    "event `{}` registered twice",
                    c.key()
                )));
            }
            keys.push(c.key().to_owned());
        }
        Ok(keys)
    }

    async fn acquire_instance_lock(&self) -> Result<LockConn> {
        let pool = self.pool.clone();
        let name = self.config.contract_address.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get()?;
            if checkpoint::try_instance_lock(&mut conn, &name)? {
                Ok(conn)
            } else {
                Err(IndexerError::AlreadyRunning(name))
            }
        })
        .await?
    }

    async fn main_loop(&self) -> Result<()> {
        let mut failures: u32 = 0;
        // Chain height reused while catching up window by window; refreshed
        // once the windows reach it, after an error, or after a pause.
        let mut known_chain: Option<u64> = None;
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }

            let paused = self.paused.load(Ordering::SeqCst);
            self.status
                .send_if_modified(|s| std::mem::replace(&mut s.paused, paused) != paused);
            if paused {
                known_chain = None;
                if self.sleep(self.config.poll_interval).await {
                    return Ok(());
                }
                continue;
            }

            let result = self.cycle(known_chain.take()).await;
            let rpc_requests = self.client.requests_sent();
            self.status.send_modify(|s| s.rpc_requests = rpc_requests);
            let delay = match result {
                Ok(CycleOutcome { caught_up, chain }) => {
                    failures = 0;
                    if !caught_up {
                        known_chain = Some(chain);
                    }
                    self.status.send_modify(|s| {
                        s.cycles_completed += 1;
                        s.consecutive_failures = 0;
                        s.last_error = None;
                        if caught_up {
                            s.mode = Mode::Live;
                        }
                    });
                    // Behind the tip: start the next window right away.
                    if caught_up {
                        self.config.poll_interval
                    } else {
                        Duration::ZERO
                    }
                }
                Err(IndexerError::Cancelled) => return Ok(()),
                Err(e) => {
                    failures = failures.saturating_add(1);
                    let delay = self.config.retry.delay(failures - 1);
                    error!(error = %e, failures, retry_in = ?delay, "indexing cycle failed");
                    let msg = e.to_string();
                    self.status.send_modify(|s| {
                        s.consecutive_failures = failures;
                        s.last_error = Some(msg);
                    });
                    delay
                }
            };

            if self.sleep(delay).await {
                return Ok(());
            }
        }
    }

    /// Sleep unless cancelled; returns true when cancelled.
    async fn sleep(&self, d: Duration) -> bool {
        if d.is_zero() {
            return self.cancel.is_cancelled();
        }
        tokio::select! {
            _ = self.cancel.cancelled() => true,
            _ = tokio::time::sleep(d) => false,
        }
    }

    /// One pass over all collectors, up to `known_chain` if given (catch-up)
    /// or the freshly fetched chain height.
    async fn cycle(&self, known_chain: Option<u64>) -> Result<CycleOutcome> {
        let chain = match known_chain {
            Some(h) => h,
            None => tokio::select! {
                _ = self.cancel.cancelled() => return Err(IndexerError::Cancelled),
                h = self.client.latest_block_height() => h?,
            },
        };
        let tip_target = chain.saturating_sub(self.config.finality_lag);

        let keys: Vec<String> = self.collectors.iter().map(|c| c.key().to_owned()).collect();
        let mut checkpoints: HashMap<String, Checkpoint> = with_conn(&self.pool, move |conn| {
            keys.into_iter()
                .map(|k| checkpoint::load(conn, &k).map(|cp| (k, cp)))
                .collect()
        })
        .await?;

        let min_next = checkpoints
            .values()
            .map(|c| c.next_block)
            .min()
            .unwrap_or(0);
        let target = tip_target
            .min(min_next.saturating_add(self.config.max_blocks_per_cycle.saturating_sub(1)));
        self.status.send_modify(|s| s.chain_height = Some(chain));

        let mut total = 0;
        for collector in &self.collectors {
            let cp = checkpoints.remove(collector.key()).ok_or_else(|| {
                IndexerError::Checkpoint(format!("no row for `{}`", collector.key()))
            })?;
            let drained = collector
                .drain(
                    &self.pool,
                    &self.client,
                    &self.cancel,
                    self.config.page_limit,
                    cp,
                    target,
                )
                .await?;
            total += drained.events;
            self.status.send_modify(|s| {
                s.events
                    .insert(collector.key().to_owned(), drained.next_block);
                s.events_indexed += drained.events as u64;
                s.pages_committed += drained.pages as u64;
                s.indexed_height = s.events.values().min().and_then(|n| n.checked_sub(1));
            });
        }

        if total > 0 {
            info!(target, chain, events = total, "cycle complete");
        }
        Ok(CycleOutcome {
            caught_up: target == tip_target,
            chain,
        })
    }
}

struct CycleOutcome {
    /// The target was the chain tip target, not a `max_blocks_per_cycle` window.
    caught_up: bool,
    chain: u64,
}

type LockConn =
    diesel::r2d2::PooledConnection<diesel::r2d2::ConnectionManager<diesel::PgConnection>>;

/// Unlock before the connection returns to the pool, otherwise the session
/// lock would stay held by an idle pooled connection.
async fn release_instance_lock(mut conn: LockConn) {
    let res = tokio::task::spawn_blocking(move || {
        use diesel::RunQueryDsl;
        diesel::sql_query("SELECT pg_advisory_unlock_all()").execute(&mut *conn)
    })
    .await;
    if !matches!(res, Ok(Ok(_))) {
        warn!("failed to release instance lock; it is freed when the connection closes");
    }
}

async fn shutdown_on_signal(cancel: CancellationToken) {
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                warn!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("SIGINT received, shutting down"),
        _ = terminate => info!("SIGTERM received, shutting down"),
        _ = cancel.cancelled() => return,
    }
    cancel.cancel();
}
