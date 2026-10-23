//! Health, readiness, status and admin HTTP endpoints (feature `server`).
//!
//! | Route                  | Auth   | Purpose                                   |
//! |------------------------|--------|-------------------------------------------|
//! | `GET /health`          | none   | Liveness: process is up                    |
//! | `GET /ready`           | none   | DB reachable, live, lag ≤ `max_ready_lag`  |
//! | `GET /status`          | none   | [`IndexerStatus`] as JSON                  |
//! | `POST /admin/pause`    | bearer | Stop starting new cycles                   |
//! | `POST /admin/resume`   | bearer | Resume cycles                              |
//! | `POST /admin/shutdown` | bearer | Graceful shutdown                          |
//!
//! Admin routes are only mounted when an admin token is configured.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use diesel::RunQueryDsl;
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::db::DbPool;
use crate::error::Result;
use crate::status::{IndexerHandle, IndexerStatus, Mode};

const DB_PING_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ServerConfig {
    pub addr: SocketAddr,
    /// Bearer token for `/admin/*`. `None` (or empty) disables admin routes.
    pub admin_token: Option<String>,
    /// `/ready` fails when the indexer is more than this many blocks behind
    /// the chain tip.
    pub max_ready_lag: u64,
}

impl ServerConfig {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            admin_token: None,
            max_ready_lag: 100,
        }
    }

    pub fn admin_token(mut self, token: Option<String>) -> Self {
        self.admin_token = token.filter(|t| !t.is_empty());
        self
    }

    pub fn max_ready_lag(mut self, blocks: u64) -> Self {
        self.max_ready_lag = blocks;
        self
    }
}

#[derive(Clone)]
struct AppState {
    handle: IndexerHandle,
    pool: DbPool,
    max_ready_lag: u64,
}

/// Bind first so a port conflict fails startup, then serve in the background
/// until the indexer shuts down.
pub(crate) async fn spawn(
    config: ServerConfig,
    handle: IndexerHandle,
    pool: DbPool,
) -> Result<JoinHandle<std::io::Result<()>>> {
    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    let cancel = handle.cancel.clone();
    let app = router(config.clone(), handle, pool);
    info!(addr = %config.addr, admin = config.admin_token.is_some(), "HTTP server listening");
    Ok(tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { cancel.cancelled().await })
            .await
    }))
}

fn router(config: ServerConfig, handle: IndexerHandle, pool: DbPool) -> Router {
    let state = AppState {
        handle,
        pool,
        max_ready_lag: config.max_ready_lag,
    };
    let mut app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
        .route("/status", get(status));

    if let Some(token) = config.admin_token {
        let token: Arc<str> = token.into();
        let admin = Router::new()
            .route("/pause", post(pause))
            .route("/resume", post(resume))
            .route("/shutdown", post(shutdown))
            .route_layer(middleware::from_fn(move |req: Request, next: Next| {
                let token = Arc::clone(&token);
                async move { require_bearer(&token, req, next).await }
            }));
        app = app.nest("/admin", admin);
    } else {
        warn!("no admin token configured; /admin routes disabled");
    }
    app.with_state(state)
}

async fn require_bearer(token: &str, req: Request, next: Next) -> Response {
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if bool::from(presented.as_bytes().ct_eq(token.as_bytes())) {
        next.run(req).await
    } else {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

async fn status(State(state): State<AppState>) -> Json<IndexerStatus> {
    Json(state.handle.status())
}

async fn ready(State(state): State<AppState>) -> Response {
    let status = state.handle.status();
    let reason = if !ping_db(&state.pool).await {
        Some("database unreachable".to_owned())
    } else if state.handle.is_shutting_down() {
        Some("shutting down".to_owned())
    } else if status.mode != Mode::Live {
        Some("reindexing".to_owned())
    } else {
        match status.lag() {
            Some(lag) if lag <= state.max_ready_lag => None,
            Some(lag) => Some(format!("lag {lag} > {}", state.max_ready_lag)),
            None => Some("no progress yet".to_owned()),
        }
    };
    match reason {
        None => (
            StatusCode::OK,
            Json(json!({ "ready": true, "status": status })),
        ),
        Some(reason) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "ready": false, "reason": reason, "status": status })),
        ),
    }
    .into_response()
}

async fn ping_db(pool: &DbPool) -> bool {
    let pool = pool.clone();
    let res = tokio::task::spawn_blocking(move || {
        let mut conn = pool.get_timeout(DB_PING_TIMEOUT).ok()?;
        diesel::sql_query("SELECT 1").execute(&mut conn).ok()
    })
    .await;
    matches!(res, Ok(Some(_)))
}

async fn pause(State(state): State<AppState>) -> Json<serde_json::Value> {
    state.handle.pause();
    info!("paused via admin API");
    Json(json!({ "paused": true }))
}

async fn resume(State(state): State<AppState>) -> Json<serde_json::Value> {
    state.handle.resume();
    info!("resumed via admin API");
    Json(json!({ "paused": false }))
}

async fn shutdown(State(state): State<AppState>) -> Json<serde_json::Value> {
    info!("shutdown requested via admin API");
    state.handle.shutdown();
    Json(json!({ "shutting_down": true }))
}
