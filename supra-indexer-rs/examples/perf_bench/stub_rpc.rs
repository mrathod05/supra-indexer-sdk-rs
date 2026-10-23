//! In-process stand-in for Supra RPC v3, faithful to the behaviour the SDK
//! depends on (all checked against `rpc-testnet.supra.com`):
//!
//! * `GET /rpc/v3/block` → `{"header":{"height":N}}`
//! * `GET /rpc/v3/events/{type}?start_height|start=&end_height=&limit=`
//!   - `end_height` is exclusive, `start_height >= end_height` is a 400
//!   - pages are capped at `min(limit, 100)` server-side
//!   - `x-supra-cursor` is set on every non-empty page, empty otherwise
//!
//! Every response is delayed by a fixed latency to model network RTT.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio::task::JoinHandle;

/// Supra RPC v3 returns at most this many events per page.
pub const SERVER_PAGE_CAP: usize = 100;

/// One pre-rendered event.
pub struct StubEvent {
    pub height: u64,
    pub json: String,
}

struct Shared {
    tip: AtomicU64,
    latency: Duration,
    data: HashMap<String, Vec<StubEvent>>,
    /// Arrival time of every events request, for page-cycle latency.
    arrivals: Mutex<Vec<Instant>>,
    event_requests: AtomicU64,
    block_requests: AtomicU64,
}

pub struct StubRpc {
    pub url: String,
    shared: Arc<Shared>,
    task: JoinHandle<()>,
}

impl StubRpc {
    /// `data` maps fully qualified event types to events sorted by height.
    pub async fn start(
        data: HashMap<String, Vec<StubEvent>>,
        tip: u64,
        latency: Duration,
    ) -> anyhow::Result<Self> {
        let shared = Arc::new(Shared {
            tip: AtomicU64::new(tip),
            latency,
            data,
            arrivals: Mutex::new(Vec::new()),
            event_requests: AtomicU64::new(0),
            block_requests: AtomicU64::new(0),
        });
        let app = Router::new()
            .route("/rpc/v3/block", get(block))
            .route("/rpc/v3/events/{event}", get(events))
            .with_state(Arc::clone(&shared));
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            url: format!("http://{addr}"),
            shared,
            task,
        })
    }

    pub fn event_requests(&self) -> u64 {
        self.shared.event_requests.load(Ordering::Relaxed)
    }

    pub fn block_requests(&self) -> u64 {
        self.shared.block_requests.load(Ordering::Relaxed)
    }

    /// Gaps between consecutive events requests: one SDK page cycle each
    /// (RPC + decode + store + checkpoint commit). Gaps across event types or
    /// cycles are included; with one event type and one window they are not.
    pub fn page_cycle_samples(&self) -> Vec<Duration> {
        let arrivals = self.shared.arrivals.lock().unwrap();
        arrivals.windows(2).map(|w| w[1] - w[0]).collect()
    }
}

impl Drop for StubRpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn block(State(s): State<Arc<Shared>>) -> Response {
    s.block_requests.fetch_add(1, Ordering::Relaxed);
    if !s.latency.is_zero() {
        tokio::time::sleep(s.latency).await;
    }
    let tip = s.tip.load(Ordering::Relaxed);
    (
        [("content-type", "application/json")],
        format!(r#"{{"header":{{"height":{tip}}}}}"#),
    )
        .into_response()
}

async fn events(
    State(s): State<Arc<Shared>>,
    Path(event): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    s.arrivals.lock().unwrap().push(Instant::now());
    s.event_requests.fetch_add(1, Ordering::Relaxed);
    if !s.latency.is_zero() {
        tokio::time::sleep(s.latency).await;
    }

    let parse = |k: &str| q.get(k).and_then(|v| v.parse::<u64>().ok());
    let (Some(limit), Some(end)) = (parse("limit"), parse("end_height")) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let events: &[StubEvent] = s.data.get(&event).map(Vec::as_slice).unwrap_or(&[]);
    let from = match (q.get("start"), parse("start_height")) {
        (Some(cursor), _) => match cursor.strip_prefix("i:").and_then(|c| c.parse().ok()) {
            Some(i) => i,
            None => return StatusCode::BAD_REQUEST.into_response(),
        },
        (None, Some(h)) if h < end => events.partition_point(|e| e.height < h),
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };

    let take = (limit as usize).min(SERVER_PAGE_CAP);
    let page: Vec<&StubEvent> = events
        .get(from..)
        .unwrap_or(&[])
        .iter()
        .take_while(|e| e.height < end)
        .take(take)
        .collect();

    let mut body = String::with_capacity(16 + page.iter().map(|e| e.json.len() + 1).sum::<usize>());
    body.push_str(r#"{"data":["#);
    for (i, e) in page.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&e.json);
    }
    body.push_str("]}");

    let cursor = if page.is_empty() {
        String::new()
    } else {
        format!("i:{}", from + page.len())
    };
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert("x-supra-cursor", HeaderValue::from_str(&cursor).unwrap());
    (headers, body).into_response()
}
