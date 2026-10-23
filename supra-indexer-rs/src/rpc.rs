//! Minimal Supra RPC v3 client: event pages and the latest block height.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use reqwest::{Client, Response, StatusCode, header::HeaderMap};
use serde::Deserialize;
use tracing::{debug, warn};
use url::Url;

use crate::config::{IndexerConfig, RetryPolicy};
use crate::error::{IndexerError, Result};

const CURSOR_HEADER: &str = "x-supra-cursor";
const OLDEST_BLOCK_HEADER: &str = "x-supra-oldest-block";
const MAX_ERROR_BODY: usize = 512;

/// Where an event page request starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageStart {
    /// First page of a block range.
    Height(u64),
    /// Continuation cursor from `x-supra-cursor`.
    Cursor(String),
}

/// One `GET /rpc/v3/events/{address}::{event}` request.
#[derive(Debug, Clone)]
pub struct EventQuery<'a> {
    /// Fully qualified event type, e.g. `0x1::coin::CoinDeposit`.
    pub event_type: &'a str,
    pub start: PageStart,
    /// Exclusive upper bound (`end_height`).
    pub end_height_exclusive: u64,
    pub limit: u16,
}

#[derive(Debug)]
pub struct EventPage {
    /// Raw JSON body, decoded by the registered response type.
    pub body: Bytes,
    /// Cursor for the next page; `None` when the header is missing or empty.
    pub next_cursor: Option<String>,
    /// Oldest block the node still serves (`x-supra-oldest-block`).
    pub oldest_block: Option<u64>,
}

#[derive(Deserialize)]
struct BlockResponse {
    header: BlockHeader,
}

#[derive(Deserialize)]
struct BlockHeader {
    height: u64,
}

#[derive(Debug, Clone)]
pub struct SupraClient {
    http: Client,
    base: Url,
    retry: RetryPolicy,
    requests: Arc<AtomicU64>,
}

impl SupraClient {
    pub fn new(config: &IndexerConfig) -> Result<Self> {
        let http = Client::builder()
            .timeout(config.http_timeout)
            .user_agent(concat!("supra-indexer-sdk/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let base = Url::parse(&config.node_url)
            .map_err(|e| IndexerError::Config(format!("node_url: {e}")))?;
        Ok(Self {
            http,
            base,
            retry: config.retry.clone(),
            requests: Arc::new(AtomicU64::new(0)),
        })
    }

    /// HTTP requests sent by this client and its clones, retries included.
    pub fn requests_sent(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    pub async fn latest_block_height(&self) -> Result<u64> {
        let url = self.url(&["rpc", "v3", "block"])?;
        let response = self.get_with_retry(&url).await?;
        let block: BlockResponse = response.json().await?;
        Ok(block.header.height)
    }

    pub async fn fetch_events(&self, query: &EventQuery<'_>) -> Result<EventPage> {
        let mut url = self.url(&["rpc", "v3", "events", query.event_type])?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("limit", &query.limit.to_string());
            match &query.start {
                PageStart::Height(h) => pairs.append_pair("start_height", &h.to_string()),
                PageStart::Cursor(c) => pairs.append_pair("start", c),
            };
            pairs.append_pair("end_height", &query.end_height_exclusive.to_string());
        }

        let response = self.get_with_retry(&url).await?;
        let next_cursor = header_str(response.headers(), CURSOR_HEADER)
            .filter(|c| !c.is_empty())
            .map(str::to_owned);
        let oldest_block =
            header_str(response.headers(), OLDEST_BLOCK_HEADER).and_then(|v| v.parse().ok());
        let body = response.bytes().await?;

        Ok(EventPage {
            body,
            next_cursor,
            oldest_block,
        })
    }

    fn url(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| IndexerError::Config("node_url cannot be a base URL".into()))?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }

    /// GET with retries on transport errors, 408, 429 and 5xx. Other 4xx
    /// responses fail immediately.
    async fn get_with_retry(&self, url: &Url) -> Result<Response> {
        let mut attempt = 0;
        loop {
            debug!(%url, attempt, "RPC request");
            self.requests.fetch_add(1, Ordering::Relaxed);
            let (err, retry_after) = match self.http.get(url.clone()).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(resp),
                Ok(resp) => {
                    let status = resp.status();
                    let retry_after = header_str(resp.headers(), "retry-after")
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(Duration::from_secs);
                    let message = error_message(resp).await;
                    let retryable = is_retryable(status, &message);
                    let err = IndexerError::Rpc {
                        url: url.to_string(),
                        status: status.as_u16(),
                        message,
                    };
                    if !retryable {
                        return Err(err);
                    }
                    (err, retry_after)
                }
                Err(e) => (IndexerError::Http(e), None),
            };

            attempt += 1;
            if attempt >= self.retry.max_attempts {
                return Err(err);
            }
            let delay = retry_after
                .map(|d| d.min(self.retry.max_delay))
                .unwrap_or_else(|| self.retry.delay(attempt - 1));
            warn!(
                error = %err,
                attempt,
                max_attempts = self.retry.max_attempts,
                ?delay,
                "RPC request failed, retrying"
            );
            tokio::time::sleep(delay).await;
        }
    }
}

/// 404 "Requested data is not yet available on this node" comes from a
/// load-balanced replica that lags the node `/rpc/v3/block` was read from.
fn is_retryable(status: StatusCode, message: &str) -> bool {
    status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
        || (status == StatusCode::NOT_FOUND && message.contains("not yet available"))
}

fn header_str<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
}

/// Prefer the `message` field of a JSON error body; fall back to the raw
/// body, truncated.
async fn error_message(resp: Response) -> String {
    let body = match resp.text().await {
        Ok(body) => body,
        Err(e) => return format!("<unreadable body: {e}>"),
    };
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
        if let Some(msg) = json.get("message").and_then(|m| m.as_str()) {
            return msg.to_owned();
        }
    }
    let mut end = body.len().min(MAX_ERROR_BODY);
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    body[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn client(server: &MockServer) -> SupraClient {
        let retry = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
        };
        SupraClient::new(&IndexerConfig::new(server.uri(), "0x1").retry(retry)).unwrap()
    }

    fn query(start: PageStart) -> EventQuery<'static> {
        EventQuery {
            event_type: "0x1::coin::CoinDeposit",
            start,
            end_height_exclusive: 200,
            limit: 50,
        }
    }

    #[tokio::test]
    async fn builds_query_and_reads_headers() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rpc/v3/events/0x1::coin::CoinDeposit"))
            .and(query_param("start_height", "100"))
            .and(query_param("end_height", "200"))
            .and(query_param("limit", "50"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(CURSOR_HEADER, "abc")
                    .insert_header(OLDEST_BLOCK_HEADER, "42")
                    .set_body_string(r#"{"data":[]}"#),
            )
            .expect(1)
            .mount(&server)
            .await;

        let page = client(&server)
            .fetch_events(&query(PageStart::Height(100)))
            .await
            .unwrap();
        assert_eq!(page.next_cursor.as_deref(), Some("abc"));
        assert_eq!(page.oldest_block, Some(42));
        assert_eq!(&page.body[..], br#"{"data":[]}"#);
    }

    #[tokio::test]
    async fn cursor_is_url_encoded_and_empty_cursor_is_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(query_param("start", "a+b/c"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(CURSOR_HEADER, "")
                    .set_body_string(r#"{"data":[]}"#),
            )
            .expect(1)
            .mount(&server)
            .await;

        let page = client(&server)
            .fetch_events(&query(PageStart::Cursor("a+b/c".into())))
            .await
            .unwrap();
        assert_eq!(page.next_cursor, None);
    }

    #[tokio::test]
    async fn retries_server_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/rpc/v3/block"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"header":{"height":7}}"#))
            .mount(&server)
            .await;

        assert_eq!(client(&server).latest_block_height().await.unwrap(), 7);
    }

    #[tokio::test]
    async fn does_not_retry_client_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(400).set_body_string(r#"{"message":"bad range"}"#))
            .expect(1)
            .mount(&server)
            .await;

        let err = client(&server)
            .fetch_events(&query(PageStart::Height(1)))
            .await
            .unwrap_err();
        match err {
            IndexerError::Rpc {
                status, message, ..
            } => {
                assert_eq!(status, 400);
                assert_eq!(message, "bad range");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn retries_lagging_replica_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404).set_body_string(
                r#"{"message":"Not Found: Requested data is not yet available on this node."}"#,
            ))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"data":[]}"#))
            .mount(&server)
            .await;

        let page = client(&server)
            .fetch_events(&query(PageStart::Height(1)))
            .await
            .unwrap();
        assert_eq!(&page.body[..], br#"{"data":[]}"#);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "0"))
            .expect(3)
            .mount(&server)
            .await;

        let err = client(&server).latest_block_height().await.unwrap_err();
        assert!(matches!(err, IndexerError::Rpc { status: 429, .. }));
    }
}
