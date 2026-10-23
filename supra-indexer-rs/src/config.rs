use std::time::Duration;

use crate::error::{IndexerError, Result};

/// Exponential backoff with jitter, used for RPC retries and for backing off
/// after a failed indexing cycle.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total attempts per RPC request (1 = no retry).
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Delay before retry number `attempt` (0-based): `base * 2^attempt`,
    /// capped at `max_delay`, plus up to 25% jitter.
    pub fn delay(&self, attempt: u32) -> Duration {
        let base = self
            .base_delay
            .saturating_mul(2u32.saturating_pow(attempt.min(16)))
            .min(self.max_delay);
        let jitter_ms = fastrand::u64(0..=(base.as_millis() as u64) / 4);
        base + Duration::from_millis(jitter_ms)
    }
}

/// Indexer configuration. Build with [`IndexerConfig::new`] and override the
/// defaults with the chained setters.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct IndexerConfig {
    /// Supra RPC base URL, e.g. `https://rpc-testnet.supra.com`.
    pub node_url: String,
    /// Address of the module publisher whose events are indexed.
    pub contract_address: String,
    /// First block to index for events that have no checkpoint yet.
    pub start_block: u64,
    /// When true, every registered event is rewound to `start_block` on
    /// startup (full reindex). Your stores must be idempotent.
    pub force_start_block: bool,
    /// Sleep between cycles once the indexer has caught up.
    pub poll_interval: Duration,
    /// Events requested per RPC page.
    pub page_limit: u16,
    /// Blocks to stay behind the chain tip.
    pub finality_lag: u64,
    /// Upper bound of blocks covered by one cycle. Keeps all events moving
    /// through history together, so cross-event ordering holds during a
    /// reindex, and keeps shutdown/pause responsive.
    pub max_blocks_per_cycle: u64,
    pub http_timeout: Duration,
    pub retry: RetryPolicy,
    /// Hold a PostgreSQL advisory lock so that only one process indexes this
    /// contract against the same database. Uses one pooled connection.
    pub instance_lock: bool,
}

impl IndexerConfig {
    pub fn new(node_url: impl Into<String>, contract_address: impl Into<String>) -> Self {
        Self {
            node_url: node_url.into().trim_end_matches('/').to_owned(),
            contract_address: contract_address.into().trim().to_owned(),
            start_block: 0,
            force_start_block: false,
            poll_interval: Duration::from_secs(2),
            page_limit: 100,
            finality_lag: 5,
            max_blocks_per_cycle: 100_000,
            http_timeout: Duration::from_secs(30),
            retry: RetryPolicy::default(),
            instance_lock: true,
        }
    }

    pub fn start_block(mut self, block: u64) -> Self {
        self.start_block = block;
        self
    }

    pub fn force_start_block(mut self, force: bool) -> Self {
        self.force_start_block = force;
        self
    }

    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn page_limit(mut self, limit: u16) -> Self {
        self.page_limit = limit;
        self
    }

    pub fn finality_lag(mut self, blocks: u64) -> Self {
        self.finality_lag = blocks;
        self
    }

    pub fn max_blocks_per_cycle(mut self, blocks: u64) -> Self {
        self.max_blocks_per_cycle = blocks;
        self
    }

    pub fn http_timeout(mut self, timeout: Duration) -> Self {
        self.http_timeout = timeout;
        self
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn instance_lock(mut self, enabled: bool) -> Self {
        self.instance_lock = enabled;
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let url = url::Url::parse(&self.node_url)
            .map_err(|e| IndexerError::Config(format!("node_url `{}`: {e}", self.node_url)))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(IndexerError::Config("node_url must be http(s)".into()));
        }
        let hex = self
            .contract_address
            .strip_prefix("0x")
            .ok_or_else(|| IndexerError::Config("contract_address must start with 0x".into()))?;
        if hex.is_empty() || hex.len() > 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(IndexerError::Config(format!(
                "contract_address `{}` is not a valid hex address",
                self.contract_address
            )));
        }
        if self.page_limit == 0 {
            return Err(IndexerError::Config("page_limit must be > 0".into()));
        }
        if self.max_blocks_per_cycle == 0 {
            return Err(IndexerError::Config(
                "max_blocks_per_cycle must be > 0".into(),
            ));
        }
        if self.retry.max_attempts == 0 {
            return Err(IndexerError::Config(
                "retry.max_attempts must be > 0".into(),
            ));
        }
        if i64::try_from(self.start_block).is_err() {
            return Err(IndexerError::Config("start_block exceeds i64::MAX".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_contract_address() {
        let ok = IndexerConfig::new("https://rpc-testnet.supra.com/", "0x1");
        assert!(ok.validate().is_ok());
        assert_eq!(ok.node_url, "https://rpc-testnet.supra.com");

        assert!(IndexerConfig::new("https://x", "1").validate().is_err());
        assert!(IndexerConfig::new("https://x", "0xzz").validate().is_err());
        assert!(IndexerConfig::new("ftp://x", "0x1").validate().is_err());
        assert!(
            IndexerConfig::new("https://x", "0x1")
                .page_limit(0)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn retry_delay_is_capped() {
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(400),
        };
        let d0 = policy.delay(0);
        assert!(d0 >= Duration::from_millis(100) && d0 <= Duration::from_millis(125));
        let d10 = policy.delay(10);
        assert!(d10 >= Duration::from_millis(400) && d10 <= Duration::from_millis(500));
    }
}
