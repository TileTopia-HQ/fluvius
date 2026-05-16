//! Multi-tenant API — rate limiting, API key management, tenant isolation.

use std::time::Instant;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors from the multi-tenant system.
#[derive(Debug, Error)]
pub enum TenantError {
    #[error("Invalid API key")]
    InvalidKey,
    #[error("Rate limit exceeded: {0} requests/s")]
    RateLimited(u32),
    #[error("Tenant quota exceeded")]
    QuotaExceeded,
    #[error("Tenant not found: {0}")]
    NotFound(String),
}

/// Tenant configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantConfig {
    /// Tenant identifier.
    pub id: String,
    /// Display name.
    pub name: String,
    /// API key (hashed in production).
    pub api_key: String,
    /// Maximum events per second.
    pub rate_limit: u32,
    /// Maximum concurrent pipelines.
    pub max_pipelines: usize,
    /// Maximum events stored in replay buffer.
    pub max_replay_buffer: usize,
    /// Whether the tenant is active.
    pub active: bool,
}

/// Rate limiter using token bucket algorithm.
#[derive(Debug)]
struct RateBucket {
    tokens: f64,
    max_tokens: f64,
    refill_rate: f64, // tokens per second
    last_refill: Instant,
}

impl RateBucket {
    fn new(rate_limit: u32) -> Self {
        Self {
            tokens: rate_limit as f64,
            max_tokens: rate_limit as f64 * 2.0, // burst allowance
            refill_rate: rate_limit as f64,
            last_refill: Instant::now(),
        }
    }

    fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.max_tokens);
        self.last_refill = now;
    }
}

/// Tenant usage statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TenantStats {
    /// Total events processed.
    pub events_processed: u64,
    /// Total events rejected (rate limited).
    pub events_rejected: u64,
    /// Active pipelines.
    pub active_pipelines: usize,
    /// Bytes processed.
    pub bytes_processed: u64,
}

/// Multi-tenant manager.
pub struct TenantManager {
    tenants: DashMap<String, TenantConfig>,
    api_key_index: DashMap<String, String>, // api_key -> tenant_id
    rate_limiters: DashMap<String, RateBucket>,
    stats: DashMap<String, TenantStats>,
}

impl TenantManager {
    pub fn new() -> Self {
        Self {
            tenants: DashMap::new(),
            api_key_index: DashMap::new(),
            rate_limiters: DashMap::new(),
            stats: DashMap::new(),
        }
    }

    /// Register a new tenant.
    pub fn register(&self, config: TenantConfig) {
        self.api_key_index
            .insert(config.api_key.clone(), config.id.clone());
        self.rate_limiters
            .insert(config.id.clone(), RateBucket::new(config.rate_limit));
        self.stats.insert(config.id.clone(), TenantStats::default());
        self.tenants.insert(config.id.clone(), config);
    }

    /// Authenticate a request by API key.
    pub fn authenticate(&self, api_key: &str) -> Result<String, TenantError> {
        self.api_key_index
            .get(api_key)
            .map(|entry| entry.value().clone())
            .ok_or(TenantError::InvalidKey)
    }

    /// Check rate limit for a tenant. Returns Ok if allowed, Err if rate limited.
    pub fn check_rate_limit(&self, tenant_id: &str) -> Result<(), TenantError> {
        let mut bucket = self
            .rate_limiters
            .get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        if bucket.try_consume() {
            Ok(())
        } else {
            let config = self
                .tenants
                .get(tenant_id)
                .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;
            // Record rejection
            if let Some(mut stats) = self.stats.get_mut(tenant_id) {
                stats.events_rejected += 1;
            }
            Err(TenantError::RateLimited(config.rate_limit))
        }
    }

    /// Record a processed event for a tenant.
    pub fn record_event(&self, tenant_id: &str, bytes: u64) {
        if let Some(mut stats) = self.stats.get_mut(tenant_id) {
            stats.events_processed += 1;
            stats.bytes_processed += bytes;
        }
    }

    /// Get tenant statistics.
    pub fn get_stats(&self, tenant_id: &str) -> Option<TenantStats> {
        self.stats.get(tenant_id).map(|s| s.clone())
    }

    /// Get tenant configuration.
    pub fn get_tenant(&self, tenant_id: &str) -> Option<TenantConfig> {
        self.tenants.get(tenant_id).map(|t| t.clone())
    }

    /// List all tenant IDs.
    pub fn list_tenants(&self) -> Vec<String> {
        self.tenants.iter().map(|e| e.key().clone()).collect()
    }

    /// Deactivate a tenant.
    pub fn deactivate(&self, tenant_id: &str) -> Result<(), TenantError> {
        let mut tenant = self
            .tenants
            .get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;
        tenant.active = false;
        Ok(())
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_tenant() -> TenantConfig {
        TenantConfig {
            id: "tenant-1".to_string(),
            name: "Test Corp".to_string(),
            api_key: "key-abc123".to_string(),
            rate_limit: 100,
            max_pipelines: 5,
            max_replay_buffer: 10_000,
            active: true,
        }
    }

    #[test]
    fn test_register_and_authenticate() {
        let manager = TenantManager::new();
        manager.register(test_tenant());

        let result = manager.authenticate("key-abc123");
        assert_eq!(result.unwrap(), "tenant-1");
    }

    #[test]
    fn test_invalid_key() {
        let manager = TenantManager::new();
        let result = manager.authenticate("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_rate_limiting() {
        let mut config = test_tenant();
        config.rate_limit = 2; // Very low limit
        let manager = TenantManager::new();
        manager.register(config);

        // First two should pass (initial tokens = rate_limit)
        assert!(manager.check_rate_limit("tenant-1").is_ok());
        assert!(manager.check_rate_limit("tenant-1").is_ok());
        // Third should fail (no time for refill)
        assert!(manager.check_rate_limit("tenant-1").is_err());
    }

    #[test]
    fn test_record_event() {
        let manager = TenantManager::new();
        manager.register(test_tenant());
        manager.record_event("tenant-1", 256);
        manager.record_event("tenant-1", 512);

        let stats = manager.get_stats("tenant-1").unwrap();
        assert_eq!(stats.events_processed, 2);
        assert_eq!(stats.bytes_processed, 768);
    }

    #[test]
    fn test_deactivate() {
        let manager = TenantManager::new();
        manager.register(test_tenant());
        manager.deactivate("tenant-1").unwrap();

        let tenant = manager.get_tenant("tenant-1").unwrap();
        assert!(!tenant.active);
    }
}
