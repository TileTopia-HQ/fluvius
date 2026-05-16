//! State management for stateful stream operators.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Thread-safe state store for operator state.
#[derive(Clone)]
pub struct StateStore {
    inner: Arc<DashMap<String, StateEntry>>,
}

/// A single state entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateEntry {
    pub value: serde_json::Value,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl StateStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// Get state for a key.
    pub fn get(&self, key: &str) -> Option<StateEntry> {
        self.inner.get(key).map(|e| e.value().clone())
    }

    /// Set state for a key.
    pub fn set(&self, key: impl Into<String>, value: serde_json::Value) {
        self.inner.insert(
            key.into(),
            StateEntry {
                value,
                updated_at: chrono::Utc::now(),
            },
        );
    }

    /// Remove state for a key.
    pub fn remove(&self, key: &str) -> Option<StateEntry> {
        self.inner.remove(key).map(|(_, v)| v)
    }

    /// Get all keys.
    pub fn keys(&self) -> Vec<String> {
        self.inner.iter().map(|e| e.key().clone()).collect()
    }

    /// Total number of entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clear all state.
    pub fn clear(&self) {
        self.inner.clear();
    }
}

impl Default for StateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_store_crud() {
        let store = StateStore::new();

        store.set(
            "vehicle-1:position",
            serde_json::json!({"lon": 1.0, "lat": 2.0}),
        );
        store.set(
            "vehicle-2:position",
            serde_json::json!({"lon": 3.0, "lat": 4.0}),
        );

        assert_eq!(store.len(), 2);

        let entry = store.get("vehicle-1:position").unwrap();
        assert_eq!(entry.value["lon"], 1.0);

        store.remove("vehicle-1:position");
        assert_eq!(store.len(), 1);
        assert!(store.get("vehicle-1:position").is_none());
    }

    #[test]
    fn test_state_store_clone_is_shared() {
        let store = StateStore::new();
        let store2 = store.clone();

        store.set("key", serde_json::json!("value"));
        assert!(store2.get("key").is_some());
    }
}
