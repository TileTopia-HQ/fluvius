//! Checkpointing — periodic state snapshots for crash recovery.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::state::StateStore;

/// A checkpoint: serialized snapshot of state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub id: u64,
    pub timestamp: DateTime<Utc>,
    pub entries: Vec<(String, serde_json::Value)>,
}

/// Manages periodic checkpoints to disk.
pub struct CheckpointManager {
    dir: PathBuf,
    next_id: u64,
    max_retained: usize,
}

impl CheckpointManager {
    /// Create a new checkpoint manager writing to the given directory.
    pub fn new(dir: impl Into<PathBuf>, max_retained: usize) -> std::io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;

        // Find the highest existing checkpoint ID
        let next_id = Self::list_checkpoints_in(&dir)
            .last()
            .map(|c| c.id + 1)
            .unwrap_or(0);

        Ok(Self {
            dir,
            next_id,
            max_retained,
        })
    }

    /// Take a checkpoint of the given state store.
    pub fn checkpoint(&mut self, state: &StateStore) -> std::io::Result<Checkpoint> {
        let entries: Vec<(String, serde_json::Value)> = state
            .keys()
            .into_iter()
            .filter_map(|k| state.get(&k).map(|e| (k, e.value)))
            .collect();

        let cp = Checkpoint {
            id: self.next_id,
            timestamp: Utc::now(),
            entries,
        };

        let path = self.checkpoint_path(cp.id);
        let data = serde_json::to_vec(&cp).map_err(|e| std::io::Error::other(e.to_string()))?;
        fs::write(&path, data)?;

        self.next_id += 1;
        self.gc()?;

        Ok(cp)
    }

    /// Restore from the latest checkpoint.
    pub fn restore_latest(&self, state: &StateStore) -> std::io::Result<Option<Checkpoint>> {
        let checkpoints = Self::list_checkpoints_in(&self.dir);
        let Some(latest) = checkpoints.last() else {
            return Ok(None);
        };

        // Apply to state store
        for (key, value) in &latest.entries {
            state.set(key.clone(), value.clone());
        }

        Ok(Some(latest.clone()))
    }

    /// List all checkpoints in the directory, sorted by ID.
    fn list_checkpoints_in(dir: &Path) -> Vec<Checkpoint> {
        let Ok(entries) = fs::read_dir(dir) else {
            return Vec::new();
        };

        let mut checkpoints: Vec<Checkpoint> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .filter_map(|e| {
                let data = fs::read(e.path()).ok()?;
                serde_json::from_slice(&data).ok()
            })
            .collect();

        checkpoints.sort_by_key(|c| c.id);
        checkpoints
    }

    /// Remove old checkpoints beyond the retention limit.
    fn gc(&self) -> std::io::Result<()> {
        let checkpoints = Self::list_checkpoints_in(&self.dir);
        if checkpoints.len() > self.max_retained {
            let to_remove = checkpoints.len() - self.max_retained;
            for cp in checkpoints.into_iter().take(to_remove) {
                let path = self.checkpoint_path(cp.id);
                let _ = fs::remove_file(path);
            }
        }
        Ok(())
    }

    fn checkpoint_path(&self, id: u64) -> PathBuf {
        self.dir.join(format!("checkpoint_{id:08}.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_checkpoint_and_restore() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = CheckpointManager::new(tmp.path(), 3).unwrap();

        let state = StateStore::new();
        state.set("key1", serde_json::json!("value1"));
        state.set("key2", serde_json::json!(42));

        let cp = mgr.checkpoint(&state).unwrap();
        assert_eq!(cp.id, 0);
        assert_eq!(cp.entries.len(), 2);

        // Restore into fresh state
        let new_state = StateStore::new();
        let restored = mgr.restore_latest(&new_state).unwrap().unwrap();
        assert_eq!(restored.id, 0);
        assert!(new_state.get("key1").is_some());
        assert!(new_state.get("key2").is_some());
    }

    #[test]
    fn test_checkpoint_gc() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = CheckpointManager::new(tmp.path(), 2).unwrap();

        let state = StateStore::new();
        state.set("k", serde_json::json!(1));

        mgr.checkpoint(&state).unwrap(); // 0
        mgr.checkpoint(&state).unwrap(); // 1
        mgr.checkpoint(&state).unwrap(); // 2

        // Should only have 2 files (GC removed oldest)
        let files: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(files.len(), 2);
    }
}
