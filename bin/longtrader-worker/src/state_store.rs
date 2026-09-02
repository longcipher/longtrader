//! File-backed strategy state store.
//!
//! Persists strategy state as JSON at a configured path so workers recover
//! their working state across restarts. This is the worker-side analogue of
//! the engine's journal-backed state records; both serialize the same
//! strategy-owned types.

use std::path::PathBuf;

use color_eyre::{Result, eyre::WrapErr};

/// JSON file state store.
#[derive(Debug, Clone)]
pub struct StateStore {
    path: PathBuf,
}

impl StateStore {
    /// Creates a store backed by `path`.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Saves `state` atomically via write-to-temp + fsync + rename.
    ///
    /// # Errors
    /// Propagates filesystem / serialization errors.
    pub fn save(&self, state: &serde_json::Value) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .wrap_err_with(|| format!("create {}", parent.display()))?;
        }
        let json = serde_json::to_string_pretty(state).wrap_err("serialize state")?;
        // Use `state.json.tmp` instead of `state.tmp` to avoid colliding when both `state` and
        // `state.json` exist.
        let tmp = {
            let file_name =
                self.path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
            self.path.with_file_name(format!("{file_name}.tmp"))
        };
        std::fs::write(&tmp, &json).wrap_err_with(|| format!("write {}", tmp.display()))?;
        // Ensure durability before rename.
        std::fs::File::open(&tmp)
            .wrap_err_with(|| format!("open {}", tmp.display()))?
            .sync_all()
            .wrap_err_with(|| format!("fsync {}", tmp.display()))?;
        std::fs::rename(&tmp, &self.path)
            .wrap_err_with(|| format!("rename {} -> {}", tmp.display(), self.path.display()))?;
        if let Some(parent) = self.path.parent() {
            // Directory fsync for crash consistency (best-effort).
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }

    /// Loads the previously saved state; `Ok(None)` when absent.
    ///
    /// # Errors
    /// Propagates filesystem / deserialization errors.
    pub fn load(&self) -> Result<Option<serde_json::Value>> {
        match std::fs::read_to_string(&self.path) {
            Ok(json) => Ok(Some(serde_json::from_str(&json).wrap_err("deserialize state")?)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).wrap_err_with(|| format!("read {}", self.path.display())),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = StateStore::new(dir.path().join("nested/state.json"));
        assert!(store.load().expect("load").is_none());
        store.save(&json!({ "levels": [1, 2, 3] })).expect("save");
        let loaded = store.load().expect("load").expect("present");
        assert_eq!(loaded["levels"][2], 3);
    }
}
