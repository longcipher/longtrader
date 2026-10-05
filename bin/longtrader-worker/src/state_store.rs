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
                self.path.file_name().map_or_default(|n| n.to_string_lossy().to_string());
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
    use proptest::prelude::*;
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

    // -----------------------------------------------------------------------
    // Round trips and atomicity
    // -----------------------------------------------------------------------

    #[test]
    fn nested_documents_survive_unchanged() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = StateStore::new(dir.path().join("state.json"));
        let state = json!({
            "levels": [{"price": "1.5", "is_buy": true}, {"price": "2.5", "is_buy": false}],
            "seq": 7u64,
            "label": "grid",
            "nested": {"a": [1, 2, {"b": null}]},
        });
        store.save(&state).expect("save");
        assert_eq!(store.load().expect("load").expect("present"), state);
    }

    /// An empty JSON object is a legitimate state, so `Some({})` must be
    /// distinguishable from "never saved" (`None`).
    #[test]
    fn an_empty_object_is_present_not_absent() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = StateStore::new(dir.path().join("state.json"));
        store.save(&json!({})).expect("save");
        assert_eq!(store.load().expect("load"), Some(json!({})));
    }

    /// Saving twice must fully replace the previous state rather than merge into
    /// it, or a shorter recovery would leave stale levels behind.
    #[test]
    fn a_second_save_replaces_the_first() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = StateStore::new(dir.path().join("state.json"));
        store.save(&json!({"a": 1, "b": 2})).expect("save");
        store.save(&json!({"a": 9})).expect("save");
        assert_eq!(store.load().expect("load"), Some(json!({"a": 9})));
    }

    /// The parent directory is created on demand, so a strategy pointing at a
    /// fresh path does not have to pre-create it.
    #[test]
    fn save_creates_missing_parent_directories() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = StateStore::new(dir.path().join("a/b/c/state.json"));
        store.save(&json!({"x": 1})).expect("save creates parents");
        assert!(dir.path().join("a/b/c/state.json").exists());
    }

    /// `save` is documented as write-temp + fsync + rename, so a completed save
    /// must leave no `.tmp` residue for the next run to trip over.
    #[test]
    fn a_completed_save_leaves_no_temp_residue() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = StateStore::new(dir.path().join("state.json"));
        store.save(&json!({"x": 1})).expect("save");
        assert!(!dir.path().join("state.json.tmp").exists(), "the temp file must be renamed away");
        let residue: Vec<String> = std::fs::read_dir(dir.path())
            .expect("readdir")
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(residue, vec!["state.json".to_string()]);
    }

    /// A crash between write and rename must leave the previous state intact,
    /// never a half-written file: the temp name must be distinct from the final
    /// one even when a sibling file shares the stem.
    #[test]
    fn the_temp_name_does_not_collide_with_a_sibling_file() {
        let dir = tempfile::tempdir().expect("tmp");
        // `state` and `state.json` can coexist, so the temp name must be
        // `state.json.tmp` rather than `state.tmp`.
        let store = StateStore::new(dir.path().join("state.json"));
        std::fs::write(dir.path().join("state"), "sibling").expect("write sibling");
        store.save(&json!({"x": 1})).expect("save");
        assert_eq!(std::fs::read_to_string(dir.path().join("state")).expect("read"), "sibling");
        assert_eq!(store.load().expect("load"), Some(json!({"x": 1})));
    }

    // -----------------------------------------------------------------------
    // Error paths
    //
    // Every failure must surface as an error rather than as `None`: reporting
    // "no state" for a corrupt or unreadable file would silently restart a
    // strategy from scratch.
    // -----------------------------------------------------------------------

    #[test]
    fn a_corrupt_state_file_is_an_error_not_an_absence() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("state.json");
        std::fs::write(&path, "{not json").expect("write");
        let err = StateStore::new(&path).load().expect_err("corrupt json must error");
        assert!(err.to_string().contains("deserialize state"), "{err}");
    }

    #[test]
    fn an_empty_state_file_is_an_error() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("state.json");
        std::fs::write(&path, "").expect("write");
        assert!(StateStore::new(&path).load().is_err(), "an empty file is not valid JSON");
    }

    /// A path whose parent is a regular file cannot be created, so `save` must
    /// report the directory failure with its path.
    #[test]
    fn saving_under_a_file_as_a_parent_reports_the_directory_failure() {
        let dir = tempfile::tempdir().expect("tmp");
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, "i am a file").expect("write");
        let store = StateStore::new(blocker.join("state.json"));
        let err = store.save(&json!({"x": 1})).expect_err("cannot mkdir under a file");
        assert!(err.to_string().contains("create"), "{err}");
    }

    #[test]
    fn loading_under_a_file_as_a_parent_is_an_error() {
        let dir = tempfile::tempdir().expect("tmp");
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, "i am a file").expect("write");
        let err = StateStore::new(blocker.join("state.json")).load().expect_err("cannot read");
        assert!(err.to_string().contains("read"), "{err}");
    }

    /// A directory in the state's place is a read error, never a silent `None`.
    #[test]
    fn loading_a_directory_is_an_error() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("state.json");
        std::fs::create_dir(&path).expect("mkdir");
        let err = StateStore::new(&path).load().expect_err("cannot read a directory");
        assert!(err.to_string().contains("read"), "{err}");
    }

    /// A missing file is the one legitimate `None`.
    #[test]
    fn a_missing_file_is_the_only_absence() {
        let dir = tempfile::tempdir().expect("tmp");
        assert_eq!(StateStore::new(dir.path().join("absent.json")).load().expect("load"), None);
    }

    /// A `f64` needing 16-17 significant digits used to lose its last digit
    /// through a serialize/parse cycle, because `serde_json` was built without
    /// its `float_roundtrip` feature. That feature is now enabled on this
    /// crate's `serde_json`, so the digits survive. If this ever fails again,
    /// the `float_roundtrip` feature has been dropped from the manifest.
    #[test]
    fn a_float_needing_seventeen_digits_survives_serde_json() {
        let value = serde_json::json!({ "price": 361_434.869_042_188_05f64 });
        let text = serde_json::to_string_pretty(&value).expect("serialize");
        let back: serde_json::Value = serde_json::from_str(&text).expect("parse");
        assert_eq!(
            back, value,
            "serde_json lost precision; the `float_roundtrip` feature must stay enabled"
        );
    }

    /// Short-decimal floats do survive, which is why the properties above
    /// generate that shape rather than raw `f64`s.
    #[test]
    fn a_short_decimal_float_survives_serde_json() {
        let value = serde_json::json!({ "price": 361_434.869f64 });
        let text = serde_json::to_string_pretty(&value).expect("serialize");
        let back: serde_json::Value = serde_json::from_str(&text).expect("parse");
        assert_eq!(back, value);
    }

    /// The store must be transparent byte-for-byte even for a value that needs
    /// full `f64` precision to be represented.
    #[test]
    fn the_store_is_transparent_even_for_a_seventeen_digit_float() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("state.json");
        let store = StateStore::new(&path);
        let value = serde_json::json!({ "price": 361_434.869_042_188_05f64 });
        store.save(&value).expect("save");
        assert_eq!(
            std::fs::read_to_string(&path).expect("read"),
            serde_json::to_string_pretty(&value).expect("serialize"),
            "the store must not alter the bytes it wrote"
        );
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    /// A single JSON scalar. The float arm is filtered because JSON has no
    /// representation for NaN or infinity.
    fn arb_json_leaf() -> proptest::strategy::BoxedStrategy<serde_json::Value> {
        proptest::prop_oneof![
            any::<bool>().prop_map(serde_json::Value::Bool),
            any::<i64>().prop_map(serde_json::Value::from),
            // `f64::NORMAL` rather than raw `f64`s: JSON has no representation
            // for NaN, the infinities, or subnormals that a serializer would
            // have to rewrite as `null`. With `float_roundtrip` enabled every
            // remaining value is recoverable exactly, so this arm no longer has
            // to stay small-scale to avoid measuring the serializer instead of
            // the store.
            proptest::num::f64::NORMAL.prop_map(serde_json::Value::from),
            proptest::collection::vec(any::<u8>(), 0..8)
                .prop_map(|b| serde_json::Value::String(String::from_utf8_lossy(&b).into_owned())),
        ]
        .boxed()
    }

    /// Arbitrary JSON documents, built to a bounded depth so every shape the
    /// store can be handed is covered without pulling in a dependency for an
    /// `Arbitrary` impl.
    fn arb_json() -> proptest::strategy::BoxedStrategy<serde_json::Value> {
        fn at_depth(depth: u32) -> proptest::strategy::BoxedStrategy<serde_json::Value> {
            if depth == 0 {
                return arb_json_leaf();
            }
            let inner = at_depth(depth - 1);
            proptest::prop_oneof![
                arb_json_leaf(),
                proptest::option::of(inner.clone()).prop_map(|_| serde_json::Value::Null),
                proptest::collection::vec(inner.clone(), 0..4).prop_map(serde_json::Value::Array),
                proptest::collection::btree_map("[a-z]{1,4}", inner, 0..4)
                    .prop_map(|m| serde_json::Value::Object(m.into_iter().collect())),
            ]
            .boxed()
        }
        at_depth(3)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// The store is transparent: whatever it writes back is byte-identical
        /// to what it serialized.
        ///
        /// The comparison is on the re-serialized text rather than on
        /// `Value` equality because `serde_json`'s float parser is not exact for
        /// every `f64`, so `Value` equality would be testing serde_json's
        /// round-trip rather than the store's.
        #[test]
        fn any_json_document_round_trips(value in arb_json()) {
            let Ok(dir) = tempfile::tempdir() else { return Ok(()) };
            let path = dir.path().join("state.json");
            let store = StateStore::new(&path);
            store.save(&value).expect("save");
            let written = std::fs::read_to_string(&path).expect("read back");
            let expected = serde_json::to_string_pretty(&value).expect("serialize");
            prop_assert_eq!(written, expected.as_str());
            let loaded = store.load().expect("load").expect("present");
            prop_assert_eq!(
                serde_json::to_string_pretty(&loaded).expect("re-serialize"),
                expected.as_str(),
                "a save/load cycle must not alter the document"
            );
        }

        /// A document without floats round trips exactly, value and all.
        #[test]
        fn an_integer_only_document_round_trips_exactly(
            pairs in proptest::collection::btree_map("[a-z]{1,6}", any::<i64>(), 0..6),
        ) {
            let Ok(dir) = tempfile::tempdir() else { return Ok(()) };
            let store = StateStore::new(dir.path().join("state.json"));
            let object: serde_json::Map<String, serde_json::Value> =
                pairs.into_iter().map(|(k, v)| (k, serde_json::Value::from(v))).collect();
            let value = serde_json::Value::Object(object);
            store.save(&value).expect("save");
            prop_assert_eq!(store.load().expect("load"), Some(value));
        }

        /// Saving twice always leaves the second document, whatever it was.
        #[test]
        fn the_last_write_always_wins(first in arb_json(), second in arb_json()) {
            let Ok(dir) = tempfile::tempdir() else { return Ok(()) };
            let store = StateStore::new(dir.path().join("state.json"));
            store.save(&first).expect("save");
            store.save(&second).expect("save");
            let loaded = store.load().expect("load").expect("present");
            prop_assert_eq!(
                serde_json::to_string_pretty(&loaded).expect("re-serialize"),
                serde_json::to_string_pretty(&second).expect("serialize")
            );
        }

        /// A successful save is atomic: the target file is always parseable and
        /// no temp file survives.
        #[test]
        fn a_saved_file_is_always_parseable(value in arb_json()) {
            let Ok(dir) = tempfile::tempdir() else { return Ok(()) };
            let path = dir.path().join("state.json");
            StateStore::new(&path).save(&value).expect("save");
            let raw = std::fs::read_to_string(&path).expect("read back");
            prop_assert!(serde_json::from_str::<serde_json::Value>(&raw).is_ok(), "{raw}");
            prop_assert!(!dir.path().join("state.json.tmp").exists());
        }
    }
}
