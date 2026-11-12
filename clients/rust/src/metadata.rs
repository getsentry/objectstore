//! Client-side handling of custom metadata keys.

use objectstore_types::metadata::{Metadata, normalize_custom_key};

/// Validates the keys of `metadata.custom` and lowercases them in place.
///
/// Fails with the first invalid key, leaving `metadata` unchanged in that case. Only keys that
/// actually change are moved, so already-lowercase keys cost no map rebuild. If two keys differ
/// only in case, one of them is kept and the other is dropped.
pub fn normalize_keys(metadata: &mut Metadata) -> crate::Result<()> {
    let mut renamed = Vec::new();
    for key in metadata.custom.keys() {
        let normalized = normalize_custom_key(key)?;
        if normalized != *key {
            renamed.push((key.clone(), normalized));
        }
    }

    for (key, normalized) in renamed {
        if let Some(value) = metadata.custom.remove(&key) {
            metadata.custom.entry(normalized).or_insert(value);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(entries: &[(&str, &str)]) -> Metadata {
        let mut metadata = Metadata::default();
        for (key, value) in entries {
            metadata
                .custom
                .insert((*key).to_owned(), (*value).to_owned());
        }
        metadata
    }

    #[test]
    fn lowercases_only_changed_keys() {
        let mut m = metadata(&[("Build_ID", "1"), ("plain", "2"), ("a:B", "3")]);
        normalize_keys(&mut m).unwrap();
        let keys: Vec<_> = m.custom.keys().map(String::as_str).collect();
        assert_eq!(keys, ["a:b", "build_id", "plain"]);
        assert_eq!(m.custom["build_id"], "1");
    }

    #[test]
    fn invalid_key_leaves_metadata_unchanged() {
        let mut m = metadata(&[("Fine", "1"), ("not valid", "2")]);
        let error = normalize_keys(&mut m).unwrap_err();
        assert!(error.to_string().contains("not valid"), "{error}");
        assert_eq!(m.custom.len(), 2);
        assert!(m.custom.contains_key("Fine"));
    }

    #[test]
    fn case_collision_keeps_one_value() {
        let mut m = metadata(&[("A", "upper"), ("a", "lower")]);
        normalize_keys(&mut m).unwrap();
        assert_eq!(m.custom.len(), 1);
        assert_eq!(m.custom["a"], "lower");
    }
}
