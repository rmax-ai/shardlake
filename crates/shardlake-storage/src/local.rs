use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use tracing::{debug, trace};

use crate::{ObjectStore, Result, StorageError};

/// Local filesystem-backed object store.
///
/// All keys are mapped to paths under `root`. Path separators in keys
/// are treated as directory separators, enabling a simple hierarchical
/// namespace without special configuration.
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// Create a new store rooted at `root`, creating the directory if needed.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|e| StorageError::Io {
            path: root.display().to_string(),
            source: e,
        })?;
        debug!(root = %root.display(), "LocalObjectStore initialised");
        Ok(Self { root })
    }

    fn full_path(&self, key: &str) -> Result<PathBuf> {
        Ok(self.root.join(sanitise_key(key, false)?))
    }

    fn full_prefix_path(&self, prefix: &str) -> Result<PathBuf> {
        Ok(self.root.join(sanitise_key(prefix, true)?))
    }
}

impl ObjectStore for LocalObjectStore {
    fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let path = self.full_path(key)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| StorageError::Io {
                path: parent.display().to_string(),
                source: e,
            })?;
        }
        trace!(key, bytes = data.len(), "put");
        fs::write(&path, data).map_err(|e| StorageError::Io {
            path: path.display().to_string(),
            source: e,
        })
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.full_path(key)?;
        trace!(key, "get");
        fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Io {
                    path: path.display().to_string(),
                    source: e,
                }
            }
        })
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.full_path(key)?.exists())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let base = self.full_prefix_path(prefix)?;
        if !base.exists() {
            return Ok(Vec::new());
        }
        let mut keys = Vec::new();
        collect_keys(&base, &self.root, &mut keys)?;
        keys.sort();
        Ok(keys)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.full_path(key)?;
        fs::remove_file(&path).map_err(|e| StorageError::Io {
            path: path.display().to_string(),
            source: e,
        })
    }
}

fn collect_keys(dir: &Path, root: &Path, out: &mut Vec<String>) -> Result<()> {
    if dir.is_file() {
        let rel = dir
            .strip_prefix(root)
            .map_err(|e| StorageError::Other(e.to_string()))?;
        out.push(
            rel.to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/"),
        );
        return Ok(());
    }
    for entry in fs::read_dir(dir).map_err(|e| StorageError::Io {
        path: dir.display().to_string(),
        source: e,
    })? {
        let entry = entry.map_err(|e| StorageError::Io {
            path: dir.display().to_string(),
            source: e,
        })?;
        collect_keys(&entry.path(), root, out)?;
    }
    Ok(())
}

fn sanitise_key(key: &str, allow_empty: bool) -> Result<PathBuf> {
    if key.contains('\\') {
        return Err(StorageError::InvalidKey {
            key: key.to_string(),
            reason: "backslashes are not allowed in object-store keys".into(),
        });
    }

    if key.is_empty() {
        return if allow_empty {
            Ok(PathBuf::new())
        } else {
            Err(StorageError::InvalidKey {
                key: key.to_string(),
                reason: "key must not be empty".into(),
            })
        };
    }

    let mut relative = PathBuf::new();
    for component in Path::new(key).components() {
        match component {
            Component::Normal(part) => relative.push(part),
            Component::CurDir => {
                return Err(StorageError::InvalidKey {
                    key: key.to_string(),
                    reason: "current-directory path segments are not allowed".into(),
                });
            }
            Component::ParentDir => {
                return Err(StorageError::InvalidKey {
                    key: key.to_string(),
                    reason: "parent-directory path segments are not allowed".into(),
                });
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(StorageError::InvalidKey {
                    key: key.to_string(),
                    reason: "absolute paths are not allowed".into(),
                });
            }
        }
    }

    if relative.as_os_str().is_empty() && !allow_empty {
        return Err(StorageError::InvalidKey {
            key: key.to_string(),
            reason: "key must contain at least one path segment".into(),
        });
    }

    Ok(relative)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn assert_invalid_key(err: StorageError) {
        assert!(matches!(err, StorageError::InvalidKey { .. }));
    }

    #[test]
    fn put_get_list_and_delete_round_trip() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();

        assert!(!store.exists("foo/bar").unwrap());
        store.put("foo/bar", b"hello".to_vec()).unwrap();
        assert!(store.exists("foo/bar").unwrap());
        assert_eq!(store.get("foo/bar").unwrap(), b"hello");
        assert_eq!(store.list("foo").unwrap(), vec!["foo/bar".to_string()]);

        store.delete("foo/bar").unwrap();
        assert!(!store.exists("foo/bar").unwrap());
    }

    #[test]
    fn rejects_path_traversal_and_absolute_keys() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();

        for key in [
            "../escape",
            "/absolute/path",
            "nested/../../escape",
            "./dot",
            "foo\\bar",
        ] {
            assert_invalid_key(store.put(key, b"nope".to_vec()).unwrap_err());
            assert_invalid_key(store.get(key).unwrap_err());
            assert_invalid_key(store.exists(key).unwrap_err());
            assert_invalid_key(store.list(key).unwrap_err());
            assert_invalid_key(store.delete(key).unwrap_err());
        }
    }
}
