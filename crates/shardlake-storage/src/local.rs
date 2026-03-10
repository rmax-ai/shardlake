use std::{
    fs,
    path::{Path, PathBuf},
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

    fn full_path(&self, key: &str) -> PathBuf {
        self.root
            .join(key.replace('/', std::path::MAIN_SEPARATOR_STR))
    }
}

impl ObjectStore for LocalObjectStore {
    fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let path = self.full_path(key);
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
        let path = self.full_path(key);
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
        Ok(self.full_path(key).exists())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let base = self.full_path(prefix);
        if !base.exists() {
            return Ok(Vec::new());
        }
        let mut keys = Vec::new();
        collect_keys(&base, &self.root, &mut keys)?;
        keys.sort();
        Ok(keys)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.full_path(key);
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
