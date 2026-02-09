use crate::errors::S3O2Error;
use crate::lister::ObjectInfo;
use aws_sdk_s3::primitives::DateTime;
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Local file metadata for sync comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalFileInfo {
    /// Path relative to sync root, normalized with forward slashes
    pub relative_path: String,
    /// Absolute path to the file
    pub absolute_path: PathBuf,
    /// File size in bytes
    pub size: u64,
    /// Last modified time
    pub modified: SystemTime,
}

impl LocalFileInfo {
    /// Creates LocalFileInfo from a file path.
    ///
    /// # Arguments
    /// * `path` - Absolute path to the file
    /// * `base_path` - Base directory for calculating relative path
    pub async fn from_path(path: PathBuf, base_path: &Path) -> Result<Self, S3O2Error> {
        let metadata = tokio::fs::metadata(&path).await?;

        let relative_path = path
            .strip_prefix(base_path)
            .map_err(|e| S3O2Error::MetadataError(format!("Failed to strip prefix: {}", e)))?
            .to_string_lossy()
            .replace('\\', "/"); // Normalize to forward slashes

        Ok(Self {
            relative_path,
            absolute_path: path,
            size: metadata.len(),
            modified: metadata.modified().map_err(|e| {
                S3O2Error::MetadataError(format!("Failed to get modified time: {}", e))
            })?,
        })
    }

    /// Converts local file info to an S3 key with the given prefix.
    pub fn to_s3_key(&self, prefix: &str) -> String {
        if prefix.is_empty() {
            self.relative_path.clone()
        } else {
            let prefix = prefix.trim_end_matches('/');
            format!("{}/{}", prefix, self.relative_path)
        }
    }
}

// Implement Ord for sorting in two-pointer merge
impl Ord for LocalFileInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.relative_path.cmp(&other.relative_path)
    }
}

impl PartialOrd for LocalFileInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Result of comparing local and S3 file metadata.
#[derive(Debug, PartialEq, Eq)]
pub enum SyncDecision {
    /// Local file is newer than S3
    LocalNewer,
    /// S3 file is newer than local
    S3Newer,
    /// Files have same timestamp (or within tolerance)
    Equal,
    /// Files have different sizes (possible corruption)
    SizeMismatch,
}

/// Compares local and S3 file metadata to determine sync decision.
///
/// # Arguments
/// * `local` - Local file metadata
/// * `s3` - S3 object metadata
/// * `size_only` - If true, only compare sizes and ignore timestamps
pub fn compare_metadata(local: &LocalFileInfo, s3: &ObjectInfo, size_only: bool) -> SyncDecision {
    // Check size first - size mismatch is always significant
    if local.size != s3.size as u64 {
        return SyncDecision::SizeMismatch;
    }

    // If size_only mode, files with same size are considered equal
    if size_only {
        return SyncDecision::Equal;
    }

    // Compare timestamps with tolerance for filesystem precision
    match compare_timestamps(&s3.last_modified, local.modified) {
        Ordering::Greater => SyncDecision::S3Newer,
        Ordering::Less => SyncDecision::LocalNewer,
        Ordering::Equal => SyncDecision::Equal,
    }
}

/// Compares S3 DateTime with local SystemTime.
///
/// Returns Ordering where:
/// - Greater = S3 is newer
/// - Less = local is newer
/// - Equal = same (within 1-second tolerance)
///
/// Tolerance accounts for filesystem timestamp precision differences.
fn compare_timestamps(s3_time: &DateTime, local_time: SystemTime) -> Ordering {
    let s3_secs = s3_time.secs();

    let local_secs = local_time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // Allow 1-second tolerance for filesystem precision issues
    let diff = s3_secs - local_secs;
    if diff.abs() <= 1 {
        Ordering::Equal
    } else {
        s3_secs.cmp(&local_secs)
    }
}

/// Converts S3 key to local path within the base directory.
pub fn s3_key_to_local_path(key: &str, base: &Path, prefix: &str) -> PathBuf {
    // Strip prefix from key if present
    let relative_key = if !prefix.is_empty() {
        key.strip_prefix(prefix)
            .and_then(|k| k.strip_prefix('/'))
            .unwrap_or(key)
    } else {
        key
    };

    // Convert S3 key (forward slashes) to platform-specific path
    let normalized = relative_key.replace('/', std::path::MAIN_SEPARATOR_STR);
    base.join(normalized)
}

/// Normalizes a path to use forward slashes (S3 convention).
pub fn normalize_path(path: &str) -> String {
    path.replace('\\', "/")
        .trim_start_matches("./")
        .trim_end_matches('/')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_file_info_from_path() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"content").await.unwrap();

        let info = LocalFileInfo::from_path(file_path.clone(), temp_dir.path())
            .await
            .unwrap();

        assert_eq!(info.relative_path, "test.txt");
        assert_eq!(info.absolute_path, file_path);
        assert_eq!(info.size, 7);
    }

    #[tokio::test]
    async fn test_local_file_info_nested_path() {
        let temp_dir = TempDir::new().unwrap();
        let nested_dir = temp_dir.path().join("dir1").join("dir2");
        tokio::fs::create_dir_all(&nested_dir).await.unwrap();
        let file_path = nested_dir.join("nested.txt");
        tokio::fs::write(&file_path, b"test").await.unwrap();

        let info = LocalFileInfo::from_path(file_path, temp_dir.path())
            .await
            .unwrap();

        assert_eq!(info.relative_path, "dir1/dir2/nested.txt");
        assert_eq!(info.size, 4);
    }

    #[test]
    fn test_to_s3_key_no_prefix() {
        let info = LocalFileInfo {
            relative_path: "dir/file.txt".to_string(),
            absolute_path: PathBuf::from("/tmp/dir/file.txt"),
            size: 100,
            modified: SystemTime::now(),
        };

        assert_eq!(info.to_s3_key(""), "dir/file.txt");
    }

    #[test]
    fn test_to_s3_key_with_prefix() {
        let info = LocalFileInfo {
            relative_path: "file.txt".to_string(),
            absolute_path: PathBuf::from("/tmp/file.txt"),
            size: 100,
            modified: SystemTime::now(),
        };

        assert_eq!(info.to_s3_key("backup"), "backup/file.txt");
        assert_eq!(info.to_s3_key("backup/"), "backup/file.txt");
    }

    #[test]
    fn test_compare_metadata_size_mismatch() {
        let local = LocalFileInfo {
            relative_path: "file.txt".to_string(),
            absolute_path: PathBuf::from("/tmp/file.txt"),
            size: 100,
            modified: SystemTime::now(),
        };

        let s3 = ObjectInfo {
            key: "file.txt".to_string(),
            size: 200,
            last_modified: DateTime::from_secs(0),
        };

        assert_eq!(
            compare_metadata(&local, &s3, false),
            SyncDecision::SizeMismatch
        );
    }

    #[test]
    fn test_compare_metadata_local_newer() {
        let now = SystemTime::now();
        let local = LocalFileInfo {
            relative_path: "file.txt".to_string(),
            absolute_path: PathBuf::from("/tmp/file.txt"),
            size: 100,
            modified: now,
        };

        let past = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64 - 60;
        let s3 = ObjectInfo {
            key: "file.txt".to_string(),
            size: 100,
            last_modified: DateTime::from_secs(past),
        };

        assert_eq!(
            compare_metadata(&local, &s3, false),
            SyncDecision::LocalNewer
        );
    }

    #[test]
    fn test_compare_metadata_s3_newer() {
        let now = SystemTime::now();
        let local = LocalFileInfo {
            relative_path: "file.txt".to_string(),
            absolute_path: PathBuf::from("/tmp/file.txt"),
            size: 100,
            modified: now,
        };

        let future = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64 + 60;
        let s3 = ObjectInfo {
            key: "file.txt".to_string(),
            size: 100,
            last_modified: DateTime::from_secs(future),
        };

        assert_eq!(compare_metadata(&local, &s3, false), SyncDecision::S3Newer);
    }

    #[test]
    fn test_compare_metadata_equal_with_tolerance() {
        let now = SystemTime::now();
        let local = LocalFileInfo {
            relative_path: "file.txt".to_string(),
            absolute_path: PathBuf::from("/tmp/file.txt"),
            size: 100,
            modified: now,
        };

        // S3 time within 1 second tolerance
        let s3_time = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let s3 = ObjectInfo {
            key: "file.txt".to_string(),
            size: 100,
            last_modified: DateTime::from_secs(s3_time),
        };

        assert_eq!(compare_metadata(&local, &s3, false), SyncDecision::Equal);
    }

    #[test]
    fn test_compare_metadata_size_only_mode() {
        let local = LocalFileInfo {
            relative_path: "file.txt".to_string(),
            absolute_path: PathBuf::from("/tmp/file.txt"),
            size: 100,
            modified: SystemTime::now(),
        };

        // S3 file is much older but same size
        let s3 = ObjectInfo {
            key: "file.txt".to_string(),
            size: 100,
            last_modified: DateTime::from_secs(0),
        };

        // In size_only mode, should be equal despite different timestamps
        assert_eq!(compare_metadata(&local, &s3, true), SyncDecision::Equal);
    }

    #[test]
    fn test_s3_key_to_local_path_no_prefix() {
        let base = Path::new("/tmp");
        let local_path = s3_key_to_local_path("dir/file.txt", base, "");

        assert_eq!(local_path, base.join("dir").join("file.txt"));
    }

    #[test]
    fn test_s3_key_to_local_path_with_prefix() {
        let base = Path::new("/tmp");
        let local_path = s3_key_to_local_path("backup/dir/file.txt", base, "backup");

        assert_eq!(local_path, base.join("dir").join("file.txt"));
    }

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("dir/file.txt"), "dir/file.txt");
        assert_eq!(normalize_path("dir\\file.txt"), "dir/file.txt");
        assert_eq!(normalize_path("./dir/file.txt"), "dir/file.txt");
        assert_eq!(normalize_path("dir/file.txt/"), "dir/file.txt");
    }

    #[test]
    fn test_local_file_info_ordering() {
        let file1 = LocalFileInfo {
            relative_path: "a.txt".to_string(),
            absolute_path: PathBuf::from("/tmp/a.txt"),
            size: 100,
            modified: SystemTime::now(),
        };

        let file2 = LocalFileInfo {
            relative_path: "b.txt".to_string(),
            absolute_path: PathBuf::from("/tmp/b.txt"),
            size: 100,
            modified: SystemTime::now(),
        };

        assert!(file1 < file2);
        assert_eq!(file1.cmp(&file1), Ordering::Equal);
    }
}
