use crate::errors::S3O2Error;
use crate::file_filter::FileFilter;
use crate::lister::{ObjectInfo, ObjectLister};
use crate::metadata::{LocalFileInfo, SyncDecision, compare_metadata};
use aws_sdk_s3::Client;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

/// Sync mode determines the direction and behavior of synchronization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Upload local files to S3, ignore S3-only files
    LocalToS3,
    /// Download S3 files to local, ignore local-only files
    S3ToLocal,
    /// Two-way sync, newest file wins on conflicts
    Bidirectional,
    /// Make destination exactly match source (includes deletes)
    Mirror { delete: bool },
}

/// Action to be taken for a file during sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncAction {
    Upload {
        local_path: PathBuf,
        s3_key: String,
        size: u64,
    },
    Download {
        s3_key: String,
        local_path: PathBuf,
        size: u64,
    },
    Delete {
        location: DeleteLocation,
        path: String,
    },
    Skip {
        path: String,
        reason: SkipReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteLocation {
    Local,
    S3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    AlreadySynced,
    FilteredOut,
    NoChange,
}

/// Complete sync plan with actions and statistics.
#[derive(Debug)]
pub struct SyncPlan {
    pub actions: Vec<SyncAction>,
    pub stats: PlanStats,
}

#[derive(Debug, Default)]
pub struct PlanStats {
    pub total_files: usize,
    pub uploads: usize,
    pub downloads: usize,
    pub deletes: usize,
    pub skipped: usize,
    pub total_bytes: u64,
}

/// Creates sync plans by comparing local and S3 file states.
pub struct SyncPlanner {
    filter: FileFilter,
    mode: SyncMode,
    size_only: bool,
}

impl SyncPlanner {
    pub fn new(filter: FileFilter, mode: SyncMode, size_only: bool) -> Self {
        Self {
            filter,
            mode,
            size_only,
        }
    }

    /// Creates a sync plan by listing and comparing local and S3 files.
    pub async fn create_plan(
        &self,
        client: Client,
        local_base: &Path,
        bucket: &str,
        s3_prefix: &str,
    ) -> Result<SyncPlan, S3O2Error> {
        // Spawn both listing tasks in parallel
        let (local_files, s3_files) = tokio::try_join!(
            self.list_local_files(local_base),
            self.list_s3_files(client, bucket, s3_prefix)
        )?;

        // Perform two-pointer merge to generate actions
        let actions = self.generate_actions(local_files, s3_files, local_base, s3_prefix)?;

        // Calculate statistics
        let stats = Self::calculate_stats(&actions);

        Ok(SyncPlan { actions, stats })
    }

    /// Lists local files with filtering applied.
    async fn list_local_files(&self, base_path: &Path) -> Result<Vec<LocalFileInfo>, S3O2Error> {
        let (tx, mut rx) = mpsc::channel(1000);
        let filter = self.filter.clone();
        let base = base_path.to_owned();

        // Spawn task to walk directory and filter files
        let task = tokio::task::spawn_blocking(move || {
            use walkdir::WalkDir;

            for entry in WalkDir::new(&base).into_iter().filter_entry(|e| {
                // Apply filter to directories to enable early pruning
                let path = e.path();
                if path == base {
                    return true; // Always include root
                }

                if let Ok(relative) = path.strip_prefix(&base) {
                    let relative_str = relative.to_string_lossy().replace('\\', "/");
                    filter.should_include(&relative_str)
                } else {
                    false
                }
            }) {
                match entry {
                    Ok(entry) if entry.file_type().is_file() => {
                        if tx.blocking_send(entry.into_path()).is_err() {
                            break; // Receiver dropped
                        }
                    }
                    Ok(_) => {} // Directory or symlink, skip
                    Err(e) => eprintln!("Warning: skipping entry: {}", e),
                }
            }
        });

        // Collect file info from spawned paths
        let mut files = Vec::new();
        while let Some(path) = rx.recv().await {
            match LocalFileInfo::from_path(path, base_path).await {
                Ok(info) => files.push(info),
                Err(e) => eprintln!("Warning: failed to get file info: {}", e),
            }
        }

        task.await
            .map_err(|e| S3O2Error::SyncError(format!("Local listing task failed: {}", e)))?;

        // Sort for two-pointer merge
        files.sort();
        Ok(files)
    }

    /// Lists S3 files with filtering applied.
    async fn list_s3_files(
        &self,
        client: Client,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<ObjectInfo>, S3O2Error> {
        let mut lister_builder = ObjectLister::builder()
            .with_client(client)
            .with_bucket(bucket);

        if !prefix.is_empty() {
            lister_builder = lister_builder.with_prefix(prefix);
        }

        let lister = lister_builder.build()?;
        let mut rx = lister.stream();

        let mut files = Vec::new();
        while let Some(result) = rx.recv().await {
            let obj = result?;

            // Apply filter to S3 key (strip prefix for matching)
            let key_for_filter = if !prefix.is_empty() {
                obj.key
                    .strip_prefix(prefix)
                    .and_then(|k| k.strip_prefix('/'))
                    .unwrap_or(&obj.key)
            } else {
                &obj.key
            };

            if self.filter.should_include(key_for_filter) {
                files.push(obj);
            }
        }

        // Sort for two-pointer merge
        files.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(files)
    }

    /// Generates sync actions using two-pointer merge algorithm.
    fn generate_actions(
        &self,
        local_files: Vec<LocalFileInfo>,
        s3_files: Vec<ObjectInfo>,
        local_base: &Path,
        s3_prefix: &str,
    ) -> Result<Vec<SyncAction>, S3O2Error> {
        let mut actions = Vec::new();
        let mut local_iter = local_files.into_iter().peekable();
        let mut s3_iter = s3_files.into_iter().peekable();

        // Two-pointer merge
        loop {
            match (local_iter.peek(), s3_iter.peek()) {
                (Some(local), Some(s3)) => {
                    let local_key = local.to_s3_key(s3_prefix);
                    let cmp = local_key.cmp(&s3.key);

                    match cmp {
                        std::cmp::Ordering::Less => {
                            // Local-only file
                            let local = local_iter.next().unwrap();
                            actions.push(self.handle_local_only(local, s3_prefix));
                        }
                        std::cmp::Ordering::Greater => {
                            // S3-only file
                            let s3 = s3_iter.next().unwrap();
                            actions.push(self.handle_s3_only(s3, local_base, s3_prefix));
                        }
                        std::cmp::Ordering::Equal => {
                            // File exists in both places
                            let local = local_iter.next().unwrap();
                            let s3 = s3_iter.next().unwrap();
                            actions.push(self.handle_both_exist(local, s3));
                        }
                    }
                }
                (Some(_), None) => {
                    // Remaining local-only files
                    let local = local_iter.next().unwrap();
                    actions.push(self.handle_local_only(local, s3_prefix));
                }
                (None, Some(_)) => {
                    // Remaining S3-only files
                    let s3 = s3_iter.next().unwrap();
                    actions.push(self.handle_s3_only(s3, local_base, s3_prefix));
                }
                (None, None) => break,
            }
        }

        Ok(actions)
    }

    fn handle_local_only(&self, local: LocalFileInfo, s3_prefix: &str) -> SyncAction {
        let s3_key = local.to_s3_key(s3_prefix);

        match self.mode {
            SyncMode::LocalToS3 | SyncMode::Bidirectional => SyncAction::Upload {
                local_path: local.absolute_path,
                s3_key,
                size: local.size,
            },
            SyncMode::Mirror { delete: true } => SyncAction::Delete {
                location: DeleteLocation::Local,
                path: local.relative_path,
            },
            _ => SyncAction::Skip {
                path: local.relative_path,
                reason: SkipReason::NoChange,
            },
        }
    }

    fn handle_s3_only(&self, s3: ObjectInfo, local_base: &Path, s3_prefix: &str) -> SyncAction {
        let local_path = crate::metadata::s3_key_to_local_path(&s3.key, local_base, s3_prefix);

        match self.mode {
            SyncMode::S3ToLocal | SyncMode::Bidirectional => SyncAction::Download {
                s3_key: s3.key,
                local_path,
                size: s3.size as u64,
            },
            SyncMode::Mirror { delete: true } => SyncAction::Delete {
                location: DeleteLocation::S3,
                path: s3.key,
            },
            _ => SyncAction::Skip {
                path: s3.key,
                reason: SkipReason::NoChange,
            },
        }
    }

    fn handle_both_exist(&self, local: LocalFileInfo, s3: ObjectInfo) -> SyncAction {
        let decision = compare_metadata(&local, &s3, self.size_only);

        match decision {
            SyncDecision::LocalNewer | SyncDecision::SizeMismatch => {
                if matches!(self.mode, SyncMode::S3ToLocal) {
                    // S3ToLocal mode: don't upload
                    SyncAction::Skip {
                        path: local.relative_path,
                        reason: SkipReason::AlreadySynced,
                    }
                } else {
                    // Upload newer/different local file
                    SyncAction::Upload {
                        local_path: local.absolute_path,
                        s3_key: s3.key,
                        size: local.size,
                    }
                }
            }
            SyncDecision::S3Newer => {
                if matches!(self.mode, SyncMode::LocalToS3) {
                    // LocalToS3 mode: don't download
                    SyncAction::Skip {
                        path: local.relative_path,
                        reason: SkipReason::AlreadySynced,
                    }
                } else {
                    // Download newer S3 file
                    SyncAction::Download {
                        s3_key: s3.key,
                        local_path: local.absolute_path,
                        size: s3.size as u64,
                    }
                }
            }
            SyncDecision::Equal => SyncAction::Skip {
                path: local.relative_path,
                reason: SkipReason::AlreadySynced,
            },
        }
    }

    fn calculate_stats(actions: &[SyncAction]) -> PlanStats {
        let mut stats = PlanStats::default();

        for action in actions {
            match action {
                SyncAction::Upload { size, .. } => {
                    stats.uploads += 1;
                    stats.total_bytes += size;
                }
                SyncAction::Download { size, .. } => {
                    stats.downloads += 1;
                    stats.total_bytes += size;
                }
                SyncAction::Delete { .. } => {
                    stats.deletes += 1;
                }
                SyncAction::Skip { .. } => {
                    stats.skipped += 1;
                }
            }
        }

        stats.total_files = actions.len();
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_filter::FileFilter;
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::Object;
    use aws_smithy_mocks::{mock, mock_client};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

    fn create_mock_s3_client(objects: Vec<(&str, i64, i64)>) -> Client {
        let mock_objects: Vec<Object> = objects
            .into_iter()
            .map(|(key, size, timestamp)| {
                Object::builder()
                    .key(key)
                    .size(size)
                    .last_modified(DateTime::from_secs(timestamp))
                    .build()
            })
            .collect();

        let list_rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(mock_objects.clone()))
                .build()
        });

        mock_client!(aws_sdk_s3, &[&list_rule])
    }

    #[tokio::test]
    async fn test_plan_local_only_files_upload_mode() {
        let temp_dir = TempDir::new().unwrap();
        tokio::fs::write(temp_dir.path().join("file1.txt"), b"content1")
            .await
            .unwrap();
        tokio::fs::write(temp_dir.path().join("file2.txt"), b"content2")
            .await
            .unwrap();

        let client = create_mock_s3_client(vec![]);
        let filter = FileFilter::builder().build().unwrap();
        let planner = SyncPlanner::new(filter, SyncMode::LocalToS3, false);

        let plan = planner
            .create_plan(client, temp_dir.path(), "test-bucket", "")
            .await
            .unwrap();

        assert_eq!(plan.stats.uploads, 2);
        assert_eq!(plan.stats.downloads, 0);
        assert_eq!(plan.stats.deletes, 0);
    }

    #[tokio::test]
    async fn test_plan_s3_only_files_download_mode() {
        let temp_dir = TempDir::new().unwrap();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let client = create_mock_s3_client(vec![("file1.txt", 100, now), ("file2.txt", 200, now)]);

        let filter = FileFilter::builder().build().unwrap();
        let planner = SyncPlanner::new(filter, SyncMode::S3ToLocal, false);

        let plan = planner
            .create_plan(client, temp_dir.path(), "test-bucket", "")
            .await
            .unwrap();

        assert_eq!(plan.stats.uploads, 0);
        assert_eq!(plan.stats.downloads, 2);
        assert_eq!(plan.stats.deletes, 0);
    }

    #[tokio::test]
    async fn test_plan_bidirectional_local_newer() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("file.txt");
        tokio::fs::write(&file_path, b"new content").await.unwrap();

        // S3 file is older
        let past = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 60;
        let client = create_mock_s3_client(vec![("file.txt", 11, past)]);

        let filter = FileFilter::builder().build().unwrap();
        let planner = SyncPlanner::new(filter, SyncMode::Bidirectional, false);

        let plan = planner
            .create_plan(client, temp_dir.path(), "test-bucket", "")
            .await
            .unwrap();

        assert_eq!(plan.stats.uploads, 1);
        assert_eq!(plan.stats.downloads, 0);
    }

    #[tokio::test]
    async fn test_plan_with_filtering() {
        let temp_dir = TempDir::new().unwrap();
        tokio::fs::write(temp_dir.path().join("data.txt"), b"keep")
            .await
            .unwrap();
        tokio::fs::write(temp_dir.path().join("temp.log"), b"exclude")
            .await
            .unwrap();

        let client = create_mock_s3_client(vec![]);
        let filter = FileFilter::builder().add_exclude("*.log").build().unwrap();
        let planner = SyncPlanner::new(filter, SyncMode::LocalToS3, false);

        let plan = planner
            .create_plan(client, temp_dir.path(), "test-bucket", "")
            .await
            .unwrap();

        // Only data.txt should be uploaded, temp.log filtered out
        assert_eq!(plan.stats.uploads, 1);
        assert!(
            matches!(&plan.actions[0], SyncAction::Upload { local_path, .. } if local_path.ends_with("data.txt"))
        );
    }

    #[tokio::test]
    async fn test_plan_already_synced() {
        let temp_dir = TempDir::new().unwrap();
        tokio::fs::write(temp_dir.path().join("file.txt"), b"content")
            .await
            .unwrap();

        // Same size, similar timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let client = create_mock_s3_client(vec![("file.txt", 7, now)]);

        let filter = FileFilter::builder().build().unwrap();
        let planner = SyncPlanner::new(filter, SyncMode::Bidirectional, false);

        let plan = planner
            .create_plan(client, temp_dir.path(), "test-bucket", "")
            .await
            .unwrap();

        assert_eq!(plan.stats.skipped, 1);
        assert_eq!(plan.stats.uploads, 0);
        assert_eq!(plan.stats.downloads, 0);
    }
}
