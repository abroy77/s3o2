use crate::downloader::Downloader;
use crate::errors::S3O2Error;
use crate::multipart_downloader::MultipartDownloader;
use crate::multipart_uploader::MultipartUploader;
use crate::sync_plan::{DeleteLocation, SyncAction, SyncPlan};
use crate::uploader::Uploader;
use aws_sdk_s3::Client;
use futures::stream::{self, StreamExt};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const MULTIPART_THRESHOLD: u64 = 5 * 1024 * 1024; // 5MB

/// Configuration for sync execution.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub chunk_size: u64,
    pub max_concurrent_transfers: usize,
    pub dry_run: bool,
    pub verbose: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            chunk_size: 8 * 1024 * 1024, // 8MB
            max_concurrent_transfers: 10,
            dry_run: false,
            verbose: false,
        }
    }
}

/// Result of sync execution.
#[derive(Debug)]
pub struct SyncResult {
    pub stats: ExecutionStats,
    pub errors: Vec<FileError>,
}

/// Statistics from sync execution.
#[derive(Debug, Default)]
pub struct ExecutionStats {
    pub uploaded: usize,
    pub downloaded: usize,
    pub deleted: usize,
    pub skipped: usize,
    pub failed: usize,
    pub bytes_transferred: u64,
    pub duration: Duration,
}

/// Per-file error information.
#[derive(Debug)]
pub struct FileError {
    pub path: String,
    pub action: String,
    pub error: String,
}

/// Executes sync plans with parallel transfers.
pub struct SyncExecutor {
    client: Client,
    bucket: String,
    uploader: Uploader,
    multipart_uploader: MultipartUploader,
    downloader: Downloader,
    multipart_downloader: MultipartDownloader,
    config: SyncConfig,
}

impl SyncExecutor {
    pub fn new(client: Client, bucket: String, config: SyncConfig) -> Self {
        let uploader = Uploader::new(client.clone());
        let multipart_uploader = MultipartUploader::new(
            client.clone(),
            config.chunk_size,
            config.max_concurrent_transfers,
        );
        let downloader = Downloader::new(client.clone());
        let multipart_downloader = MultipartDownloader::new(
            client.clone(),
            config.chunk_size,
            config.max_concurrent_transfers,
        );

        Self {
            client,
            bucket,
            uploader,
            multipart_uploader,
            downloader,
            multipart_downloader,
            config,
        }
    }

    /// Executes the sync plan.
    pub async fn execute(&self, plan: SyncPlan) -> Result<SyncResult, S3O2Error> {
        let start = Instant::now();

        if self.config.dry_run {
            return Ok(self.dry_run_result(plan, start));
        }

        let actions = plan.actions;
        let max_concurrent = self.config.max_concurrent_transfers;

        // Execute actions in parallel with bounded concurrency
        let results: Vec<Result<ActionResult, FileError>> = stream::iter(actions)
            .map(|action| self.execute_action(action))
            .buffer_unordered(max_concurrent)
            .collect()
            .await;

        // Aggregate results
        let mut stats = ExecutionStats::default();
        let mut errors = Vec::new();

        for result in results {
            match result {
                Ok(action_result) => {
                    stats.bytes_transferred += action_result.bytes_transferred;
                    match action_result.action_type {
                        ActionType::Upload => stats.uploaded += 1,
                        ActionType::Download => stats.downloaded += 1,
                        ActionType::Delete => stats.deleted += 1,
                        ActionType::Skip => stats.skipped += 1,
                    }
                }
                Err(file_error) => {
                    stats.failed += 1;
                    errors.push(file_error);
                }
            }
        }

        stats.duration = start.elapsed();

        Ok(SyncResult { stats, errors })
    }

    async fn execute_action(&self, action: SyncAction) -> Result<ActionResult, FileError> {
        match action {
            SyncAction::Upload {
                local_path,
                s3_key,
                size,
            } => self.execute_upload(local_path, s3_key, size).await,
            SyncAction::Download {
                s3_key,
                local_path,
                size,
            } => self.execute_download(s3_key, local_path, size).await,
            SyncAction::Delete { location, path } => self.execute_delete(location, path).await,
            SyncAction::Skip { .. } => Ok(ActionResult {
                action_type: ActionType::Skip,
                bytes_transferred: 0,
            }),
        }
    }

    async fn execute_upload(
        &self,
        local_path: PathBuf,
        s3_key: String,
        size: u64,
    ) -> Result<ActionResult, FileError> {
        if self.config.verbose {
            println!(
                "Uploading: {} → s3://{}/{}",
                local_path.display(),
                self.bucket,
                s3_key
            );
        }

        let result = if size < MULTIPART_THRESHOLD {
            self.uploader
                .upload_file(&local_path, &self.bucket, &s3_key)
                .await
        } else {
            self.multipart_uploader
                .upload_file(&local_path, &self.bucket, &s3_key)
                .await
                .map(|bytes| bytes as usize)
        };

        result
            .map(|bytes| ActionResult {
                action_type: ActionType::Upload,
                bytes_transferred: bytes as u64,
            })
            .map_err(|e| FileError {
                path: local_path.display().to_string(),
                action: "upload".to_string(),
                error: e.to_string(),
            })
    }

    async fn execute_download(
        &self,
        s3_key: String,
        local_path: PathBuf,
        size: u64,
    ) -> Result<ActionResult, FileError> {
        if self.config.verbose {
            println!(
                "Downloading: s3://{}/{} → {}",
                self.bucket,
                s3_key,
                local_path.display()
            );
        }

        // Ensure parent directory exists
        if let Some(parent) = local_path.parent()
            && let Err(e) = tokio::fs::create_dir_all(parent).await
        {
            return Err(FileError {
                path: s3_key,
                action: "download".to_string(),
                error: format!("Failed to create parent directory: {}", e),
            });
        }

        let result = if size < MULTIPART_THRESHOLD {
            self.downloader
                .download_file(&self.bucket, &s3_key, &local_path)
                .await
        } else {
            self.multipart_downloader
                .download_file(&self.bucket, &s3_key, &local_path)
                .await
                .map(|bytes| bytes as usize)
        };

        result
            .map(|bytes| ActionResult {
                action_type: ActionType::Download,
                bytes_transferred: bytes as u64,
            })
            .map_err(|e| FileError {
                path: s3_key,
                action: "download".to_string(),
                error: e.to_string(),
            })
    }

    async fn execute_delete(
        &self,
        location: DeleteLocation,
        path: String,
    ) -> Result<ActionResult, FileError> {
        match location {
            DeleteLocation::Local => {
                if self.config.verbose {
                    println!("Deleting local: {}", path);
                }

                tokio::fs::remove_file(&path)
                    .await
                    .map(|_| ActionResult {
                        action_type: ActionType::Delete,
                        bytes_transferred: 0,
                    })
                    .map_err(|e| FileError {
                        path,
                        action: "delete_local".to_string(),
                        error: e.to_string(),
                    })
            }
            DeleteLocation::S3 => {
                if self.config.verbose {
                    println!("Deleting S3: s3://{}/{}", self.bucket, path);
                }

                self.client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(&path)
                    .send()
                    .await
                    .map(|_| ActionResult {
                        action_type: ActionType::Delete,
                        bytes_transferred: 0,
                    })
                    .map_err(|e| FileError {
                        path,
                        action: "delete_s3".to_string(),
                        error: e.to_string(),
                    })
            }
        }
    }

    fn dry_run_result(&self, plan: SyncPlan, start: Instant) -> SyncResult {
        println!("=== Dry Run ===");
        println!("Actions to be performed:");

        for action in &plan.actions {
            match action {
                SyncAction::Upload {
                    local_path,
                    s3_key,
                    size,
                } => {
                    println!(
                        "  Upload: {} → s3://{}/{} ({} bytes)",
                        local_path.display(),
                        self.bucket,
                        s3_key,
                        size
                    );
                }
                SyncAction::Download {
                    s3_key,
                    local_path,
                    size,
                } => {
                    println!(
                        "  Download: s3://{}/{} → {} ({} bytes)",
                        self.bucket,
                        s3_key,
                        local_path.display(),
                        size
                    );
                }
                SyncAction::Delete { location, path } => {
                    let loc = match location {
                        DeleteLocation::Local => "local",
                        DeleteLocation::S3 => "S3",
                    };
                    println!("  Delete ({}): {}", loc, path);
                }
                SyncAction::Skip { path, .. } => {
                    if self.config.verbose {
                        println!("  Skip: {}", path);
                    }
                }
            }
        }

        println!("\nSummary:");
        println!("  Uploads: {}", plan.stats.uploads);
        println!("  Downloads: {}", plan.stats.downloads);
        println!("  Deletes: {}", plan.stats.deletes);
        println!("  Skipped: {}", plan.stats.skipped);
        println!("  Total bytes: {}", plan.stats.total_bytes);

        SyncResult {
            stats: ExecutionStats {
                uploaded: 0,
                downloaded: 0,
                deleted: 0,
                skipped: plan.stats.skipped,
                failed: 0,
                bytes_transferred: 0,
                duration: start.elapsed(),
            },
            errors: Vec::new(),
        }
    }
}

struct ActionResult {
    action_type: ActionType,
    bytes_transferred: u64,
}

enum ActionType {
    Upload,
    Download,
    Delete,
    Skip,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_plan::{PlanStats, SkipReason};
    use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_smithy_mocks::{RuleMode, mock, mock_client};
    use tempfile::TempDir;

    fn create_mock_client() -> Client {
        let put_rule = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| PutObjectOutput::builder().e_tag("\"mock\"").build());

        let get_rule = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(10)
                .body(ByteStream::from(b"test data".to_vec()))
                .build()
        });

        let head_rule = mock!(aws_sdk_s3::Client::head_object).then_output(|| {
            aws_sdk_s3::operation::head_object::HeadObjectOutput::builder()
                .content_length(10)
                .build()
        });

        let delete_rule = mock!(aws_sdk_s3::Client::delete_object)
            .then_output(|| DeleteObjectOutput::builder().build());

        mock_client!(
            aws_sdk_s3,
            RuleMode::MatchAny,
            &[&put_rule, &get_rule, &head_rule, &delete_rule]
        )
    }

    #[tokio::test]
    async fn test_execute_upload_actions() {
        let temp_dir = TempDir::new().unwrap();
        let file1 = temp_dir.path().join("file1.txt");
        let file2 = temp_dir.path().join("file2.txt");

        tokio::fs::write(&file1, b"content1").await.unwrap();
        tokio::fs::write(&file2, b"content2").await.unwrap();

        let client = create_mock_client();
        let config = SyncConfig::default();
        let executor = SyncExecutor::new(client, "test-bucket".to_string(), config);

        let plan = SyncPlan {
            actions: vec![
                SyncAction::Upload {
                    local_path: file1,
                    s3_key: "file1.txt".to_string(),
                    size: 8,
                },
                SyncAction::Upload {
                    local_path: file2,
                    s3_key: "file2.txt".to_string(),
                    size: 8,
                },
            ],
            stats: PlanStats {
                uploads: 2,
                ..Default::default()
            },
        };

        let result = executor.execute(plan).await.unwrap();

        assert_eq!(result.stats.uploaded, 2);
        assert_eq!(result.stats.failed, 0);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_execute_download_actions() {
        let temp_dir = TempDir::new().unwrap();

        let client = create_mock_client();
        let config = SyncConfig::default();
        let executor = SyncExecutor::new(client, "test-bucket".to_string(), config);

        let plan = SyncPlan {
            actions: vec![SyncAction::Download {
                s3_key: "file.txt".to_string(),
                local_path: temp_dir.path().join("file.txt"),
                size: 10,
            }],
            stats: PlanStats {
                downloads: 1,
                ..Default::default()
            },
        };

        let result = executor.execute(plan).await.unwrap();

        assert_eq!(result.stats.downloaded, 1);
        assert_eq!(result.stats.failed, 0);
        assert!(temp_dir.path().join("file.txt").exists());
    }

    #[tokio::test]
    async fn test_execute_skip_actions() {
        let client = create_mock_client();
        let config = SyncConfig::default();
        let executor = SyncExecutor::new(client, "test-bucket".to_string(), config);

        let plan = SyncPlan {
            actions: vec![SyncAction::Skip {
                path: "file.txt".to_string(),
                reason: SkipReason::AlreadySynced,
            }],
            stats: PlanStats {
                skipped: 1,
                ..Default::default()
            },
        };

        let result = executor.execute(plan).await.unwrap();

        assert_eq!(result.stats.skipped, 1);
        assert_eq!(result.stats.uploaded, 0);
        assert_eq!(result.stats.downloaded, 0);
    }

    #[tokio::test]
    async fn test_dry_run_mode() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("file.txt");
        tokio::fs::write(&file, b"content").await.unwrap();

        let client = create_mock_client();
        let config = SyncConfig {
            dry_run: true,
            ..Default::default()
        };
        let executor = SyncExecutor::new(client, "test-bucket".to_string(), config);

        let plan = SyncPlan {
            actions: vec![SyncAction::Upload {
                local_path: file,
                s3_key: "file.txt".to_string(),
                size: 7,
            }],
            stats: PlanStats {
                uploads: 1,
                ..Default::default()
            },
        };

        let result = executor.execute(plan).await.unwrap();

        // Dry run should not actually execute
        assert_eq!(result.stats.uploaded, 0);
        assert_eq!(result.stats.skipped, 0);
    }
}
