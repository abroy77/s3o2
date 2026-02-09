use crate::errors::S3O2Error;
use crate::file_filter::FileFilter;
use crate::sync_executor::{SyncConfig, SyncExecutor, SyncResult};
use crate::sync_plan::{SyncMode, SyncPlanner};
use aws_sdk_s3::Client;
use std::path::Path;

/// Main orchestrator for S3 synchronization.
pub struct Syncer {
    client: Client,
    filter: FileFilter,
    config: SyncConfig,
}

impl Syncer {
    /// Creates a new Syncer.
    pub fn new(client: Client, filter: FileFilter, config: SyncConfig) -> Self {
        Self {
            client,
            filter,
            config,
        }
    }

    /// Synchronizes between local directory and S3 bucket/prefix.
    ///
    /// # Arguments
    /// * `local_path` - Local directory path
    /// * `bucket` - S3 bucket name
    /// * `prefix` - S3 prefix (can be empty)
    /// * `mode` - Sync mode (LocalToS3, S3ToLocal, Bidirectional, Mirror)
    /// * `size_only` - If true, only compare file sizes (ignore timestamps)
    pub async fn sync(
        &self,
        local_path: &Path,
        bucket: &str,
        prefix: &str,
        mode: SyncMode,
        size_only: bool,
    ) -> Result<SyncResult, S3O2Error> {
        // Validate inputs
        if !local_path.exists() {
            return Err(S3O2Error::Value(format!(
                "Local path does not exist: {}",
                local_path.display()
            )));
        }

        if !local_path.is_dir() {
            return Err(S3O2Error::Value(format!(
                "Local path is not a directory: {}",
                local_path.display()
            )));
        }

        // Create sync plan
        println!("Analyzing files...");
        let planner = SyncPlanner::new(self.filter.clone(), mode, size_only);
        let plan = planner
            .create_plan(self.client.clone(), local_path, bucket, prefix)
            .await?;

        // Show plan summary
        println!("\nSync Plan:");
        println!("  Uploads: {}", plan.stats.uploads);
        println!("  Downloads: {}", plan.stats.downloads);
        println!("  Deletes: {}", plan.stats.deletes);
        println!("  Skipped: {}", plan.stats.skipped);
        println!("  Total bytes: {}", plan.stats.total_bytes);

        // Warn about deletes
        if plan.stats.deletes > 0 && !self.config.dry_run {
            println!(
                "\nWARNING: This operation will delete {} files!",
                plan.stats.deletes
            );
            println!("Continue? (yes/no)");

            let mut input = String::new();
            std::io::stdin()
                .read_line(&mut input)
                .map_err(|e| S3O2Error::Value(format!("Failed to read input: {}", e)))?;

            if input.trim().to_lowercase() != "yes" {
                return Err(S3O2Error::Value("Operation aborted by user".to_string()));
            }
        }

        // Execute sync plan
        if self.config.dry_run {
            println!("\nDry run mode: no changes will be made\n");
        } else {
            println!("\nExecuting sync...");
        }

        let executor =
            SyncExecutor::new(self.client.clone(), bucket.to_string(), self.config.clone());
        let result = executor.execute(plan).await?;

        // Print results
        println!("\n=== Sync Complete ===");
        println!("Uploaded: {}", result.stats.uploaded);
        println!("Downloaded: {}", result.stats.downloaded);
        println!("Deleted: {}", result.stats.deleted);
        println!("Skipped: {}", result.stats.skipped);
        println!("Failed: {}", result.stats.failed);
        println!("Bytes transferred: {}", result.stats.bytes_transferred);
        println!("Duration: {:?}", result.stats.duration);

        if !result.errors.is_empty() {
            println!("\n=== Errors ===");
            for error in &result.errors {
                eprintln!("  {} ({}): {}", error.path, error.action, error.error);
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_smithy_mocks::{RuleMode, mock, mock_client};
    use tempfile::TempDir;

    fn create_test_client() -> Client {
        let list_rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![]))
                .build()
        });

        let put_rule = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| PutObjectOutput::builder().e_tag("\"mock\"").build());

        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list_rule, &put_rule])
    }

    #[tokio::test]
    async fn test_sync_upload_mode() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.txt");
        tokio::fs::write(&file, b"content").await.unwrap();

        let client = create_test_client();
        let filter = FileFilter::builder().build().unwrap();
        let config = SyncConfig {
            dry_run: true, // Use dry run for testing
            ..Default::default()
        };

        let syncer = Syncer::new(client, filter, config);

        let result = syncer
            .sync(
                temp_dir.path(),
                "test-bucket",
                "",
                SyncMode::LocalToS3,
                false,
            )
            .await
            .unwrap();

        // In dry run mode, nothing is actually executed
        assert_eq!(result.stats.failed, 0);
    }

    #[tokio::test]
    async fn test_sync_invalid_local_path() {
        let client = create_test_client();
        let filter = FileFilter::builder().build().unwrap();
        let config = SyncConfig::default();

        let syncer = Syncer::new(client, filter, config);

        let result = syncer
            .sync(
                Path::new("/nonexistent/path"),
                "test-bucket",
                "",
                SyncMode::LocalToS3,
                false,
            )
            .await;

        assert!(result.is_err());
        if let Err(S3O2Error::Value(msg)) = result {
            assert!(msg.contains("does not exist"));
        } else {
            panic!("Expected Value error");
        }
    }

    #[tokio::test]
    async fn test_sync_with_filtering() {
        let temp_dir = TempDir::new().unwrap();
        tokio::fs::write(temp_dir.path().join("keep.txt"), b"keep")
            .await
            .unwrap();
        tokio::fs::write(temp_dir.path().join("exclude.log"), b"exclude")
            .await
            .unwrap();

        let client = create_test_client();
        let filter = FileFilter::builder().add_exclude("*.log").build().unwrap();
        let config = SyncConfig {
            dry_run: true,
            verbose: false,
            ..Default::default()
        };

        let syncer = Syncer::new(client, filter, config);

        let result = syncer
            .sync(
                temp_dir.path(),
                "test-bucket",
                "",
                SyncMode::LocalToS3,
                false,
            )
            .await
            .unwrap();

        // Should succeed without errors
        assert_eq!(result.stats.failed, 0);
    }
}
