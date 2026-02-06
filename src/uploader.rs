use crate::errors::S3O2Error;
use aws_sdk_s3::primitives::ByteStream;
use std::path::PathBuf;

const SMALL_SIZE_THRESH: u64 = 5 * 1024 * 1024;

pub struct Uploader {
    client: aws_sdk_s3::Client,
}

impl Uploader {
    pub fn new(client: aws_sdk_s3::Client) -> Self {
        Self { client }
    }

    pub async fn upload_file(
        &self,
        local_path: &PathBuf,
        bucket: &str,
        key: &str,
    ) -> Result<usize, S3O2Error> {
        let file_size = tokio::fs::metadata(local_path).await?.len();
        if file_size < SMALL_SIZE_THRESH {
            self.upload_small_file(local_path, bucket, key).await
        } else {
            self.upload_medium_file(local_path, bucket, key).await
        }
    }

    async fn upload_small_file(
        &self,
        local_path: &PathBuf,
        bucket: &str,
        key: &str,
    ) -> Result<usize, S3O2Error> {
        let data = tokio::fs::read(local_path).await?;
        let data_len = data.len();
        let byte_stream = ByteStream::from(data);
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(byte_stream)
            .send()
            .await?;
        Ok(data_len)
    }

    async fn upload_medium_file(
        &self,
        local_path: &PathBuf,
        bucket: &str,
        key: &str,
    ) -> Result<usize, S3O2Error> {
        let file_size = tokio::fs::metadata(local_path).await?.len() as usize;
        let byte_stream = ByteStream::from_path(local_path)
            .await
            .map_err(|e| S3O2Error::Value(format!("Failed to create ByteStream from path: {e}")))?;
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(byte_stream)
            .send()
            .await?;
        Ok(file_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
    use aws_smithy_mocks::{mock, mock_client};
    use rand::Rng;
    use tempfile::TempDir;

    struct PutObjectFixture {
        uploader: Uploader,
        temp_dir: TempDir,
    }

    impl PutObjectFixture {
        fn success() -> Self {
            let rule = mock!(aws_sdk_s3::Client::put_object)
                .then_output(|| PutObjectOutput::builder().e_tag("\"mock-etag\"").build());
            let client = mock_client!(aws_sdk_s3, [&rule]);
            Self {
                uploader: Uploader::new(client),
                temp_dir: TempDir::new().unwrap(),
            }
        }

        fn with_error() -> Self {
            let rule = mock!(aws_sdk_s3::Client::put_object)
                .then_error(|| PutObjectError::unhandled("Upload failed"));
            let client = mock_client!(aws_sdk_s3, [&rule]);
            Self {
                uploader: Uploader::new(client),
                temp_dir: TempDir::new().unwrap(),
            }
        }

        async fn create_test_file(&self, name: &str, content: &[u8]) -> PathBuf {
            let path = self.temp_dir.path().join(name);
            tokio::fs::write(&path, content).await.unwrap();
            path
        }
    }

    #[tokio::test]
    async fn test_upload_small_file_success() {
        let content = b"Hello there dearie. How's your mum doing these days?";
        let fixture = PutObjectFixture::success();
        let path = fixture.create_test_file("small.txt", content).await;

        let bytes = fixture
            .uploader
            .upload_small_file(&path, "bucket", "key")
            .await
            .unwrap();

        assert_eq!(bytes, content.len());
    }

    #[tokio::test]
    async fn test_upload_medium_file_success() {
        let size = 15 * 1024 * 1024;
        let mut rng = rand::rng();
        let content: Vec<u8> = (0..size).map(|_| rng.random()).collect();
        let fixture = PutObjectFixture::success();
        let path = fixture.create_test_file("medium.bin", &content).await;

        let bytes = fixture
            .uploader
            .upload_medium_file(&path, "bucket", "key")
            .await
            .unwrap();

        assert_eq!(bytes, content.len());
    }

    #[tokio::test]
    async fn test_upload_file_dispatches_small() {
        let content = b"small file content";
        let fixture = PutObjectFixture::success();
        let path = fixture.create_test_file("dispatch.txt", content).await;

        let bytes = fixture
            .uploader
            .upload_file(&path, "bucket", "key")
            .await
            .unwrap();

        assert_eq!(bytes, content.len());
    }

    #[tokio::test]
    async fn test_upload_file_error() {
        let content = b"some content";
        let fixture = PutObjectFixture::with_error();
        let path = fixture.create_test_file("error.txt", content).await;

        let result = fixture.uploader.upload_file(&path, "bucket", "key").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_upload_nonexistent_file() {
        let fixture = PutObjectFixture::success();
        let path = fixture.temp_dir.path().join("nonexistent.txt");

        let result = fixture.uploader.upload_file(&path, "bucket", "key").await;

        assert!(result.is_err());
    }
}
