use crate::errors::S3O2Error;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

const SMALL_SIZE_THRESH: f64 = 5.0;

pub struct Downloader {
    client: aws_sdk_s3::Client,
}
impl Downloader {
    pub fn new(client: aws_sdk_s3::Client) -> Self {
        Self { client }
    }
    async fn get_obj_size_mb(&self, bucket: &str, key: &str) -> Result<f64, S3O2Error> {
        let len = self.get_obj_size(bucket, key).await?;
        Ok(len as f64 / (1024.0 * 1024.0))
    }

    async fn get_obj_size(&self, bucket: &str, key: &str) -> Result<u64, S3O2Error> {
        let response = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        let len = response.content_length().ok_or(S3O2Error::Value(
            "Could not read content length from head object".into(),
        ))?;
        match len {
            len if len < 0 => Err(S3O2Error::Value(format!(
                "Head object size returned less than 0: {len}"
            ))),
            len => Ok(len as u64),
        }
    }

    pub async fn download_file(
        &self,
        bucket: &str,
        key: &str,
        local_path: &PathBuf,
    ) -> Result<usize, S3O2Error> {
        let size_mb = self.get_obj_size_mb(bucket, key).await?;
        let bytes = if size_mb > SMALL_SIZE_THRESH {
            self.download_small_file(bucket, key, local_path).await?
        } else {
            self.download_medium_file(bucket, key, local_path).await?
        };

        Ok(bytes)
    }
    async fn download_small_file(
        &self,
        bucket: &str,
        key: &str,
        local_path: &PathBuf,
    ) -> Result<usize, S3O2Error> {
        let response = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        let content = match response.body.collect().await {
            Ok(result) => result,
            Err(_) => return Err(S3O2Error::Value("Download Error on collection".into())),
        };

        let bytes = content.into_bytes();
        let mut file = File::create(local_path).await?;
        file.write_all(&bytes).await?;
        Ok(bytes.len())
    }
    async fn download_medium_file(
        &self,
        bucket: &str,
        key: &str,
        local_path: &PathBuf,
    ) -> Result<usize, S3O2Error> {
        let mut response = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        let file = File::create(local_path).await?;
        let mut writer = BufWriter::new(file);
        let mut byte_count = 0;
        while let Some(bytes) = response.body.try_next().await.unwrap() {
            let bytes_len = bytes.len();
            writer.write_all(&bytes).await?;
            byte_count += bytes_len;
        }
        Ok(byte_count)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_file_content;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::{self, operation::get_object::GetObjectError};
    use aws_smithy_mocks::{mock, mock_client};
    use rand::Rng;
    use tempfile::TempDir;

    struct GetObjectFixture {
        downloader: Downloader,
        temp_dir: TempDir,
    }
    impl GetObjectFixture {
        fn with_content(content: Vec<u8>) -> Self {
            let content_clone = content.clone();
            let rule = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
                GetObjectOutput::builder()
                    .content_length(content_clone.len() as i64)
                    .body(ByteStream::from(content_clone.clone()))
                    .build()
            });

            let client = mock_client!(aws_sdk_s3, [&rule]);

            Self {
                downloader: Downloader::new(client),
                temp_dir: TempDir::new().unwrap(),
            }
        }
        fn with_error<F>(error_fn: F) -> Self
        where
            F: Fn() -> GetObjectError + Send + Sync + 'static,
        {
            let rule = mock!(aws_sdk_s3::Client::get_object).then_error(error_fn);
            let client = mock_client!(aws_sdk_s3, [&rule]);
            Self {
                downloader: Downloader::new(client),
                temp_dir: TempDir::new().unwrap(),
            }
        }
        fn path(&self, filename: &str) -> PathBuf {
            self.temp_dir.path().join(filename)
        }
    }
    #[tokio::test]
    async fn test_download_small_file_success() {
        let content = b"Hello there dearie. How's your mum doing these days?";
        let test_setup = GetObjectFixture::with_content(content.into());
        let output_path = test_setup.path("output.txt");
        let bytes = test_setup
            .downloader
            .download_small_file("bucket", "key", &output_path)
            .await
            .unwrap();

        assert_eq!(bytes, content.len());
        assert_file_content!(output_path, content);
    }
    #[tokio::test]
    async fn test_download_medium_file_success() {
        let size = 15 * 1024 * 1024;
        let mut rng = rand::rng();
        let content: Vec<u8> = (0..size).map(|_| rng.random()).collect();
        let test_setup = GetObjectFixture::with_content(content.clone());
        let output_path = test_setup.path("output.txt");
        let bytes = test_setup
            .downloader
            .download_medium_file("bucket", "key", &output_path)
            .await
            .unwrap();

        assert_eq!(bytes, content.len());
        assert_file_content!(output_path, content);
    }
}
