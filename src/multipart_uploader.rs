use crate::errors::S3O2Error;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub struct MultipartUploader {
    client: aws_sdk_s3::Client,
    chunk_size: u64,
    max_concurrent_chunks: usize,
}

struct UploadChunk {
    part_number: i32,
    data: Vec<u8>,
}

impl UploadChunk {
    fn new(part_number: i32, data: Vec<u8>) -> Self {
        Self { part_number, data }
    }
}

impl MultipartUploader {
    pub fn new(client: aws_sdk_s3::Client, chunk_size: u64, max_concurrent_chunks: usize) -> Self {
        Self {
            client,
            chunk_size,
            max_concurrent_chunks,
        }
    }

    fn calculate_num_chunks(&self, file_size: u64) -> usize {
        file_size.div_ceil(self.chunk_size) as usize
    }

    fn spawn_reader(
        &self,
        tx: mpsc::Sender<UploadChunk>,
        source: &PathBuf,
        file_size: u64,
    ) -> JoinHandle<Result<(), S3O2Error>> {
        let source = source.to_owned();
        let chunk_size = self.chunk_size;
        let num_chunks = self.calculate_num_chunks(file_size);

        tokio::spawn(async move {
            let mut file = File::open(&source).await?;
            for i in 0..num_chunks {
                let offset = chunk_size * i as u64;
                let length = chunk_size.min(file_size - offset) as usize;

                file.seek(SeekFrom::Start(offset)).await?;
                let mut buffer = vec![0u8; length];
                file.read_exact(&mut buffer).await?;

                let part_number = (i + 1) as i32; // S3 parts are 1-based
                tx.send(UploadChunk::new(part_number, buffer))
                    .await
                    .map_err(|_| S3O2Error::ChannelClosed)?;
            }
            Ok(())
        })
    }

    async fn upload_single_chunk(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        chunk: UploadChunk,
    ) -> Result<CompletedPart, S3O2Error> {
        let part_number = chunk.part_number;
        let body = ByteStream::from(chunk.data);

        let response = self
            .client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(body)
            .send()
            .await?;

        let e_tag = response
            .e_tag()
            .ok_or(S3O2Error::Value("No ETag in upload_part response".into()))?
            .to_string();

        Ok(CompletedPart::builder()
            .part_number(part_number)
            .e_tag(e_tag)
            .build())
    }

    async fn upload_chunks(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        mut rx: mpsc::Receiver<UploadChunk>,
    ) -> Result<Vec<CompletedPart>, S3O2Error> {
        let mut chunks = Vec::new();
        while let Some(chunk) = rx.recv().await {
            chunks.push(chunk);
        }

        let mut parts: Vec<CompletedPart> = stream::iter(chunks)
            .map(|chunk| self.upload_single_chunk(bucket, key, upload_id, chunk))
            .buffer_unordered(self.max_concurrent_chunks)
            .try_collect()
            .await?;

        parts.sort_by_key(|p| p.part_number());
        Ok(parts)
    }

    async fn initiate_upload(&self, bucket: &str, key: &str) -> Result<String, S3O2Error> {
        let response = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        response
            .upload_id()
            .map(|s| s.to_string())
            .ok_or(S3O2Error::Value(
                "No upload_id in create_multipart_upload response".into(),
            ))
    }

    async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<(), S3O2Error> {
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await?;

        Ok(())
    }

    async fn abort_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), S3O2Error> {
        self.client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await?;
        Ok(())
    }

    pub async fn upload_file(
        &self,
        local_path: &PathBuf,
        bucket: &str,
        key: &str,
    ) -> Result<u64, S3O2Error> {
        let file_size = tokio::fs::metadata(local_path).await?.len();
        let upload_id = self.initiate_upload(bucket, key).await?;

        let (tx, rx) = mpsc::channel::<UploadChunk>(self.max_concurrent_chunks);
        let reader = self.spawn_reader(tx, local_path, file_size);

        let upload_result = self.upload_chunks(bucket, key, &upload_id, rx).await;

        reader.await??;

        match upload_result {
            Ok(parts) => {
                self.complete_upload(bucket, key, &upload_id, parts).await?;
                Ok(file_size)
            }
            Err(e) => {
                let _ = self.abort_upload(bucket, key, &upload_id).await;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput;
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::{
        CreateMultipartUploadError, CreateMultipartUploadOutput,
    };
    use aws_sdk_s3::operation::upload_part::{UploadPartError, UploadPartOutput};
    use aws_smithy_mocks::{Rule, RuleMode, mock, mock_client};
    use tempfile::TempDir;

    const MB: usize = 1024 * 1024;

    struct TestData;

    impl TestData {
        fn sequential(size: usize) -> Vec<u8> {
            (0..size).map(|i| (i % 256) as u8).collect()
        }
    }

    struct MockS3UploadBuilder {
        upload_id: String,
        num_parts: usize,
        fail_at_part: Option<i32>,
        fail_at_create: bool,
    }

    impl MockS3UploadBuilder {
        fn new(num_parts: usize) -> Self {
            Self {
                upload_id: "test-upload-id".to_string(),
                num_parts,
                fail_at_part: None,
                fail_at_create: false,
            }
        }

        fn fail_at_part(mut self, part_number: i32) -> Self {
            self.fail_at_part = Some(part_number);
            self
        }

        fn fail_at_create(mut self) -> Self {
            self.fail_at_create = true;
            self
        }

        fn expects_failure(&self) -> bool {
            self.fail_at_part.is_some() || self.fail_at_create
        }

        fn build(&self) -> aws_sdk_s3::Client {
            let mut rules = vec![self.create_initiate_rule()];
            rules.extend(self.create_upload_part_rules());
            if self.expects_failure() {
                rules.push(self.create_abort_rule());
            } else {
                rules.push(self.create_complete_rule());
            }
            let rule_refs: Vec<_> = rules.iter().collect();
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, rule_refs.as_slice())
        }

        fn create_initiate_rule(&self) -> Rule {
            if self.fail_at_create {
                mock!(aws_sdk_s3::Client::create_multipart_upload).then_error(|| {
                    CreateMultipartUploadError::unhandled("Create multipart upload failed")
                })
            } else {
                let upload_id = self.upload_id.clone();
                mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(move || {
                    CreateMultipartUploadOutput::builder()
                        .upload_id(upload_id.clone())
                        .build()
                })
            }
        }

        fn create_upload_part_rules(&self) -> Vec<Rule> {
            (1..=self.num_parts as i32)
                .map(|part_num| {
                    if Some(part_num) == self.fail_at_part {
                        let pn = part_num;
                        mock!(aws_sdk_s3::Client::upload_part)
                            .match_requests(move |req| req.part_number() == Some(pn))
                            .then_error(|| UploadPartError::unhandled("Upload part failed"))
                    } else {
                        let pn = part_num;
                        let etag = format!("\"etag-part-{}\"", part_num);
                        mock!(aws_sdk_s3::Client::upload_part)
                            .match_requests(move |req| req.part_number() == Some(pn))
                            .then_output(move || {
                                UploadPartOutput::builder().e_tag(etag.clone()).build()
                            })
                    }
                })
                .collect()
        }

        fn create_complete_rule(&self) -> Rule {
            mock!(aws_sdk_s3::Client::complete_multipart_upload).then_output(|| {
                CompleteMultipartUploadOutput::builder()
                    .e_tag("\"final-etag\"")
                    .build()
            })
        }

        fn create_abort_rule(&self) -> Rule {
            mock!(aws_sdk_s3::Client::abort_multipart_upload)
                .then_output(|| AbortMultipartUploadOutput::builder().build())
        }
    }

    async fn create_test_file(dir: &TempDir, name: &str, content: &[u8]) -> PathBuf {
        let path = dir.path().join(name);
        tokio::fs::write(&path, content).await.unwrap();
        path
    }

    #[tokio::test]
    async fn test_successful_multipart_upload() {
        let chunk_size = 8 * MB;
        let test_data = TestData::sequential(100 * MB);
        let test_data_len = test_data.len();
        let num_chunks = test_data_len.div_ceil(chunk_size);

        let mock_client = MockS3UploadBuilder::new(num_chunks).build();
        let uploader = MultipartUploader::new(mock_client, chunk_size as u64, 4);

        let temp_dir = TempDir::new().unwrap();
        let file_path = create_test_file(&temp_dir, "upload.bin", &test_data).await;

        let result = uploader
            .upload_file(&file_path, "test-bucket", "test-key")
            .await
            .unwrap();

        assert_eq!(result as usize, test_data_len);
    }

    #[tokio::test]
    async fn test_multipart_upload_partial_last_chunk() {
        let chunk_size = 8 * MB;
        // 20MB = 2 full chunks (8MB each) + 1 partial chunk (4MB)
        let test_data = TestData::sequential(20 * MB);
        let test_data_len = test_data.len();
        let num_chunks = test_data_len.div_ceil(chunk_size);

        assert_eq!(num_chunks, 3);

        let mock_client = MockS3UploadBuilder::new(num_chunks).build();
        let uploader = MultipartUploader::new(mock_client, chunk_size as u64, 4);

        let temp_dir = TempDir::new().unwrap();
        let file_path = create_test_file(&temp_dir, "partial.bin", &test_data).await;

        let result = uploader
            .upload_file(&file_path, "test-bucket", "test-key")
            .await
            .unwrap();

        assert_eq!(result as usize, test_data_len);
    }

    #[tokio::test]
    async fn test_upload_part_failure() {
        let chunk_size = 8 * MB;
        let test_data = TestData::sequential(24 * MB);
        let num_chunks = test_data.len().div_ceil(chunk_size);

        let mock_client = MockS3UploadBuilder::new(num_chunks).fail_at_part(2).build();
        let uploader = MultipartUploader::new(mock_client, chunk_size as u64, 4);

        let temp_dir = TempDir::new().unwrap();
        let file_path = create_test_file(&temp_dir, "fail.bin", &test_data).await;

        let result = uploader
            .upload_file(&file_path, "test-bucket", "test-key")
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_multipart_upload_failure() {
        let chunk_size = 8 * MB;

        let mock_client = MockS3UploadBuilder::new(1).fail_at_create().build();
        let uploader = MultipartUploader::new(mock_client, chunk_size as u64, 4);

        let temp_dir = TempDir::new().unwrap();
        let test_data = TestData::sequential(chunk_size);
        let file_path = create_test_file(&temp_dir, "create_fail.bin", &test_data).await;

        let result = uploader
            .upload_file(&file_path, "test-bucket", "test-key")
            .await;

        assert!(result.is_err());
    }
}
