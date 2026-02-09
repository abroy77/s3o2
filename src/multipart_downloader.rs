use crate::errors::S3O2Error;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::mpsc::{self, Sender};
use tokio::task::JoinHandle;

pub struct MultipartDownloader {
    client: aws_sdk_s3::Client,
    chunk_size: u64,
    max_concurrent_chunks: usize,
}
pub struct Chunk {
    offset: u64,
    data: Vec<u8>,
}
impl Chunk {
    fn new(offset: u64, data: Vec<u8>) -> Self {
        Self { offset, data }
    }
}

impl MultipartDownloader {
    pub fn new(client: aws_sdk_s3::Client, chunk_size: u64, max_concurrent_chunks: usize) -> Self {
        Self {
            client,
            chunk_size,
            max_concurrent_chunks,
        }
    }
    fn calculate_num_chunks(&self, content_len: &u64) -> usize {
        content_len.div_ceil(self.chunk_size) as usize
    }
    fn calculate_chunk_range(&self, chunk_index: usize, content_len: &u64) -> (usize, usize) {
        let start = self.chunk_size * chunk_index as u64;
        let end = (self.chunk_size * (1 + chunk_index as u64)).min(*content_len) - 1;
        (start as usize, end as usize)
    }
    #[allow(dead_code)]
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

    fn spawn_writer(
        &self,
        mut rx: mpsc::Receiver<Chunk>,
        destination: &PathBuf,
        total_size: u64,
    ) -> JoinHandle<Result<(), S3O2Error>> {
        let destination = destination.to_owned();
        tokio::spawn(async move {
            let mut file = File::create(destination).await?;
            file.set_len(total_size).await?;

            while let Some(chunk) = rx.recv().await {
                file.seek(SeekFrom::Start(chunk.offset)).await?;
                file.write_all(&chunk.data).await?;
            }
            file.flush().await?;
            Ok(())
        })
    }
    // async fn download_single_chunk_with_retry(
    //     &self,
    //     bucket: &str,
    //     key: &str,
    //     chunk_index: u64,
    //     tx: &Sender<Chunk>,
    // ) -> Result<(), S3O2Error> {
    //     for attempt in 0..self.max_retries {
    //         match self
    //             .download_single_chunk(bucket, key, chunk_index, tx)
    //             .await
    //         {
    //             Ok(()) => return Ok(()),
    //             Err(_) if attempt < self.max_retries => {
    //                 sleep(Duration::from_secs(2_u64.pow(attempt as u32))).await;
    //                 continue;
    //             }
    //             Err(e) => return Err(e),
    //         }
    //     }
    //     unreachable!()
    // }
    fn get_range_str(&self, start: &usize, end: &usize) -> String {
        format!("bytes={}-{}", start, end)
    }
    async fn download_single_chunk(
        &self,
        bucket: &str,
        key: &str,
        start: usize,
        end: usize,
        tx: &Sender<Chunk>,
    ) -> Result<(), S3O2Error> {
        let range_str = self.get_range_str(&start, &end);
        let obj = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(format!("bytes={}-{}", start, end))
            .send()
            .await?;
        let data = match obj.body.collect().await {
            Ok(data) => data,
            Err(_) => {
                return Err(S3O2Error::Value(format!(
                    "Error Downloading Range\n \
            Range: {range_str}"
                )));
            }
        };
        let byte_vec = data.into_bytes().to_vec();
        tx.send(Chunk::new(start as u64, byte_vec))
            .await
            .map_err(|_| S3O2Error::ChannelClosed)?;
        Ok(())
    }
    async fn download_chunks(
        &self,
        bucket: &str,
        key: &str,
        total_size: u64,
        tx: &mpsc::Sender<Chunk>,
    ) -> Result<(), S3O2Error> {
        let num_chunks = self.calculate_num_chunks(&total_size);
        stream::iter(0..num_chunks)
            .map(|i| {
                let (start, end) = self.calculate_chunk_range(i, &total_size);
                self.download_single_chunk(bucket, key, start, end, tx)
            })
            .buffer_unordered(self.max_concurrent_chunks)
            .try_collect::<()>()
            .await?;

        Ok(())
    }

    pub async fn download_file(
        &self,
        bucket: &str,
        key: &str,
        output_path: &PathBuf,
    ) -> Result<u64, S3O2Error> {
        let total_size = self.get_obj_size(bucket, key).await?;
        let (tx, rx) = mpsc::channel::<Chunk>(self.max_concurrent_chunks);

        let writer = self.spawn_writer(rx, output_path, total_size);

        self.download_chunks(bucket, key, total_size, &tx).await?;
        drop(tx);

        writer.await??;

        Ok(total_size)
    }
}
#[cfg(test)]
mod tests {

    use super::*;
    use crate::macros::assert_file_content;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::error::{NoSuchKey, NotFound};
    use aws_sdk_s3::{self, operation::get_object::GetObjectError};
    use aws_smithy_mocks::{Rule, mock, mock_client};
    use tempfile::TempDir;

    const MB: usize = 1024 * 1024;
    // const GB: usize = 1024 * 1024 * 1024;

    pub struct MockS3Builder {
        content: Vec<u8>,
        chunk_size: usize,
        fail_at_chunk: Option<usize>,
        fail_at_head: bool,
    }

    impl MockS3Builder {
        pub fn new(content: Vec<u8>) -> Self {
            Self {
                content,
                chunk_size: 8 * 1024 * 1024,
                fail_at_chunk: None,
                fail_at_head: false,
            }
        }
        pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
            self.chunk_size = chunk_size;
            self
        }
        pub fn build(&self) -> aws_sdk_s3::Client {
            let mut rules = vec![self.create_head_rule()];
            rules.extend(self.create_get_rules());
            let rule_refs: Vec<_> = rules.iter().collect();
            mock_client!(aws_sdk_s3, rule_refs.as_slice())
        }
        fn create_head_success(&self) -> Rule {
            let content_length = self.content.len() as i64;
            mock!(aws_sdk_s3::Client::head_object).then_output(move || {
                HeadObjectOutput::builder()
                    .content_length(content_length)
                    .build()
            })
        }
        fn create_head_error(&self) -> aws_smithy_mocks::Rule {
            mock!(aws_sdk_s3::Client::head_object).then_error(|| {
                HeadObjectError::NotFound(NotFound::builder().message("Object not found").build())
            })
        }
        fn create_head_rule(&self) -> Rule {
            if self.fail_at_head {
                self.create_head_error()
            } else {
                self.create_head_success()
            }
        }
        fn calculate_num_chunks(&self) -> usize {
            self.content.len().div_ceil(self.chunk_size)
        }
        fn calculate_chunk_range(&self, chunk_index: usize) -> (usize, usize) {
            let start = self.chunk_size * chunk_index;
            let end = (self.chunk_size * (1 + chunk_index)).min(self.content.len()) - 1;
            (start, end)
        }
        fn extract_chunk_data(&self, start: usize, end: usize) -> Vec<u8> {
            self.content[start..=end].to_vec()
        }
        fn create_get_success(&self, range_str: String, chunk_data: Vec<u8>) -> Rule {
            let content_length = chunk_data.len() as i64;
            mock!(aws_sdk_s3::Client::get_object)
                .match_requests(move |req| req.range() == Some(range_str.as_str()))
                .then_output(move || {
                    GetObjectOutput::builder()
                        .content_length(content_length)
                        .body(ByteStream::from(chunk_data.clone()))
                        .build()
                })
        }
        fn should_chunk_fail(&self, chunk_idx: usize) -> bool {
            Some(chunk_idx) == self.fail_at_chunk
        }
        fn create_get_error(&self, range_str: String) -> Rule {
            mock!(aws_sdk_s3::Client::get_object)
                .match_requests(move |req| req.range() == Some(range_str.as_str()))
                .then_error(|| {
                    GetObjectError::NoSuchKey(
                        NoSuchKey::builder()
                            .message("No Key Found for chunk")
                            .build(),
                    )
                })
        }
        fn create_get_rule_for_chunk(&self, chunk_idx: usize) -> Rule {
            let (start, end) = self.calculate_chunk_range(chunk_idx);
            let range_str = format!("bytes={}-{}", start, end);

            if self.should_chunk_fail(chunk_idx) {
                self.create_get_error(range_str)
            } else {
                let chunk_data = self.extract_chunk_data(start, end);
                self.create_get_success(range_str, chunk_data)
            }
        }
        fn create_get_rules(&self) -> Vec<Rule> {
            let num_chunks = self.calculate_num_chunks();

            (0..num_chunks)
                .map(|chunk_idx| self.create_get_rule_for_chunk(chunk_idx))
                .collect()
        }
    }

    pub struct TestData;

    impl TestData {
        pub fn sequential(size: usize) -> Vec<u8> {
            (0..size).map(|i| (i % 256) as u8).collect()
        }
    }

    // fn path(&self, filename: &str) -> PathBuf {
    //     self.temp_dir.path().join(filename)
    // }
    // }

    #[tokio::test]
    async fn test_successful_download() {
        let test_data = TestData::sequential(100 * MB);
        let test_data_len = test_data.len();
        let chunk_size = 8 * MB; // 8MB
        let mock_client = MockS3Builder::new(test_data.clone())
            .with_chunk_size(chunk_size)
            .build();
        let download_dir = TempDir::new().unwrap();
        let download_path = download_dir.path().join("test_file.txt");

        let downloader = MultipartDownloader::new(mock_client, chunk_size as u64, 4);

        let total_size = downloader
            .download_file("test_bucket", "test_key", &download_path)
            .await
            .unwrap();

        assert_eq!(total_size as usize, test_data_len);
        assert_file_content!(download_path, test_data);
    }
}
