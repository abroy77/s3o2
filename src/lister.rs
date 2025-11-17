use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder;
use aws_sdk_s3::types::Object;
use aws_sdk_s3::{Client, primitives::DateTime};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use walkdir::WalkDir;

use crate::errors::S3O2Error;

pub fn local_contents(path: &Path) -> impl Iterator<Item = PathBuf> {
    WalkDir::new(path)
        .contents_first(true)
        .sort_by_file_name()
        .into_iter()
        .flat_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
}

#[derive(Default)]
pub struct ObjectListerBuilder {
    client: Option<Client>,
    bucket: Option<String>,
    prefix: Option<String>,
    delimiter: Option<char>,
}

pub struct ObjectLister {
    request_builder: ListObjectsV2FluentBuilder,
}
#[derive(PartialEq, Hash, Eq, Debug)]
pub struct ObjectInfo {
    pub key: String,
    pub size: i64,
    pub last_modified: DateTime,
}
impl TryFrom<&Object> for ObjectInfo {
    type Error = S3O2Error;
    fn try_from(value: &Object) -> Result<Self, Self::Error> {
        if let (Some(key), Some(size), Some(last_modified)) =
            (value.key(), value.size(), value.last_modified())
        {
            Ok(ObjectInfo {
                key: key.to_string(),
                size,
                last_modified: *last_modified,
            })
        } else {
            Err(S3O2Error::Value("Could not get da objecto".to_owned()))
        }
    }
}

impl ObjectListerBuilder {
    pub fn with_client(mut self, client: Client) -> ObjectListerBuilder {
        self.client = Some(client);
        self
    }
    pub fn with_bucket(mut self, bucket: &str) -> ObjectListerBuilder {
        self.bucket = Some(bucket.to_string());
        self
    }
    pub fn with_prefix(mut self, prefix: &str) -> ObjectListerBuilder {
        self.prefix = Some(prefix.to_string());
        self
    }
    pub fn with_delimiter(mut self, delimiter: &char) -> ObjectListerBuilder {
        self.delimiter = Some(delimiter.to_owned());
        self
    }
    pub fn build(self) -> Result<ObjectLister, S3O2Error> {
        let client = self
            .client
            .ok_or(S3O2Error::Value("client is required".to_string()))?;
        let bucket = self
            .bucket
            .ok_or(S3O2Error::Value("bucket is required".to_string()))?;

        let mut request = client.list_objects_v2().bucket(bucket);
        if let Some(prefix) = self.prefix {
            request = request.prefix(prefix);
        }

        if let Some(delimiter) = self.delimiter {
            request = request.delimiter(delimiter);
        }

        Ok(ObjectLister {
            request_builder: request,
        })
    }
}
impl ObjectLister {
    pub fn builder() -> ObjectListerBuilder {
        ObjectListerBuilder::default()
    }
    pub fn stream(&self) -> mpsc::Receiver<Result<ObjectInfo, S3O2Error>> {
        let (tx, rx) = mpsc::channel(1000);
        let request = self.request_builder.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::stream_inner(request, tx).await {
                eprintln!("Error streaming objects: {}", e);
            }
        });
        rx
    }

    async fn stream_inner(
        request_builder: ListObjectsV2FluentBuilder,
        tx: mpsc::Sender<Result<ObjectInfo, S3O2Error>>,
    ) -> Result<(), S3O2Error> {
        let mut paginator = request_builder.into_paginator().send();
        while let Some(page_result) = paginator.next().await {
            match page_result {
                Ok(page) => {
                    if !Self::process_page(&page, &tx).await? {
                        break; // drop reciever
                    }
                }
                Err(e) => {
                    let e_msg = e.to_string();
                    let _ = tx.send(Err(S3O2Error::Value(e_msg.clone()))).await;
                    return Err(S3O2Error::S3Error(e_msg));
                }
            }
        }
        Ok(())
    }
    async fn process_page(
        page: &ListObjectsV2Output,
        tx: &mpsc::Sender<Result<ObjectInfo, S3O2Error>>,
    ) -> Result<bool, S3O2Error> {
        let contents = page.contents();
        for object in contents {
            if let Ok(object_info) = object.try_into() {
                if tx.send(Ok(object_info)).await.is_err() {
                    return Ok(false); // drop reciever
                }
            } else {
                eprintln!("Skipping object with missing fields");
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_mocks::{mock, mock_client};
    use std::{
        collections::HashSet,
        fs::{File, create_dir},
        io::Write,
    };
    use tempfile::TempDir;

    fn make_test_dir_tree() -> (TempDir, Vec<PathBuf>) {
        let dir = TempDir::new().unwrap();
        let d1 = dir.path().join("d1");
        create_dir(&d1).unwrap();
        let d2 = dir.path().join("d2");
        create_dir(&d2).unwrap();
        let d3 = d1.join("d3");
        create_dir(&d3).unwrap();
        let d1f_path = d1.join("d1.txt");
        let mut d1f = File::create(&d1f_path).unwrap();
        d1f.write_all("Well hello there D1".as_bytes()).unwrap();
        let d2f_path = d2.join("d2.txt");
        let mut d2f = File::create(&d2f_path).unwrap();
        d2f.write_all("Well hello there D2".as_bytes()).unwrap();
        let d3f_path = d3.join("d3.txt");
        let mut d3f = File::create(&d3f_path).unwrap();
        d3f.write_all("Well hello there D3".as_bytes()).unwrap();

        return (dir, vec![d1f_path, d3f_path, d2f_path]);
    }

    #[test]
    fn test_tree_lister() {
        let (dir, f_paths) = make_test_dir_tree();
        let tree = local_contents(&dir.path()).collect::<Vec<_>>();
        assert_eq!(tree.len(), f_paths.len());
        assert_eq!(f_paths, tree);
    }

    fn create_mock_object(key: &str, size: i64, last_modified: DateTime) -> Object {
        Object::builder()
            .key(key)
            .size(size)
            .last_modified(last_modified)
            .build()
    }
    #[tokio::test]
    async fn test_s3_lister() {
        let input_data = [
            ("dir1/data.txt", 9812, DateTime::from_secs(109838984)),
            ("dir2/data2.txt", 102948, DateTime::from_secs(8983009589)),
            (
                "dir3/subdir/data_inner.txt",
                89898898,
                DateTime::from_secs(9093029094898),
            ),
            (
                "dir4/dir4_inner/omg_another/data_hidden.txt",
                8939283885,
                DateTime::from_secs(9849809909098),
            ),
        ];
        let expected_object_infos: HashSet<ObjectInfo> = input_data
            .clone()
            .into_iter()
            .map(|(key, size, last_modified)| ObjectInfo {
                key: key.to_string(),
                size,
                last_modified,
            })
            .collect();

        // make the mocking rules
        let mock_objects: Vec<Object> = input_data
            .into_iter()
            .map(|(key, size, last_modified)| create_mock_object(key, size, last_modified))
            .collect();

        let list_rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(mock_objects.clone()))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, &[&list_rule]);
        // make the Lister
        let lister = ObjectLister::builder()
            .with_client(client)
            .with_bucket("test_bucket")
            .with_prefix("prefixarooni")
            .build()
            .unwrap();

        let mut rx = lister.stream();
        let mut results = HashSet::new();
        while let Some(result) = rx.recv().await {
            results.insert(result.unwrap());
        }
        assert_eq!(results, expected_object_infos);
    }
}
