use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use clap::{Parser, Subcommand};
use s3o2::lister::ObjectLister;
use s3o2::multipart_downloader::MultipartDownloader;
use s3o2::utils::parse_uri;
use s3o2::{downloader::Downloader, errors::S3O2Error};
use std::path::PathBuf;

const MB: usize = 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "s3o2")]
#[command(
    author = "Abhishek Roy",
    version = "0.1.0",
    about = "A s3 downloader using multipart downloads"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(name = "mpdl")]
    MultiPartDownload {
        #[arg(value_name = "URI")]
        source: String,
        #[arg(value_name = "PATH")]
        target: PathBuf,
        #[arg(long, value_name = "MB")]
        chunk_size: Option<usize>,
        #[arg(long, value_name = "INT")]
        max_concurrent_chunks: Option<usize>,
    },
    #[command(name = "dl")]
    Download {
        #[arg(value_name = "URI")]
        source: String,
        #[arg(value_name = "PATH")]
        target: PathBuf,
    },
    #[command(name = "ls")]
    S3List {
        #[arg(value_name = "URI")]
        target: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), S3O2Error> {
    let cli = Cli::parse();
    let aws_configuration = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let s3_client = Client::new(&aws_configuration);
    match &cli.command {
        Commands::MultiPartDownload {
            source,
            target,
            chunk_size,
            max_concurrent_chunks,
        } => {
            let (bucket, key) = parse_uri(source)?;
            let target_path = PathBuf::from(target);
            let parent = target_path.parent().ok_or_else(|| {
                S3O2Error::Value("Parent Directory of target does not exist".into())
            })?;

            if !parent.exists() {
                return Err(S3O2Error::Value(
                    "Parent Directory of target does not exist".into(),
                ));
            }
            let downloader = MultipartDownloader::new(
                s3_client,
                chunk_size.unwrap_or(8 * MB) as u64,
                max_concurrent_chunks.unwrap_or(10),
            );

            downloader
                .download_file(&bucket, &key, &target_path)
                .await?;
            Ok(())
        }
        Commands::Download { source, target } => {
            let (bucket, key) = parse_uri(source)?;
            let target_path = PathBuf::from(target);
            let parent = target_path.parent().ok_or_else(|| {
                S3O2Error::Value("Parent Directory of target does not exist".into())
            })?;

            if !parent.exists() {
                return Err(S3O2Error::Value(
                    "Parent Directory of target does not exist".into(),
                ));
            }
            let downloader = Downloader::new(s3_client);

            downloader
                .download_file(&bucket, &key, &target_path)
                .await?;
            Ok(())
        }
        Commands::S3List { target } => {
            let (bucket, prefix) = parse_uri(target)?;
            let mut lister_builder = ObjectLister::builder()
                .with_client(s3_client)
                .with_bucket(&bucket);
            if !prefix.is_empty() {
                lister_builder = lister_builder.with_prefix(&prefix);
            }
            let lister = lister_builder.with_delimiter(&'/').build()?;
            let mut rx = lister.stream();
            let mut results = Vec::new();
            while let Some(result) = rx.recv().await {
                results.push(result.unwrap());
            }
            results.iter().for_each(|o| {
                println!("{}", o.key);
            });
            Ok(())
        }
    }
}
