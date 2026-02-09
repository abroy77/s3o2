use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use clap::{Parser, Subcommand, ValueEnum};
use s3o2::file_filter::FileFilter;
use s3o2::lister::ObjectLister;
use s3o2::multipart_downloader::MultipartDownloader;
use s3o2::multipart_uploader::MultipartUploader;
use s3o2::sync_executor::SyncConfig;
use s3o2::sync_plan::SyncMode;
use s3o2::syncer::Syncer;
use s3o2::uploader::Uploader;
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
    #[command(name = "ul")]
    Upload {
        #[arg(value_name = "PATH")]
        source: PathBuf,
        #[arg(value_name = "URI")]
        target: String,
    },
    #[command(name = "mpul")]
    MultiPartUpload {
        #[arg(value_name = "PATH")]
        source: PathBuf,
        #[arg(value_name = "URI")]
        target: String,
        #[arg(long, value_name = "MB")]
        chunk_size: Option<usize>,
        #[arg(long, value_name = "INT")]
        max_concurrent_chunks: Option<usize>,
    },
    #[command(name = "ls")]
    S3List {
        #[arg(value_name = "URI")]
        target: String,
    },
    #[command(name = "sync")]
    Sync {
        /// Source path (local or s3://bucket/prefix)
        #[arg(value_name = "SOURCE")]
        source: String,

        /// Destination path (local or s3://bucket/prefix)
        #[arg(value_name = "DEST")]
        dest: String,

        /// Sync mode (auto, local-to-s3, s3-to-local, bidirectional)
        #[arg(long, value_enum, default_value = "auto")]
        mode: SyncModeArg,

        /// Delete files in destination not present in source (mirror mode)
        #[arg(long)]
        delete: bool,

        /// Perform a trial run with no changes made
        #[arg(long)]
        dry_run: bool,

        /// Exclude files matching pattern (can be specified multiple times)
        #[arg(long, value_name = "PATTERN")]
        exclude: Vec<String>,

        /// Include files matching pattern (can be specified multiple times)
        #[arg(long, value_name = "PATTERN")]
        include: Vec<String>,

        /// Read exclude patterns from file
        #[arg(long, value_name = "PATH")]
        exclude_from: Option<PathBuf>,

        /// Read include patterns from file
        #[arg(long, value_name = "PATH")]
        include_from: Option<PathBuf>,

        /// Use only size, ignore timestamps
        #[arg(long)]
        size_only: bool,

        /// Chunk size for multipart transfers (MB)
        #[arg(long, default_value = "8")]
        chunk_size: usize,

        /// Maximum concurrent file transfers
        #[arg(long, default_value = "10")]
        max_concurrent: usize,

        /// Show detailed progress
        #[arg(long, short = 'v')]
        verbose: bool,
    },
}

#[derive(ValueEnum, Clone, Debug)]
enum SyncModeArg {
    Auto,
    LocalToS3,
    S3ToLocal,
    Bidirectional,
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
        Commands::Upload { source, target } => {
            let (bucket, key) = parse_uri(target)?;
            if !source.exists() {
                return Err(S3O2Error::Value("Source file does not exist".into()));
            }
            let uploader = Uploader::new(s3_client);
            uploader.upload_file(source, &bucket, &key).await?;
            Ok(())
        }
        Commands::MultiPartUpload {
            source,
            target,
            chunk_size,
            max_concurrent_chunks,
        } => {
            let (bucket, key) = parse_uri(target)?;
            if !source.exists() {
                return Err(S3O2Error::Value("Source file does not exist".into()));
            }
            let uploader = MultipartUploader::new(
                s3_client,
                chunk_size.unwrap_or(8 * MB) as u64,
                max_concurrent_chunks.unwrap_or(10),
            );
            uploader.upload_file(source, &bucket, &key).await?;
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
        Commands::Sync {
            source,
            dest,
            mode,
            delete,
            dry_run,
            exclude,
            include,
            exclude_from,
            include_from,
            size_only,
            chunk_size,
            max_concurrent,
            verbose,
        } => {
            // Determine sync direction and extract paths
            let (local_path, bucket, prefix, detected_mode) =
                detect_sync_params(source, dest, mode, *delete)?;

            // Build file filter
            let mut filter_builder = FileFilter::builder();

            for pattern in exclude {
                filter_builder = filter_builder.add_exclude(pattern);
            }

            for pattern in include {
                filter_builder = filter_builder.add_include(pattern);
            }

            if let Some(path) = exclude_from {
                filter_builder = filter_builder.add_excludes_from_file(path)?;
            }

            if let Some(path) = include_from {
                filter_builder = filter_builder.add_includes_from_file(path)?;
            }

            let filter = filter_builder.build()?;

            // Create sync config
            let config = SyncConfig {
                chunk_size: (*chunk_size * MB) as u64,
                max_concurrent_transfers: *max_concurrent,
                dry_run: *dry_run,
                verbose: *verbose,
            };

            // Create syncer and execute
            let syncer = Syncer::new(s3_client, filter, config);

            syncer
                .sync(&local_path, &bucket, &prefix, detected_mode, *size_only)
                .await?;

            Ok(())
        }
    }
}

fn detect_sync_params(
    source: &str,
    dest: &str,
    mode_arg: &SyncModeArg,
    delete: bool,
) -> Result<(PathBuf, String, String, SyncMode), S3O2Error> {
    let source_is_s3 = source.starts_with("s3://");
    let dest_is_s3 = dest.starts_with("s3://");

    match (source_is_s3, dest_is_s3) {
        (false, true) => {
            // Local to S3
            let local_path = PathBuf::from(source);
            let (bucket, prefix) = parse_uri(dest)?;
            let mode = match mode_arg {
                SyncModeArg::Auto | SyncModeArg::LocalToS3 => {
                    if delete {
                        SyncMode::Mirror { delete: true }
                    } else {
                        SyncMode::LocalToS3
                    }
                }
                SyncModeArg::Bidirectional => SyncMode::Bidirectional,
                SyncModeArg::S3ToLocal => {
                    return Err(S3O2Error::Value(
                        "Cannot use s3-to-local mode with local source and S3 destination".into(),
                    ));
                }
            };
            Ok((local_path, bucket, prefix, mode))
        }
        (true, false) => {
            // S3 to Local
            let (bucket, prefix) = parse_uri(source)?;
            let local_path = PathBuf::from(dest);
            let mode = match mode_arg {
                SyncModeArg::Auto | SyncModeArg::S3ToLocal => {
                    if delete {
                        SyncMode::Mirror { delete: true }
                    } else {
                        SyncMode::S3ToLocal
                    }
                }
                SyncModeArg::Bidirectional => SyncMode::Bidirectional,
                SyncModeArg::LocalToS3 => {
                    return Err(S3O2Error::Value(
                        "Cannot use local-to-s3 mode with S3 source and local destination".into(),
                    ));
                }
            };
            Ok((local_path, bucket, prefix, mode))
        }
        (true, true) => Err(S3O2Error::Value(
            "Cannot sync between two S3 locations".into(),
        )),
        (false, false) => Err(S3O2Error::Value(
            "Cannot sync between two local locations".into(),
        )),
    }
}
