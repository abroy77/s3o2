# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Ignored Paths

Do not read or assess files under `target/` or `Cargo.lock`. These are build artifacts and dependency locks.

## Build & Test Commands

```bash
cargo build                          # Debug build
cargo build --release                # Release build
cargo test                           # Run all tests
cargo test <test_name>               # Run a single test by name
cargo test --lib <module>::tests     # Run tests for a specific module (e.g., cargo test --lib downloader::tests)
cargo clippy                         # Lint
cargo fmt                            # Format code
cargo fmt -- --check                 # Check formatting without modifying
```

## Project Overview

s3o2 is a Rust CLI tool and library for S3 file operations (upload, download, list) with support for both single-request and multipart/concurrent transfers. It uses the AWS SDK for Rust with tokio for async I/O.

### CLI Subcommands (defined in `main.rs`)

| Command | Alias | Description |
|---------|-------|-------------|
| `Download` | `dl` | Single-request download |
| `MultiPartDownload` | `mpdl` | Concurrent chunked download |
| `Upload` | `ul` | Single-request upload |
| `MultiPartUpload` | `mpul` | Concurrent multipart upload (S3 multipart upload API) |
| `S3List` | `ls` | List objects in an S3 bucket/prefix |

All S3 URIs use the `s3://bucket/key` format, parsed by `utils::parse_uri`.

### Architecture

- **Downloader** (`downloader.rs`): Dispatches between `download_small_file` (collect full body) and `download_medium_file` (streaming via `BufWriter`) based on a 5MB threshold.
- **MultipartDownloader** (`multipart_downloader.rs`): Downloads chunks concurrently using `futures::stream::buffer_unordered`. A spawned writer task receives `Chunk` structs via an `mpsc` channel and writes them to the output file at the correct offset using seek.
- **Uploader** (`uploader.rs`): Dispatches between small (in-memory `ByteStream`) and medium (file-backed `ByteStream::from_path`) based on a 5MB threshold.
- **MultipartUploader** (`multipart_uploader.rs`): Uses the S3 multipart upload API (create → upload parts → complete/abort). A spawned reader task sends `UploadChunk` structs via `mpsc` channel, and parts are uploaded concurrently with `buffer_unordered`. Aborts the upload on failure.
- **ObjectLister** (`lister.rs`): Builder pattern (`ObjectListerBuilder`). Returns a `mpsc::Receiver` stream of `ObjectInfo` results. Handles pagination internally. Also provides `local_contents()` for walking local directory trees.
- **Errors** (`errors.rs`): Unified `S3O2Error` enum using `thiserror`, with `From` impl for SDK errors.
- **Macros** (`macros.rs`): `assert_file_content!` test helper for verifying downloaded file contents.

### Testing Patterns

Tests use `aws-smithy-mocks` (`mock!` and `mock_client!` macros) to create mock S3 clients. Each module has its own test fixtures:
- Builder-pattern mock constructors (e.g., `MockS3Builder`, `MockS3UploadBuilder`) that configure success/failure scenarios per chunk or operation
- `tempfile::TempDir` for isolated filesystem assertions
- `TestData` helpers for generating deterministic byte sequences

## Code Style

- Rust 2024 edition
- Use `thiserror` for error types with `From` conversions
- Async runtime: tokio with `features = ["full"]`
- Concurrency pattern: `mpsc` channels between reader/writer tasks and `futures::stream::buffer_unordered` for parallel chunk operations
- Use builder pattern for configurable components (see `ObjectListerBuilder`)
- Document all public-facing types, traits, and functions with `///` doc comments
- Write safe, performant Rust: prefer zero-copy where practical, avoid unnecessary allocations, use `&Path`/`&PathBuf` over owned paths in function signatures
