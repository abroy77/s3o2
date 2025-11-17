use aws_smithy_runtime_api::client::result::SdkError;
use thiserror::Error;
use tokio::io;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum S3O2Error {
    #[error("S3  SDK error: {0}")]
    S3Error(String),
    #[error("IO error: {0}")]
    IO(#[from] io::Error),
    #[error("Value Error: {0}")]
    Value(String),
    #[error("Channel closed error")]
    ChannelClosed,
    #[error("Writer failed")]
    WriterError(#[from] JoinError),
}
impl<E: std::fmt::Display, R> From<SdkError<E, R>> for S3O2Error {
    fn from(value: SdkError<E, R>) -> Self {
        S3O2Error::S3Error(value.to_string())
    }
}
