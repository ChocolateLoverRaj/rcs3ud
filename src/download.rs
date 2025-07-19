use std::{io, num::TryFromIntError, time::Duration};

use aws_sdk_s3::{
    error::SdkError,
    operation::{get_object::GetObjectError, restore_object::RestoreObjectError},
    primitives::ByteStreamError,
    types::{RestoreRequest, Tier},
};
use sipper::{Sender, Straw, sipper};
use thiserror::Error;
use tokio::{io::AsyncWriteExt, time::sleep};

pub enum DownloadStrategy {
    /// For storage classes that don't need a restore, such as `STANDARD`.
    Warm,
    Cold(WaitForRestoreStrategy),
}

pub enum WaitForRestoreStrategy {
    /// Polls the object until it's restored.
    ///
    /// # Warning
    /// Using a short durations, such as duration shorter than 30 minutes, will result in high costs.
    /// Even with a 30 minute interval, restoring will cost $0.0048 if it takes 48 hours.
    PollGet(Duration),
}

pub struct S3Src<'a> {
    pub bucket: &'a str,
    pub object_key: &'a str,
}

pub struct DownloadInput<'a> {
    pub client: &'a aws_sdk_s3::Client,
    pub src: S3Src<'a>,
    pub dest: &'a mut tokio::fs::File,
    pub strategy: DownloadStrategy,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("No content length")]
    NoContentLength,
    #[error("For some reason the content length is an i64 and could not be converted to usize")]
    ContentLengthConversion(TryFromIntError),
    #[error("Error creating the download request")]
    GetObjectError(SdkError<GetObjectError>),
    #[error("Error while downloading the object")]
    DownloadStreamError(ByteStreamError),
    #[error("Error writing to the file")]
    WriteError(io::Error),
    #[error("Error restoring the object")]
    RestoreError(SdkError<RestoreObjectError>),
    #[error("Expected object to be restoring but restored, but it isn't")]
    NotRestoringOrRestored,
    #[error("Could not parse the value of x-amz-restore")]
    UnknownRestoreString,
}

#[derive(Debug, Clone, Copy)]
pub struct DownloadProgress {
    pub downloaded_from_s3: usize,
    pub written_to_file: usize,
    pub total: usize,
}

async fn download_warm(
    input: DownloadInput<'_>,
    mut sender: Sender<DownloadProgress>,
) -> Result<(), DownloadError> {
    let mut output = input
        .client
        .get_object()
        .bucket(input.src.bucket)
        .key(input.src.object_key)
        .send()
        .await
        .map_err(DownloadError::GetObjectError)?;
    let mut progress = DownloadProgress {
        total: output
            .content_length
            .ok_or(DownloadError::NoContentLength)?
            .try_into()
            .map_err(DownloadError::ContentLengthConversion)?,
        downloaded_from_s3: 0,
        written_to_file: 0,
    };
    while let Some(bytes) = output
        .body
        .try_next()
        .await
        .map_err(DownloadError::DownloadStreamError)?
    {
        progress.downloaded_from_s3 += bytes.len();
        sender.send(progress).await;
        input
            .dest
            .write_all(&bytes)
            .await
            .map_err(DownloadError::WriteError)?;
        progress.written_to_file += bytes.len();
        sender.send(progress).await;
    }
    Ok(())
}

pub async fn download(input: DownloadInput<'_>) -> impl Straw<(), DownloadProgress, DownloadError> {
    sipper(async move |sender| {
        match input.strategy {
            DownloadStrategy::Warm => {
                download_warm(input, sender).await?;
            }
            DownloadStrategy::Cold(WaitForRestoreStrategy::PollGet(poll_interval)) => {
                input
                    .client
                    .restore_object()
                    .bucket(input.src.bucket)
                    .key(input.src.object_key)
                    .restore_request(RestoreRequest::builder().tier(Tier::Bulk).days(1).build())
                    .send()
                    .await
                    .map_err(DownloadError::RestoreError)?;
                loop {
                    sleep(poll_interval).await;
                    match input
                        .client
                        .head_object()
                        .bucket(input.src.bucket)
                        .key(input.src.object_key)
                        .send()
                        .await
                        .unwrap()
                        .restore()
                    {
                        None => break Err(DownloadError::NotRestoringOrRestored),
                        Some(message) => {
                            if message.starts_with("ongoing-request=\"false\"") {
                                break Ok(());
                            } else if message.starts_with("ongoing-request=\"true\"") {
                                // Repeat loop
                            } else {
                                break Err(DownloadError::UnknownRestoreString);
                            }
                        }
                    }
                }?;
                download_warm(input, sender).await?;
            }
        }
        Ok(())
    })
}
