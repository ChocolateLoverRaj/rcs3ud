use std::{io, num::TryFromIntError, time::Duration};

use crate::retry::KeepRetryingExt;
use aws_sdk_s3::{
    error::SdkError,
    operation::{
        get_object::GetObjectError, head_object::HeadObjectError,
        restore_object::RestoreObjectError,
    },
    primitives::ByteStreamError,
    types::{GlacierJobParameters, RestoreRequest, Tier},
};
use sipper::{Sipper, Straw, sipper};
use thiserror::Error;
use tokio::{io::AsyncWriteExt, time::sleep};

use crate::maybe_retryable_sdk_error::IntoMaybeRetryable;

pub struct DownloadColdInput {
    pub tier: Tier,
    pub wait_for_restore_stratey: WaitForRestoreStrategy,
}

pub enum DownloadStrategy {
    /// For storage classes that don't need a restore, such as `STANDARD`.
    Warm,
    Cold(DownloadColdInput),
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
    pub retry_interval: Duration,
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
    #[error("Error checking the restore status of the object")]
    HeadError(SdkError<HeadObjectError>),
}

#[derive(Debug, Clone, Copy)]
pub struct DownloadProgress {
    pub downloaded_from_s3: usize,
    pub written_to_file: usize,
    pub total: usize,
}

#[derive(Debug)]
pub enum DownloadEvent {
    DownloadError(SdkError<GetObjectError>),
    DownloadProgress(DownloadProgress),
    RestoreError(SdkError<RestoreObjectError>),
    RestoreInitiated,
    /// Restore status was checked, and restoring is in progress
    NotYetRestored,
    /// The object is restored and available to download
    RestoreComplete,
    CheckStatusError(SdkError<HeadObjectError>),
}

fn download_warm(input: DownloadInput<'_>) -> impl Straw<(), DownloadEvent, DownloadError> {
    sipper(async move |mut sender| {
        let mut output = (async || {
            input
                .client
                .get_object()
                .bucket(input.src.bucket)
                .key(input.src.object_key)
                .send()
                .await
                .map_err(|e| e.into_maybe_retryable().map(DownloadError::GetObjectError))
        })
        .keep_retrying(input.retry_interval)
        .with(DownloadEvent::DownloadError)
        .run(sender.clone())
        .await?;
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
            sender.send(DownloadEvent::DownloadProgress(progress)).await;
            input
                .dest
                .write_all(&bytes)
                .await
                .map_err(DownloadError::WriteError)?;
            progress.written_to_file += bytes.len();
            sender.send(DownloadEvent::DownloadProgress(progress)).await;
        }
        Ok(())
    })
}

pub async fn download(input: DownloadInput<'_>) -> impl Straw<(), DownloadEvent, DownloadError> {
    sipper(async move |mut sender| {
        match &input.strategy {
            DownloadStrategy::Warm => {
                download_warm(input).run(sender).await?;
            }
            DownloadStrategy::Cold(cold_input) => {
                match cold_input.wait_for_restore_stratey {
                    WaitForRestoreStrategy::PollGet(poll_interval) => {
                        (async || {
                            input
                                .client
                                .restore_object()
                                .bucket(input.src.bucket)
                                .key(input.src.object_key)
                                .restore_request(
                                    RestoreRequest::builder()
                                        .days(1)
                                        .glacier_job_parameters(
                                            GlacierJobParameters::builder()
                                                .tier(cold_input.tier.clone())
                                                .build()
                                                // Will always be Ok since we specified tier
                                                .unwrap(),
                                        )
                                        .build(),
                                )
                                .send()
                                .await
                                .map_err(|e| {
                                    e.into_maybe_retryable().map(DownloadError::RestoreError)
                                })
                        })
                        .keep_retrying(input.retry_interval)
                        .with(DownloadEvent::RestoreError)
                        .run(sender.clone())
                        .await?;
                        sender.send(DownloadEvent::RestoreInitiated).await;
                        loop {
                            sleep(poll_interval).await;
                            match (async || {
                                input
                                    .client
                                    .head_object()
                                    .bucket(input.src.bucket)
                                    .key(input.src.object_key)
                                    .send()
                                    .await
                                    .map_err(|e| {
                                        e.into_maybe_retryable().map(DownloadError::HeadError)
                                    })
                            })
                            .keep_retrying(input.retry_interval)
                            .with(DownloadEvent::CheckStatusError)
                            .run(sender.clone())
                            .await?
                            .restore()
                            {
                                None => break Err(DownloadError::NotRestoringOrRestored),
                                Some(message) => {
                                    if message.starts_with("ongoing-request=\"false\"") {
                                        break Ok(());
                                    } else if message.starts_with("ongoing-request=\"true\"") {
                                        sender.send(DownloadEvent::NotYetRestored).await;
                                    } else {
                                        break Err(DownloadError::UnknownRestoreString);
                                    }
                                }
                            }
                        }?;
                    }
                }
                sender.send(DownloadEvent::RestoreComplete).await;
                download_warm(input).run(sender).await?;
            }
        }
        Ok(())
    })
}
