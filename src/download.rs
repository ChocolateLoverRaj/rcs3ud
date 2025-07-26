use std::{
    io,
    num::TryFromIntError,
    time::{Duration, SystemTime},
};

use crate::{AmountLimiter, retry::KeepRetryingExt};
use aws_sdk_s3::{
    error::SdkError,
    operation::{
        get_object::GetObjectError, head_object::HeadObjectError,
        restore_object::RestoreObjectError,
    },
    primitives::ByteStreamError,
    types::{GlacierJobParameters, RestoreRequest, Tier},
};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreInitiatedProgress {
    /// Contains the time right after the restore request was completed, or the time after the last head object request was completed.
    last_checked: SystemTime,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub enum DownloadStage {
    #[default]
    WillInitiateRestore,
    RestoreInitiated(RestoreInitiatedProgress),
    /// The file is ready to download or downloading
    RestoreComplete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedReservation {
    amount: usize,
    /// Will be false if program terminated after getting object len but before reserving
    reserved: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SavedProgress {
    reservation: Option<SavedReservation>,
    stage: DownloadStage,
}

pub struct DownloadInput<'a> {
    pub client: &'a aws_sdk_s3::Client,
    pub src: S3Src<'a>,
    pub dest: &'a mut tokio::fs::File,
    pub strategy: DownloadStrategy,
    pub retry_interval: Duration,
    /// It is recommended to save progress when downloading cold objects.
    /// Otherwise you can set this to `Default::default()`.
    pub saved_progress: SavedProgress,
    pub amount_limiter: Option<Box<dyn AmountLimiter>>,
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
    GettingObjectLen,
    ReservingDownloadAmount,
    CheckObjectLenError(SdkError<HeadObjectError>),
    DownloadError(SdkError<GetObjectError>),
    DownloadProgress(DownloadProgress),
    RestoreError(SdkError<RestoreObjectError>),
    RestoreInitiated,
    /// Restore status was checked, and restoring is in progress
    NotYetRestored,
    /// The object is restored and available to download
    RestoreComplete,
    CheckStatusError(SdkError<HeadObjectError>),
    UpdateSavedProgress(SavedProgress),
    MarkingReservationComplete,
}

fn download_warm(input: &mut DownloadInput<'_>) -> impl Straw<(), DownloadEvent, DownloadError> {
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

pub async fn download(
    mut input: DownloadInput<'_>,
) -> impl Straw<(), DownloadEvent, DownloadError> {
    sipper(async move |mut sender| {
        let amount_limiter = input.amount_limiter.clone();
        let id = format!("download:{}/{}", input.src.bucket, input.src.object_key);
        let reservation = if let Some(amount_limiter) = &amount_limiter {
            Some({
                if let Some(reservation) = &input.saved_progress.reservation {
                    if let Some(reservation) = amount_limiter.get_reservation(&id).await {
                        reservation
                    } else {
                        sender.send(DownloadEvent::ReservingDownloadAmount).await;
                        amount_limiter.reserve(reservation.amount, &id).await
                    }
                } else {
                    sender.send(DownloadEvent::GettingObjectLen).await;
                    let len: usize = (async || {
                        input
                            .client
                            .head_object()
                            .bucket(input.src.bucket)
                            .key(input.src.object_key)
                            .send()
                            .await
                            .map_err(|e| e.into_maybe_retryable().map(DownloadError::HeadError))
                    })
                    .keep_retrying(input.retry_interval)
                    .with(DownloadEvent::CheckObjectLenError)
                    .run(sender.clone())
                    .await?
                    .content_length()
                    .unwrap()
                    .try_into()
                    .unwrap();
                    sender.send(DownloadEvent::ReservingDownloadAmount).await;
                    amount_limiter.reserve(len, &id).await
                }
            })
        } else {
            None
        };
        let mut progress = input.saved_progress.clone();
        loop {
            match progress.stage {
                DownloadStage::WillInitiateRestore => {
                    match &input.strategy {
                        DownloadStrategy::Warm => {
                            download_warm(&mut input).run(sender.clone()).await?;
                            break;
                        }
                        DownloadStrategy::Cold(cold_input) => {
                            match (async || {
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
                                    .map_err(|e| e.into_maybe_retryable())
                            })
                            .keep_retrying(input.retry_interval)
                            .with(DownloadEvent::RestoreError)
                            .run(sender.clone())
                            .await
                            {
                                Ok(_) => Ok(()),
                                Err(e) => {
                                    if let SdkError::ServiceError(e) = &e
                                        && e.err().meta().code() == Some("RestoreAlreadyInProgress")
                                    {
                                        // This is ok, we can just wait for it to be restored
                                        Ok(())
                                    } else {
                                        Err(DownloadError::RestoreError(e))
                                    }
                                }
                            }?;
                            sender.send(DownloadEvent::RestoreInitiated).await;
                            progress.stage =
                                DownloadStage::RestoreInitiated(RestoreInitiatedProgress {
                                    last_checked: SystemTime::now(),
                                });
                            sender
                                .send(DownloadEvent::UpdateSavedProgress(progress.clone()))
                                .await;
                        }
                    }
                }
                DownloadStage::RestoreInitiated(restore_progress) => match &input.strategy {
                    DownloadStrategy::Warm => unreachable!(),
                    DownloadStrategy::Cold(cold_input) => {
                        match cold_input.wait_for_restore_stratey {
                            WaitForRestoreStrategy::PollGet(poll_interval) => {
                                sleep(poll_interval.saturating_sub(
                                    restore_progress.last_checked.elapsed().unwrap_or_default(),
                                ))
                                .await;
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
                                    None => {
                                        // The restored object probably expired and became cold again since we restored it.
                                        // Let's restore it again.
                                        progress.stage = DownloadStage::WillInitiateRestore;
                                        sender
                                            .send(DownloadEvent::UpdateSavedProgress(
                                                progress.clone(),
                                            ))
                                            .await;
                                    }
                                    Some(message) => {
                                        if message.starts_with("ongoing-request=\"false\"") {
                                            sender.send(DownloadEvent::RestoreComplete).await;
                                            progress.stage = DownloadStage::RestoreComplete;
                                            sender
                                                .send(DownloadEvent::UpdateSavedProgress(
                                                    progress.clone(),
                                                ))
                                                .await;
                                        } else if message.starts_with("ongoing-request=\"true\"") {
                                            sender.send(DownloadEvent::NotYetRestored).await;
                                            progress.stage = DownloadStage::RestoreInitiated(
                                                RestoreInitiatedProgress {
                                                    last_checked: SystemTime::now(),
                                                },
                                            );
                                            sender
                                                .send(DownloadEvent::UpdateSavedProgress(
                                                    progress.clone(),
                                                ))
                                                .await;
                                        } else {
                                            break Err(DownloadError::UnknownRestoreString)?;
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                DownloadStage::RestoreComplete => {
                    match download_warm(&mut input).run(sender.clone()).await {
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            if let DownloadError::GetObjectError(SdkError::ServiceError(
                                service_error,
                            )) = &e
                                && service_error.err().is_invalid_object_state()
                            {
                                // The restored object probably expired and became cold again since we restored it.
                                // Let's restore it again.
                                progress.stage = DownloadStage::WillInitiateRestore;
                                sender
                                    .send(DownloadEvent::UpdateSavedProgress(progress.clone()))
                                    .await;
                            } else {
                                Err(e)?;
                            }
                        }
                    }
                }
            }
        }
        if let Some(reservation) = reservation {
            sender.send(DownloadEvent::MarkingReservationComplete).await;
            reservation.mark_complete().await;
        }
        Ok(())
    })
}
