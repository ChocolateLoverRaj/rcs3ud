use std::{io, time::Duration};

use crate::{
    AmountLimiter, OperationScheduler, StartTime,
    maybe_retryable_sdk_error::IntoMaybeRetryable,
    retry::{KeepRetryingExt, MaybeRetryable},
};
use aws_sdk_s3::{
    error::SdkError,
    operation::put_object::PutObjectError,
    primitives::{ByteStream, ByteStreamError},
    types::StorageClass,
};
use sipper::{Sipper, Straw, sipper};
use thiserror::Error;
use time::UtcDateTime;
use tokio::time::sleep;

pub struct S3Dest<'a> {
    pub bucket: &'a str,
    pub object_key: &'a str,
    pub storage_class: StorageClass,
}

pub struct UploadInput<'a> {
    pub client: &'a aws_sdk_s3::Client,
    pub src: &'a str,
    pub dest: S3Dest<'a>,
    pub retry_interval: Duration,
    pub operation_scheduler: Box<dyn OperationScheduler>,
    /// Note that if an upload fails in the middle of uploading, we don't know how much data was actually uploaded.
    /// So we assume that the entire file len was uploaded before the operation failed.
    pub amount_limiter: Box<dyn AmountLimiter>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Error)]
pub enum UploadError {
    #[error("Error getting file metadata")]
    MetadatError(io::Error),
    #[error("Error with input file")]
    ByteStream(ByteStreamError),
    #[error("Error uploading file")]
    PutObjectError(SdkError<PutObjectError>),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum UploadEvent {
    ReadingMetadata,
    ReservingUploadAmount,
    CreatingByteStream,
    ScheduledStart(UtcDateTime),
    StartingUpload,
    UploadError(SdkError<PutObjectError>),
}

pub fn upload(input: UploadInput<'_>) -> impl Straw<(), UploadEvent, UploadError> {
    sipper(async move |sender| {
        ({
            let mut sender = sender.clone();
            let id = format!("upload:{}/{}", input.dest.bucket, input.dest.object_key);
            sender.send(UploadEvent::ReadingMetadata).await;
            let bytes_to_upload = tokio::fs::metadata(input.src)
                .await
                .map_err(UploadError::MetadatError)?
                .len() as usize;
            async move || {
                sender.send(UploadEvent::ReservingUploadAmount).await;
                let reservation = input.amount_limiter.reserve(bytes_to_upload, &id).await;
                sender.send(UploadEvent::CreatingByteStream).await;
                let byte_stream = ByteStream::from_path(input.src)
                    .await
                    .map_err(|e| MaybeRetryable::NotRetryable(UploadError::ByteStream(e)))?;
                match input.operation_scheduler.get_start_time(bytes_to_upload) {
                    StartTime::Now => {}
                    StartTime::Later(time) => {
                        sender.send(UploadEvent::ScheduledStart(time)).await;
                        let duration = time - UtcDateTime::now();
                        if let Ok(duration) = duration.try_into() {
                            // FIXME: If the computer suspends, the sleep will be too long
                            sleep(duration).await
                        } else {
                            // Negative duration, so we should start right away
                        }
                    }
                };
                sender.send(UploadEvent::StartingUpload).await;
                match input
                    .client
                    .put_object()
                    .bucket(input.dest.bucket)
                    .key(input.dest.object_key)
                    .storage_class(input.dest.storage_class.clone())
                    .body(byte_stream)
                    .send()
                    .await
                {
                    Ok(output) => {
                        reservation.mark_complete().await;
                        Ok(output)
                    }
                    Err(e) => Err(e.into_maybe_retryable().map(UploadError::PutObjectError)),
                }
            }
        })
        .keep_retrying(input.retry_interval)
        .with(UploadEvent::UploadError)
        .run(sender)
        .await?;
        Ok(())
    })
}
