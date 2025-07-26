use std::{io, time::Duration};

use crate::{
    AmountLimiter, OperationScheduler, StartTime,
    maybe_retryable_sdk_error::IntoMaybeRetryable,
    retry::{KeepRetryingExt, MaybeRetryable},
};
use aws_sdk_s3::{
    error::SdkError, operation::put_object::PutObjectError, primitives::ByteStream,
    types::StorageClass,
};
use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};
use sipper::{Sipper, Straw, sipper};
use thiserror::Error;
use time::UtcDateTime;
use tokio::time::sleep;

pub struct S3Dest<'a> {
    pub bucket: &'a str,
    pub object_key: &'a str,
    pub storage_class: StorageClass,
}

pub trait UploadSrcStream {
    fn get_stream(
        &self,
    ) -> BoxFuture<Result<BoxStream<'static, Result<Bytes, io::Error>>, io::Error>>;
}

pub struct UploadSrc {
    pub stream: Box<dyn UploadSrcStream>,
    pub len: usize,
}

pub struct UploadInput<'a> {
    pub client: &'a aws_sdk_s3::Client,
    pub src: UploadSrc,
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
    Metadata(io::Error),
    #[error("Error getting upload stream")]
    UploadStream(io::Error),
    #[error("Error uploading file")]
    PutObject(SdkError<PutObjectError>),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum UploadEvent {
    ReadingMetadata,
    ReservingUploadAmount,
    GettingUploadStream,
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
            async move || {
                sender.send(UploadEvent::ReservingUploadAmount).await;
                let reservation = input.amount_limiter.reserve(input.src.len, &id).await;
                match input.operation_scheduler.get_start_time(input.src.len) {
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
                sender.send(UploadEvent::GettingUploadStream).await;
                let stream = input
                    .src
                    .stream
                    .get_stream()
                    .await
                    .map_err(|e| MaybeRetryable::NotRetryable(UploadError::UploadStream(e)))?;
                sender.send(UploadEvent::StartingUpload).await;
                match input
                    .client
                    // TODO: Compute checksum so we don't forget, or end up with a DEEP_ARCHIVE object which is corrupted
                    .put_object()
                    .bucket(input.dest.bucket)
                    .key(input.dest.object_key)
                    .storage_class(input.dest.storage_class.clone())
                    .body(ByteStream::from_body_1_x(reqwest::Body::wrap_stream(
                        stream,
                    )))
                    .content_length(input.src.len.try_into().unwrap())
                    .send()
                    .await
                {
                    Ok(output) => {
                        reservation.mark_complete().await;
                        Ok(output)
                    }
                    Err(e) => Err(e.into_maybe_retryable().map(UploadError::PutObject)),
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
