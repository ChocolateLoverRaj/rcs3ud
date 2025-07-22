use std::time::Duration;

use crate::{
    OperationScheduler, StartTime,
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
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Error)]
pub enum UploadError {
    #[error("Error with input file")]
    ByteStream(ByteStreamError),
    #[error("Error uploading file")]
    PutObjectError(SdkError<PutObjectError>),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum UploadEvent {
    ScheduledStart(UtcDateTime),
    StartingUpload,
    UploadError(SdkError<PutObjectError>),
}

pub fn upload(input: UploadInput<'_>) -> impl Straw<(), UploadEvent, UploadError> {
    sipper(async move |sender| {
        ({
            let mut sender = sender.clone();
            async move || {
                let byte_stream = ByteStream::from_path(input.src)
                    .await
                    .map_err(|e| MaybeRetryable::NotRetryable(UploadError::ByteStream(e)))?;
                match input
                    .operation_scheduler
                    .get_start_time(byte_stream.size_hint().0 as usize)
                {
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
                input
                    .client
                    .put_object()
                    .bucket(input.dest.bucket)
                    .key(input.dest.object_key)
                    .storage_class(input.dest.storage_class.clone())
                    .body(byte_stream)
                    .send()
                    .await
                    .map_err(IntoMaybeRetryable::into_maybe_retryable)
                    .map_err(|e| e.map(UploadError::PutObjectError))
            }
        })
        .keep_retrying(input.retry_interval)
        .with(UploadEvent::UploadError)
        .run(sender)
        .await?;
        Ok(())
    })
}
