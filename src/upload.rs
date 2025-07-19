use std::time::Duration;

use crate::retry::{KeepRetryingExt, MaybeRetryable};
use aws_sdk_s3::{
    error::SdkError,
    operation::put_object::PutObjectError,
    primitives::{ByteStream, ByteStreamError},
    types::StorageClass,
};
use sipper::{Straw, sipper};
use thiserror::Error;

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
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Error)]
pub enum UploadError {
    #[error("Error with input file")]
    ByteStream(ByteStreamError),
    #[error("Error uploading file")]
    PutObjectError(SdkError<PutObjectError>),
}

pub fn upload(input: UploadInput<'_>) -> impl Straw<(), SdkError<PutObjectError>, UploadError> {
    sipper(async move |mut sender| {
        (async move || {
            let byte_stream = ByteStream::from_path(input.src)
                .await
                .map_err(|e| MaybeRetryable::NotRetryable(UploadError::ByteStream(e)))?;
            match {
                input
                    .client
                    .put_object()
                    .bucket(input.dest.bucket)
                    .key(input.dest.object_key)
                    .storage_class(input.dest.storage_class.clone())
                    .body(byte_stream)
                    .send()
                    .await
            } {
                Ok(output) => Ok(output),
                Err(e) => match &e {
                    SdkError::DispatchFailure(_)
                    | SdkError::TimeoutError(_)
                    | SdkError::ResponseError(_) => {
                        sender.send(e).await;
                        Err(MaybeRetryable::Retryable)
                    }
                    SdkError::ServiceError(service_error) => {
                        if service_error.raw().status().is_server_error() {
                            sender.send(e).await;
                            Err(MaybeRetryable::Retryable)
                        } else {
                            Err(MaybeRetryable::NotRetryable(UploadError::PutObjectError(e)))
                        }
                    }
                    SdkError::ConstructionFailure(_) | _ => {
                        Err(MaybeRetryable::NotRetryable(UploadError::PutObjectError(e)))
                    }
                },
            }
        })
        .keep_retrying(input.retry_interval)
        .await?;
        Ok(())
    })
}
