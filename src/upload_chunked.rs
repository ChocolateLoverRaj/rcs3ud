use std::{
    io::{self},
    num::NonZeroUsize,
    path::PathBuf,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use sipper::{Sipper, Straw, sipper};
use thiserror::Error;
use tokio::fs::metadata;

use crate::{
    AmountLimiter, OperationScheduler, S3Dest, UploadError, UploadEvent, UploadInput, UploadSrc,
    upload,
};

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct UploadChunkedProgress {
    pub len: Option<usize>,
    pub parts_uploaded: usize,
}

pub struct UploadChunkedInput<'a> {
    pub client: &'a aws_sdk_s3::Client,
    pub src: PathBuf,
    pub dest: S3Dest<'a>,
    pub retry_interval: Duration,
    pub operation_scheduler: Box<dyn OperationScheduler>,
    /// Note that if an upload fails in the middle of uploading, we don't know how much data was actually uploaded.
    /// So we assume that the entire file len was uploaded before the operation failed.
    pub amount_limiter: Box<dyn AmountLimiter>,
    pub chunk_size: NonZeroUsize,
    pub progress: UploadChunkedProgress,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Error)]
pub enum UploadChunkedError {
    #[error("Error getting metadata of file")]
    Metadata(io::Error),
    #[error("Error uploading a chunk")]
    Upload(UploadError),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum UploadChunkedEvent {
    GettingMetadata,
    StartingChunk(usize),
    SaveProgress(UploadChunkedProgress),
    UploadEvent(UploadEvent),
}

pub fn upload_chunked(
    input: UploadChunkedInput<'_>,
) -> impl Straw<(), UploadChunkedEvent, UploadChunkedError> {
    sipper(async move |mut sender| {
        let mut progress = input.progress;
        let len = if let Some(len) = progress.len {
            len
        } else {
            sender.send(UploadChunkedEvent::GettingMetadata).await;
            let len = metadata(&input.src)
                .await
                .map_err(UploadChunkedError::Metadata)?
                .len()
                .try_into()
                .unwrap();
            progress.len = Some(len);
            sender
                .send(UploadChunkedEvent::SaveProgress(progress.clone()))
                .await;
            len
        };
        let total_chunks = len.div_ceil(input.chunk_size.into());
        while progress.parts_uploaded < total_chunks {
            upload(UploadInput {
                client: input.client,
                amount_limiter: input.amount_limiter.clone(),
                dest: S3Dest {
                    bucket: input.dest.bucket,
                    object_key: &format!("{}/{}", input.dest.object_key, progress.parts_uploaded),
                    storage_class: input.dest.storage_class.clone(),
                },
                operation_scheduler: input.operation_scheduler.clone(),
                retry_interval: input.retry_interval,
                src: {
                    let len = (len - progress.parts_uploaded * input.chunk_size.get())
                        .min(input.chunk_size.get());
                    UploadSrc {
                        len,
                        path: input.src.clone(),
                        offset: progress.parts_uploaded * input.chunk_size.get(),
                    }
                },
                tagging: &format!(
                    "file={}&total_len={}&chunks_count={}&chunk_size={}&chunk_number={}",
                    input.dest.object_key,
                    len,
                    total_chunks,
                    input.chunk_size,
                    progress.parts_uploaded
                ),
            })
            .with(UploadChunkedEvent::UploadEvent)
            .run(sender.clone())
            .await
            .map_err(UploadChunkedError::Upload)?;
            progress.parts_uploaded += 1;
            sender
                .send(UploadChunkedEvent::SaveProgress(progress.clone()))
                .await;
        }
        Ok(())
    })
}
