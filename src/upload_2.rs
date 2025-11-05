use std::path::Path;

use async_trait::async_trait;
use aws_sdk_s3::{
    error::SdkError,
    operation::put_object::PutObjectError,
    primitives::{FsBuilder, Length},
};
use serde::{Deserialize, Serialize};

use crate::{AmountLimiter2, OperationScheduler2, S3Dest};

#[async_trait]
pub trait UploadCallbacks {
    type SaveError;
    /// Save the upload progress / state / checkpoints.
    /// Note that this method should ensure that the data is saved to non-volatile storage
    /// before returning (be fully written to the disk).
    async fn save(&mut self, data: &UploadSaveData) -> Result<(), Self::SaveError>;
}

pub struct UploadSrc2<'a> {
    pub path: &'a Path,
    pub offset: usize,
    pub len: usize,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub enum UploadSaveData {
    #[default]
    ReservingAmount,
    Uploading,
    MarkingAmountUsed,
}

#[derive(Debug)]
pub enum UploadError2<ReserveError, MarkUsedError, SaveError> {
    Reserve(ReserveError),
    MarkUsed(MarkUsedError),
    Save(SaveError),
    ByteStream(aws_smithy_types::byte_stream::error::Error),
    Upload(SdkError<PutObjectError>),
}

pub async fn upload_2<ReserveError, MarkUsedError, SaveError>(
    client: &aws_sdk_s3::Client,
    src: UploadSrc2<'_>,
    dest: S3Dest<'_>,
    tagging: &str,
    mut save_data: UploadSaveData,
    amount_limiter: &mut Box<
        dyn AmountLimiter2<ReserveError = ReserveError, MarkUsedError = MarkUsedError> + Send,
    >,
    operation_scheduler: &mut Box<dyn OperationScheduler2 + Send>,
    save: &mut impl AsyncFnMut(&UploadSaveData) -> Result<(), SaveError>,
) -> Result<(), UploadError2<ReserveError, MarkUsedError, SaveError>> {
    let id = format!("upload:{}/{}", dest.bucket, dest.object_key);
    if matches!(save_data, UploadSaveData::Uploading) {
        // Assume that the entire file length was uploaded
        amount_limiter
            .mark_used(&id, src.len)
            .await
            .map_err(UploadError2::MarkUsed)?;
        save_data = UploadSaveData::ReservingAmount;
        save(&save_data).await.map_err(UploadError2::Save)?;
    }
    // Reserve an amount
    // Do not create a save "checkpoint", because the reservation could be expired next time this program runs
    amount_limiter
        .reserve(&id, src.len)
        .await
        .map_err(UploadError2::Reserve)?;
    // Wait to schedule a time to start uploading
    operation_scheduler.wait_until_upload_start(src.len).await;
    let byte_stream = FsBuilder::new()
        .path(&src.path)
        .offset(src.offset.try_into().unwrap())
        .length(Length::Exact(src.len.try_into().unwrap()))
        .build()
        .await
        .map_err(UploadError2::ByteStream)?;
    // Save that we are uploading, so that we can mark the data as used if the program crashes
    save_data = UploadSaveData::Uploading;
    save(&save_data).await.map_err(UploadError2::Save)?;
    // Actually do put_object to upload
    client
        .put_object()
        .bucket(dest.bucket)
        .key(dest.object_key)
        .storage_class(dest.storage_class.clone())
        .body(byte_stream)
        .content_length(src.len.try_into().unwrap())
        .tagging(tagging)
        .if_none_match("*")
        .send()
        .await
        .map_err(UploadError2::Upload)?;
    // Save that we are successfully done uploading so we don't try to re-upload
    save_data = UploadSaveData::MarkingAmountUsed;
    save(&save_data).await.map_err(UploadError2::Save)?;
    // Update the amount of data used
    amount_limiter
        .mark_used(&id, src.len)
        .await
        .map_err(UploadError2::MarkUsed)?;
    Ok(())
}
