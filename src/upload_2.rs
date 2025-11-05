use std::path::Path;

use aws_sdk_s3::{
    error::SdkError,
    operation::put_object::PutObjectError,
    primitives::{FsBuilder, Length},
};
use serde::{Deserialize, Serialize};

use crate::S3Dest;

pub trait UploadCallbacks {
    type ReserveError;
    /// Reserve a certain amount of data to be used by the caller.
    /// The id can be used to check if the data was already reserved for this upload.
    async fn reserve(&mut self, id: &str, amount: usize) -> Result<(), Self::ReserveError> {
        let _ = id;
        let _ = amount;
        Ok(())
    }

    type MarkUsedError;
    /// Mark data as used.
    async fn mark_used(&mut self, id: &str, amount: usize) -> Result<(), Self::MarkUsedError> {
        let _ = id;
        let _ = amount;
        Ok(())
    }

    /// This function will be called before starting the upload, and can be used for time based upload scheduling.
    /// Uploading will start immediately after this function returns.
    async fn wait_until_upload_start(&mut self, bytes_to_upload: usize) {
        let _ = bytes_to_upload;
    }

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

pub async fn upload_2<C: UploadCallbacks>(
    client: &aws_sdk_s3::Client,
    src: UploadSrc2<'_>,
    dest: S3Dest<'_>,
    callbacks: &mut C,
    tagging: &str,
    mut save_data: UploadSaveData,
) -> Result<(), UploadError2<C::ReserveError, C::MarkUsedError, C::SaveError>> {
    let id = format!("upload:{}/{}", dest.bucket, dest.object_key);
    if matches!(save_data, UploadSaveData::Uploading) {
        // Assume that the entire file length was uploaded
        callbacks
            .mark_used(&id, src.len)
            .await
            .map_err(UploadError2::MarkUsed)?;
        save_data = UploadSaveData::ReservingAmount;
        callbacks
            .save(&save_data)
            .await
            .map_err(UploadError2::Save)?;
    }
    // Reserve an amount
    // Do not create a save "checkpoint", because the reservation could be expired next time this program runs
    callbacks
        .reserve(&id, src.len)
        .await
        .map_err(UploadError2::Reserve)?;
    // Wait to schedule a time to start uploading
    callbacks.wait_until_upload_start(src.len).await;
    let byte_stream = FsBuilder::new()
        .path(&src.path)
        .offset(src.offset.try_into().unwrap())
        .length(Length::Exact(src.len.try_into().unwrap()))
        .build()
        .await
        .map_err(UploadError2::ByteStream)?;
    // Save that we are uploading, so that we can mark the data as used if the program crashes
    save_data = UploadSaveData::Uploading;
    callbacks
        .save(&save_data)
        .await
        .map_err(UploadError2::Save)?;
    // Actually do put_object to upload
    client
        .put_object()
        .bucket(dest.bucket)
        .key(dest.object_key)
        .storage_class(dest.storage_class.clone())
        .body(byte_stream)
        .content_length(src.len.try_into().unwrap())
        .tagging(tagging)
        .send()
        .await
        .map_err(UploadError2::Upload)?;
    // Save that we are successfully done uploading so we don't try to re-upload
    save_data = UploadSaveData::MarkingAmountUsed;
    callbacks
        .save(&save_data)
        .await
        .map_err(UploadError2::Save)?;
    // Update the amount of data used
    callbacks
        .mark_used(&id, src.len)
        .await
        .map_err(UploadError2::MarkUsed)?;
    Ok(())
}
