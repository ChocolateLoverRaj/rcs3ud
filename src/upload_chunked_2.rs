use std::{io, num::NonZeroUsize, path::Path};

use serde::{Deserialize, Serialize};
use tokio::fs::metadata;

use crate::{
    AmountLimiter2, OperationScheduler2, S3Dest, UploadError2, UploadSaveData, UploadSrc2, upload_2,
};

#[derive(Debug)]
pub enum UploadChunkedError2<ReserveError, MarkUsedError, SaveError> {
    Metadata(io::Error),
    Save(SaveError),
    Upload(UploadError2<ReserveError, MarkUsedError, SaveError>),
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UploadChunkedSaveData2 {
    parts_uploaded: usize,
    upload_save_data: UploadSaveData,
}

pub async fn upload_chunked_2<ReserveError, MarkUsedError, SaveError>(
    client: &aws_sdk_s3::Client,
    src: &Path,
    dest: S3Dest<'_>,
    chunk_size: NonZeroUsize,
    mut save_data: UploadChunkedSaveData2,
    amount_limiter: &mut Box<
        dyn AmountLimiter2<ReserveError = ReserveError, MarkUsedError = MarkUsedError> + Send,
    >,
    operation_scheduler: &mut Box<dyn OperationScheduler2 + Send>,
    save: &mut impl AsyncFnMut(&UploadChunkedSaveData2) -> Result<(), SaveError>,
) -> Result<(), UploadChunkedError2<ReserveError, MarkUsedError, SaveError>> {
    let len = usize::try_from(
        metadata(src)
            .await
            .map_err(UploadChunkedError2::Metadata)?
            .len(),
    )
    .unwrap();
    let total_chunks = len.div_ceil(chunk_size.get());
    while save_data.parts_uploaded < total_chunks {
        upload_2(
            client,
            UploadSrc2 {
                path: src,
                offset: save_data.parts_uploaded * chunk_size.get(),
                len: (len - save_data.parts_uploaded * chunk_size.get()).min(chunk_size.get()),
            },
            S3Dest {
                bucket: dest.bucket,
                object_key: &format!("{}/{}", dest.object_key, save_data.parts_uploaded),
                storage_class: dest.storage_class.clone(),
            },
            &format!(
                "file={}&total_len={}&chunks_count={}&chunk_size={}&chunk_number={}",
                dest.object_key, len, total_chunks, chunk_size, save_data.parts_uploaded
            ),
            save_data.upload_save_data.clone(),
            amount_limiter,
            operation_scheduler,
            &mut async |upload_save_data: &UploadSaveData| {
                save_data.upload_save_data = upload_save_data.clone();
                save(&save_data).await?;
                Ok(())
            },
        )
        .await
        .map_err(UploadChunkedError2::Upload)?;
        save_data.parts_uploaded += 1;
        if save_data.parts_uploaded < total_chunks {
            save(&save_data).await.map_err(UploadChunkedError2::Save)?;
        }
    }
    Ok(())
}
