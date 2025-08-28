use std::{io, path::PathBuf};

use tokio::fs::metadata;

use crate::UploadSrc;

pub async fn upload_file(path: PathBuf) -> Result<UploadSrc, io::Error> {
    let len = metadata(&path).await?.len().try_into().unwrap();
    Ok(UploadSrc {
        len,
        path,
        offset: 0,
    })
}
