use std::{io, path::PathBuf};

use futures::TryStreamExt;
use sipper::{FutureExt, StreamExt};
use tokio::fs::{File, metadata};
use tokio_util::io::ReaderStream;

use crate::{UploadSrc, UploadSrcStream};

struct FileUploadStream {
    path: PathBuf,
}

impl UploadSrcStream for FileUploadStream {
    fn get_stream(
        &self,
    ) -> futures::future::BoxFuture<
        Result<futures::stream::BoxStream<'static, Result<bytes::Bytes, io::Error>>, io::Error>,
    > {
        async {
            let file = File::open(&self.path).await?;
            let s = ReaderStream::new(file);
            Ok(s.into_stream().boxed())
        }
        .boxed()
    }
}

pub async fn upload_file(path: PathBuf) -> Result<UploadSrc, io::Error> {
    let len = metadata(&path).await?.len().try_into().unwrap();
    Ok(UploadSrc {
        stream: Box::new(FileUploadStream { path }) as Box<dyn UploadSrcStream>,
        len,
    })
}
