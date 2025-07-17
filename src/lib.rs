use std::{io, num::TryFromIntError};

use aws_sdk_s3::{
    error::SdkError,
    operation::{get_object::GetObjectError, put_object::PutObjectError},
    primitives::{ByteStream, ByteStreamError, SdkBody},
    types::StorageClass,
};
use bytes::BytesMut;
use pin_project_lite::pin_project;
use sipper::{Straw, Stream, sipper};
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

pub struct S3Dest<'a> {
    pub bucket: &'a str,
    pub object_key: &'a str,
    pub storage_class: StorageClass,
}

pub struct UploadInput<'a> {
    pub client: &'a aws_sdk_s3::Client,
    pub src: File,
    pub dest: S3Dest<'a>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Error)]
pub enum UploadError {
    #[error("Error getting metadata")]
    Metadata(io::Error),
    #[error("This should never happen")]
    LenConvertError(TryFromIntError),
    #[error("Error with input file")]
    ByteStream(ByteStreamError),
    #[error("Error uploading file")]
    PutObjectError(SdkError<PutObjectError>),
}

pub async fn upload(input: UploadInput<'_>) -> impl Straw<(), usize, UploadError> {
    sipper(async move |sender| {
        let src_len = input
            .src
            .metadata()
            .await
            .map_err(UploadError::Metadata)?
            .len();
        input
            .client
            .put_object()
            .bucket(input.dest.bucket)
            .key(input.dest.object_key)
            .storage_class(input.dest.storage_class)
            .content_length(src_len.try_into().map_err(UploadError::LenConvertError)?)
            .body(ByteStream::from_body_1_x(SdkBody::from_body_1_x(
                reqwest::Body::wrap_stream({
                    pin_project! {
                        struct StreamWithSizeHint<T, S: Stream<Item = T>> {
                            #[pin]
                            stream: S,
                            size_hint: (usize, Option<usize>),
                        }
                    }
                    impl<T, S: Stream<Item = T> + Unpin> Stream for StreamWithSizeHint<T, S> {
                        type Item = T;
                        fn poll_next(
                            self: std::pin::Pin<&mut Self>,
                            cx: &mut std::task::Context<'_>,
                        ) -> std::task::Poll<Option<Self::Item>> {
                            self.project().stream.poll_next(cx)
                        }
                        fn size_hint(&self) -> (usize, Option<usize>) {
                            panic!();
                            self.size_hint
                        }
                    }
                    StreamWithSizeHint {
                        stream: Box::pin(futures::stream::try_unfold((input.src, 0, sender), {
                            async move |(mut src, read_count, mut sender)| {
                                let mut bytes = BytesMut::new();
                                let count = src.read_buf(&mut bytes).await?;
                                if count > 0 {
                                    let new_read_count = read_count + count;
                                    sender.send(new_read_count).await;
                                    Ok::<_, io::Error>(Some((bytes, (src, new_read_count, sender))))
                                } else {
                                    Ok(None)
                                }
                            }
                        })),
                        size_hint: {
                            let len = src_len.try_into().map_err(UploadError::LenConvertError)?;
                            (len, Some(len))
                        },
                    }
                }),
            )))
            .content_length(src_len.try_into().map_err(UploadError::LenConvertError)?)
            .send()
            .await
            .map_err(UploadError::PutObjectError)?;
        Ok(())
    })
}

pub struct S3Src<'a> {
    pub bucket: &'a str,
    pub object_key: &'a str,
}

pub struct DownloadInput<'a> {
    pub client: &'a aws_sdk_s3::Client,
    pub src: S3Src<'a>,
    pub dest: &'a mut tokio::fs::File,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("No content length")]
    NoContentLength,
    #[error("For some reason the content length is an i64 and could not be converted to usize")]
    ContentLengthConversion(TryFromIntError),
    #[error("Error creating the download request")]
    GetObjectError(SdkError<GetObjectError>),
    #[error("Error while downloading the object")]
    DownloadStreamError(ByteStreamError),
    #[error("Error writing to the file")]
    WriteError(io::Error),
}

#[derive(Debug, Clone, Copy)]
pub struct DownloadProgress {
    pub downloaded_from_s3: usize,
    pub written_to_file: usize,
    pub total: usize,
}

pub async fn download(input: DownloadInput<'_>) -> impl Straw<(), DownloadProgress, DownloadError> {
    sipper(async move |mut sender| {
        let mut output = input
            .client
            .get_object()
            .bucket(input.src.bucket)
            .key(input.src.object_key)
            .send()
            .await
            .map_err(DownloadError::GetObjectError)?;
        let mut progress = DownloadProgress {
            total: output
                .content_length
                .ok_or(DownloadError::NoContentLength)?
                .try_into()
                .map_err(DownloadError::ContentLengthConversion)?,
            downloaded_from_s3: 0,
            written_to_file: 0,
        };
        while let Some(bytes) = output
            .body
            .try_next()
            .await
            .map_err(DownloadError::DownloadStreamError)?
        {
            progress.downloaded_from_s3 += bytes.len();
            sender.send(progress).await;
            input
                .dest
                .write_all(&bytes)
                .await
                .map_err(DownloadError::WriteError)?;
            progress.written_to_file += bytes.len();
            sender.send(progress).await;
        }
        Ok(())
    })
}
