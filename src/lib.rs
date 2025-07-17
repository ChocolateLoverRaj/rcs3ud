use std::{io, num::TryFromIntError};

use aws_sdk_s3::{
    error::SdkError,
    operation::{get_object::GetObjectError, put_object::PutObjectError},
    primitives::{ByteStream, ByteStreamError},
    types::StorageClass,
};
use sipper::{Straw, sipper};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

pub struct S3Dest<'a> {
    pub bucket: &'a str,
    pub object_key: &'a str,
    pub storage_class: StorageClass,
}

pub struct UploadInput<'a> {
    pub client: &'a aws_sdk_s3::Client,
    pub src: &'a str,
    pub dest: S3Dest<'a>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Error)]
pub enum UploadError {
    #[error("Error with input file")]
    ByteStream(ByteStreamError),
    #[error("Error uploading file")]
    PutObjectError(SdkError<PutObjectError>),
}

pub async fn upload(input: UploadInput<'_>) -> Result<(), UploadError> {
    input
        .client
        .put_object()
        .bucket(input.dest.bucket)
        .key(input.dest.object_key)
        .storage_class(input.dest.storage_class)
        .body(
            ByteStream::from_path(input.src)
                .await
                .map_err(UploadError::ByteStream)?,
        )
        .send()
        .await
        .map_err(UploadError::PutObjectError)?;
    Ok(())
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
