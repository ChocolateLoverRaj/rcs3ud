use std::{io::ErrorKind, num::NonZero, time::Duration};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::RequestChecksumCalculation, types::StorageClass};
use clap::Parser;
use rcs3ud::{
    AmountLimiter, AnyTime, FileBackedAmountLimiter, S3Dest, UnlimitedAmountLimiter,
    UploadChunkedEvent, UploadChunkedInput, UploadChunkedProgress, UploadInput, upload,
    upload_chunked, upload_file,
};
use sipper::Sipper;
use tokio::{
    fs::{File, remove_file},
    io::{AsyncReadExt, AsyncWriteExt},
};

#[derive(Debug, Parser)]
#[command(version, about)]
enum Command {
    Upload {
        #[arg(long)]
        src: String,
        #[arg(long)]
        bucket: String,
        #[arg(long)]
        object_key: String,
        #[arg(long)]
        storage_class: StorageClass,
        #[arg(long)]
        retry_interval: Option<f64>,
        #[arg(long)]
        amount_limiter_file: Option<String>,
        #[arg(long)]
        amount_limit: Option<usize>,
        #[arg(long)]
        description: Option<String>,
        #[arg(long)]
        chunked: bool,
        #[arg(long)]
        max_chunk_size: Option<NonZero<usize>>,
        #[arg(long)]
        progress_file: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let command = Command::parse();
    match command {
        Command::Upload {
            src,
            bucket,
            object_key,
            storage_class,
            retry_interval,
            amount_limiter_file,
            amount_limit,
            description,
            chunked,
            max_chunk_size,
            progress_file,
        } => {
            let amount_limiter: Box<dyn AmountLimiter> =
                amount_limiter_file.map_or(Box::new(UnlimitedAmountLimiter), |file| {
                    Box::new(FileBackedAmountLimiter::new(
                        file.into(),
                        amount_limit.expect("Must specify amount limit to use amount limiter file"),
                        description.unwrap_or_default().into(),
                    ))
                });
            let retry_interval =
                retry_interval.map_or(Duration::from_secs(5), |s| Duration::from_secs_f64(s));
            let operation_scheduler = Box::new(AnyTime);
            let dest = S3Dest {
                bucket: &bucket,
                object_key: &object_key,
                storage_class: storage_class,
            };
            let config = aws_config::defaults(BehaviorVersion::latest())
                .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
                .load()
                .await;
            let client = aws_sdk_s3::Client::new(&config);
            if !chunked {
                let mut straw = upload(UploadInput {
                    client: &client,
                    src: upload_file(src.into()).await.unwrap(),
                    dest,
                    retry_interval,
                    operation_scheduler,
                    amount_limiter,
                    tagging: Default::default(),
                })
                .pin();
                while let Some(event) = straw.sip().await {
                    println!("{event:#?}");
                }
                straw.await.unwrap();
                println!("Uploaded successfully.");
            } else {
                let progress_file =
                    progress_file.expect("Must specify progress file with chunked uploads");
                let mut straw = upload_chunked(UploadChunkedInput {
                    client: &client,
                    src: src.into(),
                    dest,
                    retry_interval,
                    operation_scheduler,
                    amount_limiter,
                    progress: {
                        match { File::options().read(true).open(&progress_file).await } {
                            Ok(mut file) => {
                                let mut s = String::new();
                                file.read_to_string(&mut s).await.unwrap();
                                let saved_progress =
                                    ron::from_str::<UploadChunkedProgress>(&s).unwrap();
                                saved_progress
                            }
                            Err(e) => match e.kind() {
                                ErrorKind::NotFound => Default::default(),
                                _ => panic!("{e:#?}"),
                            },
                        }
                    },
                    chunk_size: max_chunk_size.unwrap_or({
                        // AWS limit of 5 GB
                        NonZero::new(5_000_000_000).unwrap()
                    }),
                })
                .pin();
                while let Some(event) = straw.sip().await {
                    println!("{event:#?}");
                    if let UploadChunkedEvent::SaveProgress(saved_progress) = event {
                        File::options()
                            .create(true)
                            .truncate(true)
                            .write(true)
                            .open(&progress_file)
                            .await
                            .unwrap()
                            .write_all(ron::to_string(&saved_progress).unwrap().as_bytes())
                            .await
                            .unwrap();
                    }
                }
                straw.await.unwrap();
                println!("Uploaded successfully.");
                remove_file(progress_file).await.unwrap();
            }
        }
    }
}
