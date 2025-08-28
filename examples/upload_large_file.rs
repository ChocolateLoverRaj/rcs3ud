use std::{io::ErrorKind, num::NonZero, path::PathBuf, str::FromStr, time::Duration};

use aws_config::BehaviorVersion;
use aws_sdk_s3::types::StorageClass;
use rcs3ud::{
    AnyTime, S3Dest, UnlimitedAmountLimiter, UploadChunkedEvent, UploadChunkedInput,
    UploadChunkedProgress, upload_chunked,
};
use sipper::Sipper;
use tokio::{
    fs::{File, remove_file},
    io::{AsyncReadExt, AsyncWriteExt},
};

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);
    // let operation_scheduler =  as Box<dyn OperationScheduler>;
    // let amount_limiter =  as Box<dyn AmountLimiter>;
    let progress_file = "upload_large_file_progress.ron";
    let mut straw = upload_chunked(UploadChunkedInput {
        client: &client,
        src: PathBuf::from_str("README.md").unwrap(),
        dest: S3Dest {
            bucket: "rcs3ud",
            object_key: "README.md",
            storage_class: StorageClass::Standard,
        },
        retry_interval: Duration::from_secs(5),
        operation_scheduler: Box::new(AnyTime),
        amount_limiter: Box::new(UnlimitedAmountLimiter),
        progress: {
            match { File::options().read(true).open(progress_file).await } {
                Ok(mut file) => {
                    let mut s = String::new();
                    file.read_to_string(&mut s).await.unwrap();
                    let saved_progress = ron::from_str::<UploadChunkedProgress>(&s).unwrap();
                    saved_progress
                }
                Err(e) => match e.kind() {
                    ErrorKind::NotFound => Default::default(),
                    _ => panic!("{e:#?}"),
                },
            }
        },
        chunk_size: NonZero::new(1000).unwrap(),
    })
    .pin();
    while let Some(event) = straw.sip().await {
        println!("{event:#?}");
        if let UploadChunkedEvent::SaveProgress(saved_progress) = event {
            File::options()
                .create(true)
                .truncate(true)
                .write(true)
                .open(progress_file)
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
