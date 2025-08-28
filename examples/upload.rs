use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::types::StorageClass;
use rcs3ud::{AnyTime, S3Dest, UnlimitedAmountLimiter, UploadInput, upload, upload_file};
use sipper::Sipper;

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);
    let mut straw = upload(UploadInput {
        client: &client,
        src: upload_file("README.md".into()).await.unwrap(),
        dest: S3Dest {
            bucket: "rcs3ud",
            object_key: "README.md",
            storage_class: StorageClass::Standard,
        },
        retry_interval: Duration::from_secs(5),
        operation_scheduler: Box::new(AnyTime),
        amount_limiter: Box::new(UnlimitedAmountLimiter),
        tagging: Default::default(),
    })
    .pin();
    while let Some(event) = straw.sip().await {
        println!("{event:#?}");
    }
    straw.await.unwrap();
    println!("Uploaded successfully.");
}
