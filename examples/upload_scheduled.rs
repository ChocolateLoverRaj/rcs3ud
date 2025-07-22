use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::types::StorageClass;
use rcs3ud::{S3Dest, TimesOfDay, UploadInput, time::Time, upload};
use sipper::Sipper;

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);
    let mut straw = upload(UploadInput {
        client: &client,
        src: "README.md",
        dest: S3Dest {
            bucket: "rcs3ud",
            object_key: "README.md",
            storage_class: StorageClass::Standard,
        },
        retry_interval: Duration::from_secs(5),
        operation_scheduler: Box::new(TimesOfDay::new(
            Box::new([Time::from_hms(21, 13, 0).unwrap()..Time::from_hms(22, 0, 0).unwrap()]),
            5_000_000.0,
        )),
    })
    .pin();
    while let Some(event) = straw.sip().await {
        println!("{event:#?}");
    }
    straw.await.unwrap();
    println!("Uploaded successfully.");
}
