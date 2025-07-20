use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::types::StorageClass;
use rcs3ud::{S3Dest, UploadInput, upload};
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
            object_key: "Cold README.md",
            storage_class: StorageClass::DeepArchive,
        },
        retry_interval: Duration::from_secs(5),
    })
    .pin();
    while let Some(e) = straw.sip().await {
        println!("Error: {e:#?}. Retrying.");
    }
    straw.await.unwrap();
    println!("Uploaded successfully.");
}
