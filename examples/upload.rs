use aws_config::BehaviorVersion;
use aws_sdk_s3::types::StorageClass;
use rcs3ud::{S3Dest, UploadInput, upload};

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);
    upload(UploadInput {
        client: &client,
        src: "README.md",
        dest: S3Dest {
            bucket: "rcs3ud",
            object_key: "README.md",
            storage_class: StorageClass::Standard,
        },
    })
    .await
    .unwrap();
}
