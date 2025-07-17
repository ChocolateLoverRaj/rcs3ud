use aws_config::BehaviorVersion;
use aws_sdk_s3::types::StorageClass;
use rcs3ud::{S3Dest, UploadInput, upload};
use sipper::Sipper;
use tokio::fs::File;

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);
    let file = File::options()
        .read(true)
        .write(false)
        .open("README.md")
        .await
        .unwrap();
    let size = file.metadata().await.unwrap().len();
    println!("Total size: {size}");
    let mut straw = upload(UploadInput {
        client: &client,
        src: file,
        dest: S3Dest {
            bucket: "rcs3ud",
            object_key: "README.md",
            storage_class: StorageClass::Standard,
        },
    })
    .await
    .pin();
    while let Some(bytes_read) = straw.sip().await {
        println!("Read bytes: {bytes_read}");
    }
    straw.await.unwrap();
    println!("Uploaded file");
}
