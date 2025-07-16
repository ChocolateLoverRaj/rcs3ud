use aws_config::BehaviorVersion;
use rcs3ud::{DownloadInput, S3Src, download};

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);
    download(DownloadInput {
        client: &client,
        src: S3Src {
            bucket: "rcs3ud",
            object_key: "README.md",
        },
        dest: "Downloaded README.md",
    })
    .await
    .unwrap();
}
