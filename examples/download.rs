use aws_config::BehaviorVersion;
use rcs3ud::{DownloadInput, DownloadStrategy, S3Src, download};
use sipper::Sipper;
use tokio::fs::File;

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);
    let mut dest = File::options()
        .truncate(true)
        .write(true)
        .open("Downloaded README.md")
        .await
        .unwrap();
    let mut straw = download(DownloadInput {
        client: &client,
        src: S3Src {
            bucket: "rcs3ud",
            object_key: "README.md",
        },
        dest: &mut dest,
        strategy: DownloadStrategy::Warm,
    })
    .await
    .pin();
    while let Some(progress) = straw.sip().await {
        println!("{progress:?}")
    }
    straw.await.unwrap();
    println!("Complete");
}
