use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::types::Tier;
use rcs3ud::{
    DownloadColdInput, DownloadInput, DownloadStrategy, S3Src, WaitForRestoreStrategy, download,
};
use sipper::Sipper;
use tokio::fs::File;

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);
    let mut dest = File::options()
        .truncate(true)
        .write(true)
        .create(true)
        .open("Downloaded README.md")
        .await
        .unwrap();
    let mut straw = download(DownloadInput {
        client: &client,
        src: S3Src {
            bucket: "rcs3ud",
            object_key: "Cold README.md",
        },
        dest: &mut dest,
        strategy: DownloadStrategy::Cold(DownloadColdInput {
            tier: Tier::Bulk,
            wait_for_restore_stratey: WaitForRestoreStrategy::PollGet(Duration::from_secs(
                // 30 minutes
                60 * 30,
            )),
        }),
        retry_interval: Duration::from_secs(5),
    })
    .await
    .pin();
    while let Some(event) = straw.sip().await {
        println!("{event:#?}")
    }
    straw.await.unwrap();
    println!("Downloaded successfully.");
}
