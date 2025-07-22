use std::{io::ErrorKind, time::Duration};

use aws_config::BehaviorVersion;
use aws_sdk_s3::types::Tier;
use rcs3ud::{
    DownloadColdInput, DownloadEvent, DownloadInput, DownloadStrategy, S3Src, SavedProgress,
    WaitForRestoreStrategy, download,
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
    let progress_file = "download_cold_progress.ron";
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
        saved_progress: {
            match { File::options().read(true).open(progress_file).await } {
                Ok(mut file) => {
                    let mut s = String::new();
                    file.read_to_string(&mut s).await.unwrap();
                    let saved_progress = ron::from_str::<SavedProgress>(&s).unwrap();
                    saved_progress
                }
                Err(e) => match e.kind() {
                    ErrorKind::NotFound => Default::default(),
                    _ => panic!("{e:#?}"),
                },
            }
        },
    })
    .await
    .pin();
    while let Some(event) = straw.sip().await {
        println!("{event:#?}");
        if let DownloadEvent::UpdateSavedProgress(saved_progress) = event {
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
    println!("Downloaded successfully.");
    remove_file(progress_file).await.unwrap();
}
