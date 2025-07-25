use std::{
    borrow::Cow,
    io::{self, SeekFrom},
};

use fs4::tokio::AsyncFileExt;
use futures::future::BoxFuture;
use ordermap::OrderMap;
use ron::de::SpannedError;
use serde::{Deserialize, Serialize};
use sipper::FutureExt;
use thiserror::Error;
use time::{Date, Time, UtcDateTime};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    time::sleep,
};

use crate::{AmountLimiter, AmountReservation, StartOfNextMonthExt};

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueItem<'a> {
    description: Cow<'a, str>,
    amount: usize,
    time_added: UtcDateTime,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileData<'a> {
    current_month: Date,
    used_this_month: usize,
    queue: OrderMap<Cow<'a, str>, QueueItem<'a>>,
}

/// An `[AmountLimiter]` which stores usage info in a file.
/// Limit gets reset at the start of every month (UTC).
#[derive(Debug, Clone)]
pub struct FileBackedAmountLimiter<'a> {
    path: &'a str,
    limit: usize,
    description: &'a str,
}

impl<'a> FileBackedAmountLimiter<'a> {
    pub fn new(path: &'a str, limit: usize, description: &'a str) -> Self {
        Self {
            path,
            limit,
            description,
        }
    }
}

struct DataFile {
    file: File,
}

#[derive(Debug, Error)]
enum OpenAndReadError {
    #[error("Failed to open file")]
    Open(io::Error),
    #[error("Failed to lock file")]
    Lock(io::Error),
    #[error("Failed to read file")]
    Read(io::Error),
    #[error("Failed to parse file")]
    Parse(SpannedError),
}

#[derive(Debug, Error)]

enum WriteAndCloseError {
    #[error("Error seeking to start of file")]
    Seek(io::Error),
    #[error("Error clearing existing file contents")]
    SetLen(io::Error),
    #[error("Error serializing data")]
    ToString(ron::Error),
    #[error("Error writing data")]
    Write(io::Error),
    #[error("Error unlocking file")]
    Unlock(io::Error),
}
impl DataFile {
    pub async fn open_and_read(path: &str) -> Result<(Self, FileData<'static>), OpenAndReadError> {
        let mut file = tokio::fs::File::options()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(path)
            .await
            .map_err(OpenAndReadError::Open)?;
        file.lock_exclusive().map_err(OpenAndReadError::Lock)?;
        let mut s = String::new();
        file.read_to_string(&mut s)
            .await
            .map_err(OpenAndReadError::Read)?;
        let now = UtcDateTime::now();
        let data = if s.is_empty() {
            FileData {
                current_month: now.date(),
                queue: Default::default(),
                used_this_month: 0,
            }
        } else {
            let mut data = ron::from_str::<FileData>(&s).map_err(OpenAndReadError::Parse)?;
            if (data.current_month.year(), data.current_month.month()) != (now.year(), now.month())
            {
                data.current_month = now.date();
                data.used_this_month = 0;
            }
            data
        };
        Ok((Self { file }, data))
    }

    pub async fn write_and_close(mut self, data: &FileData<'_>) -> Result<(), WriteAndCloseError> {
        self.file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(WriteAndCloseError::Seek)?;
        self.file
            .set_len(0)
            .await
            .map_err(WriteAndCloseError::SetLen)?;
        self.file
            .write_all(
                ron::Options::default()
                    .to_string_pretty(data, Default::default())
                    .map_err(WriteAndCloseError::ToString)?
                    .as_bytes(),
            )
            .await
            .map_err(WriteAndCloseError::Write)?;
        self.file
            .unlock_async()
            .await
            .map_err(WriteAndCloseError::Unlock)?;
        Ok(())
    }
}

impl AmountLimiter for FileBackedAmountLimiter<'_> {
    fn reserve<'a>(
        &'a self,
        len: usize,
        id: &'a str,
    ) -> BoxFuture<'a, Box<dyn AmountReservation + 'a>> {
        async move {
            let (file, mut data) = DataFile::open_and_read(self.path).await.unwrap();
            data.queue.entry(id.into()).or_insert(QueueItem {
                description: self.description.into(),
                amount: len,
                time_added: UtcDateTime::now(),
            });
            file.write_and_close(&data).await.unwrap();
            loop {
                let (_file, data) = DataFile::open_and_read(self.path).await.unwrap();
                let already_used = data.used_this_month
                    + data.queue[..data.queue.get_index_of(id).unwrap()]
                        .iter()
                        .map(|(_, item)| item.amount)
                        .sum::<usize>();
                let months_to_wait = (already_used + len) / self.limit;
                if months_to_wait == 0 {
                    break;
                } else {
                    // It's not *guaranteed* that after that time it will be our turn again, because a process could end up using its reserved data in the next month.
                    let now = UtcDateTime::now();
                    let time_to_re_check = {
                        let mut time_to_recheck = now.date();
                        for _ in 0..months_to_wait {
                            time_to_recheck = time_to_recheck.start_of_next_month();
                        }
                        time_to_recheck
                    };
                    let duration = UtcDateTime::new(time_to_re_check, Time::MIDNIGHT) - now;
                    // FIXME: Time during suspend doesn't get counted
                    sleep(duration.try_into().unwrap()).await;
                }
            }
            Box::new(FileBackedAmountReservation {
                limiter: self.clone(),
                id,
            }) as Box<dyn AmountReservation>
        }
        .boxed()
    }
}

pub struct FileBackedAmountReservation<'a> {
    limiter: FileBackedAmountLimiter<'a>,
    id: &'a str,
}

impl AmountReservation for FileBackedAmountReservation<'_> {
    fn mark_complete(&self) -> BoxFuture<()> {
        async {
            let (file, mut data) = DataFile::open_and_read(self.limiter.path).await.unwrap();
            let item = data.queue.remove(self.id).unwrap();
            data.used_this_month += item.amount;
            file.write_and_close(&data).await.unwrap();
        }
        .boxed()
    }
}
