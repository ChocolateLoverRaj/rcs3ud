use std::time::Duration;

use tokio::time::sleep;

pub enum MaybeRetryable<E> {
    Retryable,
    NotRetryable(E),
}

pub trait KeepRetryingExt<T, E> {
    fn keep_retrying(&mut self, interval: Duration) -> impl Future<Output = Result<T, E>>;
}

impl<T, E, F: AsyncFnMut() -> Result<T, MaybeRetryable<E>>> KeepRetryingExt<T, E> for F {
    async fn keep_retrying(&mut self, interval: Duration) -> Result<T, E> {
        loop {
            match self().await {
                Ok(value) => break Ok(value),
                Err(MaybeRetryable::NotRetryable(e)) => break Err(e),
                Err(MaybeRetryable::Retryable) => {}
            };
            sleep(interval).await;
        }
    }
}
