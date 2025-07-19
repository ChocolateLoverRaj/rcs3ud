use std::time::Duration;

use sipper::{Straw, sipper};
use tokio::time::sleep;

pub enum MaybeRetryable<E, R> {
    Retryable(R),
    NotRetryable(E),
}

impl<E, R> MaybeRetryable<E, R> {
    pub fn map<F, O: FnOnce(E) -> F>(self, op: O) -> MaybeRetryable<F, R> {
        match self {
            Self::Retryable(e) => MaybeRetryable::Retryable(e),
            Self::NotRetryable(e) => MaybeRetryable::NotRetryable(op(e)),
        }
    }
}

pub trait KeepRetryingExt<T, E, R> {
    fn keep_retrying(&mut self, interval: Duration) -> impl Straw<T, R, E>;
}

impl<T, E, R, F: AsyncFnMut() -> Result<T, MaybeRetryable<E, R>>> KeepRetryingExt<T, E, R> for F {
    fn keep_retrying(&mut self, interval: Duration) -> impl Straw<T, R, E> {
        sipper(async move |mut sender| {
            loop {
                match self().await {
                    Ok(value) => break Ok(value),
                    Err(MaybeRetryable::NotRetryable(e)) => break Err(e),
                    Err(MaybeRetryable::Retryable(e)) => {
                        sender.send(e).await;
                    }
                };
                sleep(interval).await;
            }
        })
    }
}
