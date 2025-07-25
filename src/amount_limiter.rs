use futures::future::BoxFuture;
use sipper::FutureExt;

pub trait AmountLimiter {
    /// This function is called before uploading or downloading.
    /// When this function resolves, it says, "Ok, you can upload/download now".
    fn reserve<'a>(
        &'a self,
        len: usize,
        id: &'a str,
    ) -> BoxFuture<'a, Box<dyn AmountReservation + 'a>>;
}

pub trait AmountReservation: Send {
    /// This function is called after uploading or downloading.
    /// This function is used to clean up any data from [`LenLimiter::reserve`].
    /// This function will only called once.
    fn mark_complete(&self) -> BoxFuture<()>;
}

pub struct UnlimitedAmountLimiter;
impl AmountLimiter for UnlimitedAmountLimiter {
    fn reserve<'a>(
        &'a self,
        _len: usize,
        _id: &'a str,
    ) -> BoxFuture<'a, Box<dyn AmountReservation + 'a>> {
        std::future::ready(Box::new(UnlimitedAmountReservation) as Box<dyn AmountReservation>)
            .boxed()
    }
}

pub struct UnlimitedAmountReservation;
impl AmountReservation for UnlimitedAmountReservation {
    fn mark_complete(&self) -> BoxFuture<()> {
        std::future::ready(()).boxed()
    }
}
