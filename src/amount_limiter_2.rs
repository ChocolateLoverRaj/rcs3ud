use async_trait::async_trait;

#[async_trait]
pub trait AmountLimiter2 {
    type ReserveError;
    /// Reserve a certain amount of data to be used by the caller.
    /// The id can be used to check if the data was already reserved for this upload.
    async fn reserve(&mut self, id: &str, amount: usize) -> Result<(), Self::ReserveError>;

    type MarkUsedError;
    /// Mark data as used.
    async fn mark_used(&mut self, id: &str, amount: usize) -> Result<(), Self::MarkUsedError>;
}

/// Implements `AmountLimiter2`, but doesn't limit anything
#[derive(Debug)]
pub struct NoOpAmountLimiter2;

#[async_trait]
impl AmountLimiter2 for NoOpAmountLimiter2 {
    type ReserveError = ();
    async fn reserve(&mut self, id: &str, amount: usize) -> Result<(), Self::ReserveError> {
        let _ = id;
        let _ = amount;
        Ok(())
    }

    type MarkUsedError = ();
    async fn mark_used(&mut self, id: &str, amount: usize) -> Result<(), Self::MarkUsedError> {
        let _ = id;
        let _ = amount;
        Ok(())
    }
}
