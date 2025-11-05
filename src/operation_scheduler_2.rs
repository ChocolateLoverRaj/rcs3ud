use async_trait::async_trait;

#[async_trait]
pub trait OperationScheduler2 {
    /// This function will be called before starting the upload, and can be used for time based upload scheduling.
    /// Uploading will start immediately after this function returns.
    async fn wait_until_upload_start(&mut self, bytes_to_upload: usize);
}

/// Implements `OperationScheduler2`, but doesn't wait at all
pub struct NoOpOperationScheduler2;
#[async_trait]
impl OperationScheduler2 for NoOpOperationScheduler2 {
    async fn wait_until_upload_start(&mut self, bytes_to_upload: usize) {
        let _ = bytes_to_upload;
    }
}
