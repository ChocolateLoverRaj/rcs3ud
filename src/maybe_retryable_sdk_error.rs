use aws_smithy_runtime_api::{client::result::SdkError, http::Response};

use crate::retry::MaybeRetryable;

pub trait IntoMaybeRetryable<E> {
    fn into_maybe_retryable(self) -> MaybeRetryable<E, E>;
}

impl<E> IntoMaybeRetryable<SdkError<E, Response>> for SdkError<E, Response> {
    fn into_maybe_retryable(self) -> MaybeRetryable<SdkError<E, Response>, SdkError<E, Response>> {
        match &self {
            SdkError::DispatchFailure(_)
            | SdkError::TimeoutError(_)
            | SdkError::ResponseError(_) => MaybeRetryable::Retryable(self),
            SdkError::ServiceError(service_error) => {
                if service_error.raw().status().is_server_error() {
                    MaybeRetryable::Retryable(self)
                } else {
                    MaybeRetryable::NotRetryable(self)
                }
            }
            SdkError::ConstructionFailure(_) | _ => MaybeRetryable::NotRetryable(self),
        }
    }
}
