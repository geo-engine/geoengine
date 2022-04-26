use crate::error::Error;
use snafu::Snafu;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinError;

pub type Result<T> = std::result::Result<T, ExecutorError>;

#[derive(Debug, Clone, Snafu)]
pub enum ExecutorError {
    Submission { message: String },
    Panic,
    Cancelled,
}

impl From<JoinError> for ExecutorError {
    fn from(src: JoinError) -> Self {
        if src.is_cancelled() {
            ExecutorError::Cancelled
        } else {
            ExecutorError::Panic
        }
    }
}

impl<T> From<SendError<T>> for ExecutorError {
    fn from(e: SendError<T>) -> Self {
        Self::Submission {
            message: e.to_string(),
        }
    }
}

impl From<RecvError> for ExecutorError {
    fn from(e: RecvError) -> Self {
        Self::Submission {
            message: e.to_string(),
        }
    }
}

impl From<crate::error::Error> for ExecutorError {
    fn from(e: Error) -> Self {
        Self::Submission {
            message: e.to_string(),
        }
    }
}
