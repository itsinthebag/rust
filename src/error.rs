use std::io;
use std::result;
use thiserror::Error;

pub type Result<T> = result::Result<T, KvsError>;

#[derive(Debug, Error)]
pub enum KvsError {
    /// IO error.
    #[error("IO failure: {0}")]
    Io(#[from] io::Error),
    /// Serialization or deserialization error.
    #[error("serde failure: {0}")]
    Serde(#[from] serde_json::Error),
    /// Removing non-existent key error.
    #[error("Key not found")]
    KeyNotFound,
    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[error("Unexpected command type")]
    UnexpectedCommandType,
}
