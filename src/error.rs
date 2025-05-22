use bytes::TryGetError;
use std::{fmt, io};
use thiserror::Error;

use crate::client;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("I/O: {0}")]
    IO(String),
    #[error("Unexpected EOF on stream")]
    UnexpectedEof,
    #[error("Bad message: {0}")]
    BadMessage(String),
    #[error("Client error. ({0})")]
    Client(String),
    #[error("Unexpected behavior: {0}")]
    UnexpectedBehavior(String),
}

impl From<client::error::Error> for Error {
    fn from(error: client::error::Error) -> Self {
        tracing::warn!(error = ?error, error_message = %error.to_string(), "Client error converted to core error");
        Self::Client(error.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        let kind = err.kind();
        let msg = err.into_inner().map_or("".to_string(), |m| format!("{m}"));
        
        match kind {
            io::ErrorKind::UnexpectedEof => {
                tracing::debug!(error_kind = ?kind, "EOF received on SFTP stream");
                Self::UnexpectedEof
            },
            io::ErrorKind::Other if msg == "EOF" => {
                tracing::debug!(error_kind = ?kind, message = %msg, "EOF message received on SFTP stream");
                Self::UnexpectedEof
            },
            e => {
                tracing::error!(error_kind = ?kind, io_error = ?e, message = %msg, "I/O error in SFTP core");
                Self::IO(e.to_string())
            },
        }
    }
}

impl From<TryGetError> for Error {
    fn from(err: TryGetError) -> Self {
        tracing::error!(
            available_bytes = err.available, 
            requested_bytes = err.requested, 
            "Buffer underflow when parsing SFTP message"
        );
        Self::BadMessage(format!(
            "only {} bytes remaining, but {} requested",
            err.available, err.requested
        ))
    }
}

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        let message = msg.to_string();
        tracing::error!(serialization_error = %message, "SFTP serialization error");
        Self::BadMessage(message)
    }
}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        let message = msg.to_string();
        tracing::error!(deserialization_error = %message, "SFTP deserialization error");
        Self::BadMessage(message)
    }
}
