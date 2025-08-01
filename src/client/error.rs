use std::io;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError as MpscSendError;
use tokio::sync::oneshot::error::RecvError as OneshotRecvError;
use tokio::time::error::Elapsed as TimeElapsed;

use crate::error;
use crate::protocol::Status;

/// Enum for client errors
#[derive(Debug, Clone, Error)]
pub enum Error {
    /// Contains an error status packet
    #[error("{}: {}", .0.status_code, .0.error_message)]
    Status(Status),
    /// Any errors related to I/O
    #[error("I/O: {0}")]
    IO(String),
    /// Time limit for receiving response packet exceeded
    #[error("Timeout")]
    Timeout,
    /// Occurs due to exceeding the limits set by the `limits@openssh.com` extension
    #[error("Limit exceeded: {0}")]
    Limited(String),
    /// Specific error for SolarWinds Serv-U server compatibility issues
    /// 
    /// This error type is triggered when the client detects error patterns
    /// commonly associated with SolarWinds Serv-U servers, particularly
    /// versions 15.3.2 and later which introduced stricter buffer management.
    /// 
    /// Common patterns that trigger this error:
    /// - "Client has exceeded the server's internal buffers"
    /// - "Too many simultaneous client requests"
    /// - "Connection reset" during file operations
    /// - "Non-RFC compliant SSH protocol version exchange"
    /// - "Buffer overflow" messages
    /// - "Internal buffer limit" exceeded
    #[error("Serv-U compatibility error: {0}")]
    ServUCompatibility(String),
    /// Occurs when an unexpected packet is sent
    #[error("Unexpected packet")]
    UnexpectedPacket,
    /// Occurs when unexpected server behavior differs from the protocol specifition
    #[error("{0}")]
    UnexpectedBehavior(String),
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        // Check for Serv-U specific error patterns
        if is_serv_u_error(&status.error_message) {
            Self::ServUCompatibility(status.error_message.clone())
        } else {
            Self::Status(status)
        }
    }
}

/// Detects SolarWinds Serv-U specific error patterns
/// 
/// This function analyzes error messages to identify patterns commonly associated
/// with SolarWinds Serv-U servers, particularly compatibility issues introduced
/// in versions 15.3.2 and later.
/// 
/// # SolarWinds Serv-U Error Patterns
/// 
/// The function checks for these known Serv-U error patterns:
/// 
/// ## Buffer Management Errors
/// - **"Client has exceeded the server's internal buffers"** - Most common Serv-U error,
///   typically caused by buffer sizes that exceed internal server limits
/// - **"Buffer overflow"** - General buffer-related errors
/// - **"Internal buffer limit"** - Server-side buffer constraints exceeded
/// 
/// ## Request Rate Limiting
/// - **"Too many simultaneous client requests"** - Server rejecting rapid requests,
///   can be resolved with request throttling
/// 
/// ## Connection Issues  
/// - **"Connection reset"** - Unexpected connection termination during operations,
///   often related to buffer or protocol issues
/// 
/// ## Protocol Compatibility
/// - **"Non-RFC compliant SSH protocol version exchange"** - Protocol negotiation
///   issues, particularly with older Java-based SFTP clients
/// 
/// # Parameters
/// 
/// - `error_message`: The error message string to analyze
/// 
/// # Returns
/// 
/// Returns `true` if the error message contains patterns associated with Serv-U
/// compatibility issues, `false` otherwise.
/// 
/// # Usage
/// 
/// This function is automatically called when converting `Status` errors to
/// determine if they should be classified as `ServUCompatibility` errors
/// rather than generic `Status` errors.
fn is_serv_u_error(error_message: &str) -> bool {
    let serv_u_patterns = [
        "Client has exceeded the server's internal buffers",
        "too many simultaneous client requests",  
        "connection reset",
        "non-RFC compliant SSH protocol version exchange",
        "buffer overflow",
        "internal buffer limit",
    ];
    
    let lower_message = error_message.to_lowercase();
    serv_u_patterns.iter().any(|pattern| lower_message.contains(&pattern.to_lowercase()))
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        tracing::error!(err = ?error, error_kind = ?error.kind(), "I/O error in SFTP client");

        Self::IO(error.to_string())
    }
}

impl<T> From<MpscSendError<T>> for Error {
    fn from(err: MpscSendError<T>) -> Self {
        tracing::error!(error = ?err, "MPSC channel send error in SFTP client");
        Self::UnexpectedBehavior(format!("SendError: {}", err))
    }
}

impl From<OneshotRecvError> for Error {
    fn from(err: OneshotRecvError) -> Self {
        tracing::error!(error = ?err, "Oneshot channel receive error in SFTP client");
        Self::UnexpectedBehavior(format!("RecvError: {}", err))
    }
}

impl From<TimeElapsed> for Error {
    fn from(elapsed: TimeElapsed) -> Self {
        tracing::warn!(error = ?elapsed, "Timeout in SFTP client operation");
        Self::Timeout
    }
}

impl From<error::Error> for Error {
    fn from(error: error::Error) -> Self {
        tracing::error!(error = ?error, error_message = %error.to_string(), "Core SFTP error converted to client error");
        Self::UnexpectedBehavior(error.to_string())
    }
}
