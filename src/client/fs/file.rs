use std::{
    future::Future,
    io::{self, SeekFrom},
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf},
    runtime::Handle,
};
use tracing::{trace, warn};

use super::Metadata;
use crate::{
    client::{error::Error, rawsession::SftpResult, session::Extensions, RawSftpSession},
    protocol::StatusCode,
};

type StateFn<T> = Option<Pin<Box<dyn Future<Output = io::Result<T>> + Send + Sync + 'static>>>;

/// Default maximum read length for SFTP operations (32KB)
/// 
/// This value was reduced from the original 255KB to improve compatibility with
/// SolarWinds Serv-U servers, which have known issues with larger buffer sizes.
const MAX_READ_LENGTH: u64 = 32 * 1024;

/// Default maximum write length for SFTP operations (32KB)
/// 
/// This value was reduced from the original 255KB to match the read buffer size
/// and improve compatibility with SolarWinds Serv-U servers that can experience
/// buffer overflow errors with asymmetric read/write buffer sizes.
const MAX_WRITE_LENGTH: u64 = 32 * 1024;

/// Conservative fallback read limit for servers without limits@openssh.com extension (16KB)
/// 
/// Used when the server doesn't support the limits extension, providing extra
/// compatibility with older or non-compliant SFTP servers, particularly
/// SolarWinds Serv-U versions 15.3.2 and later which introduced stricter
/// buffer management.
const CONSERVATIVE_MAX_READ_LENGTH: u64 = 16 * 1024;

/// Conservative fallback write limit for servers without limits@openssh.com extension (16KB)
/// 
/// Used when the server doesn't support the limits extension, providing extra  
/// compatibility with older or non-compliant SFTP servers, particularly
/// SolarWinds Serv-U versions 15.3.2 and later which introduced stricter
/// buffer management.
const CONSERVATIVE_MAX_WRITE_LENGTH: u64 = 16 * 1024;

struct FileState {
    f_read: StateFn<Option<Vec<u8>>>,
    f_seek: StateFn<u64>,
    f_write: StateFn<usize>,
    f_flush: StateFn<()>,
    f_shutdown: StateFn<()>,
}

/// Provides high-level methods for interaction with a remote file.
///
/// In order to properly close the handle, [`shutdown`] on a file should be called.
/// Also implement [`AsyncSeek`] and other async i/o implementations.
///
/// # Weakness
/// Using [`SeekFrom::End`] is costly and time-consuming because we need to
/// request the actual file size from the remote server.
pub struct File {
    session: Arc<RawSftpSession>,
    handle: String,
    state: FileState,
    pos: u64,
    closed: bool,
    extensions: Arc<Extensions>,
}

impl File {
    pub(crate) fn new(
        session: Arc<RawSftpSession>,
        handle: String,
        extensions: Arc<Extensions>,
    ) -> Self {
        Self {
            session,
            handle,
            state: FileState {
                f_read: None,
                f_seek: None,
                f_write: None,
                f_flush: None,
                f_shutdown: None,
            },
            pos: 0,
            closed: false,
            extensions,
        }
    }

    /// Determines if we should use conservative limits for better Serv-U compatibility
    /// 
    /// This method checks if the server supports the `limits@openssh.com` extension.
    /// If not, it returns true to indicate that conservative buffer sizes should be used.
    /// 
    /// # SolarWinds Serv-U Compatibility
    /// 
    /// SolarWinds Serv-U servers, particularly versions 15.3.2 and later, have strict
    /// internal buffer management that can cause connection failures with larger buffer
    /// sizes. Using conservative limits helps prevent these errors:
    /// 
    /// - "Client has exceeded the server's internal buffers"
    /// - "Too many simultaneous client requests"
    /// - Connection reset errors during file transfers
    /// 
    /// # Returns
    /// 
    /// Returns `true` if conservative limits should be used (server lacks limits extension),
    /// `false` if the server's advertised limits should be respected.
    fn should_use_conservative_limits(&self) -> bool {
        // Use conservative limits if server doesn't support limits extension
        // This is particularly important for SolarWinds Serv-U compatibility
        self.extensions.limits.is_none()
    }

    /// Queries metadata about the remote file.
    pub async fn metadata(&self) -> SftpResult<Metadata> {
        Ok(self.session.fstat(self.handle.as_str()).await?.attrs)
    }

    /// Sets metadata for a remote file.
    pub async fn set_metadata(&self, metadata: Metadata) -> SftpResult<()> {
        self.session
            .fsetstat(self.handle.as_str(), metadata)
            .await
            .map(|_| ())
    }

    /// Attempts to sync all data.
    ///
    /// If the server does not support `fsync@openssh.com` sending the request will
    /// be omitted, but will still pseudo-successfully
    pub async fn sync_all(&self) -> SftpResult<()> {
        if !self.extensions.fsync {
            return Ok(());
        }

        self.session.fsync(self.handle.as_str()).await.map(|_| ())
    }
}

impl Drop for File {
    fn drop(&mut self) {
        if self.closed {
            return;
        }

        if let Ok(handle) = Handle::try_current() {
            let session = self.session.clone();
            let file_handle = self.handle.clone();

            handle.spawn(async move {
                let _ = session.close(file_handle).await;
            });
        }
    }
}

impl AsyncRead for File {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Safety check: Prevent concurrent read operations
        // The future in f_read should be polled to completion before creating a new one
        // This is enforced by Rust's borrow checker (&mut self), but we add explicit handling
        let poll = Pin::new(match self.state.f_read.as_mut() {
            Some(f) => {
                // Read operation already in progress, poll existing future
                trace!("Polling existing read operation");
                f
            },
            None => {
                // No read in progress, create new read operation
                let session = self.session.clone();
                let max_read_len = if let Some(limits) = &self.extensions.limits {
                    limits.read_len.unwrap_or(MAX_READ_LENGTH)
                } else if self.should_use_conservative_limits() {
                    CONSERVATIVE_MAX_READ_LENGTH
                } else {
                    MAX_READ_LENGTH
                } as usize;

                let file_handle = self.handle.clone();

                // Capture current file position - this ensures sequential reads
                let offset = self.pos;
                let len = if buf.remaining() > max_read_len {
                    max_read_len
                } else {
                    buf.remaining()
                };

                trace!(offset = offset, len = len, "Starting new read operation");

                self.state.f_read.get_or_insert(Box::pin(async move {
                    let result = session.read(file_handle, offset, len as u32).await;

                    match result {
                        Ok(data) => {
                            // Only treat explicit EOF status as end-of-file
                            // Empty data packets should not be assumed to be EOF
                            if data.data.is_empty() {
                                warn!(
                                    offset = offset,
                                    "Received empty data packet (not EOF status) - this may indicate a server issue"
                                );
                            }
                            Ok(Some(data.data))
                        }
                        Err(Error::Status(status)) if status.status_code == StatusCode::Eof => {
                            // Explicit EOF from server - this is the only reliable EOF indicator
                            trace!(offset = offset, "Reached end of file (explicit EOF status)");
                            Ok(None)
                        }
                        Err(e) => Err(io::Error::other(e)),
                    }
                }))
            }
        })
        .poll(cx);

        if poll.is_ready() {
            // Clear the future slot so a new read can be started
            self.state.f_read = None;
            trace!("Read operation completed, clearing future slot");
        }

        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(Some(data))) => {
                self.pos += data.len() as u64;
                buf.put_slice(&data[..]);
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl AsyncSeek for File {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        match self.state.f_seek {
            Some(_) => Err(io::Error::other(
                "other file operation is pending, call poll_complete before start_seek",
            )),
            None => {
                let session = self.session.clone();
                let file_handle = self.handle.clone();
                let cur_pos = self.pos as i64;

                self.state.f_seek = Some(Box::pin(async move {
                    let new_pos = match position {
                        SeekFrom::Start(pos) => pos as i64,
                        SeekFrom::Current(pos) => cur_pos + pos,
                        SeekFrom::End(pos) => {
                            let result =
                                session.fstat(file_handle).await.map_err(io::Error::other)?;

                            match result.attrs.size {
                                Some(size) => size as i64 + pos,
                                None => {
                                    return Err(io::Error::other("file size unknown"));
                                }
                            }
                        }
                    };

                    if new_pos < 0 {
                        return Err(io::Error::other(
                            "cannot move file pointer before the beginning",
                        ));
                    }

                    Ok(new_pos as u64)
                }));

                Ok(())
            }
        }
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        match self.state.f_seek.as_mut() {
            None => Poll::Ready(Ok(self.pos)),
            Some(f) => {
                self.pos = ready!(Pin::new(f).poll(cx))?;
                self.state.f_seek = None;
                Poll::Ready(Ok(self.pos))
            }
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let poll = Pin::new(match self.state.f_write.as_mut() {
            Some(f) => f,
            None => {
                let session = self.session.clone();
                let max_write_len = if let Some(limits) = &self.extensions.limits {
                    limits.write_len.unwrap_or(MAX_WRITE_LENGTH)
                } else if self.should_use_conservative_limits() {
                    CONSERVATIVE_MAX_WRITE_LENGTH
                } else {
                    MAX_WRITE_LENGTH
                } as usize;

                let file_handle = self.handle.clone();
                let data = buf.to_vec();

                let offset = self.pos;
                let len = if data.len() > max_write_len {
                    max_write_len
                } else {
                    data.len()
                };

                self.state.f_write.get_or_insert(Box::pin(async move {
                    session
                        .write(file_handle, offset, data[..len].to_vec())
                        .await
                        .map_err(io::Error::other)?;
                    Ok(len)
                }))
            }
        })
        .poll(cx);

        if poll.is_ready() {
            self.state.f_write = None;
        }

        if let Poll::Ready(Ok(len)) = poll {
            self.pos += len as u64;
        }

        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if !self.extensions.fsync {
            return Poll::Ready(Ok(()));
        }

        let poll = Pin::new(match self.state.f_flush.as_mut() {
            Some(f) => f,
            None => {
                let session = self.session.clone();
                let file_handle = self.handle.clone();

                self.state.f_flush.get_or_insert(Box::pin(async move {
                    session
                        .fsync(file_handle)
                        .await
                        .map(|_| ())
                        .map_err(io::Error::other)
                }))
            }
        })
        .poll(cx);

        if poll.is_ready() {
            self.state.f_flush = None;
        }

        poll
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let poll = Pin::new(match self.state.f_shutdown.as_mut() {
            Some(f) => f,
            None => {
                let session = self.session.clone();
                let file_handle = self.handle.clone();

                self.state.f_shutdown.get_or_insert(Box::pin(async move {
                    session.close(file_handle).await.map_err(io::Error::other)?;
                    Ok(())
                }))
            }
        })
        .poll(cx);

        if poll.is_ready() {
            self.state.f_shutdown = None;
            self.closed = true;
        }

        poll
    }
}
