use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{
    error::Error,
    fs::{File, Metadata, ReadDir},
    rawsession::{Limits, SftpResult},
    RawSftpSession,
};
use crate::{
    extensions::{self, Statvfs},
    protocol::{FileAttributes, OpenFlags, StatusCode},
};

#[derive(Debug, Default)]
pub(crate) struct Extensions {
    pub hardlink: bool,
    pub fsync: bool,
    pub statvfs: bool,
    pub limits: Option<Arc<Limits>>,
}

/// High-level SFTP implementation for easy interaction with a remote file system.
/// Contains most methods similar to the native [filesystem](std::fs)
pub struct SftpSession {
    session: Arc<RawSftpSession>,
    extensions: Arc<Extensions>,
}

impl SftpSession {
    /// Creates a new session by initializing the protocol and extensions
    pub async fn new<S>(stream: S) -> SftpResult<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::new_opts(stream, None).await
    }

    /// Creates a new session with timeout opt before the first request
    pub async fn new_opts<S>(stream: S, timeout: Option<u64>) -> SftpResult<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut session = RawSftpSession::new(stream);

        // todo: for new options we need builder
        if let Some(timeout) = timeout {
            session.set_timeout(timeout).await;
        }

        let version = session.init().await?;
        let mut extensions = Extensions {
            hardlink: version
                .extensions
                .get(extensions::HARDLINK)
                .is_some_and(|e| e == "1"),
            fsync: version
                .extensions
                .get(extensions::FSYNC)
                .is_some_and(|e| e == "1"),
            statvfs: version
                .extensions
                .get(extensions::STATVFS)
                .is_some_and(|e| e == "2"),
            limits: None,
        };

        if version
            .extensions
            .get(extensions::LIMITS)
            .is_some_and(|e| e == "1")
        {
            let limits = session.limits().await?;
            let limits = Arc::new(Limits::from(limits));

            session.set_limits(limits.clone());
            extensions.limits = Some(limits);
        }

        Ok(Self {
            session: Arc::new(session),
            extensions: Arc::new(extensions),
        })
    }

    /// Set the maximum response time in seconds.
    /// Default: 10 seconds
    pub async fn set_timeout(&self, secs: u64) {
        self.session.set_timeout(secs).await;
    }

    /// Enable all SolarWinds Serv-U compatibility features
    /// 
    /// This convenience method enables all compatibility features designed to work
    /// around known issues with SolarWinds Serv-U servers, particularly versions
    /// 15.3.2 and later which introduced stricter buffer management.
    /// 
    /// # Features Enabled
    /// 
    /// This method enables the following compatibility features:
    /// 
    /// ## Request Throttling (10ms delay)
    /// - Adds a minimum 10ms delay between consecutive SFTP requests
    /// - Prevents "too many simultaneous client requests" errors
    /// - Reduces buffer overflow errors from rapid request sequences
    /// 
    /// ## Conservative Connection Limits (10 handles)
    /// - Limits concurrent file handles to 10 to prevent buffer exhaustion
    /// - Works around servers that don't properly advertise handle limits
    /// - Provides stability under high concurrent load
    /// 
    /// ## Automatic Buffer Size Management
    /// - Uses conservative buffer sizes (16KB) when server lacks limits extension
    /// - Prevents "Client has exceeded server's internal buffers" errors
    /// - Automatically applied based on server capabilities
    /// 
    /// # SolarWinds Serv-U Compatibility Issues
    /// 
    /// SolarWinds Serv-U servers, especially versions 15.3.2 and later, have several
    /// known compatibility issues that this method addresses:
    /// 
    /// - **Buffer Management**: Strict internal buffer limits that aren't properly
    ///   advertised to clients, leading to buffer overflow errors
    /// - **Request Rate Limiting**: Poor handling of rapid consecutive requests,
    ///   causing connection resets and "too many requests" errors  
    /// - **Handle Management**: Inadequate handle limit reporting, leading to
    ///   silent failures when limits are exceeded
    /// - **Protocol Changes**: Updates in 15.3.2+ that affect legacy client compatibility
    /// 
    /// # Performance Impact
    /// 
    /// Enabling these features introduces minimal performance overhead:
    /// - **Latency**: ~10ms additional latency per request (usually negligible)
    /// - **Throughput**: Reduced concurrent operations may slightly impact throughput
    /// - **Memory**: No significant memory overhead
    /// - **Reliability**: Significantly improved connection stability and error rates
    /// 
    /// For most applications, the reliability improvements far outweigh the minor
    /// performance impact.
    /// 
    /// # Usage Scenarios
    /// 
    /// **Recommended for:**
    /// - Any application connecting to SolarWinds Serv-U servers
    /// - Production environments where connection stability is critical
    /// - Applications that perform many concurrent file operations
    /// - Environments with older or heavily loaded Serv-U installations
    /// 
    /// **Consider alternatives for:**
    /// - High-throughput applications where latency is critical
    /// - Modern, well-configured SFTP servers that properly advertise limits
    /// - Servers that are known to work well with standard SFTP clients
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use russh_sftp::client::SftpSession;
    /// use tokio::net::TcpStream;
    /// 
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let stream = TcpStream::connect("serv-u-server:22").await?;
    /// let sftp = SftpSession::new(stream).await?;
    /// 
    /// // Enable all Serv-U compatibility features
    /// sftp.enable_serv_u_compatibility().await;
    /// 
    /// // Now safe to perform file operations with improved reliability
    /// let file = sftp.open("remote_file.txt").await?;
    /// # Ok(())
    /// # }
    /// ```
    /// 
    /// # Advanced Configuration
    /// 
    /// For fine-tuned control, use the individual methods instead:
    /// 
    /// ```rust
    /// # use russh_sftp::client::SftpSession;
    /// # use tokio::net::TcpStream;
    /// # async fn example(sftp: &SftpSession) {
    /// // Custom throttling - lighter for modern servers
    /// sftp.session.set_request_throttling(5).await;
    /// 
    /// // Custom handle limit - higher for well-configured servers  
    /// sftp.session.set_max_concurrent_handles(20).await;
    /// # }
    /// ```
    pub async fn enable_serv_u_compatibility(&self) {
        self.session.enable_serv_u_throttling().await;
        self.session.enable_serv_u_connection_limits().await;
    }

    /// Closes the inner channel stream.
    pub async fn close(&self) -> SftpResult<()> {
        self.session.close_session()
    }

    /// Attempts to open a file in read-only mode.
    pub async fn open<T: Into<String>>(&self, filename: T) -> SftpResult<File> {
        self.open_with_flags(filename, OpenFlags::READ).await
    }

    /// Opens a file in write-only mode.
    ///
    /// This function will create a file if it does not exist, and will truncate it if it does.
    pub async fn create<T: Into<String>>(&self, filename: T) -> SftpResult<File> {
        self.open_with_flags(
            filename,
            OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
        )
        .await
    }

    /// Attempts to open or create the file in the specified mode
    pub async fn open_with_flags<T: Into<String>>(
        &self,
        filename: T,
        flags: OpenFlags,
    ) -> SftpResult<File> {
        self.open_with_flags_and_attributes(filename, flags, FileAttributes::empty())
            .await
    }

    /// Attempts to open or create the file in the specified mode and with specified file attributes
    pub async fn open_with_flags_and_attributes<T: Into<String>>(
        &self,
        filename: T,
        flags: OpenFlags,
        attributes: FileAttributes,
    ) -> SftpResult<File> {
        let handle = self.session.open(filename, flags, attributes).await?.handle;
        Ok(File::new(
            self.session.clone(),
            handle,
            self.extensions.clone(),
        ))
    }

    /// Requests the remote party for the absolute from the relative path.
    pub async fn canonicalize<T: Into<String>>(&self, path: T) -> SftpResult<String> {
        let name = self.session.realpath(path).await?;
        match name.files.first() {
            Some(file) => Ok(file.filename.to_owned()),
            None => Err(Error::UnexpectedBehavior("no file".to_owned())),
        }
    }

    /// Creates a new empty directory.
    pub async fn create_dir<T: Into<String>>(&self, path: T) -> SftpResult<()> {
        self.session
            .mkdir(path, FileAttributes::empty())
            .await
            .map(|_| ())
    }

    /// Reads the contents of a file located at the specified path to the end.
    pub async fn read<P: Into<String>>(&self, path: P) -> SftpResult<Vec<u8>> {
        let path_str = path.into();
        let mut file = self.open(&path_str).await?;
        let mut buffer = Vec::new();

        file.read_to_end(&mut buffer).await.map_err(|err| {
            tracing::error!(err = ?err, path = ?path_str,
                "Failed to read file");
            err
        })?;

        Ok(buffer)
    }

    /// Writes the contents to a file whose path is specified.
    pub async fn write<P: Into<String>>(&self, path: P, data: &[u8]) -> SftpResult<()> {
        let mut file = self.open_with_flags(path, OpenFlags::WRITE).await?;
        file.write_all(data).await?;
        Ok(())
    }

    /// Checks a file or folder exists at the specified path
    pub async fn try_exists<P: Into<String>>(&self, path: P) -> SftpResult<bool> {
        match self.metadata(path).await {
            Ok(_) => Ok(true),
            Err(Error::Status(status)) if status.status_code == StatusCode::NoSuchFile => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Returns an iterator over the entries within a directory.
    pub async fn read_dir<P: Into<String>>(&self, path: P) -> SftpResult<ReadDir> {
        let mut files = vec![];
        let handle = self.session.opendir(path).await?.handle;

        loop {
            match self.session.readdir(handle.as_str()).await {
                Ok(name) => {
                    files = name
                        .files
                        .into_iter()
                        .map(|f| (f.filename, f.attrs))
                        .chain(files.into_iter())
                        .collect();
                }
                Err(Error::Status(status)) if status.status_code == StatusCode::Eof => break,
                Err(err) => return Err(err),
            }
        }

        self.session.close(handle).await?;

        Ok(ReadDir {
            entries: files.into(),
        })
    }

    /// Reads a symbolic link, returning the file that the link points to.
    pub async fn read_link<P: Into<String>>(&self, path: P) -> SftpResult<String> {
        let name = self.session.readlink(path).await?;
        match name.files.first() {
            Some(file) => Ok(file.filename.to_owned()),
            None => Err(Error::UnexpectedBehavior("no file".to_owned())),
        }
    }

    /// Removes the specified folder.
    pub async fn remove_dir<P: Into<String>>(&self, path: P) -> SftpResult<()> {
        self.session.rmdir(path).await.map(|_| ())
    }

    /// Removes the specified file.
    pub async fn remove_file<T: Into<String>>(&self, filename: T) -> SftpResult<()> {
        self.session.remove(filename).await.map(|_| ())
    }

    /// Rename a file or directory to a new name.
    pub async fn rename<O, N>(&self, oldpath: O, newpath: N) -> SftpResult<()>
    where
        O: Into<String>,
        N: Into<String>,
    {
        self.session.rename(oldpath, newpath).await.map(|_| ())
    }

    /// Creates a symlink of the specified target.
    pub async fn symlink<P, T>(&self, path: P, target: T) -> SftpResult<()>
    where
        P: Into<String>,
        T: Into<String>,
    {
        self.session.symlink(path, target).await.map(|_| ())
    }

    /// Queries metadata about the remote file.
    pub async fn metadata<P: Into<String>>(&self, path: P) -> SftpResult<Metadata> {
        Ok(self.session.stat(path).await?.attrs)
    }

    /// Sets metadata for a remote file.
    pub async fn set_metadata<P: Into<String>>(
        &self,
        path: P,
        metadata: Metadata,
    ) -> Result<(), Error> {
        self.session.setstat(path, metadata).await.map(|_| ())
    }

    pub async fn symlink_metadata<P: Into<String>>(&self, path: P) -> SftpResult<Metadata> {
        Ok(self.session.lstat(path).await?.attrs)
    }

    pub async fn hardlink<O, N>(&self, oldpath: O, newpath: N) -> SftpResult<bool>
    where
        O: Into<String>,
        N: Into<String>,
    {
        if !self.extensions.hardlink {
            return Ok(false);
        }

        self.session.hardlink(oldpath, newpath).await.map(|_| true)
    }

    /// Performs a statvfs on the remote file system path.
    /// Returns [`Ok(None)`] if the remote SFTP server does not support `statvfs@openssh.com` extension v2.
    pub async fn fs_info<P: Into<String>>(&self, path: P) -> SftpResult<Option<Statvfs>> {
        if !self.extensions.statvfs {
            return Ok(None);
        }

        self.session.statvfs(path).await.map(Some)
    }
}
