use bytes::Bytes;
use flurry::HashMap;
use std::{
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, RwLock},
    time,
};
use tracing::{debug, error, info, trace, warn};

use super::{error::Error, run, Handler};
use crate::{
    de,
    extensions::{
        self, FsyncExtension, HardlinkExtension, LimitsExtension, Statvfs, StatvfsExtension,
    },
    protocol::{
        Attrs, Close, Data, Extended, ExtendedReply, FSetStat, FileAttributes, Fstat, Handle, Init,
        Lstat, MkDir, Name, Open, OpenDir, OpenFlags, Packet, Read, ReadDir, ReadLink, RealPath,
        Remove, Rename, RmDir, SetStat, Stat, Status, StatusCode, Symlink, Version, Write,
    },
};

pub type SftpResult<T> = Result<T, Error>;
type SharedRequests = HashMap<Option<u32>, mpsc::Sender<SftpResult<Packet>>>;

pub(crate) struct SessionInner {
    version: Option<u32>,
    requests: Arc<SharedRequests>,
}

impl SessionInner {
    pub async fn reply(&mut self, id: Option<u32>, packet: Packet) -> SftpResult<()> {
        trace!(packet_id = ?id, packet_type = ?packet.packet_type(), "Received packet");
        
        if let Some(sender) = self.requests.pin().remove(&id) {
            let validate = if id.is_some() && self.version.is_none() {
                warn!(packet_id = ?id, "Unexpected packet: received ID when version is none");
                Err(Error::UnexpectedPacket)
            } else if id.is_none() && self.version.is_some() {
                warn!("Unexpected behavior: duplicate version");
                Err(Error::UnexpectedBehavior("Duplicate version".to_owned()))
            } else {
                trace!(packet_id = ?id, "Packet validation successful");
                Ok(())
            };

            match sender.try_send(validate.clone().map(|_| packet)) {
                Ok(_) => {
                    debug!(packet_id = ?id, "Successfully processed packet");
                }
                Err(e) => {
                    error!(packet_id = ?id, error = %e, "Failed to send packet to recipient");
                    return Err(Error::UnexpectedBehavior(e.to_string()));
                }
            }

            return validate;
        }

        error!(packet_id = ?id, "Packet for unknown recipient");
        Err(Error::UnexpectedBehavior(format!(
            "Packet {:?} for unknown recipient",
            id
        )))
    }
}

#[cfg_attr(feature = "async-trait", async_trait::async_trait)]
impl Handler for SessionInner {
    type Error = Error;

    async fn version(&mut self, packet: Version) -> Result<(), Self::Error> {
        let version = packet.version;
        self.reply(None, packet.into()).await?;
        self.version = Some(version);
        Ok(())
    }

    async fn name(&mut self, name: Name) -> Result<(), Self::Error> {
        self.reply(Some(name.id), name.into()).await
    }

    async fn status(&mut self, status: Status) -> Result<(), Self::Error> {
        self.reply(Some(status.id), status.into()).await
    }

    async fn handle(&mut self, handle: Handle) -> Result<(), Self::Error> {
        self.reply(Some(handle.id), handle.into()).await
    }

    async fn data(&mut self, data: Data) -> Result<(), Self::Error> {
        self.reply(Some(data.id), data.into()).await
    }

    async fn attrs(&mut self, attrs: Attrs) -> Result<(), Self::Error> {
        self.reply(Some(attrs.id), attrs.into()).await
    }

    async fn extended_reply(&mut self, reply: ExtendedReply) -> Result<(), Self::Error> {
        self.reply(Some(reply.id), reply.into()).await
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    // todo: implement
    //pub packet_len: Option<u64>,
    pub read_len: Option<u64>,
    pub write_len: Option<u64>,
    pub open_handles: Option<u64>,
}

impl From<LimitsExtension> for Limits {
    fn from(limits: LimitsExtension) -> Self {
        Self {
            read_len: if limits.max_read_len > 0 {
                Some(limits.max_read_len)
            } else {
                None
            },
            write_len: if limits.max_write_len > 0 {
                Some(limits.max_write_len)
            } else {
                None
            },
            open_handles: if limits.max_open_handles > 0 {
                Some(limits.max_open_handles)
            } else {
                None
            },
        }
    }
}

pub(crate) struct Options {
    timeout: RwLock<u64>,
    limits: Arc<Limits>,
}

/// Implements raw work with the protocol in request-response format.
/// If the server returns a `Status` packet and it has the code Ok
/// then the packet is returned as Ok in other error cases
/// the packet is stored as Err.
pub struct RawSftpSession {
    tx: mpsc::UnboundedSender<Bytes>,
    requests: Arc<SharedRequests>,
    next_req_id: AtomicU32,
    handles: AtomicU64,
    options: Options,
}

macro_rules! into_with_status {
    ($result:ident, $packet:ident) => {
        match $result {
            Packet::$packet(p) => Ok(p),
            Packet::Status(p) => Err(p.into()),
            _ => Err(Error::UnexpectedPacket),
        }
    };
}

macro_rules! into_status {
    ($result:ident) => {
        match $result {
            Packet::Status(status) if status.status_code == StatusCode::Ok => Ok(status),
            Packet::Status(status) => Err(status.into()),
            _ => Err(Error::UnexpectedPacket),
        }
    };
}

impl RawSftpSession {
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let req_map = Arc::new(HashMap::new());
        let inner = SessionInner {
            version: None,
            requests: req_map.clone(),
        };

        Self {
            tx: run(stream, inner),
            requests: req_map,
            next_req_id: AtomicU32::new(1),
            handles: AtomicU64::new(0),
            options: Options {
                timeout: RwLock::new(10),
                limits: Arc::new(Limits::default()),
            },
        }
    }

    /// Set the maximum response time in seconds.
    /// Default: 10 seconds
    pub async fn set_timeout(&self, secs: u64) {
        *self.options.timeout.write().await = secs;
    }

    /// Setting limits. For the `limits@openssh.com` extension
    pub fn set_limits(&mut self, limits: Arc<Limits>) {
        self.options.limits = limits;
    }

    async fn send(&self, id: Option<u32>, packet: Packet) -> SftpResult<Packet> {
        debug!(request_id = ?id, packet_type = ?packet.packet_type(), "Sending packet");
        
        if self.tx.is_closed() {
            error!("Cannot send packet: session closed");
            return Err(Error::UnexpectedBehavior("session closed".into()));
        }

        let (tx, mut rx) = mpsc::channel(1);

        self.requests.pin().insert(id, tx);
        match self.tx.send(Bytes::try_from(packet)?) {
            Ok(_) => trace!(request_id = ?id, "Packet successfully queued for sending"),
            Err(e) => {
                error!(request_id = ?id, error = ?e, "Failed to send packet");
                return Err(e.into());
            }
        }

        let timeout = *self.options.timeout.read().await;
        trace!(request_id = ?id, timeout_secs = timeout, "Waiting for response");

        match time::timeout(Duration::from_secs(timeout), rx.recv()).await {
            Ok(Some(result)) => {
                trace!(request_id = ?id, response_type = ?result.as_ref().map(|p| p.packet_type()), "Response received");
                result
            },
            Ok(None) => {
                warn!(request_id = ?id, "Received None message instead of packet");
                self.requests.pin().remove(&id);
                Err(Error::UnexpectedBehavior("recv none message".into()))
            }
            Err(error) => {
                warn!(request_id = ?id, error = ?error, "Request timed out");
                self.requests.pin().remove(&id);
                Err(error.into())
            }
        }
    }

    fn use_next_id(&self) -> u32 {
        self.next_req_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Closes the inner channel stream. Called by [`Drop`]
    pub fn close_session(&self) -> SftpResult<()> {
        debug!("Closing SFTP session");
        
        if self.tx.is_closed() {
            trace!("Session already closed");
            return Ok(());
        }

        match self.tx.send(Bytes::new()) {
            Ok(_) => {
                info!("SFTP session closed successfully");
                Ok(())
            }
            Err(e) => {
                error!(error = ?e, "Failed to close SFTP session");
                Err(e.into())
            }
        }
    }

    pub async fn init(&self) -> SftpResult<Version> {
        debug!("Initializing SFTP session");
        
        let result = self.send(None, Init::default().into()).await?;
        
        if let Packet::Version(version) = result {
            info!(sftp_version = version.version, "SFTP session initialized successfully");
            Ok(version)
        } else {
            error!(actual_packet = ?result.packet_type(), "Unexpected packet received during initialization");
            Err(Error::UnexpectedPacket)
        }
    }

    pub async fn open<T: Into<String>>(
        &self,
        filename: T,
        flags: OpenFlags,
        attrs: FileAttributes,
    ) -> SftpResult<Handle> {
        let filename_str = filename.into();
        debug!(request_id = ?self.next_req_id.load(Ordering::SeqCst), filename = %filename_str, flags = ?flags, "Opening file");
        
        if self
            .options
            .limits
            .open_handles
            .is_some_and(|h| self.handles.load(Ordering::SeqCst) >= h)
        {
            warn!(
                current_handles = self.handles.load(Ordering::SeqCst),
                max_handles = ?self.options.limits.open_handles,
                "Handle limit reached"
            );
            return Err(Error::Limited("handle limit reached".to_owned()));
        }

        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                Open {
                    id,
                    filename: filename_str,
                    pflags: flags,
                    attrs,
                }
                .into(),
            )
            .await?;

        if let Packet::Handle(_) = result {
            let new_handle_count = self.handles.fetch_add(1, Ordering::SeqCst) + 1;
            debug!(request_id = id, handle_count = new_handle_count, "File opened successfully");
        }

        into_with_status!(result, Handle)
    }

    pub async fn close<H: Into<String>>(&self, handle: H) -> SftpResult<Status> {
        let handle_str = handle.into();
        debug!(handle = %handle_str, "Closing handle");
        
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                Close {
                    id,
                    handle: handle_str,
                }
                .into(),
            )
            .await?;

        if let Packet::Status(status) = &result {
            if status.status_code == StatusCode::Ok {
                let update_result = self
                    .handles
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |h| {
                        if h > 0 {
                            Some(h - 1)
                        } else {
                            None
                        }
                    });
                
                if update_result.is_err() {
                    warn!(request_id = id, "Attempt to close more handles than exist");
                } else {
                    let remaining_handles = self.handles.load(Ordering::SeqCst);
                    debug!(request_id = id, remaining_handles = remaining_handles, "Handle closed successfully");
                }
            } else {
                warn!(request_id = id, status_code = ?status.status_code, error = %status.error_message, "Failed to close handle");
            }
        }

        into_status!(result)
    }

    pub async fn read<H: Into<String>>(
        &self,
        handle: H,
        offset: u64,
        len: u32,
    ) -> SftpResult<Data> {
        let handle_str = handle.into();
        debug!(handle = %handle_str, offset = offset, length = len, "Reading from file");
        
        if self.options.limits.read_len.is_some_and(|r| len as u64 > r) {
            warn!(
                requested_length = len,
                max_length = ?self.options.limits.read_len,
                "Read limit exceeded"
            );
            return Err(Error::Limited("read limit reached".to_owned()));
        }

        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                Read {
                    id,
                    handle: handle_str,
                    offset,
                    len,
                }
                .into(),
            )
            .await?;

        match &result {
            Packet::Data(data) => {
                trace!(request_id = id, data_length = data.data.len(), "Read operation successful");
            }
            Packet::Status(status) => {
                warn!(request_id = id, status_code = ?status.status_code, error = %status.error_message, "Read operation failed");
            }
            _ => {
                error!(request_id = id, packet_type = ?result.packet_type(), "Unexpected packet received for read operation");
            }
        }

        into_with_status!(result, Data)
    }

    pub async fn write<H: Into<String>>(
        &self,
        handle: H,
        offset: u64,
        data: Vec<u8>,
    ) -> SftpResult<Status> {
        let handle_str = handle.into();
        let data_len = data.len();
        debug!(handle = %handle_str, offset = offset, data_length = data_len, "Writing to file");
        
        if self
            .options
            .limits
            .write_len
            .is_some_and(|w| data_len as u64 > w)
        {
            warn!(
                requested_length = data_len,
                max_length = ?self.options.limits.write_len,
                "Write limit exceeded"
            );
            return Err(Error::Limited("write limit reached".to_owned()));
        }

        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                Write {
                    id,
                    handle: handle_str,
                    offset,
                    data,
                }
                .into(),
            )
            .await?;

        if let Packet::Status(status) = &result {
            if status.status_code == StatusCode::Ok {
                debug!(request_id = id, bytes_written = data_len, "Write operation successful");
            } else {
                warn!(request_id = id, status_code = ?status.status_code, error = %status.error_message, "Write operation failed");
            }
        }

        into_status!(result)
    }

    pub async fn lstat<P: Into<String>>(&self, path: P) -> SftpResult<Attrs> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                Lstat {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Attrs)
    }

    pub async fn fstat<H: Into<String>>(&self, handle: H) -> SftpResult<Attrs> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                Fstat {
                    id,
                    handle: handle.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Attrs)
    }

    pub async fn setstat<P: Into<String>>(
        &self,
        path: P,
        attrs: FileAttributes,
    ) -> SftpResult<Status> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                SetStat {
                    id,
                    path: path.into(),
                    attrs,
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn fsetstat<H: Into<String>>(
        &self,
        handle: H,
        attrs: FileAttributes,
    ) -> SftpResult<Status> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                FSetStat {
                    id,
                    handle: handle.into(),
                    attrs,
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn opendir<P: Into<String>>(&self, path: P) -> SftpResult<Handle> {
        if self
            .options
            .limits
            .open_handles
            .is_some_and(|h| self.handles.load(Ordering::SeqCst) >= h)
        {
            return Err(Error::Limited("Handle limit reached".to_owned()));
        }

        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                OpenDir {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        if let Packet::Handle(_) = result {
            self.handles.fetch_add(1, Ordering::SeqCst);
        }

        into_with_status!(result, Handle)
    }

    pub async fn readdir<H: Into<String>>(&self, handle: H) -> SftpResult<Name> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                ReadDir {
                    id,
                    handle: handle.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Name)
    }

    pub async fn remove<T: Into<String>>(&self, filename: T) -> SftpResult<Status> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                Remove {
                    id,
                    filename: filename.into(),
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn mkdir<P: Into<String>>(
        &self,
        path: P,
        attrs: FileAttributes,
    ) -> SftpResult<Status> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                MkDir {
                    id,
                    path: path.into(),
                    attrs,
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn rmdir<P: Into<String>>(&self, path: P) -> SftpResult<Status> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                RmDir {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn realpath<P: Into<String>>(&self, path: P) -> SftpResult<Name> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                RealPath {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Name)
    }

    pub async fn stat<P: Into<String>>(&self, path: P) -> SftpResult<Attrs> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                Stat {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Attrs)
    }

    pub async fn rename<O, N>(&self, oldpath: O, newpath: N) -> SftpResult<Status>
    where
        O: Into<String>,
        N: Into<String>,
    {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                Rename {
                    id,
                    oldpath: oldpath.into(),
                    newpath: newpath.into(),
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn readlink<P: Into<String>>(&self, path: P) -> SftpResult<Name> {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                ReadLink {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Name)
    }

    pub async fn symlink<P, T>(&self, path: P, target: T) -> SftpResult<Status>
    where
        P: Into<String>,
        T: Into<String>,
    {
        let id = self.use_next_id();
        let result = self
            .send(
                Some(id),
                Symlink {
                    id,
                    linkpath: path.into(),
                    targetpath: target.into(),
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    /// Equivalent to `SSH_FXP_EXTENDED`. Allows protocol expansion.
    /// The extension can return any packet, so it's not specific
    pub async fn extended<R: Into<String>>(&self, request: R, data: Vec<u8>) -> SftpResult<Packet> {
        let id = self.use_next_id();
        self.send(
            Some(id),
            Extended {
                id,
                request: request.into(),
                data,
            }
            .into(),
        )
        .await
    }

    pub async fn limits(&self) -> SftpResult<LimitsExtension> {
        match self.extended(extensions::LIMITS, vec![]).await? {
            Packet::ExtendedReply(reply) => {
                Ok(de::from_bytes::<LimitsExtension>(&mut reply.data.into())?)
            }
            Packet::Status(status) if status.status_code != StatusCode::Ok => {
                Err(Error::Status(status))
            }
            _ => Err(Error::UnexpectedPacket),
        }
    }

    pub async fn hardlink<O, N>(&self, oldpath: O, newpath: N) -> SftpResult<Status>
    where
        O: Into<String>,
        N: Into<String>,
    {
        let result = self
            .extended(
                extensions::HARDLINK,
                HardlinkExtension {
                    oldpath: oldpath.into(),
                    newpath: newpath.into(),
                }
                .try_into()?,
            )
            .await?;

        into_status!(result)
    }

    pub async fn fsync<H: Into<String>>(&self, handle: H) -> SftpResult<Status> {
        let result = self
            .extended(
                extensions::FSYNC,
                FsyncExtension {
                    handle: handle.into(),
                }
                .try_into()?,
            )
            .await?;

        into_status!(result)
    }

    pub async fn statvfs<P>(&self, path: P) -> SftpResult<Statvfs>
    where
        P: Into<String>,
    {
        let result = self
            .extended(
                extensions::STATVFS,
                StatvfsExtension { path: path.into() }.try_into()?,
            )
            .await?;

        match result {
            Packet::ExtendedReply(reply) => Ok(de::from_bytes::<Statvfs>(&mut reply.data.into())?),
            Packet::Status(status) if status.status_code != StatusCode::Ok => {
                Err(Error::Status(status))
            }
            _ => Err(Error::UnexpectedPacket),
        }
    }
}

impl Drop for RawSftpSession {
    fn drop(&mut self) {
        trace!("Dropping RawSftpSession, closing session");
        if let Err(err) = self.close_session() {
            warn!(error = ?err, "Error during session cleanup in Drop");
        }
    }
}
