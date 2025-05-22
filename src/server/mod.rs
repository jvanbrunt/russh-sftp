mod handler;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, info, trace, warn};

pub use self::handler::Handler;

use crate::{
    error::Error,
    protocol::{Packet, StatusCode},
    utils::read_packet,
};

macro_rules! into_wrap {
    ($id:expr, $handler:expr, $var:ident; $($arg:ident),*) => {
        match $handler.$var($($var.$arg),*).await {
            Err(err) => Packet::error($id, err.into()),
            Ok(packet) => packet.into(),
        }
    };
}

async fn process_request<H>(packet: Packet, handler: &mut H) -> Packet
where
    H: Handler + Send,
{
    let id = packet.get_request_id();
    debug!(request_id = id, packet_type = ?packet, "Processing SFTP request");

    match packet {
        Packet::Init(init) => into_wrap!(id, handler, init; version, extensions),
        Packet::Open(open) => into_wrap!(id, handler, open; id, filename, pflags, attrs),
        Packet::Close(close) => into_wrap!(id, handler, close; id, handle),
        Packet::Read(read) => into_wrap!(id, handler, read; id, handle, offset, len),
        Packet::Write(write) => into_wrap!(id, handler, write; id, handle, offset, data),
        Packet::Lstat(lstat) => into_wrap!(id, handler, lstat; id, path),
        Packet::Fstat(fstat) => into_wrap!(id, handler, fstat; id, handle),
        Packet::SetStat(setstat) => into_wrap!(id, handler, setstat; id, path, attrs),
        Packet::FSetStat(fsetstat) => into_wrap!(id, handler, fsetstat; id, handle, attrs),
        Packet::OpenDir(opendir) => into_wrap!(id, handler, opendir; id, path),
        Packet::ReadDir(readdir) => into_wrap!(id, handler, readdir; id, handle),
        Packet::Remove(remove) => into_wrap!(id, handler, remove; id, filename),
        Packet::MkDir(mkdir) => into_wrap!(id, handler, mkdir; id, path, attrs),
        Packet::RmDir(rmdir) => into_wrap!(id, handler, rmdir; id, path),
        Packet::RealPath(realpath) => into_wrap!(id, handler, realpath; id, path),
        Packet::Stat(stat) => into_wrap!(id, handler, stat; id, path),
        Packet::Rename(rename) => into_wrap!(id, handler, rename; id, oldpath, newpath),
        Packet::ReadLink(readlink) => into_wrap!(id, handler, readlink; id, path),
        Packet::Symlink(symlink) => into_wrap!(id, handler, symlink; id, linkpath, targetpath),
        Packet::Extended(extended) => into_wrap!(id, handler, extended; id, request, data),
        _ => Packet::error(0, StatusCode::BadMessage),
    }
}

async fn process_handler<H, S>(stream: &mut S, handler: &mut H) -> Result<(), Error>
where
    H: Handler + Send,
    S: AsyncRead + AsyncWrite + Unpin,
{
    trace!("Reading SFTP packet from stream");
    let mut bytes = read_packet(stream).await?;

    let response = match Packet::try_from(&mut bytes) {
        Ok(request) => process_request(request, handler).await,
        Err(e) => {
            error!(error = ?e, "Failed to parse SFTP packet");
            Packet::error(0, StatusCode::BadMessage)
        },
    };

    trace!(response_type = ?response, "Sending SFTP response");
    let packet = match Bytes::try_from(response) {
        Ok(p) => p,
        Err(e) => {
            error!(error = ?e, "Failed to serialize SFTP response");
            return Err(e);
        }
    };
    
    match stream.write_all(&packet).await {
        Ok(_) => trace!(bytes_written = packet.len(), "Wrote response to stream"),
        Err(e) => {
            error!(error = ?e, "Failed to write SFTP response to stream");
            return Err(e.into());
        }
    }
    
    if let Err(e) = stream.flush().await {
        error!(error = ?e, "Failed to flush stream");
        return Err(e.into());
    }

    Ok(())
}

/// Run processing stream as SFTP
pub async fn run<S, H>(mut stream: S, mut handler: H)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Handler + Send + 'static,
{
    info!("Starting SFTP server handler");
    tokio::spawn(async move {
        loop {
            match process_handler(&mut stream, &mut handler).await {
                Err(Error::UnexpectedEof) => {
                    info!("SFTP connection closed by client (EOF)");
                    break;
                },
                Err(err) => {
                    warn!(error = ?err, "Error processing SFTP request");
                },
                Ok(_) => trace!("Successfully processed SFTP request"),
            }
        }

        debug!("SFTP server stream ended");
    });
}
