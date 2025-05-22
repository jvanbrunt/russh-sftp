pub mod error;
pub mod fs;
mod handler;
pub mod rawsession;
mod session;

pub use handler::Handler;
pub use rawsession::RawSftpSession;
pub use session::SftpSession;

use bytes::Bytes;
use tokio::{
    io::{self, AsyncRead, AsyncWrite, AsyncWriteExt},
    select,
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::{error::Error, protocol::Packet, utils::read_packet};

macro_rules! into_wrap {
    ($handler:expr) => {
        match $handler.await {
            Err(error) => Err(error.into()),
            Ok(()) => Ok(()),
        }
    };
}

async fn execute_handler<H>(bytes: &mut Bytes, handler: &mut H) -> Result<(), error::Error>
where
    H: Handler + Send,
{
    trace!(bytes_len = bytes.len(), "Executing SFTP client handler on received packet");
    match Packet::try_from(bytes)? {
        Packet::Version(p) => into_wrap!(handler.version(p)),
        Packet::Status(p) => into_wrap!(handler.status(p)),
        Packet::Handle(p) => into_wrap!(handler.handle(p)),
        Packet::Data(p) => into_wrap!(handler.data(p)),
        Packet::Name(p) => into_wrap!(handler.name(p)),
        Packet::Attrs(p) => into_wrap!(handler.attrs(p)),
        Packet::ExtendedReply(p) => into_wrap!(handler.extended_reply(p)),
        packet => {
            warn!(packet_type = ?packet, "Received unexpected packet type in client handler");
            Err(error::Error::UnexpectedBehavior(
                "A packet was received that could not be processed.".to_owned(),
            ))
        },
    }
}

async fn process_handler<S, H>(stream: &mut S, handler: &mut H) -> Result<(), Error>
where
    S: AsyncRead + Unpin,
    H: Handler + Send,
{
    trace!("Reading packet from SFTP client stream");
    let mut bytes = read_packet(stream).await?;
    trace!(bytes_len = bytes.len(), "Processing packet in SFTP client handler");
    match execute_handler(&mut bytes, handler).await {
        Ok(()) => {
            trace!("Successfully processed SFTP client packet");
            Ok(())
        },
        Err(e) => {
            error!(error = ?e, "Error executing SFTP client handler");
            Err(e.into())
        }
    }
}

/// Run processing stream as SFTP client. Is a simple handler of incoming
/// and outgoing packets. Can be used for non-standard implementations
pub fn run<S, H>(stream: S, mut handler: H) -> mpsc::UnboundedSender<Bytes>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Handler + Send + 'static,
{
    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    let (mut rd, mut wr) = io::split(stream);

    let rc = CancellationToken::new();
    let wc = rc.clone();
    {
        tokio::spawn(async move {
            info!("Starting SFTP client read handler");
            loop {
                select! {
                    result = process_handler(&mut rd, &mut handler) => {
                        match result {
                            Err(Error::UnexpectedEof) => {
                                info!("SFTP client connection closed (EOF)");
                                break;
                            },
                            Err(err) => {
                                warn!(error = ?err, "Error processing SFTP client handler");
                            },
                            Ok(_) => {
                                trace!("Successfully processed SFTP client packet");
                            },
                        }
                    },
                    _ = rc.cancelled() => {
                        debug!("SFTP client read handler cancelled");
                        break;
                    },
                }
            }

            rc.cancel();

            debug!("read half of sftp stream ended");
        });
    }

    tokio::spawn(async move {
        info!("Starting SFTP client write handler");
        loop {
            select! {
                Some(data) = rx.recv() => {
                    if data.is_empty() {
                        debug!("Received empty data packet, shutting down SFTP client write stream");
                        if let Err(e) = wr.shutdown().await {
                            error!(error = ?e, "Error shutting down SFTP client write stream");
                        }
                        break;
                    }
                    
                    trace!(bytes_len = data.len(), "Writing data to SFTP client stream");
                    if let Err(e) = wr.write_all(&data[..]).await {
                        error!(error = ?e, "Error writing to SFTP client stream");
                        break;
                    }
                },
                _ = wc.cancelled() => {
                    debug!("SFTP client write handler cancelled");
                    break;
                },
            }
        }

        wc.cancel();
        debug!("SFTP client write stream ended");
    });

    tx
}
