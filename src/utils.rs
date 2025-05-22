use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::time::SystemTime;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{debug, error};

use crate::error::Error;

pub fn unix(time: SystemTime) -> u32 {
    DateTime::<Utc>::from(time).timestamp() as u32
}

pub async fn read_packet<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<Bytes, Error> {
    let length = match stream.read_u32().await {
        Ok(len) => {
            debug!(packet_length = len, "Reading packet");
            len
        },
        Err(e) => {
            error!(error = ?e, "Failed to read packet length");
            return Err(e.into());
        }
    };

    let mut buf = vec![0; length as usize];
    match stream.read_exact(&mut buf).await {
        Ok(_) => {
            debug!(bytes_read = length, "Packet read successfully");
            Ok(Bytes::from(buf))
        },
        Err(e) => {
            error!(error = ?e, packet_length = length, "Failed to read packet data");
            Err(e.into())
        }
    }
}
