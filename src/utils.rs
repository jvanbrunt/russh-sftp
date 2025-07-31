use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::time::SystemTime;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{debug, error, warn};

use crate::error::Error;

/// Maximum allowed packet size (16MB) to prevent memory exhaustion attacks
const MAX_PACKET_SIZE: u32 = 16 * 1024 * 1024;

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

    // Validate packet length to prevent memory exhaustion
    if length == 0 {
        warn!(packet_length = length, "Received empty packet");
        return Err(Error::BadMessage("Empty packet received".to_owned()));
    }
    
    if length > MAX_PACKET_SIZE {
        error!(packet_length = length, max_size = MAX_PACKET_SIZE, "Packet size exceeds maximum allowed");
        return Err(Error::BadMessage(format!(
            "Packet size {} exceeds maximum allowed size of {}",
            length, MAX_PACKET_SIZE
        )));
    }

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
