use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::time;
use tracing::{debug, error, warn};

use crate::error::Error;

/// Maximum allowed packet size (16MB) to prevent memory exhaustion attacks
const MAX_PACKET_SIZE: u32 = 16 * 1024 * 1024;

/// Default timeout for packet read operations (30 seconds)
///
/// This timeout is applied to both reading the packet length and the packet data
/// to prevent indefinite hangs if the connection drops mid-packet. Set higher than
/// the typical request timeout to accommodate large packets.
const PACKET_READ_TIMEOUT_SECS: u64 = 30;

pub fn unix(time: SystemTime) -> u32 {
    DateTime::<Utc>::from(time).timestamp() as u32
}

pub async fn read_packet<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<Bytes, Error> {
    let timeout_duration = Duration::from_secs(PACKET_READ_TIMEOUT_SECS);

    // Read packet length with timeout
    let length = match time::timeout(timeout_duration, stream.read_u32()).await {
        Ok(Ok(len)) => {
            debug!(packet_length = len, "Reading packet");
            len
        },
        Ok(Err(e)) => {
            error!(error = ?e, "Failed to read packet length");
            return Err(e.into());
        },
        Err(_) => {
            error!(timeout_secs = PACKET_READ_TIMEOUT_SECS, "Timeout reading packet length");
            return Err(Error::IO(format!(
                "Timeout reading packet length after {} seconds",
                PACKET_READ_TIMEOUT_SECS
            )));
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

    // Read packet data with timeout
    let mut buf = vec![0; length as usize];
    match time::timeout(timeout_duration, stream.read_exact(&mut buf)).await {
        Ok(Ok(_)) => {
            debug!(bytes_read = length, "Packet read successfully");
            Ok(Bytes::from(buf))
        },
        Ok(Err(e)) => {
            error!(error = ?e, packet_length = length, "Failed to read packet data");
            Err(e.into())
        },
        Err(_) => {
            error!(
                timeout_secs = PACKET_READ_TIMEOUT_SECS,
                packet_length = length,
                "Timeout reading packet data"
            );
            Err(Error::IO(format!(
                "Timeout reading {} byte packet after {} seconds",
                length, PACKET_READ_TIMEOUT_SECS
            )))
        }
    }
}
