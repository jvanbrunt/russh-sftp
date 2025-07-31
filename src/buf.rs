use bytes::Buf;

use crate::error::Error;

pub trait TryBuf: Buf {
    fn try_get_bytes(&mut self) -> Result<Vec<u8>, Error>;
    fn try_get_string(&mut self) -> Result<String, Error>;
}

impl<T: Buf> TryBuf for T {
    fn try_get_bytes(&mut self) -> Result<Vec<u8>, Error> {
        let len = self
            .try_get_u32()
            .map_err(|e| {
                tracing::error!(error = ?e, "Failed to read length prefix for byte array");
                Error::UnexpectedBehavior(e.to_string())
            })? as usize;
            
        // Additional safety check for reasonable buffer sizes
        const MAX_FIELD_SIZE: usize = 1024 * 1024; // 1MB per field
        if len > MAX_FIELD_SIZE {
            tracing::error!(requested_length = len, max_length = MAX_FIELD_SIZE, "Field size exceeds maximum allowed");
            return Err(Error::BadMessage(format!(
                "Field size {} exceeds maximum allowed size of {}",
                len, MAX_FIELD_SIZE
            )));
        }
            
        if self.remaining() < len {
            tracing::error!(
                remaining_bytes = self.remaining(),
                requested_bytes = len,
                "Insufficient bytes remaining in buffer for field"
            );
            return Err(Error::BadMessage(format!(
                "Insufficient bytes: need {}, have {}",
                len, self.remaining()
            )));
        }

        Ok(self.copy_to_bytes(len).to_vec())
    }

    fn try_get_string(&mut self) -> Result<String, Error> {
        let bytes = self.try_get_bytes()?;
        
        // Use from_utf8_lossy but log when we encounter invalid UTF-8
        let string = String::from_utf8_lossy(&bytes);
        if string.contains('\u{FFFD}') {
            tracing::warn!(
                original_length = bytes.len(),
                "String field contained invalid UTF-8, using lossy conversion"
            );
        }
        
        Ok(string.into())
    }
}
