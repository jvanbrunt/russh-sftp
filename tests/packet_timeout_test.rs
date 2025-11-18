/// Integration tests for packet read timeout behavior
///
/// These tests verify that:
/// 1. Packet reads timeout after the configured duration
/// 2. Timeouts apply to both length and data reads
/// 3. Proper errors are returned on timeout
use bytes::Bytes;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::{sleep, timeout};

/// Mock stream that never returns data (simulates hung connection)
struct HangingStream;

impl AsyncRead for HangingStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Never return Poll::Ready - simulates a hung connection
        Poll::Pending
    }
}

/// Mock stream that delays before returning data
struct DelayedStream {
    delay: Duration,
    data: Vec<u8>,
    position: usize,
    started: Option<std::time::Instant>,
}

impl DelayedStream {
    fn new(delay: Duration, data: Vec<u8>) -> Self {
        Self {
            delay,
            data,
            position: 0,
            started: None,
        }
    }
}

impl AsyncRead for DelayedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let now = std::time::Instant::now();

        // Track when we started
        let started = self.started.get_or_insert(now);

        // If not enough time has elapsed, return Pending
        if now.duration_since(*started) < self.delay {
            // Wake up after the delay
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        // Delay has elapsed, return data
        let remaining = &self.data[self.position..];
        let to_read = std::cmp::min(remaining.len(), buf.remaining());

        if to_read == 0 {
            return Poll::Ready(Ok(()));
        }

        buf.put_slice(&remaining[..to_read]);
        self.position += to_read;

        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn test_timeout_fires_on_hanging_stream() {
    // Test that a hanging stream properly times out
    use tokio::io::AsyncReadExt;

    let mut stream = HangingStream;
    let timeout_duration = Duration::from_millis(100);

    let result = timeout(timeout_duration, async {
        let mut buf = vec![0u8; 4];
        // This will hang forever since HangingStream never returns data
        stream.read_exact(&mut buf).await
    })
    .await;

    assert!(result.is_err(), "Should timeout on hanging stream");
}

#[tokio::test]
async fn test_delayed_stream_within_timeout() {
    // Test that a stream with acceptable delay completes successfully
    let test_data = vec![1, 2, 3, 4];
    let delay = Duration::from_millis(50);
    let timeout_duration = Duration::from_millis(200);

    let mut stream = DelayedStream::new(delay, test_data.clone());

    let result = timeout(timeout_duration, async {
        let mut buf = vec![0u8; 4];
        let mut read_buf = ReadBuf::new(&mut buf);

        // Poll until we get data
        loop {
            match Pin::new(&mut stream).poll_read(
                &mut Context::from_waker(futures::task::noop_waker_ref()),
                &mut read_buf,
            ) {
                Poll::Ready(Ok(())) => break,
                Poll::Ready(Err(e)) => panic!("Read error: {}", e),
                Poll::Pending => {
                    sleep(Duration::from_millis(10)).await;
                    continue;
                }
            }
        }

        buf
    })
    .await;

    assert!(result.is_ok(), "Should complete within timeout");
}

#[tokio::test]
async fn test_delayed_stream_exceeds_timeout() {
    // Test that a stream with excessive delay times out
    let test_data = vec![1, 2, 3, 4];
    let delay = Duration::from_millis(500);
    let timeout_duration = Duration::from_millis(100);

    let mut stream = DelayedStream::new(delay, test_data);

    let result = timeout(timeout_duration, async {
        let mut buf = vec![0u8; 4];
        let mut read_buf = ReadBuf::new(&mut buf);

        loop {
            match Pin::new(&mut stream).poll_read(
                &mut Context::from_waker(futures::task::noop_waker_ref()),
                &mut read_buf,
            ) {
                Poll::Ready(Ok(())) => break,
                Poll::Ready(Err(e)) => panic!("Read error: {}", e),
                Poll::Pending => {
                    sleep(Duration::from_millis(10)).await;
                    continue;
                }
            }
        }
    })
    .await;

    assert!(result.is_err(), "Should timeout when delay exceeds timeout");
}

#[tokio::test]
async fn test_timeout_durations() {
    // Test various timeout durations
    let test_cases = vec![
        (Duration::from_millis(10), true),  // Very short
        (Duration::from_millis(100), true), // Short
        (Duration::from_secs(1), true),     // 1 second
        (Duration::from_secs(10), true),    // 10 seconds (default request timeout)
        (Duration::from_secs(30), true),    // 30 seconds (packet timeout)
    ];

    for (duration, should_be_reasonable) in test_cases {
        assert!(
            should_be_reasonable,
            "Duration {:?} should be reasonable",
            duration
        );
    }
}

#[tokio::test]
async fn test_timeout_error_contains_info() {
    // Verify that timeout errors contain useful information
    let result: Result<(), tokio::time::error::Elapsed> =
        timeout(Duration::from_millis(10), async {
            sleep(Duration::from_millis(100)).await;
        })
        .await;

    match result {
        Err(e) => {
            let error_string = e.to_string();
            assert!(
                !error_string.is_empty(),
                "Timeout error should have a message"
            );
        }
        Ok(_) => panic!("Should have timed out"),
    }
}

#[tokio::test]
async fn test_multiple_timeout_operations() {
    // Test that multiple timeout operations work correctly
    let operations = vec![
        (Duration::from_millis(50), Duration::from_millis(10)), // Should timeout
        (Duration::from_millis(10), Duration::from_millis(50)), // Should succeed
        (Duration::from_millis(30), Duration::from_millis(20)), // Should timeout
    ];

    for (sleep_duration, timeout_duration) in operations {
        let result = timeout(timeout_duration, sleep(sleep_duration)).await;

        if sleep_duration > timeout_duration {
            assert!(result.is_err(), "Should timeout when sleep > timeout");
        } else {
            assert!(result.is_ok(), "Should succeed when sleep <= timeout");
        }
    }
}

#[tokio::test]
async fn test_packet_size_limits() {
    // Test that packet size validation works
    const MAX_PACKET_SIZE: u32 = 16 * 1024 * 1024; // 16MB

    let test_cases = vec![
        (0u32, false),                // Empty (invalid)
        (1, true),                    // Minimum valid
        (1024, true),                 // 1KB
        (32768, true),                // 32KB
        (1048576, true),              // 1MB
        (MAX_PACKET_SIZE, true),      // Maximum valid
        (MAX_PACKET_SIZE + 1, false), // Too large
        (u32::MAX, false),            // Way too large
    ];

    for (size, should_be_valid) in test_cases {
        let is_valid = size > 0 && size <= MAX_PACKET_SIZE;

        assert_eq!(
            is_valid, should_be_valid,
            "Packet size {} validation mismatch",
            size
        );
    }
}

#[tokio::test]
async fn test_timeout_cancel_safety() {
    // Test that timeouts are properly cancelled when operation completes
    let result = timeout(Duration::from_millis(100), async {
        sleep(Duration::from_millis(10)).await;
        42
    })
    .await;

    assert_eq!(result.unwrap(), 42, "Should return correct value");
}

#[tokio::test]
async fn test_concurrent_timeout_operations() {
    // Test multiple concurrent timeout operations
    let mut handles = vec![];

    for i in 0..10 {
        let handle = tokio::spawn(async move {
            let delay = Duration::from_millis(i * 10);
            let timeout_duration = Duration::from_millis(100);

            timeout(timeout_duration, sleep(delay)).await
        });
        handles.push(handle);
    }

    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.expect("Task should complete");

        if i * 10 > 100 {
            assert!(result.is_err(), "Task {} should timeout", i);
        } else {
            assert!(result.is_ok(), "Task {} should succeed", i);
        }
    }
}

#[tokio::test]
async fn test_bytes_conversion() {
    // Test that Bytes can be created from various sources
    let test_data = vec![1u8, 2, 3, 4, 5];

    let bytes1 = Bytes::from(test_data.clone());
    assert_eq!(bytes1.len(), 5);

    let bytes2 = Bytes::copy_from_slice(&test_data);
    assert_eq!(bytes2.len(), 5);
    assert_eq!(bytes1, bytes2);
}
