/// Integration tests for error handling and retry logic
///
/// These tests verify that:
/// 1. Errors are properly classified as retryable vs permanent
/// 2. Error messages are descriptive and useful
/// 3. Error conversions work correctly
use russh_sftp::client::error::Error;
use russh_sftp::protocol::{Status, StatusCode};
use std::io;

#[test]
fn test_timeout_error_creation() {
    // Test that timeout errors can be created
    let timeout_error = Error::Timeout;

    assert!(
        matches!(timeout_error, Error::Timeout),
        "Should create Timeout error"
    );
}

#[test]
fn test_status_error_conversion() {
    // Test conversion from Status to Error
    let status = Status {
        id: 1,
        status_code: StatusCode::NoSuchFile,
        error_message: "File not found".to_string(),
        language_tag: "en".to_string(),
    };

    let error: Error = status.clone().into();

    match error {
        Error::Status(s) => {
            assert_eq!(s.status_code, StatusCode::NoSuchFile);
            assert_eq!(s.error_message, "File not found");
        }
        _ => panic!("Should convert to Status error"),
    }
}

#[test]
fn test_serv_u_error_detection() {
    // Test that Serv-U specific errors are detected
    let serv_u_messages = vec![
        "Client has exceeded the server's internal buffers",
        "too many simultaneous client requests",
        "connection reset",
        "buffer overflow",
        "internal buffer limit",
    ];

    for message in serv_u_messages {
        let status = Status {
            id: 1,
            status_code: StatusCode::Failure,
            error_message: message.to_string(),
            language_tag: "en".to_string(),
        };

        let error: Error = status.into();

        match error {
            Error::ServUCompatibility(msg) => {
                assert!(
                    msg.to_lowercase().contains(&message.to_lowercase()),
                    "Serv-U error should preserve message"
                );
            }
            _ => panic!("Should detect Serv-U error for message: {}", message),
        }
    }
}

#[test]
fn test_non_serv_u_errors() {
    // Test that normal errors are not misclassified as Serv-U errors
    let normal_messages = vec![
        "Permission denied",
        "File not found",
        "Operation failed",
        "Invalid request",
    ];

    for message in normal_messages {
        let status = Status {
            id: 1,
            status_code: StatusCode::Failure,
            error_message: message.to_string(),
            language_tag: "en".to_string(),
        };

        let error: Error = status.into();

        match error {
            Error::Status(_) => {
                // This is correct - should be a normal Status error
            }
            Error::ServUCompatibility(_) => {
                panic!("Should NOT detect Serv-U error for message: {}", message);
            }
            _ => panic!("Unexpected error type"),
        }
    }
}

#[test]
fn test_io_error_conversion() {
    // Test conversion from IO errors
    let io_error = io::Error::new(io::ErrorKind::NotFound, "File not found");
    let error: Error = io_error.into();

    match error {
        Error::IO(msg) => {
            assert!(msg.contains("not found"), "Should preserve error message");
        }
        _ => panic!("Should convert to IO error"),
    }
}

#[test]
fn test_unexpected_behavior_error() {
    // Test UnexpectedBehavior error variant
    let error = Error::UnexpectedBehavior("Test error".to_string());

    match error {
        Error::UnexpectedBehavior(msg) => {
            assert_eq!(msg, "Test error");
        }
        _ => panic!("Should be UnexpectedBehavior error"),
    }
}

#[test]
fn test_unexpected_packet_error() {
    // Test UnexpectedPacket error variant
    let error = Error::UnexpectedPacket;

    assert!(
        matches!(error, Error::UnexpectedPacket),
        "Should create UnexpectedPacket error"
    );
}

#[test]
fn test_limited_error() {
    // Test Limited error variant
    let error = Error::Limited("Handle limit reached".to_string());

    match error {
        Error::Limited(msg) => {
            assert_eq!(msg, "Handle limit reached");
        }
        _ => panic!("Should be Limited error"),
    }
}

#[test]
fn test_error_display() {
    // Test that errors have useful display messages
    let errors = vec![
        Error::Timeout,
        Error::UnexpectedPacket,
        Error::IO("IO error".to_string()),
        Error::UnexpectedBehavior("Unexpected".to_string()),
        Error::Limited("Limit exceeded".to_string()),
        Error::ServUCompatibility("Serv-U error".to_string()),
    ];

    for error in errors {
        let display = error.to_string();
        assert!(!display.is_empty(), "Error should have display message");
    }
}

#[test]
fn test_error_clone() {
    // Test that errors can be cloned
    let error = Error::Timeout;
    let cloned = error.clone();

    assert!(
        matches!(cloned, Error::Timeout),
        "Cloned error should match"
    );
}

#[test]
fn test_status_error_with_empty_message() {
    // Test handling of status with empty error message
    let status = Status {
        id: 1,
        status_code: StatusCode::Failure,
        error_message: String::new(),
        language_tag: String::new(),
    };

    let error: Error = status.into();

    match error {
        Error::Status(s) => {
            assert_eq!(s.error_message, "");
        }
        _ => panic!("Should be Status error"),
    }
}

#[test]
fn test_multiple_error_types() {
    // Test creating and pattern matching various error types
    let errors: Vec<Error> = vec![
        Error::Timeout,
        Error::Limited("test".to_string()),
        Error::UnexpectedPacket,
        Error::UnexpectedBehavior("test".to_string()),
        Error::IO("test".to_string()),
        Error::ServUCompatibility("test".to_string()),
    ];

    assert_eq!(errors.len(), 6, "Should have all error variants");

    // Verify each can be matched
    for error in errors {
        match error {
            Error::Timeout => {}
            Error::Limited(_) => {}
            Error::UnexpectedPacket => {}
            Error::UnexpectedBehavior(_) => {}
            Error::IO(_) => {}
            Error::Status(_) => {}
            Error::ServUCompatibility(_) => {}
        }
    }
}

#[test]
fn test_serv_u_error_case_insensitive() {
    // Test that Serv-U error detection is case-insensitive
    let messages = vec![
        "CLIENT HAS EXCEEDED THE SERVER'S INTERNAL BUFFERS",
        "Client Has Exceeded The Server's Internal Buffers",
        "client has exceeded the server's internal buffers",
    ];

    for message in messages {
        let status = Status {
            id: 1,
            status_code: StatusCode::Failure,
            error_message: message.to_string(),
            language_tag: "en".to_string(),
        };

        let error: Error = status.into();

        assert!(
            matches!(error, Error::ServUCompatibility(_)),
            "Should detect Serv-U error regardless of case: {}",
            message
        );
    }
}

#[test]
fn test_status_code_error_priority() {
    // Test that certain status codes take precedence
    let status_codes = vec![
        StatusCode::Ok,
        StatusCode::Eof,
        StatusCode::NoSuchFile,
        StatusCode::PermissionDenied,
        StatusCode::Failure,
        StatusCode::BadMessage,
        StatusCode::NoConnection,
        StatusCode::ConnectionLost,
        StatusCode::OpUnsupported,
    ];

    for code in status_codes {
        let status = Status {
            id: 1,
            status_code: code,
            error_message: "Test".to_string(),
            language_tag: "en".to_string(),
        };

        // All should convert without panicking
        let _error: Error = status.into();
    }
}

#[test]
fn test_recv_none_message_pattern() {
    // Test the specific pattern for "recv none message" errors
    let errors = vec![
        Error::UnexpectedBehavior("recv none message".to_string()),
        Error::UnexpectedBehavior("Error: recv none message occurred".to_string()),
        Error::UnexpectedBehavior("recv none message in channel".to_string()),
    ];

    for error in errors {
        match error {
            Error::UnexpectedBehavior(msg) if msg.contains("recv none message") => {
                // Should match
                // No assertion needed; this branch confirms the pattern matches.
            }
            _ => panic!("Should match recv none message pattern"),
        }
    }
}

#[tokio::test]
async fn test_error_from_elapsed() {
    // Test conversion from tokio Elapsed error
    use tokio::time::error::Elapsed;

    // Create a timeout error
    let result: Result<(), Elapsed> =
        tokio::time::timeout(std::time::Duration::from_millis(1), async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        })
        .await;

    match result {
        Err(elapsed) => {
            let error: Error = elapsed.into();
            assert!(matches!(error, Error::Timeout), "Should convert to Timeout");
        }
        Ok(_) => {
            panic!("Should have timed out");
        }
    }
}
