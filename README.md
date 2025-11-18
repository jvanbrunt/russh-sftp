# Russh SFTP
SFTP subsystem supported server and client for [Russh](https://github.com/warp-tech/russh) and more!

Crate can provide compatibility with anything that can provide the raw data stream in and out of the subsystem channel.\
Implemented according to [version 3 specifications](https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-02) (most popular).

The main idea of the project is to provide an implementation for interacting with the protocol at any level.

## Features

### Reliability & Performance
- **Automatic Retry Logic**: Exponential backoff for transient failures (default: 3 retries, 100ms→200ms→400ms)
- **Timeout Protection**: 30-second packet read timeout prevents indefinite hangs
- **Connection Health Monitoring**: Real-time tracking of connection state (Healthy, Degraded, Disconnected)
- **Request Metrics**: Latency percentiles (P50, P95, P99), error rates, retry counts
- **Graceful Shutdown**: Wait for pending requests before closing session

### SolarWinds Serv-U Compatibility
- **Request Throttling**: Configurable delays between requests to prevent buffer overflow
- **Conservative Buffer Management**: Reduced buffer sizes for problematic servers
- **Handle Limiting**: Prevent resource exhaustion
- **Error Detection**: Automatic detection of Serv-U specific error patterns

### Testing
- 52+ comprehensive tests covering retry logic, EOF detection, timeouts, and error handling
- Benchmark suite for upload/download performance testing
- Docker-based integration test infrastructure

## Examples
- [Client example](https://github.com/AspectUnk/russh-sftp/blob/master/examples/client.rs)
- [Simple server](https://github.com/AspectUnk/russh-sftp/blob/master/examples/server.rs)

## Quick Start

### Basic Client Usage

```rust
use russh::client;
use russh_sftp::client::SftpSession;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to SSH server
    let config = client::Config::default();
    let mut session = client::connect(
        Arc::new(config),
        ("example.com", 22),
        YourSSHHandler {},
    ).await?;

    // Authenticate
    session.authenticate_password("username", "password").await?;

    // Open SFTP subsystem
    let channel = session.channel_open_session().await?;
    channel.request_subsystem(true, "sftp").await?;
    let sftp = SftpSession::new(channel.into_stream()).await?;

    // Upload a file
    let mut file = sftp.create("remote_file.txt").await?;
    file.write_all(b"Hello, SFTP!").await?;

    Ok(())
}
```

### With Serv-U Compatibility

```rust
// Enable all Serv-U compatibility features
sftp.enable_serv_u_throttling().await;
sftp.enable_serv_u_handle_limits().await;

// Or configure individually
sftp.set_request_throttling(10).await;  // 10ms between requests
sftp.set_max_concurrent_handles(10).await;  // Max 10 open files
```

### Monitoring Connection Health

```rust
use russh_sftp::client::ConnectionState;

// Check connection health
let state = sftp.connection_state().await;
match state {
    ConnectionState::Healthy => println!("Connection is healthy"),
    ConnectionState::Degraded => println!("Connection degraded, consider reconnecting"),
    ConnectionState::Disconnected => println!("Connection lost"),
    _ => {}
}

// Get detailed metrics
let metrics = sftp.metrics().await;
println!("Success rate: {:.1}%", metrics.success_rate);
println!("Active requests: {}", metrics.active_requests);
println!("Retry rate: {:.1}%",
    (metrics.retried_requests as f64 / metrics.total_requests as f64) * 100.0);

if let Some(p99) = metrics.latency_p99_ms {
    println!("P99 latency: {}ms", p99);
}
```

### Graceful Shutdown

```rust
// Wait up to 30 seconds for pending requests to complete
let graceful = sftp.graceful_shutdown(30).await?;
if graceful {
    println!("All requests completed successfully");
} else {
    println!("Some requests were interrupted");
}
```

## Reliability Features

### Automatic Retry Logic

The client automatically retries transient failures with exponential backoff:

```rust
// Configure retry behavior
sftp.set_max_retries(5).await;      // More aggressive retries
sftp.set_retry_delay(200).await;    // Start with 200ms delay

// Default: 3 retries with 100ms initial delay
// Retry sequence: 100ms → 200ms → 400ms
```

**Retryable Errors:**
- Timeout errors
- "recv none message" errors
- SolarWinds Serv-U buffer overflow errors

**Non-Retryable Errors:**
- Permission denied
- File not found
- Protocol violations

### Timeout Protection

All packet reads have a 30-second timeout to prevent indefinite hangs:

```rust
// Configure request timeout (default: 10 seconds)
sftp.set_timeout(30).await;  // 30 second timeout for responses
```

**Two-Layer Timeout System:**
1. **Request timeout** (10s default): Maximum time to wait for server response
2. **Packet read timeout** (30s): Maximum time for any packet I/O operation

### EOF Detection

Fixed premature EOF detection that caused download failures:

- **Before**: Empty data packets incorrectly treated as EOF
- **After**: Only explicit `StatusCode::Eof` triggers end-of-file

This ensures reliable downloads even with servers that send empty data packets.

## SolarWinds Serv-U Compatibility

Serv-U servers (especially v15.3.2+) have strict buffer management. Enable compatibility mode:

```rust
// Quick setup - enables all Serv-U features
sftp.enable_serv_u_throttling().await;
sftp.enable_serv_u_handle_limits().await;
```

### Request Throttling

Prevents "Client has exceeded the server's internal buffers" errors:

```rust
// Recommended values:
sftp.set_request_throttling(10).await;   // Standard (10ms)
sftp.set_request_throttling(25).await;   // Heavy load (25ms)
sftp.set_request_throttling(0).await;    // Disable throttling
```

### Handle Limiting

Prevents silent failures from handle exhaustion:

```rust
// Conservative limit for Serv-U
sftp.set_max_concurrent_handles(10).await;

// Higher throughput for modern servers
sftp.set_max_concurrent_handles(30).await;
```

### Error Detection

Automatic detection of Serv-U specific errors:
- "Client has exceeded the server's internal buffers"
- "Too many simultaneous client requests"
- Connection resets and buffer issues

These errors trigger automatic retry with backoff.

## Observability

### Connection States

The library tracks connection health in real-time:

- **Connecting**: Initial connection negotiation
- **Healthy**: < 10% error rate, normal operation
- **Degraded**: > 10% error rate, connection issues
- **Disconnected**: Connection closed

### Request Metrics

Track performance and reliability:

```rust
let metrics = sftp.metrics().await;

// Request counts
println!("Total: {}", metrics.total_requests);
println!("Success: {}", metrics.successful_requests);
println!("Failed: {}", metrics.failed_requests);
println!("Retried: {}", metrics.retried_requests);
println!("Active: {}", metrics.active_requests);

// Error breakdown
println!("Timeout errors: {}", metrics.timeout_errors);
println!("I/O errors: {}", metrics.io_errors);
println!("Protocol errors: {}", metrics.protocol_errors);

// Latency percentiles (in milliseconds)
if let Some(p50) = metrics.latency_p50_ms {
    println!("P50 latency: {}ms", p50);
}
if let Some(p95) = metrics.latency_p95_ms {
    println!("P95 latency: {}ms", p95);
}
if let Some(p99) = metrics.latency_p99_ms {
    println!("P99 latency: {}ms", p99);
}

// Overall health
println!("Success rate: {:.1}%", metrics.success_rate);
println!("Connection: {:?}", metrics.connection_state);
```

### Slow Request Warnings

Requests taking > 5 seconds trigger automatic warnings:

```
WARN russh_sftp::client::metrics: Slow SFTP request detected (> 5 seconds) {latency_ms=7342}
```

## Testing

Run the comprehensive test suite:

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test retry_logic      # Retry and exponential backoff tests
cargo test eof_detection    # EOF detection tests
cargo test packet_timeout   # Timeout tests
cargo test error_handling   # Error classification tests

# Run with logging
RUST_LOG=debug cargo test

# Run benchmarks (requires Docker SFTP server)
docker-compose up -d
cargo bench --bench upload_benchmark
```

## Troubleshooting

### Common Issues

**Slow downloads/uploads:**
```rust
// Check metrics to diagnose
let metrics = sftp.metrics().await;
if metrics.retry_rate > 0.2 {  // > 20% retry rate
    println!("High retry rate, network may be unstable");
}
```

**Connection degraded:**
```rust
// Monitor connection state
let state = sftp.connection_state().await;
if state == ConnectionState::Degraded {
    // Consider reconnecting or adjusting retry settings
    sftp.set_max_retries(5).await;
    sftp.set_retry_delay(500).await;
}
```

**Serv-U buffer errors:**
```rust
// Enable compatibility mode
sftp.enable_serv_u_throttling().await;
sftp.set_request_throttling(25).await;  // Increase if needed
```

**Timeout errors:**
```rust
// Increase timeout for slow networks
sftp.set_timeout(60).await;  // 60 seconds
```

## What's ready?
- [x] Basic packets
- [x] Extended packets
- [x] Simplification for file attributes
- [x] Client side with automatic retry logic
- [x] Connection health monitoring & metrics
- [x] Request metrics and observability
- [x] Graceful shutdown
- [x] Client example
- [x] Server side
- [x] Simple server example
- [x] Extension support: `limits@openssh.com`, `hardlink@openssh.com`, `fsync@openssh.com`, `statvfs@openssh.com`
- [x] SolarWinds Serv-U compatibility features
- [x] Comprehensive test suite (52+ tests)
- [ ] Full server example
- [ ] Workflow

## Planned Enhancements

The following features are planned for future releases. Contributions are welcome!

### High Priority

- **Enhanced Error Context**: Add request ID, timestamp, retry count, and operation name to all errors for better debugging
  - Current: Basic error types with messages
  - Planned: Structured error context for easier debugging and logging

- **Request Cancellation**: Ability to cancel in-flight requests via cancellation tokens
  - Useful for timeout scenarios or when user cancels an operation

- **Concurrent Request Limiting**: Configurable limit on total concurrent requests (not just file handles)
  - Current: Only file handle limiting
  - Planned: Overall request concurrency limiting to prevent overwhelming servers

- **Smart Retry Classification**: More intelligent retry decisions based on error type
  - Current: Basic classification (timeout, recv none message, Serv-U errors are retryable)
  - Planned: Network errors → immediate retry, server busy → longer backoff, permission errors → no retry

### Medium Priority

- **Connection Keepalive**: Periodic keepalive packets to detect and prevent connection timeout
- **Automatic Reconnection**: Transparent reconnection on connection loss with request replay
- **Circuit Breaker Pattern**: Fail-fast when server is consistently unresponsive to avoid cascading failures
- **Request Priority Queue**: Priority-based request scheduling (critical operations like open/close get priority)
- **Automatic Server Detection**: Detect server type (OpenSSH, Serv-U, etc.) and auto-configure compatibility settings
- **Request Batching**: Automatic batching of small consecutive requests for improved throughput

### Low Priority

- **Performance Optimizations**:
  - Zero-copy buffer management where possible
  - Connection pooling for multi-session applications
  - Request pipelining for better throughput
  - Request coalescing for small operations

- **Enhanced Benchmarks**:
  - Download performance benchmarks (not just upload)
  - Small file operations (1KB files)
  - Directory operation benchmarks
  - Concurrent session benchmarks
  - Different buffer size comparisons

- **Telemetry Integration**:
  - OpenTelemetry for distributed tracing
  - Prometheus metrics export
  - Optional structured logging with `tracing-subscriber` presets

- **Advanced Testing**:
  - Integration tests with multiple real SFTP servers (OpenSSH, Serv-U, etc.)
  - Compatibility test matrix
  - Chaos engineering tests (network failures, slow servers, etc.)

- **Memory Usage Monitoring**:
  - Track memory for long-lived sessions
  - Buffer pool size monitoring
  - Alert on potential memory leaks

- **Compression Support**: Optional compression for large file transfers over slow connections

### Want to Contribute?

We welcome contributions! If you'd like to implement any of these features:

1. Open an issue to discuss the implementation approach
2. Reference the feature from this list in your PR
3. Include comprehensive tests
4. Update documentation

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines (if you have one, or just link to GitHub issues).

## Logging and Debugging

The library uses the `tracing` crate for structured logging. Log levels include:

- **ERROR**: Serious issues requiring attention
- **WARN**: Unusual or unexpected conditions that don't cause failures
- **INFO**: Important lifecycle events
- **DEBUG**: Detailed protocol operation information
- **TRACE**: Low-level packet details and I/O operations

### Configuring Logging

Configure logging levels via the `RUST_LOG` environment variable:

```bash
# Set logging level for all components
RUST_LOG=debug cargo run --example client

# Set different levels for different components
RUST_LOG=russh_sftp=trace,info cargo run --example server
```

### Structured Logging

Logs include structured fields for easier analysis:

```
2023-05-22T15:42:09.123Z ERROR russh_sftp::client::error: I/O error in SFTP client {err=Os { code: 2, kind: NotFound, message: "No such file or directory" }}
```

For applications using this library, you can initialize tracing as shown in the examples:

```rust
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)  // Include module paths
        .init();
}
```

## Adopters

- [kty](https://github.com/grampelberg/kty) - The terminal for Kubernetes.

## Some words
Thanks to [@Eugeny](https://github.com/Eugeny) (author of the [Russh](https://github.com/warp-tech/russh)) for his prompt help and finalization of Russh API