# Russh SFTP
SFTP subsystem supported server and client for [Russh](https://github.com/warp-tech/russh) and more!

Crate can provide compatibility with anything that can provide the raw data stream in and out of the subsystem channel.\
Implemented according to [version 3 specifications](https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-02) (most popular).

The main idea of the project is to provide an implementation for interacting with the protocol at any level.

## Examples
- [Client example](https://github.com/AspectUnk/russh-sftp/blob/master/examples/client.rs)
- [Simple server](https://github.com/AspectUnk/russh-sftp/blob/master/examples/server.rs)

## What's ready?
- [x] Basic packets
- [x] Extended packets
- [x] Simplification for file attributes
- [x] Client side
- [x] Client example
- [x] Server side
- [x] Simple server example
- [x] Extension support: `limits@openssh.com`, `hardlink@openssh.com`, `fsync@openssh.com`, `statvfs@openssh.com`
- [ ] Full server example
- [ ] Unit tests
- [ ] Workflow

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