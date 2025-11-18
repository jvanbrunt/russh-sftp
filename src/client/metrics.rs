//! Request metrics and connection health monitoring
//!
//! This module provides observability features for SFTP sessions including:
//! - Request latency tracking (P50, P95, P99)
//! - Retry rate monitoring
//! - Error rate by type
//! - Connection health status
//! - Active request counting

use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Connection health state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is being established
    Connecting,
    /// Connection is healthy and responsive
    Healthy,
    /// Connection is experiencing issues (high latency or errors)
    Degraded,
    /// Connection has been lost
    Disconnected,
}

/// Request metrics tracker
#[derive(Debug)]
pub struct Metrics {
    // Request counts
    total_requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    retried_requests: AtomicU64,

    // Active operations
    active_requests: AtomicU32,

    // Latency tracking (in milliseconds)
    latency_samples: RwLock<Vec<u64>>,
    max_latency_samples: usize,

    // Connection health
    connection_state: RwLock<ConnectionState>,
    last_successful_request: RwLock<Option<Instant>>,
    last_error: RwLock<Option<(Instant, String)>>,

    // Error tracking by type
    timeout_errors: AtomicU64,
    io_errors: AtomicU64,
    protocol_errors: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a new metrics tracker
    pub fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            retried_requests: AtomicU64::new(0),
            active_requests: AtomicU32::new(0),
            latency_samples: RwLock::new(Vec::with_capacity(1000)),
            max_latency_samples: 1000,
            connection_state: RwLock::new(ConnectionState::Connecting),
            last_successful_request: RwLock::new(None),
            last_error: RwLock::new(None),
            timeout_errors: AtomicU64::new(0),
            io_errors: AtomicU64::new(0),
            protocol_errors: AtomicU64::new(0),
        }
    }

    /// Record the start of a request
    pub fn record_request_start(&self) -> RequestHandle<'_> {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.active_requests.fetch_add(1, Ordering::Relaxed);
        RequestHandle {
            start_time: Instant::now(),
            metrics: self,
        }
    }

    /// Record a retry attempt
    pub fn record_retry(&self) {
        self.retried_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error by type
    pub async fn record_error(&self, error_type: &str, error_message: String) {
        self.failed_requests.fetch_add(1, Ordering::Relaxed);

        match error_type {
            "timeout" => self.timeout_errors.fetch_add(1, Ordering::Relaxed),
            "io" => self.io_errors.fetch_add(1, Ordering::Relaxed),
            "protocol" => self.protocol_errors.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };

        *self.last_error.write().await = Some((Instant::now(), error_message));

        // Update connection state based on error patterns
        self.update_connection_state().await;
    }

    /// Record latency for a completed request
    async fn record_latency(&self, duration: Duration) {
        let ms = duration.as_millis() as u64;

        let mut samples = self.latency_samples.write().await;
        samples.push(ms);

        // Keep only the most recent samples
        if samples.len() > self.max_latency_samples {
            samples.remove(0);
        }

        // Warn about slow requests (> 5 seconds)
        if ms > 5000 {
            tracing::warn!(
                latency_ms = ms,
                "Slow SFTP request detected (> 5 seconds)"
            );
        }
    }

    /// Get current connection state
    pub async fn connection_state(&self) -> ConnectionState {
        *self.connection_state.read().await
    }

    /// Set connection state
    pub async fn set_connection_state(&self, state: ConnectionState) {
        *self.connection_state.write().await = state;
    }

    /// Update connection state based on current metrics
    async fn update_connection_state(&self) {
        let current_state = *self.connection_state.read().await;

        // Don't auto-update if already disconnected
        if current_state == ConnectionState::Disconnected {
            return;
        }

        let total = self.total_requests.load(Ordering::Relaxed);
        let failed = self.failed_requests.load(Ordering::Relaxed);

        // If we have at least 10 requests, check error rate
        if total >= 10 {
            let error_rate = (failed as f64) / (total as f64);

            let new_state = if error_rate > 0.5 {
                // More than 50% errors = degraded
                ConnectionState::Degraded
            } else if error_rate > 0.1 {
                // More than 10% errors = degraded
                ConnectionState::Degraded
            } else {
                ConnectionState::Healthy
            };

            if new_state != current_state {
                tracing::info!(
                    old_state = ?current_state,
                    new_state = ?new_state,
                    error_rate = error_rate,
                    "Connection state changed"
                );
                *self.connection_state.write().await = new_state;
            }
        }
    }

    /// Get current statistics
    pub async fn snapshot(&self) -> MetricsSnapshot {
        let samples = self.latency_samples.read().await;
        let mut sorted = samples.clone();
        sorted.sort_unstable();

        let (p50, p95, p99) = if !sorted.is_empty() {
            let p50_idx = (sorted.len() as f64 * 0.50) as usize;
            let p95_idx = (sorted.len() as f64 * 0.95) as usize;
            let p99_idx = (sorted.len() as f64 * 0.99) as usize;

            (
                sorted.get(p50_idx).copied(),
                sorted.get(p95_idx).copied(),
                sorted.get(p99_idx).copied(),
            )
        } else {
            (None, None, None)
        };

        let total = self.total_requests.load(Ordering::Relaxed);
        let failed = self.failed_requests.load(Ordering::Relaxed);
        let success_rate = if total > 0 {
            ((total - failed) as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        MetricsSnapshot {
            total_requests: total,
            successful_requests: self.successful_requests.load(Ordering::Relaxed),
            failed_requests: failed,
            retried_requests: self.retried_requests.load(Ordering::Relaxed),
            active_requests: self.active_requests.load(Ordering::Relaxed),
            timeout_errors: self.timeout_errors.load(Ordering::Relaxed),
            io_errors: self.io_errors.load(Ordering::Relaxed),
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed),
            latency_p50_ms: p50,
            latency_p95_ms: p95,
            latency_p99_ms: p99,
            connection_state: *self.connection_state.read().await,
            success_rate,
        }
    }

    /// Reset all metrics (useful for testing)
    pub async fn reset(&self) {
        self.total_requests.store(0, Ordering::Relaxed);
        self.successful_requests.store(0, Ordering::Relaxed);
        self.failed_requests.store(0, Ordering::Relaxed);
        self.retried_requests.store(0, Ordering::Relaxed);
        self.active_requests.store(0, Ordering::Relaxed);
        self.timeout_errors.store(0, Ordering::Relaxed);
        self.io_errors.store(0, Ordering::Relaxed);
        self.protocol_errors.store(0, Ordering::Relaxed);
        self.latency_samples.write().await.clear();
        *self.connection_state.write().await = ConnectionState::Connecting;
        *self.last_successful_request.write().await = None;
        *self.last_error.write().await = None;
    }
}

/// Handle for tracking a single request's lifecycle
pub struct RequestHandle<'a> {
    start_time: Instant,
    metrics: &'a Metrics,
}

impl<'a> RequestHandle<'a> {
    /// Record successful completion
    pub async fn record_success(self) {
        let duration = self.start_time.elapsed();
        self.metrics.successful_requests.fetch_add(1, Ordering::Relaxed);
        self.metrics.active_requests.fetch_sub(1, Ordering::Relaxed);
        self.metrics.record_latency(duration).await;
        *self.metrics.last_successful_request.write().await = Some(Instant::now());

        // Update to healthy if we were connecting
        if *self.metrics.connection_state.read().await == ConnectionState::Connecting {
            *self.metrics.connection_state.write().await = ConnectionState::Healthy;
        }
    }

    /// Record failure
    pub fn record_failure(self) {
        self.metrics.active_requests.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Snapshot of current metrics
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub retried_requests: u64,
    pub active_requests: u32,
    pub timeout_errors: u64,
    pub io_errors: u64,
    pub protocol_errors: u64,
    pub latency_p50_ms: Option<u64>,
    pub latency_p95_ms: Option<u64>,
    pub latency_p99_ms: Option<u64>,
    pub connection_state: ConnectionState,
    pub success_rate: f64,
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SFTP Session Metrics:")?;
        writeln!(f, "  Connection State: {:?}", self.connection_state)?;
        writeln!(f, "  Total Requests: {}", self.total_requests)?;
        writeln!(f, "  Success Rate: {:.1}%", self.success_rate)?;
        writeln!(f, "  Active Requests: {}", self.active_requests)?;
        writeln!(f, "  Retried Requests: {}", self.retried_requests)?;
        writeln!(f, "  Errors:")?;
        writeln!(f, "    Timeout: {}", self.timeout_errors)?;
        writeln!(f, "    I/O: {}", self.io_errors)?;
        writeln!(f, "    Protocol: {}", self.protocol_errors)?;

        if let (Some(p50), Some(p95), Some(p99)) =
            (self.latency_p50_ms, self.latency_p95_ms, self.latency_p99_ms) {
            writeln!(f, "  Latency:")?;
            writeln!(f, "    P50: {}ms", p50)?;
            writeln!(f, "    P95: {}ms", p95)?;
            writeln!(f, "    P99: {}ms", p99)?;
        }

        Ok(())
    }
}
