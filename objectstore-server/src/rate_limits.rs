use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::Stream;
use objectstore_service::PayloadStream;
use objectstore_service::id::ObjectContext;
use objectstore_types::scope::Scopes;
use serde::{Deserialize, Serialize};

/// Rate limits for objectstore.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct RateLimits {
    /// Limits the number of requests per second per service instance.
    pub throughput: ThroughputLimits,
    /// Limits the concurrent bandwidth per service instance.
    pub bandwidth: BandwidthLimits,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ThroughputLimits {
    /// The overall maximum number of requests per second per service instance.
    ///
    /// Defaults to `None`, meaning no global rate limit is enforced.
    pub global_rps: Option<u32>,

    /// The maximum burst for each rate limit.
    ///
    /// Defaults to `0`, meaning no bursting is allowed. If set to a value greater than `0`,
    /// short spikes above the rate limit are allowed up to the burst size.
    pub burst: u32,

    /// The maximum percentage of the global rate limit that can be used by any usecase.
    ///
    /// Value from `0` to `100`. Defaults to `None`, meaning no per-usecase limit is enforced.
    pub usecase_pct: Option<u8>,

    /// The maximum percentage of the global rate limit that can be used by any scope.
    ///
    /// This treats each full scope separately and applies across all use cases:
    ///  - Two requests with exact same scopes count against the same limit regardless of use case.
    ///  - Two requests that share the same top scope but differ in inner scopes count separately.
    ///
    /// Value from `0` to `100`. Defaults to `None`, meaning no per-scope limit is enforced.
    pub scope_pct: Option<u8>,

    /// Overrides for specific usecases and scopes.
    pub rules: Vec<ThroughputRule>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ThroughputRule {
    /// Optional usecase to match.
    ///
    /// If `None`, matches any usecase.
    pub usecase: Option<String>,

    /// Scopes to match.
    ///
    /// If empty, matches any scopes. Additional scopes in the context are ignored, so a rule
    /// matches if all of the specified scopes are present in the request with matching values.
    pub scopes: Vec<(String, String)>,

    /// The rate limit to apply when this rule matches.
    ///
    /// If both a rate and pct are specified, the more restrictive limit applies.
    /// Should be greater than `0`. To block traffic entirely, use killswitches instead.
    pub rps: Option<u32>,

    /// The percentage of the global rate limit to apply when this rule matches.
    ///
    /// If both a rate and pct are specified, the more restrictive limit applies.
    /// Should be greater than `0`. To block traffic entirely, use killswitches instead.
    pub pct: Option<u8>,
}

impl ThroughputRule {
    /// Returns `true` if this rule matches the given context.
    pub fn matches(&self, context: &ObjectContext) -> bool {
        if let Some(ref rule_usecase) = self.usecase
            && rule_usecase != &context.usecase
        {
            return false;
        }

        for (scope_name, scope_value) in &self.scopes {
            match context.scopes.get_value(scope_name) {
                Some(value) if value == scope_value => (),
                _ => return false,
            }
        }

        true
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct BandwidthLimits {
    /// The maximum bandwidth (in bytes per second) per service instance, applied independently
    /// to ingress and egress.
    ///
    /// Defaults to `None`, meaning no global bandwidth limit is enforced.
    pub global_bps: Option<u64>,
}

#[derive(Debug)]
pub struct RateLimiter {
    bandwidth: BandwidthRateLimiter,
    throughput: ThroughputRateLimiter,
}

impl RateLimiter {
    pub fn new(config: RateLimits) -> Self {
        Self {
            bandwidth: BandwidthRateLimiter::new(config.bandwidth),
            throughput: ThroughputRateLimiter::new(config.throughput),
        }
    }

    /// Starts background tasks for rate limiting (e.g. the bandwidth EWMA estimator).
    pub fn start(&self) {
        self.bandwidth.start();
    }

    /// Checks if the given context is within the rate limits.
    ///
    /// Returns `true` if the context is within the rate limits, `false` otherwise.
    pub fn check(&self, context: &ObjectContext) -> bool {
        self.throughput.check(context) && self.bandwidth.check()
    }

    /// Returns a reference to the shared ingress bytes accumulator, used for bandwidth-based rate-limiting.
    pub fn ingress_accumulator(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.bandwidth.ingress_accumulator)
    }

    /// Returns a reference to the shared egress bytes accumulator, used for bandwidth-based rate-limiting.
    pub fn egress_accumulator(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.bandwidth.egress_accumulator)
    }
}

#[derive(Debug)]
struct BandwidthRateLimiter {
    config: BandwidthLimits,
    /// Accumulator incremented for ingress (upload) bandwidth.
    ingress_accumulator: Arc<AtomicU64>,
    /// Accumulator incremented for egress (download) bandwidth.
    egress_accumulator: Arc<AtomicU64>,
    /// An estimate of ingress bandwidth in bytes per second.
    ingress_estimate: Arc<AtomicU64>,
    /// An estimate of egress bandwidth in bytes per second.
    egress_estimate: Arc<AtomicU64>,
}

impl BandwidthRateLimiter {
    fn new(config: BandwidthLimits) -> Self {
        Self {
            config,
            ingress_accumulator: Arc::new(AtomicU64::new(0)),
            egress_accumulator: Arc::new(AtomicU64::new(0)),
            ingress_estimate: Arc::new(AtomicU64::new(0)),
            egress_estimate: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Spawns the background EWMA estimator task.
    fn start(&self) {
        let ingress_acc = Arc::clone(&self.ingress_accumulator);
        let egress_acc = Arc::clone(&self.egress_accumulator);
        let ingress_est = Arc::clone(&self.ingress_estimate);
        let egress_est = Arc::clone(&self.egress_estimate);
        tokio::task::spawn(async move {
            Self::estimator(ingress_acc, egress_acc, ingress_est, egress_est).await;
        });
    }

    /// Estimates the current bandwidth utilization using an exponentially weighted moving average.
    ///
    /// Computes independent EWMAs for ingress and egress, emitting separate metrics for each.
    async fn estimator(
        ingress_accumulator: Arc<AtomicU64>,
        egress_accumulator: Arc<AtomicU64>,
        ingress_estimate: Arc<AtomicU64>,
        egress_estimate: Arc<AtomicU64>,
    ) {
        const TICK: Duration = Duration::from_millis(50); // Recompute EWMA on every TICK
        const ALPHA: f64 = 0.2; // EWMA alpha parameter: 20% weight to new sample, 80% to previous average

        let mut interval = tokio::time::interval(TICK);
        let to_bps = 1.0 / TICK.as_secs_f64(); // Conversion factor from bytes to bps
        let mut ingress_ewma: f64 = 0.0;
        let mut egress_ewma: f64 = 0.0;
        loop {
            interval.tick().await;

            let ingress_bytes =
                ingress_accumulator.swap(0, std::sync::atomic::Ordering::Relaxed);
            let ingress_bps = (ingress_bytes as f64) * to_bps;
            ingress_ewma = ALPHA * ingress_bps + (1.0 - ALPHA) * ingress_ewma;
            let ingress_ewma_int = ingress_ewma.floor() as u64;
            ingress_estimate.store(ingress_ewma_int, std::sync::atomic::Ordering::Relaxed);
            merni::gauge!("server.bandwidth.ingress.ewma"@b: ingress_ewma_int);

            let egress_bytes =
                egress_accumulator.swap(0, std::sync::atomic::Ordering::Relaxed);
            let egress_bps = (egress_bytes as f64) * to_bps;
            egress_ewma = ALPHA * egress_bps + (1.0 - ALPHA) * egress_ewma;
            let egress_ewma_int = egress_ewma.floor() as u64;
            egress_estimate.store(egress_ewma_int, std::sync::atomic::Ordering::Relaxed);
            merni::gauge!("server.bandwidth.egress.ewma"@b: egress_ewma_int);
        }
    }

    fn check(&self) -> bool {
        let Some(bps) = self.config.global_bps else {
            return true;
        };
        let ingress = self
            .ingress_estimate
            .load(std::sync::atomic::Ordering::Relaxed);
        let egress = self
            .egress_estimate
            .load(std::sync::atomic::Ordering::Relaxed);
        ingress <= bps && egress <= bps
    }
}

#[derive(Debug)]
struct ThroughputRateLimiter {
    config: ThroughputLimits,
    global: Option<Mutex<TokenBucket>>,
    // NB: These maps grow unbounded but we accept this as we expect an overall limited
    // number of usecases and scopes. We emit gauge metrics to monitor their size.
    usecases: papaya::HashMap<String, Mutex<TokenBucket>>,
    scopes: papaya::HashMap<Scopes, Mutex<TokenBucket>>,
    rules: papaya::HashMap<usize, Mutex<TokenBucket>>,
}

impl ThroughputRateLimiter {
    fn new(config: ThroughputLimits) -> Self {
        let global = config
            .global_rps
            .map(|rps| Mutex::new(TokenBucket::new(rps, config.burst)));

        Self {
            config,
            global,
            usecases: papaya::HashMap::new(),
            scopes: papaya::HashMap::new(),
            rules: papaya::HashMap::new(),
        }
    }

    fn check(&self, context: &ObjectContext) -> bool {
        // NB: We intentionally use unwrap and crash the server if the mutexes are poisoned.

        // Global check
        if let Some(ref global) = self.global
            && !global.lock().unwrap().try_acquire()
        {
            return false;
        }

        // Usecase check - only if both global_rps and usecase_pct are set
        if let Some(usecase_rps) = self.usecase_rps() {
            let guard = self.usecases.pin();
            let bucket = guard
                .get_or_insert_with(context.usecase.clone(), || self.create_bucket(usecase_rps));
            if !bucket.lock().unwrap().try_acquire() {
                return false;
            }
        }

        // Scope check - only if both global_rps and scope_pct are set
        if let Some(scope_rps) = self.scope_rps() {
            let guard = self.scopes.pin();
            let bucket =
                guard.get_or_insert_with(context.scopes.clone(), || self.create_bucket(scope_rps));
            if !bucket.lock().unwrap().try_acquire() {
                return false;
            }
        }

        // Rule checks - each matching rule has its own dedicated bucket
        for (idx, rule) in self.config.rules.iter().enumerate() {
            if !rule.matches(context) {
                continue;
            }
            let Some(rule_rps) = self.rule_rps(rule) else {
                continue;
            };
            let guard = self.rules.pin();
            let bucket = guard.get_or_insert_with(idx, || self.create_bucket(rule_rps));
            if !bucket.lock().unwrap().try_acquire() {
                return false;
            }
        }

        true
    }

    fn create_bucket(&self, rps: u32) -> Mutex<TokenBucket> {
        Mutex::new(TokenBucket::new(rps, self.config.burst))
    }

    /// Returns the effective RPS for per-usecase limiting, if configured.
    fn usecase_rps(&self) -> Option<u32> {
        let global_rps = self.config.global_rps?;
        let pct = self.config.usecase_pct?;
        Some(((global_rps as f64) * (pct as f64 / 100.0)) as u32)
    }

    /// Returns the effective RPS for per-scope limiting, if configured.
    fn scope_rps(&self) -> Option<u32> {
        let global_rps = self.config.global_rps?;
        let pct = self.config.scope_pct?;
        Some(((global_rps as f64) * (pct as f64 / 100.0)) as u32)
    }

    /// Returns the effective RPS for a rule, if it has a valid limit.
    fn rule_rps(&self, rule: &ThroughputRule) -> Option<u32> {
        let pct_limit = rule.pct.and_then(|p| {
            self.config
                .global_rps
                .map(|g| ((g as f64) * (p as f64 / 100.0)) as u32)
        });

        match (rule.rps, pct_limit) {
            (Some(r), Some(p)) => Some(r.min(p)),
            (Some(r), None) => Some(r),
            (None, Some(p)) => Some(p),
            (None, None) => None,
        }
    }
}

/// A token bucket rate limiter.
///
/// Tokens refill at a constant rate up to capacity. Each request consumes one token.
/// When no tokens are available, requests are rejected.
///
/// This implementation is not thread-safe on its own. Wrap in a `Mutex` for concurrent access.
#[derive(Debug)]
struct TokenBucket {
    refill_rate: f64,
    capacity: f64,
    tokens: f64,
    last_update: Instant,
}

impl TokenBucket {
    /// Creates a new, full token bucket with the specified rate limit and burst capacity.
    ///
    /// - `rps`: tokens refilled per second (sustained rate limit)
    /// - `burst`: initial tokens and burst allowance above sustained rate
    pub fn new(rps: u32, burst: u32) -> Self {
        Self {
            refill_rate: rps as f64,
            capacity: (rps + burst) as f64,
            tokens: (rps + burst) as f64,
            last_update: Instant::now(),
        }
    }

    /// Attempts to acquire a token from the bucket.
    ///
    /// Returns `true` if a token was acquired, `false` if no tokens available.
    pub fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let refill = now.duration_since(self.last_update).as_secs_f64() * self.refill_rate;
        let refilled = (self.tokens + refill).min(self.capacity);

        // Only apply refill if we'd gain at least 1 whole token
        if refilled.floor() > self.tokens.floor() {
            self.last_update = now;
            self.tokens = refilled;
        }

        // Try to consume one token
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// A wrapper around a `PayloadStream` that measures bandwidth usage.
///
/// This behaves exactly as a `PayloadStream`, except that every time an item is polled,
/// the accumulator is incremented by the size of the returned `Bytes` chunk.
///
/// The `Ingress` variant tracks upload bandwidth; `Egress` tracks download bandwidth.
pub(crate) enum MeteredPayloadStream {
    Ingress {
        inner: PayloadStream,
        accumulator: Arc<AtomicU64>,
    },
    Egress {
        inner: PayloadStream,
        accumulator: Arc<AtomicU64>,
    },
}

impl MeteredPayloadStream {
    pub fn ingress(inner: PayloadStream, accumulator: Arc<AtomicU64>) -> Self {
        Self::Ingress { inner, accumulator }
    }

    pub fn egress(inner: PayloadStream, accumulator: Arc<AtomicU64>) -> Self {
        Self::Egress { inner, accumulator }
    }
}

impl std::fmt::Debug for MeteredPayloadStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ingress { accumulator, .. } => f
                .debug_struct("MeteredPayloadStream::Ingress")
                .field("accumulator", accumulator)
                .finish(),
            Self::Egress { accumulator, .. } => f
                .debug_struct("MeteredPayloadStream::Egress")
                .field("accumulator", accumulator)
                .finish(),
        }
    }
}

impl Stream for MeteredPayloadStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let (inner, accumulator) = match this {
            Self::Ingress { inner, accumulator } | Self::Egress { inner, accumulator } => {
                (inner, accumulator)
            }
        };
        let res = inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(ref bytes))) = res {
            accumulator.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        res
    }
}
