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
    /// The overall maximum bandwidth (in bytes per second) per service instance.
    ///
    /// Defaults to `None`, meaning no global bandwidth limit is enforced.
    pub global_bps: Option<u64>,

    /// The maximum percentage of the global bandwidth limit that can be used by any usecase.
    ///
    /// Value from `0` to `100`. Defaults to `None`, meaning no per-usecase bandwidth limit is enforced.
    pub usecase_pct: Option<u8>,

    /// The maximum percentage of the global bandwidth limit that can be used by any scope.
    ///
    /// Value from `0` to `100`. Defaults to `None`, meaning no per-scope bandwidth limit is enforced.
    pub scope_pct: Option<u8>,
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

    /// Starts background tasks for rate limit estimation and monitoring.
    ///
    /// Must be called from within a Tokio runtime.
    pub fn start(&self) {
        self.bandwidth.start();
        self.throughput.start();
    }

    /// Checks if the given context is within the rate limits.
    ///
    /// Returns `true` if the context is within the rate limits, `false` otherwise.
    pub fn check(&self, context: &ObjectContext) -> bool {
        self.throughput.check(context) && self.bandwidth.check(context)
    }

    /// Returns all bandwidth accumulators (global + per-usecase + per-scope) for the given context.
    ///
    /// Creates entries in the per-usecase/per-scope maps if they don't exist yet.
    pub fn bytes_accumulators(&self, context: &ObjectContext) -> Vec<Arc<AtomicU64>> {
        self.bandwidth.accumulators(context)
    }

    /// Records bandwidth usage across all accumulators for the given context.
    ///
    /// This is used for cases where bytes are known upfront (e.g. batch INSERT) rather than
    /// streamed through a `MeteredPayloadStream`.
    pub fn record_bandwidth(&self, context: &ObjectContext, bytes: u64) {
        for acc in self.bandwidth.accumulators(context) {
            acc.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// Shared EWMA state for a single bandwidth estimator entry.
///
/// The accumulator is incremented as bytes flow through streams, and the estimate is updated
/// periodically by a background task using an exponentially weighted moving average.
#[derive(Debug)]
struct BandwidthEstimator {
    accumulator: Arc<AtomicU64>,
    estimate: Arc<AtomicU64>,
}

impl BandwidthEstimator {
    fn new() -> Self {
        Self {
            accumulator: Arc::new(AtomicU64::new(0)),
            estimate: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[derive(Debug)]
struct BandwidthRateLimiter {
    config: BandwidthLimits,
    /// Global accumulator/estimator pair.
    global: Arc<BandwidthEstimator>,
    // NB: These maps grow unbounded but we accept this as we expect an overall limited
    // number of usecases and scopes. We emit gauge metrics to monitor their size.
    usecases: Arc<papaya::HashMap<String, Arc<BandwidthEstimator>>>,
    scopes: Arc<papaya::HashMap<Scopes, Arc<BandwidthEstimator>>>,
}

impl BandwidthRateLimiter {
    fn new(config: BandwidthLimits) -> Self {
        Self {
            config,
            global: Arc::new(BandwidthEstimator::new()),
            usecases: Arc::new(papaya::HashMap::new()),
            scopes: Arc::new(papaya::HashMap::new()),
        }
    }

    fn start(&self) {
        let global = Arc::clone(&self.global);
        let usecases = Arc::clone(&self.usecases);
        let scopes = Arc::clone(&self.scopes);
        // NB: This task has no shutdown mechanism — the rate limiter is only created once.
        // The task is aborted when the Tokio runtime is dropped on process exit.
        tokio::task::spawn(async move {
            Self::estimator(global, usecases, scopes).await;
        });
    }

    /// Estimates the current bandwidth utilization using an exponentially weighted moving average.
    ///
    /// Iterates over the global estimator as well as all per-usecase and per-scope estimators
    /// on each tick, updating their EWMAs.
    async fn estimator(
        global: Arc<BandwidthEstimator>,
        usecases: Arc<papaya::HashMap<String, Arc<BandwidthEstimator>>>,
        scopes: Arc<papaya::HashMap<Scopes, Arc<BandwidthEstimator>>>,
    ) {
        const TICK: Duration = Duration::from_millis(50); // Recompute EWMA on every TICK

        let mut interval = tokio::time::interval(TICK);
        let to_bps = 1.0 / TICK.as_secs_f64(); // Conversion factor from bytes to bps
        let mut global_ewma: f64 = 0.0;
        // Shadow EWMAs for per-usecase/per-scope entries, keyed the same way as the maps.
        let mut usecase_ewmas: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        let mut scope_ewmas: std::collections::HashMap<Scopes, f64> =
            std::collections::HashMap::new();

        loop {
            interval.tick().await;

            // Global
            Self::update_ewma(&global, &mut global_ewma, to_bps);
            merni::gauge!("server.bandwidth.ewma"@b: global_ewma.floor() as u64);

            // Per-usecase
            {
                let guard = usecases.pin();
                for (key, estimator) in guard.iter() {
                    let ewma = usecase_ewmas.entry(key.clone()).or_insert(0.0);
                    Self::update_ewma(estimator, ewma, to_bps);
                }
            }

            // Per-scope
            {
                let guard = scopes.pin();
                for (key, estimator) in guard.iter() {
                    let ewma = scope_ewmas.entry(key.clone()).or_insert(0.0);
                    Self::update_ewma(estimator, ewma, to_bps);
                }
            }

            merni::gauge!("server.rate_limiter.bandwidth.scope_map_size": scopes.len());
            merni::gauge!("server.rate_limiter.bandwidth.usecase_map_size": usecases.len());
        }
    }

    /// Updates a single EWMA estimator from its accumulator.
    fn update_ewma(estimator: &BandwidthEstimator, ewma: &mut f64, to_bps: f64) {
        const ALPHA: f64 = 0.2;
        let current = estimator
            .accumulator
            .swap(0, std::sync::atomic::Ordering::Relaxed);
        let bps = (current as f64) * to_bps;
        *ewma = ALPHA * bps + (1.0 - ALPHA) * *ewma;
        estimator
            .estimate
            .store(ewma.floor() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    fn check(&self, context: &ObjectContext) -> bool {
        let Some(global_bps) = self.config.global_bps else {
            return true;
        };

        // Global check
        if self
            .global
            .estimate
            .load(std::sync::atomic::Ordering::Relaxed)
            > global_bps
        {
            return false;
        }

        // Per-usecase check
        if let Some(usecase_bps) = self.usecase_bps() {
            let guard = self.usecases.pin();
            if let Some(estimator) = guard.get(&context.usecase)
                && estimator
                    .estimate
                    .load(std::sync::atomic::Ordering::Relaxed)
                    > usecase_bps
            {
                return false;
            }
        }

        // Per-scope check
        if let Some(scope_bps) = self.scope_bps() {
            let guard = self.scopes.pin();
            if let Some(estimator) = guard.get(&context.scopes)
                && estimator
                    .estimate
                    .load(std::sync::atomic::Ordering::Relaxed)
                    > scope_bps
            {
                return false;
            }
        }

        true
    }

    /// Returns all accumulators (global + per-usecase + per-scope) for the given context.
    ///
    /// Creates entries in the per-usecase/per-scope maps if they don't exist yet.
    fn accumulators(&self, context: &ObjectContext) -> Vec<Arc<AtomicU64>> {
        let mut accs = vec![Arc::clone(&self.global.accumulator)];

        if self.usecase_bps().is_some() {
            let guard = self.usecases.pin();
            let estimator = guard.get_or_insert_with(context.usecase.clone(), || {
                Arc::new(BandwidthEstimator::new())
            });
            accs.push(Arc::clone(&estimator.accumulator));
        }

        if self.scope_bps().is_some() {
            let guard = self.scopes.pin();
            let estimator = guard.get_or_insert_with(context.scopes.clone(), || {
                Arc::new(BandwidthEstimator::new())
            });
            accs.push(Arc::clone(&estimator.accumulator));
        }

        accs
    }

    /// Returns the effective BPS for per-usecase limiting, if configured.
    fn usecase_bps(&self) -> Option<u64> {
        let global_bps = self.config.global_bps?;
        let pct = self.config.usecase_pct?;
        Some(((global_bps as f64) * (pct as f64 / 100.0)) as u64)
    }

    /// Returns the effective BPS for per-scope limiting, if configured.
    fn scope_bps(&self) -> Option<u64> {
        let global_bps = self.config.global_bps?;
        let pct = self.config.scope_pct?;
        Some(((global_bps as f64) * (pct as f64 / 100.0)) as u64)
    }
}

#[derive(Debug)]
struct ThroughputRateLimiter {
    config: ThroughputLimits,
    global: Option<Mutex<TokenBucket>>,
    // NB: These maps grow unbounded but we accept this as we expect an overall limited
    // number of usecases and scopes. We emit gauge metrics to monitor their size.
    usecases: Arc<papaya::HashMap<String, Mutex<TokenBucket>>>,
    scopes: Arc<papaya::HashMap<Scopes, Mutex<TokenBucket>>>,
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
            usecases: Arc::new(papaya::HashMap::new()),
            scopes: Arc::new(papaya::HashMap::new()),
            rules: papaya::HashMap::new(),
        }
    }

    fn start(&self) {
        let usecases = Arc::clone(&self.usecases);
        let scopes = Arc::clone(&self.scopes);
        // NB: This task has no shutdown mechanism — the rate limiter is only created once.
        // The task is aborted when the Tokio runtime is dropped on process exit.
        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            loop {
                interval.tick().await;
                merni::gauge!("server.rate_limiter.throughput.scope_map_size": scopes.len());
                merni::gauge!("server.rate_limiter.throughput.usecase_map_size": usecases.len());
            }
        });
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
/// all accumulators are incremented by the size of the returned `Bytes` chunk.
pub(crate) struct MeteredPayloadStream {
    inner: PayloadStream,
    accumulators: Vec<Arc<AtomicU64>>,
}

impl MeteredPayloadStream {
    pub fn new(inner: PayloadStream, accumulators: Vec<Arc<AtomicU64>>) -> Self {
        Self {
            inner,
            accumulators,
        }
    }
}

impl std::fmt::Debug for MeteredPayloadStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteredPayloadStream")
            .field("accumulators", &self.accumulators)
            .finish()
    }
}

impl Stream for MeteredPayloadStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let res = this.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(ref bytes))) = res {
            let len = bytes.len() as u64;
            for acc in &this.accumulators {
                acc.fetch_add(len, std::sync::atomic::Ordering::Relaxed);
            }
        }
        res
    }
}
