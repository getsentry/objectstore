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

/// Checks if a rule with the given usecase and scopes matches the given context.
///
/// Shared by [`ThroughputRule`] and [`BandwidthRule`].
fn rule_matches(
    usecase: Option<&str>,
    scopes: &[(String, String)],
    context: &ObjectContext,
) -> bool {
    if let Some(rule_usecase) = usecase
        && rule_usecase != context.usecase
    {
        return false;
    }

    for (scope_name, scope_value) in scopes {
        match context.scopes.get_value(scope_name) {
            Some(value) if value == scope_value => (),
            _ => return false,
        }
    }

    true
}

impl ThroughputRule {
    /// Returns `true` if this rule matches the given context.
    pub fn matches(&self, context: &ObjectContext) -> bool {
        rule_matches(self.usecase.as_deref(), &self.scopes, context)
    }
}

impl BandwidthRule {
    /// Returns `true` if this rule matches the given context.
    pub fn matches(&self, context: &ObjectContext) -> bool {
        rule_matches(self.usecase.as_deref(), &self.scopes, context)
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
    /// Value from `0` to `100`. Defaults to `None`, meaning no per-usecase limit is enforced.
    pub usecase_pct: Option<u8>,

    /// The maximum percentage of the global bandwidth limit that can be used by any scope.
    ///
    /// Value from `0` to `100`. Defaults to `None`, meaning no per-scope limit is enforced.
    pub scope_pct: Option<u8>,

    /// Overrides for specific usecases and scopes.
    pub rules: Vec<BandwidthRule>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct BandwidthRule {
    /// Optional usecase to match.
    ///
    /// If `None`, matches any usecase.
    pub usecase: Option<String>,

    /// Scopes to match.
    ///
    /// If empty, matches any scopes. Additional scopes in the context are ignored, so a rule
    /// matches if all of the specified scopes are present in the request with matching values.
    pub scopes: Vec<(String, String)>,

    /// The bandwidth limit to apply when this rule matches, in bytes per second.
    ///
    /// If both a bps and pct are specified, the more restrictive limit applies.
    /// Should be greater than `0`. To block traffic entirely, use killswitches instead.
    pub bps: Option<u64>,

    /// The percentage of the global bandwidth limit to apply when this rule matches.
    ///
    /// If both a bps and pct are specified, the more restrictive limit applies.
    /// Should be greater than `0`. To block traffic entirely, use killswitches instead.
    pub pct: Option<u8>,
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

    /// Checks if the given context is within the rate limits.
    ///
    /// Returns `true` if the context is within the rate limits, `false` otherwise.
    pub fn check(&self, context: &ObjectContext) -> bool {
        self.throughput.check(context) && self.bandwidth.check(context)
    }

    /// Returns the global bandwidth accumulator and all per-key bandwidth buckets
    /// matching the given context.
    pub(crate) fn bandwidth_context(
        &self,
        context: &ObjectContext,
    ) -> (Arc<AtomicU64>, Vec<Arc<BandwidthBucket>>) {
        self.bandwidth.accumulators_for_context(context)
    }

    /// Returns only the global bandwidth accumulator, for cases where no context is available.
    pub(crate) fn global_bandwidth_accumulator(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.bandwidth.global_accumulator)
    }
}

/// Per-key bandwidth tracker using lazy EWMA.
///
/// The accumulator is incremented lock-free by `MeteredPayloadStream`. The EWMA state is updated
/// lazily when `estimated_bps()` is called (at admission time), avoiding background iteration.
pub(crate) struct BandwidthBucket {
    accumulator: AtomicU64,
    state: Mutex<BandwidthBucketState>,
}

struct BandwidthBucketState {
    ewma: f64,
    last_update: Instant,
}

impl BandwidthBucket {
    fn new() -> Self {
        Self {
            accumulator: AtomicU64::new(0),
            state: Mutex::new(BandwidthBucketState {
                ewma: 0.0,
                last_update: Instant::now(),
            }),
        }
    }

    /// Lock-free byte accumulation, called from `MeteredPayloadStream::poll_next`.
    pub(crate) fn add_bytes(&self, n: u64) {
        self.accumulator
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// Computes the current estimated bandwidth using a variable-interval EWMA.
    ///
    /// This is called at admission time. It swaps the accumulator to 0 and updates the EWMA
    /// based on the elapsed time since the last call. When dt = 50ms this is mathematically
    /// equivalent to the fixed-interval EWMA (alpha = 0.2).
    fn estimated_bps(&self) -> u64 {
        let bytes = self.accumulator.swap(0, std::sync::atomic::Ordering::Relaxed);
        let mut state = self.state.lock().unwrap();

        let now = Instant::now();
        let dt = now.duration_since(state.last_update).as_secs_f64();
        state.last_update = now;

        if dt <= 0.0 {
            return state.ewma.floor() as u64;
        }

        // Variable-interval EWMA: alpha = 1 - 0.8^(dt / 0.05)
        // At dt=50ms this gives alpha=0.2, matching the fixed-interval estimator.
        let alpha = 1.0 - 0.8_f64.powf(dt / 0.05);
        let bps = (bytes as f64) / dt;
        state.ewma = alpha * bps + (1.0 - alpha) * state.ewma;

        state.ewma.floor() as u64
    }
}

impl std::fmt::Debug for BandwidthBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BandwidthBucket")
            .field(
                "accumulator",
                &self
                    .accumulator
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish()
    }
}

#[derive(Debug)]
struct BandwidthRateLimiter {
    config: BandwidthLimits,
    // Global: existing background EWMA (unchanged)
    global_accumulator: Arc<AtomicU64>,
    global_estimate: Arc<AtomicU64>,
    // Per-key: lazy EWMA (no background task)
    usecases: papaya::HashMap<String, Arc<BandwidthBucket>>,
    scopes: papaya::HashMap<Scopes, Arc<BandwidthBucket>>,
    rules: papaya::HashMap<usize, Arc<BandwidthBucket>>,
}

impl BandwidthRateLimiter {
    fn new(config: BandwidthLimits) -> Self {
        let global_accumulator = Arc::new(AtomicU64::new(0));
        let global_estimate = Arc::new(AtomicU64::new(0));

        let accumulator_clone = Arc::clone(&global_accumulator);
        let estimate_clone = Arc::clone(&global_estimate);
        tokio::task::spawn(async move {
            Self::estimator(accumulator_clone, estimate_clone).await;
        });

        Self {
            config,
            global_accumulator,
            global_estimate,
            usecases: papaya::HashMap::new(),
            scopes: papaya::HashMap::new(),
            rules: papaya::HashMap::new(),
        }
    }

    /// Estimates the current global bandwidth utilization using an exponentially weighted moving average.
    ///
    /// The calculation is based on the increments of `self.global_accumulator` happened in the last `TICK`.
    /// The estimate is stored in `self.global_estimate`, which can be queried for bandwidth-based rate-limiting.
    async fn estimator(accumulator: Arc<AtomicU64>, estimate: Arc<AtomicU64>) {
        const TICK: Duration = Duration::from_millis(50); // Recompute EWMA on every TICK
        const ALPHA: f64 = 0.2; // EWMA alpha parameter: 20% weight to new sample, 80% to previous average

        let mut interval = tokio::time::interval(TICK);
        let to_bps = 1.0 / TICK.as_secs_f64(); // Conversion factor from bytes to bps
        let mut ewma: f64 = 0.0;
        loop {
            interval.tick().await;
            let current = accumulator.swap(0, std::sync::atomic::Ordering::Relaxed);
            let bps = (current as f64) * to_bps;
            ewma = ALPHA * bps + (1.0 - ALPHA) * ewma;

            let ewma_int = ewma.floor() as u64;
            estimate.store(ewma_int, std::sync::atomic::Ordering::Relaxed);
            merni::gauge!("server.bandwidth.ewma"@b: ewma_int);
        }
    }

    fn check(&self, context: &ObjectContext) -> bool {
        // Global check
        if let Some(bps) = self.config.global_bps
            && self
                .global_estimate
                .load(std::sync::atomic::Ordering::Relaxed)
                > bps
        {
            return false;
        }

        // Usecase check
        if let Some(usecase_bps) = self.usecase_bps() {
            let guard = self.usecases.pin();
            let bucket = guard.get_or_insert_with(context.usecase.clone(), || {
                Arc::new(BandwidthBucket::new())
            });
            if bucket.estimated_bps() > usecase_bps {
                return false;
            }
        }

        // Scope check
        if let Some(scope_bps) = self.scope_bps() {
            let guard = self.scopes.pin();
            let bucket = guard
                .get_or_insert_with(context.scopes.clone(), || Arc::new(BandwidthBucket::new()));
            if bucket.estimated_bps() > scope_bps {
                return false;
            }
        }

        // Rule checks
        for (idx, rule) in self.config.rules.iter().enumerate() {
            if !rule.matches(context) {
                continue;
            }
            let Some(rule_bps) = self.rule_bps(rule) else {
                continue;
            };
            let guard = self.rules.pin();
            let bucket = guard.get_or_insert_with(idx, || Arc::new(BandwidthBucket::new()));
            if bucket.estimated_bps() > rule_bps {
                return false;
            }
        }

        true
    }

    /// Returns the global accumulator and all per-key buckets matching the given context.
    fn accumulators_for_context(
        &self,
        context: &ObjectContext,
    ) -> (Arc<AtomicU64>, Vec<Arc<BandwidthBucket>>) {
        let mut buckets = Vec::new();

        if self.usecase_bps().is_some() {
            let guard = self.usecases.pin();
            let bucket = guard.get_or_insert_with(context.usecase.clone(), || {
                Arc::new(BandwidthBucket::new())
            });
            buckets.push(Arc::clone(bucket));
        }

        if self.scope_bps().is_some() {
            let guard = self.scopes.pin();
            let bucket = guard
                .get_or_insert_with(context.scopes.clone(), || Arc::new(BandwidthBucket::new()));
            buckets.push(Arc::clone(bucket));
        }

        for (idx, rule) in self.config.rules.iter().enumerate() {
            if !rule.matches(context) {
                continue;
            }
            if self.rule_bps(rule).is_none() {
                continue;
            }
            let guard = self.rules.pin();
            let bucket = guard.get_or_insert_with(idx, || Arc::new(BandwidthBucket::new()));
            buckets.push(Arc::clone(bucket));
        }

        (Arc::clone(&self.global_accumulator), buckets)
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

    /// Returns the effective BPS for a rule, if it has a valid limit.
    fn rule_bps(&self, rule: &BandwidthRule) -> Option<u64> {
        let pct_limit = rule.pct.and_then(|p| {
            self.config
                .global_bps
                .map(|g| ((g as f64) * (p as f64 / 100.0)) as u64)
        });

        match (rule.bps, pct_limit) {
            (Some(r), Some(p)) => Some(r.min(p)),
            (Some(r), None) => Some(r),
            (None, Some(p)) => Some(p),
            (None, None) => None,
        }
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
/// the global accumulator and all per-key buckets are incremented by the size of the
/// returned `Bytes` chunk.
pub(crate) struct MeteredPayloadStream {
    inner: PayloadStream,
    global_accumulator: Arc<AtomicU64>,
    buckets: Vec<Arc<BandwidthBucket>>,
}

impl MeteredPayloadStream {
    pub fn new(
        inner: PayloadStream,
        global_accumulator: Arc<AtomicU64>,
        buckets: Vec<Arc<BandwidthBucket>>,
    ) -> Self {
        Self {
            inner,
            global_accumulator,
            buckets,
        }
    }

    pub fn global_only(inner: PayloadStream, global_accumulator: Arc<AtomicU64>) -> Self {
        Self {
            inner,
            global_accumulator,
            buckets: Vec::new(),
        }
    }
}

impl std::fmt::Debug for MeteredPayloadStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteredPayloadStream")
            .field("global_accumulator", &self.global_accumulator)
            .field("buckets", &self.buckets.len())
            .finish()
    }
}

impl Stream for MeteredPayloadStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let res = this.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(ref bytes))) = res {
            let n = bytes.len() as u64;
            this.global_accumulator
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
            for bucket in &this.buckets {
                bucket.add_bytes(n);
            }
        }
        res
    }
}
