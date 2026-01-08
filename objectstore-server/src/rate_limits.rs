use std::sync::Arc;
use std::sync::{Mutex, atomic::AtomicUsize};
use std::time::Instant;

use objectstore_service::id::ObjectContext;
use objectstore_types::scope::Scopes;
use serde::{Deserialize, Serialize};

/// Rate limits for objectstore.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RateLimits {
    /// Limits the number of requests per second per service instance.
    pub throughput: ThroughputLimits,
    /// Limits the concurrent bandwidth per service instance.
    pub bandwidth: BandwidthLimits,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct BandwidthLimits {
    /// The overall maximum bandwidth (in bytes per second) per service instance.
    ///
    /// Defaults to `None`, meaning no global bandwidth limit is enforced.
    pub global_bps: Option<usize>,
}

#[derive(Debug)]
pub struct RateLimiter {
    config: RateLimits,
    bandwidth: BandwidthRateLimiter,
    global: Option<Mutex<TokenBucket>>,
    // NB: These maps grow unbounded but we accept this as we expect an overall limited
    // number of usecases and scopes. We emit gauge metrics to monitor their size.
    usecases: papaya::HashMap<String, Mutex<TokenBucket>>,
    scopes: papaya::HashMap<Scopes, Mutex<TokenBucket>>,
    rules: papaya::HashMap<usize, Mutex<TokenBucket>>,
}

#[derive(Debug)]
struct BandwidthRateLimiter {
    config: BandwidthLimits,
    /// Accumulator that's incremented every time an operation that uses bandwidth is executed.
    accumulator: Arc<AtomicUsize>,
    /// An estimate of the bandwidth that's currently being utilized in bytes per second.
    estimate: Arc<AtomicUsize>,
}

impl BandwidthRateLimiter {
    fn new(config: BandwidthLimits) -> Self {
        let accumulator = Arc::new(AtomicUsize::new(0));
        let estimate = Arc::new(AtomicUsize::new(0));

        let accumulator_clone = Arc::clone(&accumulator);
        let estimate_clone = Arc::clone(&estimate);
        tokio::task::spawn(async move {
            Self::estimator(accumulator_clone, estimate_clone).await;
        });

        Self {
            config,
            accumulator,
            estimate,
        }
    }

    /// Estimates the current bandwidth utilization using an exponentially weighted moving average.
    ///
    /// The calculation is based on the increments of `self.accumulator` happened in the last `tick`.
    /// The estimate is stored in `self.estimate`, which can be queried for bandwidth-based rate-limiting.
    async fn estimator(accumulator: Arc<AtomicUsize>, average: Arc<AtomicUsize>) {
        let tick = std::time::Duration::from_millis(50);
        let mut interval = tokio::time::interval(tick);

        let to_bps = 1.0 / tick.as_secs_f64(); // Conversion factor from bytes to bps
        const ALPHA: f64 = 0.2; // 20% weight to new sample, 80% to previous average
        let mut ewma: f64 = 0.0;
        loop {
            interval.tick().await;
            let current = accumulator.swap(0, std::sync::atomic::Ordering::Relaxed);
            let bps = (current as f64) * to_bps;
            ewma = ALPHA * bps + (1.0 - ALPHA) * ewma;

            let ewma_usize = ewma.floor() as usize;
            average.store(ewma_usize, std::sync::atomic::Ordering::Relaxed);
            merni::gauge!("server.bandwidth.ewma"@b: ewma_usize);
        }
    }

    fn check(&self) -> bool {
        let Some(bps) = self.config.global_bps else {
            return true;
        };
        self.estimate.load(std::sync::atomic::Ordering::Relaxed) <= bps
    }
}

impl RateLimiter {
    pub fn new(config: RateLimits) -> Self {
        let global = config
            .throughput
            .global_rps
            .map(|rps| Mutex::new(TokenBucket::new(rps, config.throughput.burst)));

        Self {
            config: config.clone(),
            bandwidth: BandwidthRateLimiter::new(config.bandwidth),
            global,
            usecases: papaya::HashMap::new(),
            scopes: papaya::HashMap::new(),
            rules: papaya::HashMap::new(),
        }
    }

    /// Returns a reference to the shared bytes accumulator, used for bandwidth-based rate-limiting.
    pub fn bytes_accumulator(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.bandwidth.accumulator)
    }

    /// Checks if the given context is within the rate limits.
    ///
    /// Returns `true` if the context is within the rate limits, `false` otherwise.
    pub fn check(&self, context: &ObjectContext) -> bool {
        self.check_throughput(context) && self.bandwidth.check()
    }

    fn check_throughput(&self, context: &ObjectContext) -> bool {
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
        for (idx, rule) in self.config.throughput.rules.iter().enumerate() {
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
        Mutex::new(TokenBucket::new(rps, self.config.throughput.burst))
    }

    /// Returns the effective RPS for per-usecase limiting, if configured.
    fn usecase_rps(&self) -> Option<u32> {
        let global_rps = self.config.throughput.global_rps?;
        let pct = self.config.throughput.usecase_pct?;
        Some(((global_rps as f64) * (pct as f64 / 100.0)) as u32)
    }

    /// Returns the effective RPS for per-scope limiting, if configured.
    fn scope_rps(&self) -> Option<u32> {
        let global_rps = self.config.throughput.global_rps?;
        let pct = self.config.throughput.scope_pct?;
        Some(((global_rps as f64) * (pct as f64 / 100.0)) as u32)
    }

    /// Returns the effective RPS for a rule, if it has a valid limit.
    fn rule_rps(&self, rule: &ThroughputRule) -> Option<u32> {
        let pct_limit = rule.pct.and_then(|p| {
            self.config
                .throughput
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
    /// Creates a new token bucket with the specified rate limit and burst capacity.
    ///
    /// - `rps`: tokens refilled per second (sustained rate limit)
    /// - `burst`: initial tokens and burst allowance above sustained rate
    pub fn new(rps: u32, burst: u32) -> Self {
        Self {
            refill_rate: rps as f64,
            capacity: (rps + burst) as f64,
            tokens: burst as f64,
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
