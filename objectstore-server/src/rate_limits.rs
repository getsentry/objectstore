//! Admission-based rate limiting for throughput and bandwidth.
//!
//! This module provides [`RateLimiter`], which enforces configurable limits at three
//! levels of granularity — global, per-usecase, and per-scope — for both request
//! throughput (requests/s) and upload/download bandwidth (bytes/s).
//!
//! Throughput is enforced using token buckets. Bandwidth uses debt-based GCRA
//! (Generic Cell Rate Algorithm) buckets that track a theoretical arrival time
//! (TAT). All rate-limit checks are synchronous, non-blocking, and lock-free.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::Stream;
use objectstore_service::id::ObjectContext;
use objectstore_types::scope::Scopes;
use serde::{Deserialize, Serialize};

/// Identifies which rate limit triggered a rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RateLimitRejection {
    /// Global bandwidth limit exceeded.
    BandwidthGlobal,
    /// Per-usecase bandwidth limit exceeded.
    BandwidthUsecase,
    /// Per-scope bandwidth limit exceeded.
    BandwidthScope,
    /// Global throughput limit exceeded.
    ThroughputGlobal,
    /// Per-usecase throughput limit exceeded.
    ThroughputUsecase,
    /// Per-scope throughput limit exceeded.
    ThroughputScope,
    /// Per-rule throughput limit exceeded.
    ThroughputRule,
}

impl RateLimitRejection {
    /// Returns a static string identifier suitable for use as a metric tag.
    pub fn as_str(self) -> &'static str {
        match self {
            RateLimitRejection::BandwidthGlobal => "bandwidth_global",
            RateLimitRejection::BandwidthUsecase => "bandwidth_usecase",
            RateLimitRejection::BandwidthScope => "bandwidth_scope",
            RateLimitRejection::ThroughputGlobal => "throughput_global",
            RateLimitRejection::ThroughputUsecase => "throughput_usecase",
            RateLimitRejection::ThroughputScope => "throughput_scope",
            RateLimitRejection::ThroughputRule => "throughput_rule",
        }
    }
}

impl fmt::Display for RateLimitRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Rate limits for objectstore.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct RateLimits {
    /// Limits the number of requests per second per service instance.
    pub throughput: ThroughputLimits,
    /// Limits the concurrent bandwidth per service instance.
    pub bandwidth: BandwidthLimits,
}

/// Request throughput limits applied at global, per-usecase, and per-scope granularity.
///
/// All limits are optional. When a limit is `None`, that level of limiting is not enforced.
/// Per-usecase and per-scope limits are expressed as a percentage of the global limit.
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

/// An override rule that applies a specific throughput limit to matching request contexts.
///
/// A rule matches when all specified fields match the request context. Fields not set match
/// any value. When multiple rules match, each is enforced independently via its own token
/// bucket — all matching rules must admit the request.
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

/// Bandwidth limits applied at global, per-usecase, and per-scope granularity.
///
/// Bandwidth is measured as bytes transferred per second (upload + download combined)
/// and tracked using debt-based GCRA buckets. All limits are optional.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct BandwidthLimits {
    /// The overall maximum bandwidth (in bytes per second) per service instance.
    ///
    /// Defaults to `None`, meaning no global bandwidth limit is enforced.
    pub global_bps: Option<u64>,

    /// Burst tolerance in milliseconds.
    ///
    /// Allows short traffic spikes up to `burst_ms * bps / 1000` bytes before
    /// rejection. Defaults to `1000` (1 second).
    #[serde(default = "default_burst_ms")]
    pub burst_ms: u64,

    /// The maximum percentage of the global bandwidth limit that can be used by any usecase.
    ///
    /// Value from `0` to `100`. Defaults to `None`, meaning no per-usecase bandwidth limit is enforced.
    pub usecase_pct: Option<u8>,

    /// The maximum percentage of the global bandwidth limit that can be used by any scope.
    ///
    /// Value from `0` to `100`. Defaults to `None`, meaning no per-scope bandwidth limit is enforced.
    pub scope_pct: Option<u8>,

    /// When `true`, bandwidth limits are evaluated and reported but never enforced.
    ///
    /// All accounting and metrics remain active, but requests exceeding the limit
    /// are not rejected. Defaults to `false`.
    #[serde(default)]
    pub report_only: bool,
}

fn default_burst_ms() -> u64 {
    1000
}

impl Default for BandwidthLimits {
    fn default() -> Self {
        Self {
            global_bps: None,
            burst_ms: default_burst_ms(),
            usecase_pct: None,
            scope_pct: None,
            report_only: false,
        }
    }
}

/// Combined rate limiter that enforces both bandwidth and throughput limits.
///
/// Checks are synchronous and non-blocking. Bandwidth is checked before
/// throughput so that rejected requests are never counted toward the admitted
/// throughput counter. See [`check`](RateLimiter::check) for details.
///
/// Call [`start`](RateLimiter::start) after construction to launch the background
/// observability tasks.
#[derive(Debug)]
pub struct RateLimiter {
    bandwidth: BandwidthRateLimiter,
    throughput: ThroughputRateLimiter,
}

impl RateLimiter {
    /// Creates a new rate limiter from the given configuration.
    ///
    /// Background observability tasks are not started until [`start`](RateLimiter::start) is called.
    pub fn new(config: RateLimits) -> Self {
        Self {
            bandwidth: BandwidthRateLimiter::new(config.bandwidth),
            throughput: ThroughputRateLimiter::new(config.throughput),
        }
    }

    /// Starts background tasks for rate limit monitoring.
    ///
    /// Must be called from within a Tokio runtime.
    pub fn start(&self) {
        self.throughput.start();
    }

    /// Checks if the given context is within the rate limits.
    ///
    /// Returns `true` if the request is admitted, `false` if it was rejected. On rejection, emits a
    /// `server.request.rate_limited` metric counter and a `warn!` log. Bandwidth is checked before
    /// throughput so that rejected requests are never counted toward admitted traffic.
    pub fn check(&self, context: &ObjectContext, key: Option<&str>) -> bool {
        // Bandwidth is checked first because it is a pure read (no token consumption).
        // Throughput counts admitted requests, so checking it second ensures rejected
        // requests are never counted toward admitted traffic.
        let rejection = self
            .bandwidth
            .check(context)
            .or_else(|| self.throughput.check(context));

        let Some(rejection) = rejection else {
            return true;
        };

        objectstore_metrics::count!(
            "server.request.rate_limited",
            reason = rejection.as_str(),
            usecase = context.usecase.clone()
        );
        objectstore_log::warn!(
            reason = rejection.as_str(),
            usecase = &context.usecase,
            scopes = %context.scopes.as_api_path(),
            key,
            "Request rejected: rate limit exceeded"
        );
        false
    }

    /// Returns bandwidth buckets and the total-bytes counter for the given context.
    ///
    /// Creates entries in the per-usecase/per-scope maps if they don't exist yet.
    pub fn bandwidth_handle(&self, context: &ObjectContext) -> BandwidthHandle {
        self.bandwidth.handle(context)
    }

    /// Records bandwidth usage across all buckets for the given context.
    ///
    /// This is used for cases where bytes are known upfront (e.g. batch INSERT) rather than
    /// streamed through a `MeteredPayloadStream`.
    pub fn record_bandwidth(&self, context: &ObjectContext, bytes: u64) {
        self.bandwidth.handle(context).record(bytes);
        objectstore_metrics::count!("server.bandwidth.bytes" += bytes);
    }

    /// Returns the configured global bandwidth limit in bytes/s, if set.
    pub fn bandwidth_limit(&self) -> Option<u64> {
        self.bandwidth.config.global_bps
    }

    /// Returns the configured global throughput limit in requests/s, if set.
    pub fn throughput_limit(&self) -> Option<u32> {
        self.throughput.config.global_rps
    }

    /// Returns total bytes transferred since startup.
    pub fn bandwidth_total_bytes(&self) -> u64 {
        self.bandwidth.total_bytes.load(Ordering::Relaxed)
    }

    /// Returns total admitted requests since startup.
    pub fn throughput_total_admitted(&self) -> u64 {
        self.throughput.total_admitted.load(Ordering::Relaxed)
    }
}

/// Debt-based GCRA bandwidth bucket.
///
/// Tracks a theoretical arrival time (TAT) that advances by `nanos_per_byte`
/// for every byte consumed. Admission is granted while TAT stays within
/// `burst_ns` nanoseconds ahead of the wall clock (burst tolerance). Recovery is
/// continuous — no background tick required.
#[derive(Debug)]
struct BandwidthBucket {
    /// Nanoseconds since `epoch` at which all recorded traffic is paid off.
    ///
    /// Saturates at `u64::MAX` after ~584 years of uptime.
    tat: AtomicU64,
    /// Nanoseconds of TAT advance per byte: `1e9 / bps`.
    nanos_per_byte: f64,
    /// Burst tolerance in nanoseconds: `burst_ms * 1_000_000`.
    burst_ns: u64,
}

impl BandwidthBucket {
    /// Creates a new bucket for the given rate and burst tolerance.
    fn new(bps: u64, burst_ms: u64) -> Self {
        let nanos_per_byte = 1_000_000_000.0 / bps as f64;
        let burst_ns = burst_ms * 1_000_000;
        Self {
            tat: AtomicU64::new(0),
            nanos_per_byte,
            burst_ns,
        }
    }

    /// Records `bytes` of consumption unconditionally (debt is allowed).
    fn spend(&self, now_nanos: u64, bytes: u64) {
        let weight = (bytes as f64 * self.nanos_per_byte) as u64;
        // Clamp + advance in one atomic step: never let TAT fall behind `now`
        // (no credit accumulation), then advance by the byte cost.
        self.tat
            .update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                old.max(now_nanos).saturating_add(weight)
            });
    }

    /// Returns `true` if the bucket admits new traffic at `now_nanos`.
    fn check(&self, now_nanos: u64) -> bool {
        self.tat.load(Ordering::Relaxed) <= now_nanos.saturating_add(self.burst_ns)
    }
}

/// Returns `value * pct / 100`, saturating at `u32::MAX`.
fn pct_of_u32(value: u32, pct: u8) -> u32 {
    let scaled = u64::from(value) * u64::from(pct) / 100;
    u32::try_from(scaled).unwrap_or(u32::MAX)
}

/// Returns `value * pct / 100`, saturating at `u64::MAX`.
fn pct_of_u64(value: u64, pct: u8) -> u64 {
    let scaled = u128::from(value) * u128::from(pct) / 100;
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

#[derive(Debug)]
struct BandwidthRateLimiter {
    config: BandwidthLimits,
    /// Global GCRA bucket.
    global: Option<Arc<BandwidthBucket>>,
    /// Shared epoch for converting `Instant` to nanos.
    epoch: Instant,
    /// Cumulative bytes transferred since startup. Never reset.
    total_bytes: Arc<AtomicU64>,
    // NB: These maps grow unbounded but we accept this as we expect an overall limited
    // number of usecases and scopes. We emit gauge metrics to monitor their size.
    usecases: Arc<papaya::HashMap<String, Arc<BandwidthBucket>>>,
    scopes: Arc<papaya::HashMap<Scopes, Arc<BandwidthBucket>>>,
}

impl BandwidthRateLimiter {
    fn new(config: BandwidthLimits) -> Self {
        let global = config
            .global_bps
            .map(|bps| Arc::new(BandwidthBucket::new(bps, config.burst_ms)));

        if let Some(limit) = config.global_bps {
            objectstore_metrics::gauge!("server.bandwidth.limit" = limit);
        }

        Self {
            global,
            epoch: Instant::now(),
            total_bytes: Arc::new(AtomicU64::new(0)),
            usecases: Arc::new(papaya::HashMap::new()),
            scopes: Arc::new(papaya::HashMap::new()),
            config,
        }
    }

    /// Returns nanoseconds elapsed since the shared epoch.
    fn now_nanos(&self) -> u64 {
        // u64 nanos overflow after ~584 years of uptime, accepted.
        self.epoch.elapsed().as_nanos() as u64
    }

    /// Checks whether the current bandwidth debt exceeds configured limits.
    ///
    /// When [`BandwidthLimits::report_only`] is `true`, returns `None` unconditionally.
    fn check(&self, context: &ObjectContext) -> Option<RateLimitRejection> {
        if self.config.report_only {
            return None;
        }

        let now_nanos = self.now_nanos();

        // Global check
        if let Some(ref global) = self.global
            && !global.check(now_nanos)
        {
            return Some(RateLimitRejection::BandwidthGlobal);
        }

        // Per-usecase check
        if self.usecase_bps().is_some() {
            let guard = self.usecases.pin();
            if let Some(bucket) = guard.get(&context.usecase)
                && !bucket.check(now_nanos)
            {
                return Some(RateLimitRejection::BandwidthUsecase);
            }
        }

        // Per-scope check
        if self.scope_bps().is_some() {
            let guard = self.scopes.pin();
            if let Some(bucket) = guard.get(&context.scopes)
                && !bucket.check(now_nanos)
            {
                return Some(RateLimitRejection::BandwidthScope);
            }
        }

        None
    }

    /// Returns a handle containing the bandwidth buckets and total-bytes counter.
    fn handle(&self, context: &ObjectContext) -> BandwidthHandle {
        let mut buckets = Vec::new();

        if let Some(ref global) = self.global {
            buckets.push(Arc::clone(global));
        }

        if let Some(usecase_bps) = self.usecase_bps() {
            let guard = self.usecases.pin();
            let bucket = guard.get_or_insert_with(context.usecase.clone(), || {
                Arc::new(BandwidthBucket::new(usecase_bps, self.config.burst_ms))
            });
            buckets.push(Arc::clone(bucket));
        }

        if let Some(scope_bps) = self.scope_bps() {
            let guard = self.scopes.pin();
            let bucket = guard.get_or_insert_with(context.scopes.clone(), || {
                Arc::new(BandwidthBucket::new(scope_bps, self.config.burst_ms))
            });
            buckets.push(Arc::clone(bucket));
        }

        objectstore_metrics::gauge!(
            "server.rate_limiter.bandwidth.usecase_map_size" = self.usecases.len()
        );
        objectstore_metrics::gauge!(
            "server.rate_limiter.bandwidth.scope_map_size" = self.scopes.len()
        );

        BandwidthHandle {
            buckets,
            total_bytes: Arc::clone(&self.total_bytes),
            epoch: self.epoch,
        }
    }

    /// Returns the effective BPS for per-usecase limiting, if configured.
    fn usecase_bps(&self) -> Option<u64> {
        let global_bps = self.config.global_bps?;
        let pct = self.config.usecase_pct?;
        Some(pct_of_u64(global_bps, pct))
    }

    /// Returns the effective BPS for per-scope limiting, if configured.
    fn scope_bps(&self) -> Option<u64> {
        let global_bps = self.config.global_bps?;
        let pct = self.config.scope_pct?;
        Some(pct_of_u64(global_bps, pct))
    }
}

/// Handle for recording bandwidth consumption against GCRA buckets.
#[derive(Debug, Clone)]
pub struct BandwidthHandle {
    buckets: Vec<Arc<BandwidthBucket>>,
    total_bytes: Arc<AtomicU64>,
    epoch: Instant,
}

impl BandwidthHandle {
    /// Records `bytes` of consumption across all buckets and the total counter.
    pub fn record(&self, bytes: u64) {
        let now_nanos = self.epoch.elapsed().as_nanos() as u64;
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        for bucket in &self.buckets {
            bucket.spend(now_nanos, bytes);
        }
    }
}

#[derive(Debug)]
struct ThroughputRateLimiter {
    config: ThroughputLimits,
    global: Option<Mutex<TokenBucket>>,
    /// Cumulative admitted requests since startup. Never reset.
    total_admitted: Arc<AtomicU64>,
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
            total_admitted: Arc::new(AtomicU64::new(0)),
            usecases: Arc::new(papaya::HashMap::new()),
            scopes: Arc::new(papaya::HashMap::new()),
            rules: papaya::HashMap::new(),
        }
    }

    fn start(&self) {
        let usecases = Arc::clone(&self.usecases);
        let scopes = Arc::clone(&self.scopes);
        let global_limit = self.config.global_rps;
        tokio::task::spawn(async move {
            const TICK: Duration = Duration::from_secs(1);
            let mut interval = tokio::time::interval(TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Some(limit) = global_limit {
                    objectstore_metrics::gauge!(
                        "server.rate_limiter.throughput.limit" = u64::from(limit)
                    );
                }
                objectstore_metrics::gauge!(
                    "server.rate_limiter.throughput.scope_map_size" = scopes.len()
                );
                objectstore_metrics::gauge!(
                    "server.rate_limiter.throughput.usecase_map_size" = usecases.len()
                );
            }
        });
    }

    fn check(&self, context: &ObjectContext) -> Option<RateLimitRejection> {
        // NB: We intentionally use unwrap and crash the server if the mutexes are poisoned.

        // Global check
        if let Some(ref global) = self.global {
            let acquired = global.lock().unwrap().try_acquire();
            if !acquired {
                return Some(RateLimitRejection::ThroughputGlobal);
            }
        }

        // Usecase check - only if both global_rps and usecase_pct are set
        if let Some(usecase_rps) = self.usecase_rps() {
            let guard = self.usecases.pin();
            let bucket = guard
                .get_or_insert_with(context.usecase.clone(), || self.create_bucket(usecase_rps));
            if !bucket.lock().unwrap().try_acquire() {
                return Some(RateLimitRejection::ThroughputUsecase);
            }
        }

        // Scope check - only if both global_rps and scope_pct are set
        if let Some(scope_rps) = self.scope_rps() {
            let guard = self.scopes.pin();
            let bucket =
                guard.get_or_insert_with(context.scopes.clone(), || self.create_bucket(scope_rps));
            if !bucket.lock().unwrap().try_acquire() {
                return Some(RateLimitRejection::ThroughputScope);
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
                return Some(RateLimitRejection::ThroughputRule);
            }
        }

        self.total_admitted.fetch_add(1, Ordering::Relaxed);

        None
    }

    fn create_bucket(&self, rps: u32) -> Mutex<TokenBucket> {
        Mutex::new(TokenBucket::new(rps, self.config.burst))
    }

    /// Returns the effective RPS for per-usecase limiting, if configured.
    fn usecase_rps(&self) -> Option<u32> {
        let global_rps = self.config.global_rps?;
        let pct = self.config.usecase_pct?;
        Some(pct_of_u32(global_rps, pct))
    }

    /// Returns the effective RPS for per-scope limiting, if configured.
    fn scope_rps(&self) -> Option<u32> {
        let global_rps = self.config.global_rps?;
        let pct = self.config.scope_pct?;
        Some(pct_of_u32(global_rps, pct))
    }

    /// Returns the effective RPS for a rule, if it has a valid limit.
    fn rule_rps(&self, rule: &ThroughputRule) -> Option<u32> {
        let pct_limit = rule
            .pct
            .and_then(|p| self.config.global_rps.map(|g| pct_of_u32(g, p)));

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

/// A wrapper around a byte stream that measures bandwidth usage.
///
/// Every time a chunk is polled successfully, all bandwidth buckets are charged
/// and the total-bytes counter is incremented. Generic over both the stream
/// type `S` and its error type.
pub(crate) struct MeteredPayloadStream<S> {
    inner: S,
    handle: BandwidthHandle,
}

impl<S> MeteredPayloadStream<S> {
    pub fn new(inner: S, handle: BandwidthHandle) -> Self {
        Self { inner, handle }
    }
}

impl<S> fmt::Debug for MeteredPayloadStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MeteredPayloadStream")
            .field("handle", &self.handle)
            .finish()
    }
}

impl<S, E> Stream for MeteredPayloadStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let res = Pin::new(&mut this.inner).poll_next(cx);
        if let Poll::Ready(Some(Ok(ref bytes))) = res {
            let len = bytes.len() as u64;
            this.handle.record(len);
            objectstore_metrics::count!("server.bandwidth.bytes" += len);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use objectstore_service::id::ObjectContext;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;

    fn make_context() -> ObjectContext {
        ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("org", "1").unwrap()]),
        }
    }

    // --- BandwidthBucket unit tests (explicit `now`, no sleeps) ---

    #[test]
    fn bucket_spend_advances_tat() {
        let bucket = BandwidthBucket::new(1000, 0); // 1000 bps, no burst
        let now = 1_000_000_000u64; // 1s after epoch

        bucket.spend(now, 500);

        // 500 bytes at 1e6 ns/byte = 500_000_000 ns ahead of `now`
        let expected_tat = now + 500_000_000;
        assert_eq!(bucket.tat.load(Ordering::Relaxed), expected_tat);
    }

    #[test]
    fn bucket_spend_clamps_credit() {
        let bucket = BandwidthBucket::new(1000, 0);
        // Set TAT in the past (idle period).
        bucket.tat.store(100, Ordering::Relaxed);
        let now = 2_000_000_000u64;

        bucket.spend(now, 100);

        // TAT should have been clamped to `now` first, then advanced.
        let weight = (100.0 * 1_000_000_000.0 / 1000.0) as u64;
        let expected = now + weight;
        assert_eq!(bucket.tat.load(Ordering::Relaxed), expected);
    }

    #[test]
    fn bucket_check_admits_within_burst() {
        let bucket = BandwidthBucket::new(1000, 1000); // 1s burst
        let now = 1_000_000_000u64;

        // Spend 1500 bytes: 1.5s of debt at 1000 bps
        bucket.spend(now, 1500);

        // debt = 1.5s, burst = 1s → rejected
        assert!(!bucket.check(now));

        // debt = 1.5s, but 0.6s have passed → debt = 0.9s < 1s burst → admitted
        assert!(bucket.check(now + 600_000_000));
    }

    #[test]
    fn bucket_check_zero_burst_rejects_any_debt() {
        let bucket = BandwidthBucket::new(1000, 0);
        let now = 1_000_000_000u64;

        bucket.spend(now, 1);

        // Any debt with burst=0 should reject
        assert!(!bucket.check(now));

        // After enough time passes, debt is paid off
        let weight = (1.0 * 1_000_000_000.0 / 1000.0) as u64;
        assert!(bucket.check(now + weight));
    }

    #[test]
    fn bucket_recovery_after_debt() {
        let bucket = BandwidthBucket::new(1000, 0);
        let now = 1_000_000_000u64;

        bucket.spend(now, 2000);

        // 2000 bytes at 1000 bps = 2s of debt
        assert!(!bucket.check(now));
        assert!(!bucket.check(now + 1_500_000_000));

        // After 2s, debt is fully paid → admitted
        assert!(bucket.check(now + 2_000_000_000));
    }

    #[test]
    fn bandwidth_check_rejects_correct_variant() {
        // Use generous burst so only the scope bucket overflows.
        // Global = 1000 bps, usecase = 500 bps, scope = 250 bps; burst = 2s.
        // Spending 600 bytes:
        //   global debt = 600/1000 = 0.6s < 2s → admits
        //   usecase debt = 600/500 = 1.2s < 2s → admits
        //   scope debt  = 600/250 = 2.4s > 2s → rejects
        let limiter = BandwidthRateLimiter::new(BandwidthLimits {
            global_bps: Some(1000),
            usecase_pct: Some(50),
            scope_pct: Some(25),
            burst_ms: 2000,
            ..Default::default()
        });

        let context = make_context();
        let handle = limiter.handle(&context);
        handle.record(600);

        let rejection = limiter.check(&context);
        assert_eq!(rejection, Some(RateLimitRejection::BandwidthScope));
    }

    // --- Throughput ---

    #[test]
    fn throughput_check_counts_admitted() {
        let limiter = ThroughputRateLimiter::new(ThroughputLimits {
            global_rps: Some(1000),
            ..Default::default()
        });

        assert_eq!(limiter.total_admitted.load(Ordering::Relaxed), 0);

        let context = make_context();
        assert!(limiter.check(&context).is_none());
        assert!(limiter.check(&context).is_none());

        assert_eq!(limiter.total_admitted.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn throughput_rejected_does_not_count() {
        let limiter = ThroughputRateLimiter::new(ThroughputLimits {
            global_rps: Some(1),
            burst: 0,
            ..Default::default()
        });

        let context = make_context();
        // First call admitted (consumes the one token), second rejected.
        assert!(limiter.check(&context).is_none());
        assert!(limiter.check(&context).is_some());

        assert_eq!(limiter.total_admitted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn bandwidth_rejection_does_not_count_throughput() {
        let limiter = RateLimiter::new(RateLimits {
            throughput: ThroughputLimits {
                global_rps: Some(1000),
                ..Default::default()
            },
            bandwidth: BandwidthLimits {
                global_bps: Some(1),
                burst_ms: 0,
                ..Default::default()
            },
        });

        // Put the bandwidth bucket in debt so the check rejects.
        let context = make_context();
        let handle = limiter.bandwidth_handle(&context);
        handle.record(1_000_000);

        assert!(!limiter.check(&context, None));

        assert_eq!(limiter.throughput.total_admitted.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn rate_limiter_accessors_with_config() {
        let rate_limiter = RateLimiter::new(RateLimits {
            throughput: ThroughputLimits {
                global_rps: Some(500),
                ..Default::default()
            },
            bandwidth: BandwidthLimits {
                global_bps: Some(1_000_000),
                ..Default::default()
            },
        });

        assert_eq!(rate_limiter.bandwidth_limit(), Some(1_000_000));
        assert_eq!(rate_limiter.throughput_limit(), Some(500));
    }

    #[test]
    fn rate_limiter_accessors_no_limits() {
        let rate_limiter = RateLimiter::new(RateLimits::default());

        assert_eq!(rate_limiter.bandwidth_limit(), None);
        assert_eq!(rate_limiter.throughput_limit(), None);
    }
}
