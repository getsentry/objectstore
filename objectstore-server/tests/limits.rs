//! Blackbox tests for limits and restrictions.
//!
//! These tests assert safety-related behavior of the objectstore-server, such as enforcing
//! maximum object sizes, rate limiting, and killswitches.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::Result;
use objectstore_server::config::{AuthZ, Config, Http, Service};
use objectstore_server::killswitches::{Killswitch, Killswitches};
use objectstore_server::rate_limits::{
    BandwidthLimits, RateLimits, ThroughputLimits, ThroughputRule,
};
use objectstore_server::usecases::{
    DurationPolicyConfig, ExpirationConfig, ManualPolicyConfig, UseCaseConfig, UseCases,
};
use objectstore_test::server::TestServer;

#[tokio::test]
async fn test_web_concurrency_limit() -> Result<()> {
    // Setting max_requests = 0 means every non-exempt request is rejected immediately
    // with 503, giving us a fully deterministic test without needing concurrent requests.
    let server = TestServer::with_config(Config {
        http: Http { max_requests: 0 },
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // Regular requests are rejected with 503 Service Unavailable.
    let response = client
        .get(server.url("/v1/objects/test/org=1/key"))
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "expected 503 for regular request at limit"
    );

    // Health endpoint bypasses the concurrency limit.
    let response = client.get(server.url("/health")).send().await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "/health must not be subject to the concurrency limit"
    );

    // Ready endpoint bypasses the concurrency limit.
    let response = client.get(server.url("/ready")).send().await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "/ready must not be subject to the concurrency limit"
    );

    Ok(())
}

#[tokio::test]
async fn test_killswitches() -> Result<()> {
    let server = TestServer::with_config(Config {
        killswitches: Killswitches::new(vec![Killswitch {
            usecase: Some("blocked".to_string()),
            scopes: BTreeMap::from_iter([("org".to_string(), "42".to_string())]),
            service: Some("test-*".to_string()),
        }]),
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // Object-level
    let response = client
        .get(server.url("/v1/objects/blocked/org=42;project=4711/foo"))
        .header("x-downstream-service", "test-service")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);

    // Collection-level
    let response = client
        .post(server.url("/v1/objects/blocked/org=42;project=4711/"))
        .header("x-downstream-service", "test-service")
        .body("test data")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);

    // Sanity check: Allowed access on non-existing object
    let response = client
        .get(server.url("/v1/objects/allowed/org=43;project=4711/foo"))
        .header("x-downstream-service", "test-service")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn test_throughput_global_rps_limit() -> Result<()> {
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        rate_limits: RateLimits {
            throughput: ThroughputLimits {
                global_rps: Some(2),
                burst: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // First three requests without waiting should succeed, using up both the regular and burst budget
    for _ in 0..3 {
        let response = client
            .get(server.url("/v1/objects/test/org=1/nonexistent"))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    }

    // Fourth request without waiting should be rate limited
    let response = client
        .get(server.url("/v1/objects/test/org=1/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Refill bucket
    tokio::time::sleep(Duration::from_secs(1)).await;

    // After waiting, request succeeds
    let response = client
        .get(server.url("/v1/objects/test/org=1/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn test_throughput_usecase_pct_limit() -> Result<()> {
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        rate_limits: RateLimits {
            throughput: ThroughputLimits {
                global_rps: Some(100),
                burst: 0,
                usecase_pct: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // First two requests to the same usecase without waiting should succeed
    for _ in 0..2 {
        let response = client
            .get(server.url("/v1/objects/test/org=1/nonexistent"))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    }

    // Third request to the same usecase without waiting should fail
    let response = client
        .get(server.url("/v1/objects/test/org=1/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Request to a different usecase should succeed
    let response = client
        .get(server.url("/v1/objects/other/org=1/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    // Refill bucket
    tokio::time::sleep(Duration::from_secs(1)).await;

    // After waiting, request to first usecase should succeed again
    let response = client
        .get(server.url("/v1/objects/test/org=1/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn test_throughput_scope_pct_limit() -> Result<()> {
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        rate_limits: RateLimits {
            throughput: ThroughputLimits {
                global_rps: Some(100),
                burst: 0,
                scope_pct: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // First two requests to the same scope without waiting should succeed
    for _ in 0..2 {
        let response = client
            .get(server.url("/v1/objects/test/org=1/nonexistent"))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    }

    // Third request to the same scope without waiting should fail
    let response = client
        .get(server.url("/v1/objects/test/org=1/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Request to a different scope should succeed
    let response = client
        .get(server.url("/v1/objects/test/org=2/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    // Refill bucket
    tokio::time::sleep(Duration::from_secs(1)).await;

    // After waiting, request to first scope should succeed again
    let response = client
        .get(server.url("/v1/objects/test/org=1/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn test_throughput_rule() -> Result<()> {
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        rate_limits: RateLimits {
            throughput: ThroughputLimits {
                global_rps: None,
                burst: 0,
                rules: vec![ThroughputRule {
                    usecase: Some("restricted".to_string()),
                    scopes: vec![("org".to_string(), "42".to_string())],
                    rps: Some(1),
                    pct: None,
                }],
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // First request matching rule should succeed
    let response = client
        .get(server.url("/v1/objects/restricted/org=42/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    // Second request matching rule should fail
    let response = client
        .get(server.url("/v1/objects/restricted/org=42/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Different usecase should not be affected by rule
    let response = client
        .get(server.url("/v1/objects/other/org=42/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    // Same usecase but different scope should not be affected by rule
    let response = client
        .get(server.url("/v1/objects/restricted/org=43/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    // Refill bucket
    tokio::time::sleep(Duration::from_secs(1)).await;

    // After waiting, request matching rule should succeed again
    let response = client
        .get(server.url("/v1/objects/restricted/org=42/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn test_bandwidth_global_bps_limit() -> Result<()> {
    // 100 bps with 0 burst: 100 bytes creates 1s of debt → immediate rejection.
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(100),
                burst_ms: 0,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let payload = vec![0xABu8; 100];

    // First upload succeeds (admitted before any debt exists).
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    // Immediately after, the bucket has 1s of debt → rejected.
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Wait for debt to drain (100 bytes / 100 bps = 1s, use 2s for CI reliability).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // After recovery, request succeeds again.
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    Ok(())
}

#[tokio::test]
async fn test_bandwidth_burst_tolerance() -> Result<()> {
    // 100 bps with 1.5s burst → burst budget = 150 bytes.
    // A 100-byte upload creates 1s of debt, within 1.5s burst → next request admitted.
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(100),
                burst_ms: 1500,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let payload = vec![0xABu8; 100];

    // First upload creates 1s of debt, within the 1.5s burst tolerance.
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    // Next request is still admitted because debt ≤ tau.
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    // Now debt ≈ 2s > 1.5s burst → rejected.
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    Ok(())
}

#[tokio::test]
async fn test_bandwidth_report_only() -> Result<()> {
    // With report_only, requests succeed even when debt exceeds burst tolerance.
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(100),
                burst_ms: 0,
                report_only: true,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let payload = vec![0xABu8; 100];

    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    // With report_only, the request should succeed even though debt exceeds the limit.
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    // KEDA metrics should still report the limit.
    let response = client.get(server.url("/keda")).send().await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let body = response.text().await?;
    assert!(
        body.contains("objectstore_bandwidth_limit 100"),
        "missing bandwidth_limit"
    );

    Ok(())
}

#[tokio::test]
async fn test_bandwidth_usecase_pct_limit() -> Result<()> {
    // 10_000 bps global, 1% per usecase = 100 bps per usecase.
    // burst_ms=100 absorbs global debt (100/10000 = 0.01s) but the
    // per-usecase debt (100/100 = 1s) exceeds it → usecase rejects.
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(10_000),
                burst_ms: 100,
                usecase_pct: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let payload = vec![0xABu8; 100];

    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    // Same usecase should be rejected (per-usecase debt).
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Different usecase has a separate bucket → admitted.
    let response = client
        .post(server.url("/v1/objects/other/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    // Wait for per-usecase debt to drain (100/100 = 1s, use 2s for CI).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // After recovery, original usecase admits again.
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    Ok(())
}

#[tokio::test]
async fn test_bandwidth_scope_pct_limit() -> Result<()> {
    // 10_000 bps global, 1% per scope = 100 bps per scope.
    // burst_ms=100 absorbs global debt but per-scope debt exceeds it.
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(10_000),
                burst_ms: 100,
                scope_pct: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let payload = vec![0xABu8; 100];

    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    // Same scope should be rejected (per-scope debt).
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Different scope has a separate bucket → admitted.
    let response = client
        .post(server.url("/v1/objects/test/org=2/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    // Wait for per-scope debt to drain (100/100 = 1s, use 2s for CI).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // After recovery, original scope admits again.
    let response = client
        .post(server.url("/v1/objects/test/org=1/"))
        .body(payload.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);

    Ok(())
}

#[tokio::test]
async fn test_batch_at_capacity_returns_429() -> Result<()> {
    // With max_concurrency=0 the service has no permits available, so
    // BatchExecutor::new() returns AtCapacity and the endpoint responds 429.
    let server = TestServer::with_config(Config {
        service: Service {
            max_concurrency: 0,
            ..Default::default()
        },
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    let body = "\
        --boundary\r\n\
         x-sn-batch-operation-key: some-key\r\n\
         x-sn-batch-operation-kind: get\r\n\
         \r\n\
         \r\n\
         --boundary--\r\n";

    let response = client
        .post(server.url("/v1/objects:batch/test/org=1/"))
        .header("content-type", "multipart/form-data; boundary=boundary")
        .body(body)
        .send()
        .await?;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "expected 429 when service has no available permits"
    );

    Ok(())
}

// --- Use case policy tests ---

#[tokio::test]
async fn test_usecase_expiration_policy() -> Result<()> {
    let server = TestServer::with_config(Config {
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        usecases: UseCases(HashMap::from([
            (
                "attachments".to_owned(),
                UseCaseConfig {
                    expiration: ExpirationConfig {
                        manual: ManualPolicyConfig { allowed: false },
                        ttl: DurationPolicyConfig {
                            allowed: true,
                            max: Some(Duration::from_hours(90 * 24)),
                        },
                        tti: DurationPolicyConfig {
                            allowed: false,
                            max: None,
                        },
                    },
                },
            ),
            (
                "debug-files".to_owned(),
                UseCaseConfig {
                    expiration: ExpirationConfig {
                        manual: ManualPolicyConfig { allowed: false },
                        ttl: DurationPolicyConfig {
                            allowed: false,
                            max: None,
                        },
                        tti: DurationPolicyConfig {
                            allowed: true,
                            max: Some(Duration::from_hours(90 * 24)),
                        },
                    },
                },
            ),
            (
                "avatars".to_owned(),
                UseCaseConfig {
                    expiration: ExpirationConfig {
                        manual: ManualPolicyConfig { allowed: true },
                        ttl: DurationPolicyConfig {
                            allowed: false,
                            max: None,
                        },
                        tti: DurationPolicyConfig {
                            allowed: false,
                            max: None,
                        },
                    },
                },
            ),
        ])),
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // Matching requests — one per use case with its permitted policy.
    let response = client
        .put(server.url("/v1/objects/attachments/org=1/key"))
        .header("x-sn-expiration", "ttl:30d")
        .body("data")
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "attachments: ttl within cap"
    );

    let response = client
        .put(server.url("/v1/objects/debug-files/org=1/key"))
        .header("x-sn-expiration", "tti:30d")
        .body("data")
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "debug-files: tti within cap"
    );

    let response = client
        .put(server.url("/v1/objects/avatars/org=1/key"))
        .header("x-sn-expiration", "manual")
        .body("data")
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "avatars: manual"
    );

    // Duration exceeded — TTL and TTI caps.
    let response = client
        .put(server.url("/v1/objects/attachments/org=1/key"))
        .header("x-sn-expiration", "ttl:100d")
        .body("data")
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "attachments: ttl exceeds 90d cap"
    );

    let response = client
        .put(server.url("/v1/objects/debug-files/org=1/key"))
        .header("x-sn-expiration", "tti:100d")
        .body("data")
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "debug-files: tti exceeds 90d cap"
    );

    // Wrong policy — one disallowed policy request per use case.
    let response = client
        .put(server.url("/v1/objects/attachments/org=1/key"))
        .header("x-sn-expiration", "manual")
        .body("data")
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "attachments: manual not allowed"
    );

    let response = client
        .put(server.url("/v1/objects/debug-files/org=1/key"))
        .header("x-sn-expiration", "ttl:30d")
        .body("data")
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "debug-files: ttl not allowed"
    );

    let response = client
        .put(server.url("/v1/objects/avatars/org=1/key"))
        .header("x-sn-expiration", "tti:30d")
        .body("data")
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "avatars: tti not allowed"
    );

    // Unconfigured use case — any policy is accepted.
    let response = client
        .put(server.url("/v1/objects/uploads/org=1/key"))
        .header("x-sn-expiration", "manual")
        .body("data")
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "uploads: unconfigured use case accepts manual"
    );

    Ok(())
}

// --- KEDA endpoint tests ---

#[tokio::test]
async fn test_keda() -> Result<()> {
    let server = TestServer::with_config(Config {
        rate_limits: RateLimits {
            throughput: ThroughputLimits {
                global_rps: Some(1000),
                ..Default::default()
            },
            bandwidth: BandwidthLimits {
                global_bps: Some(10_000_000),
                ..Default::default()
            },
        },
        http: Http { max_requests: 0 },
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // /keda must bypass the web concurrency limit (max_requests: 0 rejects everything else).
    let response = client
        .get(server.url("/v1/objects/test/org=1/key"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    let response = client.get(server.url("/keda")).send().await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/plain; version=0.0.4; charset=utf-8"
    );

    let body = response.text().await?;

    // Optional gauges are present when the corresponding limits are configured.
    assert!(
        body.contains("objectstore_bandwidth_limit 10000000"),
        "missing bandwidth_limit"
    );
    assert!(
        body.contains("objectstore_throughput_limit 1000"),
        "missing throughput_limit"
    );

    // Counters are present.
    assert!(
        body.contains("objectstore_bytes_total "),
        "missing bytes_total"
    );
    assert!(
        body.contains("objectstore_requests_total "),
        "missing requests_total"
    );

    Ok(())
}
