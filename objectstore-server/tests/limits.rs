//! Blackbox tests for limits and restrictions.
//!
//! These tests assert safety-related behavior of the objectstore-server, such as enforcing
//! maximum object sizes, rate limiting, and killswitches.

use std::collections::BTreeMap;

use anyhow::Result;
use objectstore_server::config::{AuthZ, Config};
use objectstore_server::killswitches::{Killswitch, Killswitches};
use objectstore_server::rate_limits::{
    BandwidthLimits, BandwidthRule, RateLimits, ThroughputLimits, ThroughputRule,
};
use objectstore_test::server::TestServer;

#[tokio::test]
async fn test_killswitches() -> Result<()> {
    let server = TestServer::with_config(Config {
        killswitches: Killswitches(vec![Killswitch {
            usecase: Some("blocked".to_string()),
            scopes: BTreeMap::from_iter([("org".to_string(), "42".to_string())]),
            service: Some("test-*".to_string()),
            service_matcher: Default::default(),
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
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

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
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

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
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

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
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

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
    // Set a very low global bandwidth limit (1000 bytes/sec).
    let server = TestServer::with_config(Config {
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(1_000),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // Upload a large object to saturate the bandwidth.
    let payload = vec![0u8; 100_000];
    let response = client
        .put(server.url("/v1/objects/test/org=1/bw-test"))
        .body(payload)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // Wait for the EWMA estimator to pick up the bandwidth spike.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // While the EWMA is still high, requests should be rate limited.
    let response = client
        .get(server.url("/v1/objects/test/org=1/bw-test"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Wait for the EWMA to decay below the limit.
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // After decay, requests should succeed again.
    let response = client
        .get(server.url("/v1/objects/test/org=1/bw-test"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_bandwidth_usecase_pct_limit() -> Result<()> {
    // Very high global limit, but only 0.01% per usecase = 1000 bytes/sec per usecase.
    let server = TestServer::with_config(Config {
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(10_000_000),
                usecase_pct: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // Upload a large object to saturate one usecase.
    let payload = vec![0u8; 100_000];
    let response = client
        .put(server.url("/v1/objects/heavy/org=1/bw-test"))
        .body(payload)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // Wait for the lazy EWMA to be computed on next check.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Same usecase should be rate limited.
    let response = client
        .get(server.url("/v1/objects/heavy/org=1/bw-test"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Different usecase should still succeed (global is not saturated).
    let response = client
        .get(server.url("/v1/objects/other/org=1/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn test_bandwidth_scope_pct_limit() -> Result<()> {
    // Very high global limit, but only 0.01% per scope = 1000 bytes/sec per scope.
    let server = TestServer::with_config(Config {
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(10_000_000),
                scope_pct: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // Upload a large object to saturate one scope.
    let payload = vec![0u8; 100_000];
    let response = client
        .put(server.url("/v1/objects/test/org=1/bw-test"))
        .body(payload)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // Wait for the lazy EWMA to be computed on next check.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Same scope should be rate limited.
    let response = client
        .get(server.url("/v1/objects/test/org=1/bw-test"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Different scope should still succeed.
    let response = client
        .get(server.url("/v1/objects/test/org=2/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn test_bandwidth_rule_limit() -> Result<()> {
    // No global limit, but a rule restricting a specific usecase+scope to 1000 bps.
    let server = TestServer::with_config(Config {
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                rules: vec![BandwidthRule {
                    usecase: Some("restricted".to_string()),
                    scopes: vec![("org".to_string(), "42".to_string())],
                    bps: Some(1_000),
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

    // Upload a large object matching the rule.
    let payload = vec![0u8; 100_000];
    let response = client
        .put(server.url("/v1/objects/restricted/org=42/bw-test"))
        .body(payload)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // Wait for the lazy EWMA to be computed on next check.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Request matching rule should be rate limited.
    let response = client
        .get(server.url("/v1/objects/restricted/org=42/bw-test"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Different usecase should not be affected.
    let response = client
        .get(server.url("/v1/objects/other/org=42/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    // Same usecase but different scope should not be affected.
    let response = client
        .get(server.url("/v1/objects/restricted/org=43/nonexistent"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}
