//! Blackbox tests for limits and restrictions.
//!
//! These tests assert safety-related behavior of the objectstore-server, such as enforcing
//! maximum object sizes, rate limiting, and killswitches.

use std::collections::BTreeMap;

use anyhow::Result;
use objectstore_server::config::{AuthZ, Config};
use objectstore_server::killswitches::{Killswitch, Killswitches};
use objectstore_server::rate_limits::{BandwidthLimits, RateLimits, ThroughputLimits, ThroughputRule};
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
    let server = TestServer::with_config(Config {
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(500),
                ..Default::default()
            },
            ..Default::default()
        },
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = objectstore_client::Client::new(server.url("/")).unwrap();
    let usecase = objectstore_client::Usecase::new("test");
    let session = client.session(usecase.for_organization(1)).unwrap();

    // Upload a 4KB payload to push the EWMA above the 500 bps limit.
    // A single 4096-byte upload in one 50ms tick produces an EWMA sample of ~16384 bps,
    // which is well above the 500 bps limit.
    let payload = bytes::Bytes::from(vec![0xABu8; 4096]);
    session
        .put(payload.clone())
        .compression(None)
        .send()
        .await
        .expect("first upload should succeed before EWMA catches up");

    // Wait a few EWMA ticks (50ms each) so the estimator incorporates the bandwidth.
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // The next request should be rejected with 429
    let result = session
        .put(payload.clone())
        .compression(None)
        .send()
        .await;
    let err = result.expect_err("expected 429 rate limit");
    match &err {
        objectstore_client::Error::Reqwest(e) => {
            assert_eq!(
                e.status(),
                Some(reqwest::StatusCode::TOO_MANY_REQUESTS),
                "expected 429, got: {err:?}"
            );
        }
        _ => panic!("expected reqwest error, got: {err:?}"),
    }

    // Wait long enough for the EWMA to decay below the limit.
    // With alpha=0.2, EWMA decays as 0.8^n per tick. Peak ~16384 needs ~16 ticks (800ms)
    // to drop below 500. Use 2s for CI reliability.
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // After decay, the request should succeed again
    session
        .put(payload.clone())
        .compression(None)
        .send()
        .await
        .expect("expected request to succeed after EWMA decay");

    Ok(())
}

#[tokio::test]
async fn test_bandwidth_usecase_pct_limit() -> Result<()> {
    let server = TestServer::with_config(Config {
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(100_000),
                usecase_pct: Some(1), // = 1000 bps per usecase
                ..Default::default()
            },
            ..Default::default()
        },
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = objectstore_client::Client::new(server.url("/")).unwrap();
    let usecase_test = objectstore_client::Usecase::new("test");
    let session_test = client
        .session(usecase_test.for_organization(1))
        .unwrap();

    // Upload a 4KB payload to push the per-usecase EWMA above the 1000 bps limit.
    // A single 4096-byte upload in one 50ms tick produces an EWMA sample of ~16384 bps,
    // which is well above the 1000 bps per-usecase limit but below the 100000 bps global limit.
    let payload = bytes::Bytes::from(vec![0xABu8; 4096]);
    session_test
        .put(payload.clone())
        .compression(None)
        .send()
        .await
        .expect("first upload should succeed before EWMA catches up");

    // Wait a few EWMA ticks so the estimator incorporates the bandwidth.
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // The next request to the same usecase should be rejected with 429
    let result = session_test
        .put(payload.clone())
        .compression(None)
        .send()
        .await;
    let err = result.expect_err("expected 429 rate limit for same usecase");
    match &err {
        objectstore_client::Error::Reqwest(e) => {
            assert_eq!(
                e.status(),
                Some(reqwest::StatusCode::TOO_MANY_REQUESTS),
                "expected 429, got: {err:?}"
            );
        }
        _ => panic!("expected reqwest error, got: {err:?}"),
    }

    // A different usecase should succeed (separate EWMA bucket)
    let usecase_other = objectstore_client::Usecase::new("other");
    let session_other = client
        .session(usecase_other.for_organization(1))
        .unwrap();
    session_other
        .put(payload.clone())
        .compression(None)
        .send()
        .await
        .expect("different usecase should succeed");

    // Wait for the EWMA to decay below the limit
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // After decay, the request to the original usecase should succeed again
    session_test
        .put(payload.clone())
        .compression(None)
        .send()
        .await
        .expect("expected request to succeed after EWMA decay");

    Ok(())
}

#[tokio::test]
async fn test_bandwidth_scope_pct_limit() -> Result<()> {
    let server = TestServer::with_config(Config {
        rate_limits: RateLimits {
            bandwidth: BandwidthLimits {
                global_bps: Some(100_000),
                scope_pct: Some(1), // = 1000 bps per scope
                ..Default::default()
            },
            ..Default::default()
        },
        auth: AuthZ {
            enforce: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;

    let client = objectstore_client::Client::new(server.url("/")).unwrap();
    let usecase = objectstore_client::Usecase::new("test");
    let session_org1 = client
        .session(usecase.for_organization(1))
        .unwrap();

    // Upload a 4KB payload to push the per-scope EWMA above the 1000 bps limit.
    // A single 4096-byte upload in one 50ms tick produces an EWMA sample of ~16384 bps,
    // which is well above the 1000 bps per-scope limit but below the 100000 bps global limit.
    let payload = bytes::Bytes::from(vec![0xABu8; 4096]);
    session_org1
        .put(payload.clone())
        .compression(None)
        .send()
        .await
        .expect("first upload should succeed before EWMA catches up");

    // Wait a few EWMA ticks so the estimator incorporates the bandwidth.
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // The next request to the same scope should be rejected with 429
    let result = session_org1
        .put(payload.clone())
        .compression(None)
        .send()
        .await;
    let err = result.expect_err("expected 429 rate limit for same scope");
    match &err {
        objectstore_client::Error::Reqwest(e) => {
            assert_eq!(
                e.status(),
                Some(reqwest::StatusCode::TOO_MANY_REQUESTS),
                "expected 429, got: {err:?}"
            );
        }
        _ => panic!("expected reqwest error, got: {err:?}"),
    }

    // A different scope should succeed (separate EWMA bucket)
    let session_org2 = client
        .session(usecase.for_organization(2))
        .unwrap();
    session_org2
        .put(payload.clone())
        .compression(None)
        .send()
        .await
        .expect("different scope should succeed");

    // Wait for the EWMA to decay below the limit
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // After decay, the request to the original scope should succeed again
    session_org1
        .put(payload.clone())
        .compression(None)
        .send()
        .await
        .expect("expected request to succeed after EWMA decay");

    Ok(())
}
