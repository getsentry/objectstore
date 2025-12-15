//! Blackbox tests for limits and restrictions.
//!
//! These tests assert safety-related behavior of the objectstore-server, such as enforcing
//! maximum object sizes, rate limiting, and killswitches.

use std::collections::BTreeMap;

use anyhow::Result;
use objectstore_server::config::Config;
use objectstore_server::killswitches::{Killswitch, Killswitches};
use objectstore_test::server::TestServer;

#[tokio::test]
async fn test_killswitches() -> Result<()> {
    let server = TestServer::with_config(Config {
        killswitches: Killswitches(vec![Killswitch {
            usecase: Some("blocked".to_string()),
            scopes: BTreeMap::from_iter([("org".to_string(), "42".to_string())]),
        }]),
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();

    // Object-level
    let response = client
        .get(server.url("/v1/objects/blocked/org=42;project=4711/foo"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);

    // Collection-level
    let response = client
        .post(server.url("/v1/objects/blocked/org=42;project=4711/"))
        .body("test data")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);

    // Sanity check: Allowed access on non-existing object
    let response = client
        .get(server.url("/v1/objects/allowed/org=43;project=4711/foo"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}
