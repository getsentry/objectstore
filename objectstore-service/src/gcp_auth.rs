//! Prefetching token refresh for GCP authentication.
//!
//! [`PrefetchingTokenProvider`] wraps a [`gcp_auth::TokenProvider`] and refreshes tokens in the
//! background before they expire, so callers always get a cached token without blocking on a
//! refresh.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// How far ahead of expiry to start refreshing.
///
/// IMPORTANT: This must be lower than the implementation-defined 30s that gcp_auth uses as a safety
/// margin when determining token expiry. Otherwise, we will end up in a refresh loop.
const REFRESH_AHEAD: Duration = Duration::from_secs(20);

/// When a token has less than this remaining, callers wait for the next refresh.
const WAIT_THRESHOLD: Duration = Duration::from_secs(10);

/// Backoff between retry attempts when a refresh fails.
const RETRY_BACKOFF: Duration = Duration::from_secs(1);

type TokenResult = Result<CachedToken, TokenError>;

/// A cached token together with its expiry deadline in monotonic time.
#[derive(Clone, Debug)]
struct CachedToken {
    token: Arc<gcp_auth::Token>,
    /// Deadline in [`tokio::time::Instant`] terms so tests with paused time work correctly.
    deadline: Instant,
}

impl CachedToken {
    /// Creates a cached token, converting the wall-clock expiry to a tokio instant.
    fn new(token: Arc<gcp_auth::Token>) -> Self {
        let remaining = (token.expires_at() - Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO);

        Self {
            token,
            deadline: Instant::now() + remaining,
        }
    }
}

/// A token provider error that can be cloned and sent across threads.
#[derive(Clone, Debug, thiserror::Error)]
#[error(transparent)]
pub struct TokenError {
    inner: Arc<gcp_auth::Error>,
}

impl From<gcp_auth::Error> for TokenError {
    fn from(err: gcp_auth::Error) -> Self {
        Self {
            inner: Arc::new(err),
        }
    }
}

impl From<TokenError> for gcp_auth::Error {
    fn from(err: TokenError) -> Self {
        gcp_auth::Error::Other("prefetching token refresh failed", Box::new(err))
    }
}

/// A token provider that refreshes tokens in the background before they expire.
///
/// Wraps a [`gcp_auth::TokenProvider`] and maintains a cached token that is refreshed
/// ahead of its expiry. Callers get the cached token without blocking on a refresh
/// unless the token is about to expire.
///
/// Scopes are fixed at construction time. Calls to [`token()`](gcp_auth::TokenProvider::token)
/// with different scopes fall through to the inner provider without caching.
pub struct PrefetchingTokenProvider {
    scopes: Vec<&'static str>,
    rx: watch::Receiver<TokenResult>,
    provider: Arc<dyn gcp_auth::TokenProvider>,
    task: JoinHandle<()>,
}

impl Drop for PrefetchingTokenProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl std::fmt::Debug for PrefetchingTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefetchingTokenProvider")
            .field("scopes", &self.scopes)
            .finish_non_exhaustive()
    }
}

impl PrefetchingTokenProvider {
    /// Creates a new prefetching token provider using default GCP authentication.
    ///
    /// Calls [`gcp_auth::provider()`] to discover credentials, then wraps the resulting
    /// provider with prefetching refresh for the given scopes.
    pub async fn gcp_auth(scopes: &'static [&'static str]) -> Result<Self, gcp_auth::Error> {
        let provider = gcp_auth::provider().await?;
        Self::new(provider, scopes).await
    }

    /// Wraps an existing token provider with prefetching refresh.
    ///
    /// Fetches an initial token for the given scopes, then spawns a background task
    /// that refreshes the token before it expires.
    pub async fn new(
        provider: Arc<dyn gcp_auth::TokenProvider>,
        scopes: &'static [&'static str],
    ) -> Result<Self, gcp_auth::Error> {
        let initial_token = provider.token(scopes).await?;
        let cached = CachedToken::new(initial_token);
        let initial_deadline = cached.deadline;

        let (tx, rx) = watch::channel(Ok(cached));
        let task = tokio::spawn(refresh_loop(provider.clone(), scopes, tx, initial_deadline));

        Ok(Self {
            scopes: scopes.to_vec(),
            rx,
            provider,
            task,
        })
    }
}

#[async_trait]
impl gcp_auth::TokenProvider for PrefetchingTokenProvider {
    /// Returns a cached token if it has sufficient remaining lifetime.
    ///
    /// If every requested scope is covered by the prefetching scopes, returns the cached
    /// token — which may be granted for a superset of the requested scopes. If the token
    /// is near expiry (within [`WAIT_THRESHOLD`]), waits for the background refresh to
    /// complete. Any scope not covered falls through to the inner provider.
    async fn token(&self, scopes: &[&str]) -> Result<Arc<gcp_auth::Token>, gcp_auth::Error> {
        if !scopes.iter().all(|s| self.scopes.contains(s)) {
            return self.provider.token(scopes).await;
        }

        let mut rx = self.rx.clone();
        loop {
            let cached = rx.borrow_and_update().clone()?;

            let remaining = cached.deadline.saturating_duration_since(Instant::now());
            if remaining > WAIT_THRESHOLD {
                return Ok(cached.token);
            }

            // A refresh is in progress; wait for the next update and re-validate.
            tracing::debug!("token near expiry, waiting for refresh");
            if rx.changed().await.is_err() {
                // Background task was aborted; return whatever is in the channel now.
                return rx.borrow().clone().map(|c| c.token).map_err(From::from);
            }
        }
    }

    /// Delegates to the inner provider.
    async fn project_id(&self) -> Result<Arc<str>, gcp_auth::Error> {
        self.provider.project_id().await
    }
}

async fn refresh_loop(
    provider: Arc<dyn gcp_auth::TokenProvider>,
    scopes: &'static [&'static str],
    tx: watch::Sender<TokenResult>,
    mut deadline: Instant,
) {
    loop {
        let remaining = deadline
            .saturating_duration_since(Instant::now())
            .saturating_sub(REFRESH_AHEAD);

        if remaining > Duration::ZERO {
            tokio::time::sleep(remaining).await;
        }

        match provider.token(scopes).await {
            Ok(new_token) => {
                tracing::debug!("prefetching token refresh succeeded");
                let cached = CachedToken::new(new_token);
                deadline = cached.deadline;
                let _ = tx.send(Ok(cached));
            }
            Err(err) => {
                tracing::warn!(error = %err, "token prefetch failed");

                // Only broadcast the error if the token is expired.
                if deadline <= Instant::now() {
                    let _ = tx.send(Err(TokenError::from(err)));
                }

                tokio::time::sleep(RETRY_BACKOFF).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use gcp_auth::TokenProvider;

    use super::*;

    /// A mock token provider that returns pre-configured responses.
    struct MockTokenProvider {
        responses: Mutex<VecDeque<Result<Arc<gcp_auth::Token>, gcp_auth::Error>>>,
        project_id: Option<Arc<str>>,
    }

    impl MockTokenProvider {
        fn new(responses: Vec<Result<Arc<gcp_auth::Token>, gcp_auth::Error>>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(responses.into()),
                project_id: Some(Arc::from("test-project")),
            })
        }
    }

    impl std::fmt::Debug for MockTokenProvider {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockTokenProvider").finish()
        }
    }

    #[async_trait]
    impl gcp_auth::TokenProvider for MockTokenProvider {
        async fn token(&self, _scopes: &[&str]) -> Result<Arc<gcp_auth::Token>, gcp_auth::Error> {
            let mut responses = self.responses.lock().expect("lock poisoned");
            responses
                .pop_front()
                .unwrap_or_else(|| Err(gcp_auth::Error::Str("no more mock responses")))
        }

        async fn project_id(&self) -> Result<Arc<str>, gcp_auth::Error> {
            self.project_id
                .clone()
                .ok_or(gcp_auth::Error::Str("no project id"))
        }
    }

    fn make_token(expires_in: Duration) -> Arc<gcp_auth::Token> {
        let json = format!(
            r#"{{"access_token":"test-token-{}","expires_in":{}}}"#,
            expires_in.as_secs(),
            expires_in.as_secs()
        );
        Arc::new(serde_json::from_str::<gcp_auth::Token>(&json).expect("valid token json"))
    }

    fn make_error() -> gcp_auth::Error {
        gcp_auth::Error::Str("mock error")
    }

    #[tokio::test]
    async fn constructor_fetches_initial_token() {
        let token = make_token(Duration::from_secs(3600));
        let mock = MockTokenProvider::new(vec![Ok(token)]);

        let provider = PrefetchingTokenProvider::new(
            mock,
            &["https://www.googleapis.com/auth/cloud-platform"],
        )
        .await;

        assert!(provider.is_ok());
    }

    #[tokio::test]
    async fn constructor_fails_on_error() {
        let mock = MockTokenProvider::new(vec![Err(make_error())]);

        let result = PrefetchingTokenProvider::new(
            mock,
            &["https://www.googleapis.com/auth/cloud-platform"],
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn returns_cached_token() {
        let token = make_token(Duration::from_secs(3600));
        let mock = MockTokenProvider::new(vec![Ok(token.clone())]);

        let provider = PrefetchingTokenProvider::new(mock, &["scope-a"])
            .await
            .expect("constructor should succeed");

        let t1 = provider
            .token(&["scope-a"])
            .await
            .expect("should return token");
        let t2 = provider
            .token(&["scope-a"])
            .await
            .expect("should return token");

        assert_eq!(t1.as_str(), t2.as_str());
    }

    #[tokio::test(start_paused = true)]
    async fn background_refresh_before_expiry() {
        let initial = make_token(Duration::from_secs(25));
        let refreshed = make_token(Duration::from_secs(3600));

        let mock = MockTokenProvider::new(vec![Ok(initial), Ok(refreshed)]);

        let provider = PrefetchingTokenProvider::new(mock, &["scope"])
            .await
            .expect("constructor should succeed");

        // Advance past the refresh point (25s - 20s REFRESH_AHEAD = 5s).
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;

        let token = provider
            .token(&["scope"])
            .await
            .expect("should return token");
        assert_eq!(token.as_str(), "test-token-3600");
    }

    #[tokio::test(start_paused = true)]
    async fn waits_when_near_expiry() {
        // Token that expires in 8 seconds (below WAIT_THRESHOLD of 10s).
        let initial = make_token(Duration::from_secs(8));
        let refreshed = make_token(Duration::from_secs(3600));

        let mock = MockTokenProvider::new(vec![Ok(initial), Ok(refreshed)]);

        let provider = PrefetchingTokenProvider::new(mock, &["scope"])
            .await
            .expect("constructor should succeed");

        // The background task should fire almost immediately (8s - 20s = 0s sleep).
        // Yield to let it run.
        tokio::task::yield_now().await;

        let token = provider
            .token(&["scope"])
            .await
            .expect("should return token");
        assert_eq!(token.as_str(), "test-token-3600");
    }

    #[tokio::test(start_paused = true)]
    async fn retries_silently_while_token_valid() {
        // Token with 60s expiry: after 40s (sleep = 60 - 20 = 40), refresh is attempted.
        let initial = make_token(Duration::from_secs(60));
        let after_retry = make_token(Duration::from_secs(3600));

        let mock = MockTokenProvider::new(vec![
            Ok(initial),
            Err(make_error()), // first refresh attempt fails
            Ok(after_retry),   // retry succeeds
        ]);

        let provider = PrefetchingTokenProvider::new(mock, &["scope"])
            .await
            .expect("constructor should succeed");

        // Advance to trigger the refresh loop.
        tokio::time::advance(Duration::from_secs(41)).await;
        tokio::task::yield_now().await;

        // The initial token is still valid (60 - 41 = 19s remaining > 10s threshold).
        let token = provider
            .token(&["scope"])
            .await
            .expect("should return token");
        assert_eq!(token.as_str(), "test-token-60");

        // Advance past the retry backoff so the retry succeeds.
        tokio::time::advance(RETRY_BACKOFF + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;

        let token = provider
            .token(&["scope"])
            .await
            .expect("should return token");
        assert_eq!(token.as_str(), "test-token-3600");
    }

    #[tokio::test(start_paused = true)]
    async fn broadcasts_error_when_expired() {
        let initial = make_token(Duration::from_secs(25));

        let mock = MockTokenProvider::new(vec![Ok(initial), Err(make_error())]);

        let provider = PrefetchingTokenProvider::new(mock, &["scope"])
            .await
            .expect("constructor should succeed");

        // Advance 6s: sleep(5s) fires, first refresh fails (error swallowed, token valid),
        // retry backoff (1s) elapses, second refresh fails ("no more mock responses",
        // also swallowed — token still valid at ~19s remaining).
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;

        // Advance 10 × 1s to drain time until the token has <WAIT_THRESHOLD remaining.
        // Each step wakes a RETRY_BACKOFF sleep, fires another failing refresh, and swallows it.
        for _ in 0..10 {
            tokio::time::advance(RETRY_BACKOFF + Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
        }

        // Token now has ~9s remaining (<WAIT_THRESHOLD), so token() blocks on rx.changed().
        // Tokio auto-advances time to service the sleeping background task, which keeps
        // retrying until the token fully expires at t=25s, at which point the error is
        // broadcast and rx.changed() resolves.
        let result = provider.token(&["scope"]).await;
        assert!(result.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn recovers_after_error() {
        let initial = make_token(Duration::from_secs(60));
        let recovered = make_token(Duration::from_secs(3600));

        let mock = MockTokenProvider::new(vec![
            Ok(initial),
            Err(make_error()), // first refresh fails
            Ok(recovered),     // retry succeeds
        ]);

        let provider = PrefetchingTokenProvider::new(mock, &["scope"])
            .await
            .expect("constructor should succeed");

        // Trigger the refresh loop. Sleep = 60 - 20 = 40s.
        tokio::time::advance(Duration::from_secs(41)).await;
        tokio::task::yield_now().await;

        // First refresh fails, token still valid (~19s remaining).
        // Advance past retry backoff.
        tokio::time::advance(RETRY_BACKOFF + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;

        // Retry succeeds with recovered token.
        let token = provider.token(&["scope"]).await.expect("should recover");
        assert_eq!(token.as_str(), "test-token-3600");
    }

    #[tokio::test]
    async fn drop_cancels_task() {
        let token = make_token(Duration::from_secs(3600));
        let mock = MockTokenProvider::new(vec![Ok(token)]);

        let provider = PrefetchingTokenProvider::new(mock, &["scope"])
            .await
            .expect("constructor should succeed");

        let mut rx = provider.rx.clone();
        drop(provider);

        // The background task should be aborted, so changed() returns an error.
        let result = rx.changed().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn project_id_delegates() {
        let token = make_token(Duration::from_secs(3600));
        let mock = MockTokenProvider::new(vec![Ok(token)]);

        let provider = PrefetchingTokenProvider::new(mock, &["scope"])
            .await
            .expect("constructor should succeed");

        let project_id = provider
            .project_id()
            .await
            .expect("should return project id");
        assert_eq!(&*project_id, "test-project");
    }

    #[tokio::test]
    async fn mismatched_scopes_fall_through() {
        let cached_token = make_token(Duration::from_secs(3600));
        let fallthrough_token = make_token(Duration::from_secs(60));

        let mock = MockTokenProvider::new(vec![Ok(cached_token), Ok(fallthrough_token)]);

        let provider = PrefetchingTokenProvider::new(mock, &["scope-a"])
            .await
            .expect("constructor should succeed");

        // Call with different scopes — should fall through to inner provider.
        let token = provider
            .token(&["scope-b"])
            .await
            .expect("should return token");
        assert_eq!(token.as_str(), "test-token-60");
    }
}
