use std::sync::Arc;

use super::s3_compatible::{S3Compatible, Token, TokenProvider};

impl TokenProvider for Arc<dyn gcp_auth::TokenProvider> {
    async fn get_token(&self) -> anyhow::Result<impl Token> {
        let token = self
            .token(&["https://www.googleapis.com/auth/devstorage.read_write"])
            .await?;
        Ok(token)
    }
}

impl Token for Arc<gcp_auth::Token> {
    fn as_str(&self) -> &str {
        gcp_auth::Token::as_str(self)
    }
}

pub async fn gcs(bucket: &str) -> anyhow::Result<super::BoxedBackend> {
    let token_provider = gcp_auth::provider().await?;
    Ok(Box::new(S3Compatible::new(
        "https://storage.googleapis.com",
        bucket,
        token_provider,
    )))
}
