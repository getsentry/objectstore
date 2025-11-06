use anyhow::Result;

use crate::config::Config;

pub async fn healthcheck(config: Config) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("http://{}/health", config.http_addr);

    tracing::debug!("sending healthcheck request to {}", url);
    let response = client.get(&url).send().await?;
    if !response.status().is_success() {
        anyhow::bail!("Bad Status: {}", response.status());
    }

    tracing::info!("OK");
    Ok(())
}
