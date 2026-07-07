use backoff::Error as BackoffError;
use backoff::future::retry;

use crate::backoff::default_backoff;
use crate::cache;

const EXCHANGE_INFO_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";

/// Fetch and cache the full exchangeInfo from Binance Futures.
pub async fn get_exchange_info(client: &reqwest::Client) -> anyhow::Result<Vec<u8>> {
    if let Some(raw) = cache::read("binance", "futures", Some(86400)) {
        return Ok(raw);
    }

    let raw = fetch(client).await?;
    cache::write("binance", "futures", &raw);
    Ok(raw)
}

async fn fetch(client: &reqwest::Client) -> anyhow::Result<Vec<u8>> {
    let op = || {
        let c = client.clone();
        async move {
            let resp =
                c.get(EXCHANGE_INFO_URL)
                    .send()
                    .await
                    .map_err(|e| BackoffError::Transient {
                        err: anyhow::anyhow!("exchange_info request: {e}"),
                        retry_after: None,
                    })?;

            let raw = resp.bytes().await.map_err(|e| BackoffError::Transient {
                err: anyhow::anyhow!("exchange_info read body: {e}"),
                retry_after: None,
            })?;

            Ok(raw.to_vec())
        }
    };

    retry(default_backoff(), op).await
}
