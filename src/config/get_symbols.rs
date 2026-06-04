use crate::AppError;

/// Resolve symbols to download: use configured list, or auto-fetch from exchangeInfo if empty.
pub async fn resolve_symbols(
    client: &reqwest::Client,
    configured: &[String],
) -> Result<Vec<String>, AppError> {
    if !configured.is_empty() {
        return Ok(configured.to_vec());
    }
    super::get_exchange_info::get_trading_symbols(client).await
}

/// Build a reqwest::Client with sensible timeouts and pool size.
pub fn build_client(max_concurrent: usize) -> Result<reqwest::Client, AppError> {
    let b = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .timeout(std::time::Duration::from_secs(30))
        .pool_max_idle_per_host(max_concurrent)
        .pool_idle_timeout(std::time::Duration::from_secs(30));
    b.build().map_err(AppError::Http)
}
