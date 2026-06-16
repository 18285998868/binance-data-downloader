use std::time::Duration;

/// Build a reqwest::Client that can reach `data.binance.vision`.
///
/// Tries direct TCP first; if unreachable, falls back to SOCKS5 proxy
/// at `127.0.0.1:7890`.
pub async fn get_test_client() -> reqwest::Client {
    if let Some(client) = try_direct().await {
        return client;
    }
    try_proxy().await.expect(
        "cannot reach data.binance.vision \
         (tried direct and SOCKS5 proxy at 127.0.0.1:7890)",
    )
}

/// Build a Binance futures daily kline download URL.
pub fn download_url(symbol: &str, timeframe: &str, date: &str) -> String {
    format!(
        "https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{date}.zip"
    )
}

async fn try_direct() -> Option<reqwest::Client> {
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect("data.binance.vision:443"),
    )
    .await
    .ok()?
    .ok()?;
    let client = reqwest::Client::builder()
        .user_agent("binance-data-downloader-test/0.1")
        .timeout(Duration::from_secs(30))
        .build()
        .ok()?;
    tracing::info!("integration test: using direct connection");
    Some(client)
}

async fn try_proxy() -> Option<reqwest::Client> {
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect("127.0.0.1:7890"),
    )
    .await
    .ok()?
    .ok()?;
    let client = reqwest::Client::builder()
        .user_agent("binance-data-downloader-test/0.1")
        .timeout(Duration::from_secs(60))
        .proxy(reqwest::Proxy::all("socks5h://127.0.0.1:7890").ok()?)
        .build()
        .ok()?;
    tracing::info!("integration test: using SOCKS5 proxy at 127.0.0.1:7890");
    Some(client)
}
