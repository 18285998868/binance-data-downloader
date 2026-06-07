use crate::AppError;

/// Resolve the list of symbols to download.
///
/// If `configured` is non-empty, return it as-is.
/// Otherwise fetch all TRADING symbols from Binance Futures exchangeInfo
/// (cached by `common::cache`).
pub async fn resolve_symbols(
    client: &reqwest::Client,
    configured: &[String],
) -> Result<Vec<String>, AppError> {
    if !configured.is_empty() {
        return Ok(configured.to_vec());
    }
    let raw = common::exchange_info::get_exchange_info(client)
        .await
        .map_err(|e| AppError::Data(e.to_string()))?;
    let info: serde_json::Value = serde_json::from_slice(&raw)
        .map_err(|e| AppError::Data(format!("parse exchange info: {e}")))?;
    let symbols = info["symbols"]
        .as_array()
        .ok_or_else(|| AppError::Data("missing symbols array".into()))?;
    Ok(symbols
        .iter()
        .filter(|s| s["status"] == "TRADING")
        .map(|s| s["symbol"].as_str().unwrap_or("").to_string())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_returns_configured_list() {
        let client = reqwest::Client::new();
        let configured = vec!["BTCUSDT".into(), "ETHUSDT".into()];
        let result = resolve_symbols(&client, &configured).await.unwrap();
        assert_eq!(result, vec!["BTCUSDT", "ETHUSDT"]);
    }

    #[ignore = "requires network; run manually with --include-ignored"]
    #[tokio::test]
    async fn test_fetch_from_exchange_info() {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap();
        let result = resolve_symbols(&client, &[]).await.unwrap();
        assert!(result.len() > 100, "should have >100 TRADING symbols");
        assert!(result.contains(&"BTCUSDT".to_string()));
        assert!(result.contains(&"ETHUSDT".to_string()));
    }

    #[tokio::test]
    async fn test_configured_list_unchanged() {
        let client = reqwest::Client::new();
        let input: Vec<String> = vec!["BTCUSDT".into()];
        let result = resolve_symbols(&client, &input).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "BTCUSDT");
    }

    #[tokio::test]
    async fn test_configured_filters_nothing() {
        let client = reqwest::Client::new();
        let input = vec!["BTCUSDT".into(), "DEFUNCTXXX".into()];
        let result = resolve_symbols(&client, &input).await.unwrap();
        assert_eq!(result, input);
    }
}
