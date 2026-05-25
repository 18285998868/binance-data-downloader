use polars::prelude::*;

pub fn clean_dataframe(df: DataFrame) -> Result<DataFrame, String> {
    let before = df.height();

    let df = sort_by_time(df).map_err(|e| format!("sort: {e}"))?;

    let df = remove_duplicates(df).map_err(|e| format!("dedup: {e}"))?;
    let after_dedup = df.height();
    if after_dedup < before {
        log::warn!("removed {} duplicate rows", before - after_dedup);
    }

    let df = validate_ohlc(df)?;
    let after_ohlc = df.height();
    if after_ohlc < after_dedup {
        log::warn!(
            "removed {} rows failing OHLC consistency",
            after_dedup - after_ohlc
        );
    }

    let df = drop_nulls(df).map_err(|e| format!("drop_nulls: {e}"))?;
    let after_nulls = df.height();
    if after_nulls < after_ohlc {
        log::warn!("removed {} rows with null values", after_ohlc - after_nulls);
    }

    log::info!("cleaned: {} / {} rows passed all checks", df.height(), before);
    Ok(df)
}

fn sort_by_time(df: DataFrame) -> Result<DataFrame, PolarsError> {
    df.sort(
        ["open_time"],
        SortMultipleOptions::default().with_order_descending(false),
    )
}

fn remove_duplicates(df: DataFrame) -> Result<DataFrame, PolarsError> {
    df.lazy()
        .unique(
            Some(vec!["open_time".to_string()]),
            UniqueKeepStrategy::First,
        )
        .collect()
}

fn validate_ohlc(df: DataFrame) -> Result<DataFrame, String> {
    let cond = col("high")
        .gt_eq(col("low"))
        .and(col("high").gt_eq(col("close")))
        .and(col("high").gt_eq(col("open")))
        .and(col("low").lt_eq(col("close")))
        .and(col("low").lt_eq(col("open")));
    df.lazy()
        .filter(cond)
        .collect()
        .map_err(|e| format!("ohlc filter: {e}"))
}

fn drop_nulls(df: DataFrame) -> Result<DataFrame, PolarsError> {
    df.drop_nulls::<String>(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_df() -> DataFrame {
        df!(
            "open_time" => &[1i64, 2, 3, 4, 5],
            "open" => &[100.0f64, 101.0, 102.0, 103.0, 104.0],
            "high" => &[105.0f64, 106.0, 107.0, 108.0, 109.0],
            "low" => &[95.0f64, 96.0, 97.0, 98.0, 99.0],
            "close" => &[102.0f64, 103.0, 104.0, 105.0, 106.0],
            "volume" => &[1000.0f64, 2000.0, 1500.0, 3000.0, 2500.0],
        ).unwrap()
    }

    #[test]
    fn test_sort_by_time() {
        let df = df!(
            "open_time" => &[3i64, 1, 2],
            "open" => &[1.0f64, 1.0, 1.0],
            "high" => &[2.0f64, 2.0, 2.0],
            "low" => &[0.5f64, 0.5, 0.5],
            "close" => &[1.5f64, 1.5, 1.5],
            "volume" => &[100.0f64, 100.0, 100.0],
        ).unwrap();
        let sorted = sort_by_time(df).unwrap();
        let times: Vec<i64> = sorted.column("open_time").unwrap().i64().unwrap().into_no_null_iter().collect();
        assert_eq!(times, vec![1, 2, 3]);
    }

    #[test]
    fn test_remove_duplicates() {
        let df = df!(
            "open_time" => &[1i64, 1, 2, 3],
            "open" => &[100.0f64, 200.0, 102.0, 103.0],
            "high" => &[105.0f64, 205.0, 107.0, 108.0],
            "low" => &[95.0f64, 95.0, 97.0, 98.0],
            "close" => &[102.0f64, 202.0, 104.0, 105.0],
            "volume" => &[1000.0f64, 2000.0, 1500.0, 3000.0],
        ).unwrap();
        let deduped = remove_duplicates(df).unwrap();
        assert_eq!(deduped.height(), 3);
        let mut times: Vec<_> = deduped
            .column("open_time")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        times.sort();
        assert_eq!(times, vec![1, 2, 3]);
    }

    #[test]
    fn test_validate_ohlc_passes() {
        let df = sample_df();
        let result = validate_ohlc(df).unwrap();
        assert_eq!(result.height(), 5);
    }

    #[test]
    fn test_validate_ohlc_fails_high_low() {
        let df = df!(
            "open_time" => &[1i64],
            "open" => &[100.0f64],
            "high" => &[90.0f64],
            "low" => &[95.0f64],
            "close" => &[92.0f64],
            "volume" => &[1000.0f64],
        ).unwrap();
        let result = validate_ohlc(df).unwrap();
        assert_eq!(result.height(), 0);
    }

    #[test]
    fn test_validate_ohlc_fails_high_close() {
        let df = df!(
            "open_time" => &[1i64],
            "open" => &[100.0f64],
            "high" => &[105.0f64],
            "low" => &[95.0f64],
            "close" => &[110.0f64],
            "volume" => &[1000.0f64],
        ).unwrap();
        let result = validate_ohlc(df).unwrap();
        assert_eq!(result.height(), 0);
    }

    #[test]
    fn test_validate_ohlc_mixed() {
        let df = df!(
            "open_time" => &[1i64, 2],
            "open" => &[100.0f64, 100.0],
            "high" => &[105.0f64, 90.0],
            "low" => &[95.0f64, 95.0],
            "close" => &[102.0f64, 92.0],
            "volume" => &[1000.0f64, 1000.0],
        ).unwrap();
        let result = validate_ohlc(df).unwrap();
        assert_eq!(result.height(), 1);
        assert_eq!(
            result.column("open_time").unwrap().i64().unwrap().get(0).unwrap(),
            1
        );
    }

    #[test]
    fn test_drop_nulls_no_nulls() {
        let df = sample_df();
        let result = drop_nulls(df).unwrap();
        assert_eq!(result.height(), 5);
    }

    #[test]
    fn test_drop_nulls_with_nulls() {
        let df = df!(
            "open_time" => &[1i64, 2],
            "open" => &[Some(100.0f64), None],
            "high" => &[105.0f64, 105.0],
            "low" => &[95.0f64, 95.0],
            "close" => &[102.0f64, 102.0],
            "volume" => &[1000.0f64, 1000.0],
        ).unwrap();
        let result = drop_nulls(df).unwrap();
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn test_clean_dataframe_full_pipeline() {
        let df = sample_df();
        let result = clean_dataframe(df).unwrap();
        assert_eq!(result.height(), 5);
    }

    #[test]
    fn test_clean_dataframe_with_dupes_and_invalid() {
        let df = df!(
            "open_time" => &[1i64, 1, 2, 3],
            "open" => &[100.0f64, 200.0, 102.0, 90.0],
            "high" => &[105.0f64, 205.0, 107.0, 80.0],
            "low" => &[95.0f64, 95.0, 97.0, 85.0],
            "close" => &[102.0f64, 202.0, 104.0, 82.0],
            "volume" => &[1000.0f64, 2000.0, 1500.0, 3000.0],
        ).unwrap();
        let result = clean_dataframe(df).unwrap();
        assert_eq!(result.height(), 2);
    }
}
