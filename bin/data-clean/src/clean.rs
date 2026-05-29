use polars::prelude::*;

use super::Error;

pub fn clean_dataframe(df: DataFrame) -> Result<DataFrame, Error> {
    let before = df.height();

    let after_clean = df
        .lazy()
        .sort(
            ["open_time"],
            SortMultipleOptions::default().with_order_descending(false),
        )
        .unique(
            Some(vec!["open_time".to_string()]),
            UniqueKeepStrategy::First,
        )
        .filter(
            col("high")
                .gt_eq(col("low"))
                .and(col("high").gt_eq(col("close")))
                .and(col("high").gt_eq(col("open")))
                .and(col("low").lt_eq(col("close")))
                .and(col("low").lt_eq(col("open"))),
        )
        .collect()
        .map_err(|e| Error::Data(format!("clean: {e}")))?;

    let after_dedup = after_clean.height();
    if after_dedup < before {
        log::warn!("removed {} duplicate rows", before - after_dedup);
    }

    let df = after_clean
        .fill_null(FillNullStrategy::Forward(None))
        .map_err(|e| Error::Data(format!("forward_fill: {e}")))?;

    let df = df
        .drop_nulls::<String>(None)
        .map_err(|e| Error::Data(format!("drop_nulls: {e}")))?;

    let after = df.height();
    if after < after_dedup {
        log::warn!("removed {} rows with remaining nulls", after_dedup - after);
    }

    log::debug!(
        "cleaned: {} / {} rows passed all checks",
        df.height(),
        before
    );
    Ok(df)
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    fn valid_df() -> DataFrame {
        df!(
            "open_time" => &[3i64, 1, 2],
            "open" => &[102.0, 100.0, 101.0],
            "high" => &[105.0, 103.0, 104.0],
            "low" => &[101.0, 99.0, 100.0],
            "close" => &[103.0, 101.0, 102.0],
        )
        .unwrap()
    }

    #[test]
    fn clean_valid_sorts_by_time() {
        let df = super::clean_dataframe(valid_df()).unwrap();
        let times: Vec<i64> = df
            .column("open_time")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(times, vec![1, 2, 3]);
    }

    #[test]
    fn clean_removes_duplicates() {
        let df = valid_df();
        let dup = df.clone();
        let _ = df.vstack(&dup);
        let cleaned = super::clean_dataframe(df).unwrap();
        assert_eq!(cleaned.height(), 3);
    }

    #[test]
    fn clean_filters_invalid_ohlc() {
        let df = df!(
            "open_time" => &[1i64],
            "open" => &[100.0],
            "high" => &[50.0],
            "low" => &[60.0],
            "close" => &[55.0],
        )
        .unwrap();
        let cleaned = super::clean_dataframe(df).unwrap();
        assert_eq!(cleaned.height(), 0);
    }

    #[test]
    fn clean_forwards_fills_extra_column_nulls() {
        let s = Series::new("volume".into(), &[Some(1000.0), None, Some(1200.0)]);
        let mut df = df!(
            "open_time" => &[1i64, 2, 3],
            "open" => &[100.0, 101.0, 102.0],
            "high" => &[105.0, 106.0, 107.0],
            "low" => &[95.0, 96.0, 97.0],
            "close" => &[102.0, 103.0, 104.0],
        )
        .unwrap();
        df.with_column(s).unwrap();
        let cleaned = super::clean_dataframe(df).unwrap();
        assert_eq!(cleaned.height(), 3);
        let vols: Vec<f64> = cleaned
            .column("volume")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(vols, vec![1000.0, 1000.0, 1200.0]);
    }

    #[test]
    fn clean_empty_df() {
        let df = df!(
            "open_time" => &[] as &[i64],
            "open" => &[] as &[f64],
            "high" => &[] as &[f64],
            "low" => &[] as &[f64],
            "close" => &[] as &[f64],
        )
        .unwrap();
        let cleaned = super::clean_dataframe(df).unwrap();
        assert_eq!(cleaned.height(), 0);
    }
}
