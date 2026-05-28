use polars::prelude::*;

/// Converts column types: `open_time`/`close_time`/`count` -> Int64, rest -> Float64.
pub fn convert_types(df: &DataFrame) -> Result<DataFrame, crate::AppError> {
    let mut series = Vec::with_capacity(df.width());
    for name in df.get_column_names() {
        let name: &str = name.as_ref();
        let target = match name {
            "open_time" | "close_time" | "count" => DataType::Int64,
            _ => DataType::Float64,
        };
        let s = df
            .column(name)
            .map_err(|e| crate::AppError::Data(format!("column {name}: {e}")))?
            .cast(&target)
            .map_err(|e| crate::AppError::Data(format!("cast {name} to {target:?}: {e}")))?;
        series.push(s);
    }
    DataFrame::new(series).map_err(|e| crate::AppError::Data(format!("build df: {e}")))
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    #[test]
    fn convert_types_int64_and_float64() {
        let df = df!(
            "open_time" => &[1i64, 2],
            "open" => &["100.5", "101.2"],
            "close_time" => &[100i64, 200],
            "count" => &[5i64, 10],
            "ignore" => &["0", "0"],
        )
        .unwrap();
        let converted = super::convert_types(&df).unwrap();
        assert_eq!(
            converted.column("open_time").unwrap().dtype(),
            &DataType::Int64
        );
        assert_eq!(
            converted.column("open").unwrap().dtype(),
            &DataType::Float64
        );
        assert_eq!(
            converted.column("close_time").unwrap().dtype(),
            &DataType::Int64
        );
        assert_eq!(converted.column("count").unwrap().dtype(), &DataType::Int64);
    }

    #[test]
    fn convert_types_float_fields() {
        let df = df!(
            "open_time" => &[1i64],
            "high" => &["200.0"],
            "low" => &["100.0"],
            "volume" => &["50.5"],
        )
        .unwrap();
        let converted = super::convert_types(&df).unwrap();
        assert_eq!(
            converted.column("high").unwrap().dtype(),
            &DataType::Float64
        );
        assert_eq!(converted.column("low").unwrap().dtype(), &DataType::Float64);
        assert_eq!(
            converted.column("volume").unwrap().dtype(),
            &DataType::Float64
        );
    }

    #[test]
    fn convert_types_no_ignore_column() {
        let df = df!(
            "open_time" => &[1i64],
            "open" => &["100.0"],
        )
        .unwrap();
        let converted = super::convert_types(&df).unwrap();
        // source file had no ignore column — it's just not present
        assert_eq!(converted.width(), 2);
    }
}
