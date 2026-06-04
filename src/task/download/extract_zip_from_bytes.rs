use std::io::Read;

pub fn extract_csv_from_bytes(zip_bytes: &[u8]) -> Result<Vec<u8>, crate::AppError> {
    let cursor = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|e| crate::AppError::Data(format!("zip archive: {e}")))?;
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| crate::AppError::Data(format!("entry {i}: {e}")))?;
        if entry.is_dir() {
            continue;
        }
        let name = entry.name();
        if name.ends_with(".csv") || name.ends_with(".CSV") {
            let cap = (entry.size() as usize).min(100 * 1024 * 1024);
            let mut buf = Vec::with_capacity(cap);
            entry
                .read_to_end(&mut buf)
                .map_err(|e| crate::AppError::Data(format!("read csv: {e}")))?;
            return Ok(buf);
        }
    }
    Err(crate::AppError::Data("no CSV entry in ZIP".to_string()))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    fn build_zip(csv_name: &str, csv_content: &[u8]) -> Vec<u8> {
        let buf = std::io::Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(buf);
        zip.start_file::<&str, ()>(csv_name, zip::write::FileOptions::default())
            .unwrap();
        zip.write_all(csv_content).unwrap();
        zip.finish().unwrap().into_inner()
    }

    #[test]
    fn extract_csv_found() {
        let csv = b"open_time,open\n1,100.0\n";
        let zip_bytes = build_zip("data.csv", csv);
        let result = super::extract_csv_from_bytes(&zip_bytes).unwrap();
        assert_eq!(result, csv);
    }

    #[test]
    fn extract_csv_case_insensitive() {
        let csv = b"a,b\n1,2\n";
        let zip_bytes = build_zip("DATA.CSV", csv);
        let result = super::extract_csv_from_bytes(&zip_bytes).unwrap();
        assert_eq!(result, csv);
    }

    #[test]
    fn extract_csv_no_csv_entry() {
        let zip_bytes = build_zip("data.txt", b"hello");
        let result = super::extract_csv_from_bytes(&zip_bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no CSV entry"));
    }

    #[test]
    fn extract_csv_skips_directories() {
        let buf = std::io::Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(buf);
        zip.add_directory::<&str, ()>("subdir/", zip::write::FileOptions::default())
            .unwrap();
        zip.start_file::<&str, ()>("subdir/data.csv", zip::write::FileOptions::default())
            .unwrap();
        zip.write_all(b"a\n1").unwrap();
        let zip_bytes = zip.finish().unwrap().into_inner();
        let result = super::extract_csv_from_bytes(&zip_bytes).unwrap();
        assert_eq!(result, b"a\n1");
    }

    #[test]
    fn extract_csv_invalid_zip() {
        let result = super::extract_csv_from_bytes(b"not a zip file");
        assert!(result.is_err());
    }
}
