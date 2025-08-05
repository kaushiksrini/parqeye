use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::ParquetMetaData;

#[derive(Debug)]
pub struct ColumnStats {
    pub min: Option<String>,
    pub max: Option<String>,
    pub nulls: u64,
    pub distinct: Option<u64>,
    pub total_compressed_size: u64,
    pub total_uncompressed_size: u64,
}

pub fn aggregate_column_stats(md: &ParquetMetaData, col_idx: usize, physical: PhysicalType) -> ColumnStats {
    let mut min_bytes: Option<Vec<u8>> = None;
    let mut max_bytes: Option<Vec<u8>> = None;
    let mut nulls: u64 = 0;
    let mut distinct: Option<u64> = None;
    let mut total_compressed_size: u64 = 0;
    let mut total_uncompressed_size: u64 = 0;

    for rg in md.row_groups() {
        let col_meta = rg.column(col_idx);
        if let Some(stats) = col_meta.statistics() {
            if let Some(n) = stats.null_count_opt() {
                nulls += n;
            }
            if let Some(d) = stats.distinct_count_opt() {
                distinct = Some(distinct.unwrap_or(0) + d);
            }
            if let Some(min_b) = stats.min_bytes_opt() {
                if min_bytes.is_none() || min_b < &min_bytes.as_ref().unwrap()[..] {
                    min_bytes = Some(min_b.to_vec());
                }
            }
            if let Some(max_b) = stats.max_bytes_opt() {
                if max_bytes.is_none() || max_b > &max_bytes.as_ref().unwrap()[..] {
                    max_bytes = Some(max_b.to_vec());
                }
            }

            total_compressed_size += col_meta.compressed_size() as u64;
            total_uncompressed_size += col_meta.uncompressed_size() as u64;
        }
    }

    let min_str = min_bytes.as_deref().map(|b| decode_value(b, physical));
    let max_str = max_bytes.as_deref().map(|b| decode_value(b, physical));

    ColumnStats {
        min: min_str,
        max: max_str,
        nulls,
        distinct,
        total_compressed_size,
        total_uncompressed_size,
    }
}

/// Decode raw statistics bytes into a readable value based on the physical type.
fn decode_value(bytes: &[u8], physical: PhysicalType) -> String {
    match physical {
        PhysicalType::INT32 if bytes.len() == 4 => {
            let v = i32::from_le_bytes(bytes.try_into().unwrap());
            v.to_string()
        }
        PhysicalType::INT64 if bytes.len() == 8 => {
            let v = i64::from_le_bytes(bytes.try_into().unwrap());
            v.to_string()
        }
        PhysicalType::FLOAT if bytes.len() == 4 => {
            let v = f32::from_le_bytes(bytes.try_into().unwrap());
            format!("{v:.4}")
        }
        PhysicalType::DOUBLE if bytes.len() == 8 => {
            let v = f64::from_le_bytes(bytes.try_into().unwrap());
            format!("{v:.4}")
        }
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            match std::str::from_utf8(bytes) {
                Ok(s) => s.to_string(),
                Err(_) => bytes.iter().map(|b| format!("{b:02X}")).collect::<Vec<_>>().join(""),
            }
        }
        _ => bytes.iter().map(|b| format!("{b:02X}")).collect::<Vec<_>>().join(""),
    }
}