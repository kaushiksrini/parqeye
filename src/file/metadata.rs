use itertools::Itertools;
use parquet::file::metadata::ParquetMetaData;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct FileMetadata {
    pub format_version: String,
    pub created_by: String,
    pub num_rows: usize,
    pub num_columns: usize,
    pub num_row_groups: usize,
    pub raw_size: u64,
    pub compressed_size: u64,
    pub compression_ratio: f64,
    pub codecs: String,
    pub encodings: String,
    pub avg_row_size: u64,
}

impl FileMetadata {
    pub fn from_metadata(md: &ParquetMetaData) -> Result<FileMetadata, Box<dyn std::error::Error>> {
        let format_version = md.file_metadata().version();
        let created_by = md.file_metadata().created_by().unwrap_or("—");
        let num_row_groups = md.num_row_groups();
        let num_rows = md.row_groups().iter().map(|rg| rg.num_rows()).sum::<i64>() as usize;
        let num_columns = md.file_metadata().schema_descr().num_columns();

        // calulcate file metadata
        let (raw_size, compressed_size, encodings_seen, codec_counts) =
            md.row_groups().iter().flat_map(|rg| rg.columns()).fold(
                (0u64, 0u64, HashSet::new(), HashMap::new()),
                |(raw, comp, mut encodings, mut codecs), col| {
                    let codec_name = format!("{:?}", col.compression());
                    *codecs.entry(codec_name).or_insert(0) += 1;

                    for enc in col.encodings() {
                        encodings.insert(format!("{enc:?}"));
                    }

                    (
                        raw + col.uncompressed_size() as u64,
                        comp + col.compressed_size() as u64,
                        encodings,
                        codecs,
                    )
                },
            );

        let compression_ratio = if compressed_size > 0 {
            raw_size as f64 / compressed_size as f64
        } else {
            0.0
        };

        let avg_row_size = if num_rows > 0 {
            raw_size as f64 / num_rows as f64
        } else {
            0.0
        };

        let codecs: String = codec_counts
            .iter()
            .map(|(c, n)| format!("{c}({n})"))
            .sorted()
            .collect::<Vec<String>>()
            .join(", ");

        let encodings: String = encodings_seen
            .into_iter()
            .sorted()
            .collect::<Vec<String>>()
            .join(", ");

        Ok(FileMetadata {
            format_version: format_version.to_string(),
            created_by: created_by.to_string(),
            num_rows,
            num_columns,
            num_row_groups,
            raw_size,
            compressed_size,
            compression_ratio,
            codecs,
            encodings,
            avg_row_size: avg_row_size as u64,
        })
    }
}
