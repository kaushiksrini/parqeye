use itertools::Itertools;
use parquet::file::metadata::ParquetMetaData;
use ratatui::widgets::Widget;
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Rect},
    prelude::Color,
    style::Stylize,
    symbols::border,
    text::Line,
    widgets::{Block, Cell, Row, Table},
};
use std::collections::{HashMap, HashSet};

use crate::file::Renderable;
use crate::file::utils::commas;
use crate::file::utils::human_readable_bytes;

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

impl Renderable for FileMetadata {
    fn render_content(&self, area: Rect, buf: &mut Buffer) {
        let kv_pairs: Vec<(String, String)> = vec![
            ("Format version".into(), self.format_version.clone()),
            ("Created by".into(), self.created_by.clone()),
            ("Rows".into(), commas(self.num_rows as u64)),
            ("Columns".into(), self.num_columns.to_string()),
            ("Row groups".into(), self.num_row_groups.to_string()),
            ("Size (raw)".into(), human_readable_bytes(self.raw_size)),
            (
                "Size (compressed)".into(),
                human_readable_bytes(self.compressed_size),
            ),
            (
                "Compression ratio".into(),
                format!("{:.2}x", self.compression_ratio),
            ),
            ("Codecs (cols)".into(), self.codecs.clone()),
            ("Encodings".into(), self.encodings.clone()),
            ("Avg row size".into(), format!("{} B", self.avg_row_size)),
        ];

        let max_value_size = kv_pairs.iter().map(|(_, v)| v.len()).max().unwrap_or(0) as u16;

        let rows: Vec<Row> = kv_pairs
            .into_iter()
            .map(|(k, v)| {
                Row::new(vec![
                    Cell::from(format!("{k:>18}")).bold().fg(Color::Blue),
                    Cell::from(format!("{v:<}")),
                ])
            })
            .collect();

        // Calculate centered area for the table
        let key_width = 18;
        let value_width = max_value_size.max(20); // Ensure minimum width
        let table_width = key_width + value_width + 3; // +3 for spacing and borders
        let table_height = rows.len() as u16;
        let center_x = area.x + (area.width.saturating_sub(table_width)) / 2;
        let center_y = area.y + (area.height.saturating_sub(table_height)) / 2;

        let centered_area = Rect {
            x: center_x,
            y: center_y,
            width: table_width + 2,
            height: table_height + 2,
        };

        let table = Table::new(
            rows,
            vec![
                Constraint::Length(key_width),
                Constraint::Length(value_width),
            ],
        )
        .block(
            Block::bordered()
                .title(Line::from("File Metadata".yellow().bold()).centered())
                .border_set(border::ROUNDED),
        );
        table.render(centered_area, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    fn load_alltypes_metadata() -> FileMetadata {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data(),
        );
        let file = File::open(path).unwrap();
        let reader = SerializedFileReader::try_from(file).unwrap();
        let metadata = reader.metadata();
        FileMetadata::from_metadata(metadata).unwrap()
    }

    #[test]
    fn test_file_metadata_basic() {
        let file_metadata = load_alltypes_metadata();

        // alltypes_plain.parquet has 8 rows and 11 columns
        assert_eq!(8, file_metadata.num_rows);
        assert_eq!(11, file_metadata.num_columns);
        
        // Should have 1 row group
        assert_eq!(1, file_metadata.num_row_groups);
    }

    #[test]
    fn test_format_version() {
        let file_metadata = load_alltypes_metadata();
        
        // Format version should be a non-empty string
        assert!(!file_metadata.format_version.is_empty());
        
        // Should be a valid parquet version
        assert_eq!("1", file_metadata.format_version);
    }

    #[test]
    fn test_created_by() {
        let file_metadata = load_alltypes_metadata();
        
        let expected_created_by = "impala version 1.3.0-INTERNAL (build 8a48ddb1eff84592b3fc06bc6f51ec120e1fffc9)";
        // Created by should be present (not the default "—")
        assert_eq!(expected_created_by, file_metadata.created_by);
    }

    #[test]
    fn test_size_metrics() {
        let file_metadata = load_alltypes_metadata();
        
        // Both sizes should be positive
        assert_eq!(671, file_metadata.raw_size);
        assert_eq!(671, file_metadata.compressed_size);
        
        // Raw size should be == compressed size for this file
        assert_eq!(file_metadata.raw_size, file_metadata.compressed_size);
    }

    #[test]
    fn test_compression_ratio() {
        let file_metadata = load_alltypes_metadata();
        
        // Compression ratio should be == 1.0 for this file
        assert_eq!(1.0, file_metadata.compression_ratio);
    }

    #[test]
    fn test_codecs() {
        let file_metadata = load_alltypes_metadata();
        
        // Codecs string should not be empty
        assert!(!file_metadata.codecs.is_empty());
        
        // Should contain at least one codec name
        // Common codecs: UNCOMPRESSED, SNAPPY, GZIP, etc.
        assert_eq!("UNCOMPRESSED(11)", file_metadata.codecs);
    }

    #[test]
    fn test_encodings() {
        let file_metadata = load_alltypes_metadata();
        
        // Encodings string should not be empty
        assert!(!file_metadata.encodings.is_empty());
        
        // Should contain at least one encoding type
        // Common encodings: PLAIN, RLE, DELTA_BINARY_PACKED, etc.
        assert!(
            file_metadata.encodings.contains("PLAIN") &&
            file_metadata.encodings.contains("RLE") &&
            file_metadata.encodings.contains("PLAIN_DICTIONARY")
        );
    }

    #[test]
    fn test_avg_row_size() {
        let file_metadata = load_alltypes_metadata();
        
        // Average row size should be positive
        assert_eq!(83_u64, file_metadata.avg_row_size);
    }

    #[test]
    fn test_from_metadata_error_handling() {
        // Test that from_metadata returns Ok for valid files
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data(),
        );
        let file = File::open(path).unwrap();
        let reader = SerializedFileReader::try_from(file).unwrap();
        let metadata = reader.metadata();
        
        let result = FileMetadata::from_metadata(metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_renderable_trait() {
        let file_metadata = load_alltypes_metadata();
        
        // Test that the Renderable trait is implemented
        // We can't easily test the actual rendering without a full terminal setup,
        // but we can verify the method exists and doesn't panic
        let mut buf = Buffer::empty(Rect::new(0, 0, 100, 50));
        let area = Rect::new(0, 0, 100, 50);
        
        // This should not panic
        file_metadata.render_content(area, &mut buf);
    }
}
