use itertools::Itertools;
use parquet::file::metadata::ParquetMetaData;
use ratatui::widgets::Widget;
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Rect},
    prelude::Color,
    style::{Style, Stylize},
    symbols::border,
    text::Line,
    widgets::{Block, Cell, Row, Table},
};
use std::collections::{HashMap, HashSet};

use crate::file::Renderable;
use crate::utils::commas;
use crate::utils::human_readable_bytes;

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
        let metadata_block = Block::bordered()
            .title(Line::from("File Metadata".yellow().bold()).centered())
            .border_set(border::ROUNDED);

        let inner = metadata_block.inner(area);
        metadata_block.render(area, buf);

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

        let rows: Vec<Row> = kv_pairs
            .into_iter()
            .map(|(k, v)| Row::new(vec![Cell::from(k).bold().fg(Color::Blue), Cell::from(v)]))
            .collect();

        let table = Table::new(rows, vec![Constraint::Length(18), Constraint::Length(200)]);
        table.render(inner, buf);
    }
}
