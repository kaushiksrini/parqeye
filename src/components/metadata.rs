use ratatui::{
    buffer::Buffer,
    layout::Rect,
    layout::{Constraint, Layout},
    prelude::Color,
    style::Stylize,
    symbols::border,
    text::Line,
    widgets::Widget,
    widgets::{Block, Cell, Paragraph, Row, Table},
};

use crate::metadata::extract_parquet_file_metadata;
use crate::utils::{commas, human_readable_bytes};

pub struct MetadataComponent {
    pub file_name: String,
    pub title: String,
}

impl MetadataComponent {
    pub fn new(file_name: String) -> Self {
        Self {
            file_name,
            title: "File Metadata".to_string(),
        }
    }

    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }
}

impl Widget for MetadataComponent {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let metadata_result = extract_parquet_file_metadata(&self.file_name);

        let (file_name_display, kv_pairs): (String, Vec<(String, String)>) = match metadata_result {
            Ok(md) => {
                let codec_summary = md.codecs.join("  ");
                let kv = vec![
                    ("Format version".into(), md.format_version),
                    ("Created by".into(), md.created_by),
                    ("Rows".into(), commas(md.rows)),
                    ("Columns".into(), md.columns.to_string()),
                    ("Row groups".into(), md.row_groups.to_string()),
                    ("Size (raw)".into(), human_readable_bytes(md.size_raw)),
                    (
                        "Size (compressed)".into(),
                        human_readable_bytes(md.size_compressed),
                    ),
                    (
                        "Compression ratio".into(),
                        format!("{:.2}x", md.compression_ratio),
                    ),
                    ("Codecs (cols)".into(), codec_summary),
                    ("Encodings".into(), md.encodings),
                    ("Avg row size".into(), format!("{} B", md.avg_row_size)),
                ];
                (md.file_name, kv)
            }
            Err(e) => (
                self.file_name.clone(),
                vec![("Error".into(), e.to_string())],
            ),
        };

        // Build a paragraph block for the file name
        let file_name_para = Paragraph::new(file_name_display.green()).block(
            Block::bordered()
                .title(Line::from("File Name".yellow().bold()).centered())
                .border_set(border::ROUNDED),
        );

        let metadata_block = Block::bordered()
            .title(Line::from("File Metadata".yellow().bold()).centered())
            .border_set(border::ROUNDED);

        let kv_len = kv_pairs.len();
        let max_key_len = kv_pairs.iter().map(|(k, _)| k.len()).max().unwrap_or(0);

        // Prepare rows for the widget
        let rows: Vec<Row> = kv_pairs
            .into_iter()
            .map(|(k, v)| Row::new(vec![Cell::from(k).bold().fg(Color::Blue), Cell::from(v)]))
            .collect();

        // Split left pane vertically: file name block on top, metadata table below
        let [file_name_area, table_container_area, _spacer] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Max(kv_len as u16 + 2),
            Constraint::Fill(1),
        ])
        .areas(area);

        // Render the file name block
        file_name_para.render(file_name_area, buf);

        // Build the table widget
        let desired_height = (rows.len() as u16 + 2).min(table_container_area.height);
        let table_full_width = table_container_area.width;
        let table = Table::new(
            rows,
            vec![
                Constraint::Length(max_key_len as u16),
                Constraint::Min(max_key_len as u16),
            ],
        )
        .column_spacing(1)
        .block(metadata_block);

        // Compute area sized to table (but not exceeding available area)
        let table_area = Rect {
            x: table_container_area.x,
            y: table_container_area.y,
            width: table_full_width,
            height: desired_height,
        };

        table.render(table_area, buf);
    }
}
