use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use ratatui::{
    buffer::Buffer,
    layout::{Rect, Layout, Constraint},
    prelude::Color,
    widgets::{Block, Table, Row, Cell, Widget},
    style::{Stylize},
    text::Line,
    symbols::border,
};

use crate::utils::{human_readable_bytes, commas};

pub struct HasStats {
    pub has_stats: bool,
    pub has_dictionary_page: bool,
    pub has_bloom_filter: bool,
    pub has_page_encoding_stats: bool
}

pub struct ColumnMetadata {
    pub file_offset: i64,
    // pub physical_type: String,
    pub file_path: String,
    pub num_values: i64,
    pub has_stats: HasStats,
    // pub statistics: Option<Statistics>,
    pub total_compressed_size: i64,
    pub total_uncompressed_size: i64,
    pub compression_type: String,
}

impl ColumnMetadata {
    pub fn from_parquet_file(metadata: &ParquetMetaData, row_group_idx: usize, column_idx: usize) -> Self {
        let column_chunk: &ColumnChunkMetaData = metadata.row_group(row_group_idx).column(column_idx);

        ColumnMetadata {
            file_offset: column_chunk.file_offset(),
            num_values: column_chunk.num_values(),
            has_stats: HasStats {
                has_stats: column_chunk.statistics().is_some(),
                has_dictionary_page: column_chunk.dictionary_page_offset().is_some(),
                has_bloom_filter: column_chunk.bloom_filter_offset().is_some(),
                has_page_encoding_stats: column_chunk.page_encoding_stats().is_some() && column_chunk.page_encoding_stats().unwrap().len() > 0,
            },
            file_path: column_chunk.column_descr().path().string(),
            total_compressed_size: column_chunk.compressed_size(),
            total_uncompressed_size: column_chunk.uncompressed_size(),
            compression_type: column_chunk.compression().to_string(),
        }
    }
}

impl Widget for ColumnMetadata {
    fn render(self, area: Rect, buf: &mut Buffer) {

        let kv_pairs = vec![
            ("File Offset", format!("{}", self.file_offset)),
            ("Num Values", commas(self.num_values as u64)),
            ("Compressed Size", format!("{}", human_readable_bytes(self.total_compressed_size as u64))),
            ("Uncompressed Size", format!("{}", human_readable_bytes(self.total_uncompressed_size as u64))),
            ("Compression Ratio", format!("{:.2}%", (self.total_compressed_size as f64 / self.total_uncompressed_size as f64) * 100.0)),
            ("Compression Type", format!("{}", self.compression_type)),
        ];

        let rows: Vec<Row> = kv_pairs
        .into_iter()
        .map(|(k, v)| {
            Row::new(vec![
                Cell::from(k).bold().fg(Color::Blue),
                Cell::from(v),
            ])
        })
        .collect();

        let table = Table::new(rows, vec![Constraint::Length(20), Constraint::Length(20)]);
        
        let title = vec!["Column: ".into(), self.file_path.clone().yellow().bold()];

        let [stats_area, table_area, _page_info_area] = Layout::vertical([
            Constraint::Min(2),
            Constraint::Fill(3),
            Constraint::Fill(2),
        ]).areas(area);

        let block = Block::bordered()
            .title(Line::from(title).centered())
            .border_set(border::DOUBLE);

        let table_area_size = Rect {
            x: table_area.x,
            y: table_area.y,
            width: table_area.width,
            height: table_area.height,
        };

        table.block(block).render(table_area_size, buf);

        // Create 1x4 horizontal grid for stats
        let horizontal_areas = Layout::horizontal([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ]).split(stats_area);
        let first = horizontal_areas[0];
        let second = horizontal_areas[1];
        let third = horizontal_areas[2];
        let fourth = horizontal_areas[3];

        // Render each stats block
        self.render_stat_block("Statistics", self.has_stats.has_stats, first, buf);
        self.render_stat_block("Dictionary Page", self.has_stats.has_dictionary_page, second, buf);
        self.render_stat_block("Bloom Filter", self.has_stats.has_bloom_filter, third, buf);
        self.render_stat_block("Page Stats", self.has_stats.has_page_encoding_stats, fourth, buf);
    }
}

impl ColumnMetadata {
    fn render_stat_block(&self, title: &str, has_stat: bool, area: Rect, buf: &mut Buffer) {
        let (symbol, color) = if has_stat {
            (format!("✓ {}", title), Color::Green)
        } else {
            (format!("✗ {}", title), Color::Red)
        };

        let block = Block::bordered()
            .title(title)
            .border_style(ratatui::style::Style::default().fg(color))
            .title_style(ratatui::style::Style::default().fg(color).bold());

        let inner = block.inner(area);
        block.render(area, buf);

        // Center the symbol in the block
        if inner.width > 0 && inner.height > 0 {
            let symbol_x = inner.x + inner.width / 2;
            let symbol_y = inner.y + inner.height / 2;
            
            if symbol_x < inner.x + inner.width && symbol_y < inner.y + inner.height {
                buf.get_mut(symbol_x, symbol_y)
                    .set_symbol(symbol.as_str())
                    .set_style(ratatui::style::Style::default().fg(color).bold());
            }
        }
    }
}