use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};
use ratatui::{
    buffer::Buffer, layout::{Constraint, Layout, Rect}, prelude::Color, style::Stylize, symbols::border, text::Line, widgets::{Block, Borders, Cell, Paragraph, Row, Table, Widget}
};

use crate::utils::{human_readable_bytes, commas};

pub struct HasStats {
    pub has_stats: bool,
    pub has_dictionary_page: bool,
    pub has_bloom_filter: bool,
    pub has_page_encoding_stats: bool
}

pub struct RowGroupStats {
    pub rows: i64,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
    pub compression_ratio: String,
}

pub struct RowGroupColumnMetadata {
    pub file_offset: i64,
    // pub physical_type: String,
    pub file_path: String,
    pub has_stats: HasStats,
    // pub statistics: Option<Statistics>,
    pub total_compressed_size: i64,
    pub total_uncompressed_size: i64,
    pub compression_type: String,
}

impl RowGroupColumnMetadata {
    pub fn from_parquet_file(metadata: &ParquetMetaData, row_group_idx: usize, column_idx: usize) -> Self {
        let column_chunk: &ColumnChunkMetaData = metadata.row_group(row_group_idx).column(column_idx);

        RowGroupColumnMetadata {
            file_offset: column_chunk.file_offset(),
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

impl RowGroupStats {
    pub fn from_parquet_file(metadata: &ParquetMetaData, row_group_idx: usize) -> Self {
        let row_group: &RowGroupMetaData = metadata.row_group(row_group_idx);
        let compressed_size = row_group.columns().iter().map(|c| c.compressed_size()).sum();
        let uncompressed_size = row_group.columns().iter().map(|c| c.uncompressed_size()).sum();
        
        RowGroupStats {
            rows: row_group.num_rows(),
            compressed_size: compressed_size,
            uncompressed_size: uncompressed_size,
            compression_ratio: format!("{:.2}x", (uncompressed_size as f64 / compressed_size as f64))
        }
    }
}

impl Widget for RowGroupStats {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title("Row Group Stats")
            .border_set(border::DOUBLE);
        
        let inner_area = block.inner(area);
        block.render(area, buf);
        
        // Create a simple stats display
        let stats_text = format!(
            "Rows: {}\nCompressed: {}\nUncompressed: {}\nRatio: {}",
            commas(self.rows as u64),
            human_readable_bytes(self.compressed_size as u64),
            human_readable_bytes(self.uncompressed_size as u64),
            self.compression_ratio
        );
        
        let paragraph = Paragraph::new(stats_text);
        paragraph.render(inner_area, buf);
    }
}

impl Widget for RowGroupColumnMetadata {
    fn render(self, area: Rect, buf: &mut Buffer) {

        let kv_pairs = vec![
            ("File Offset", format!("{}", self.file_offset)),
            ("Compressed Size", format!("{}", human_readable_bytes(self.total_compressed_size as u64))),
            ("Uncompressed Size", format!("{}", human_readable_bytes(self.total_uncompressed_size as u64))),
            ("Compression Ratio", format!("{:.2}x", (self.total_uncompressed_size as f64 / self.total_compressed_size as f64))),
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
        
        let title = vec![" Column: ".into(), self.file_path.clone().yellow().bold(), " ".into()];

        // First, create and render the outer block
        let block = Block::bordered()
            .title(Line::from(title).centered())
            .borders(Borders::TOP)
            .border_set(border::DOUBLE);
        
        let inner_area = block.inner(area);
        block.render(area, buf);

        // Now subdivide the inner area for stats and table
        let [stats_area, table_area] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Fill(1),
        ]).areas(inner_area);

        // Render the table in the table area
        table.render(table_area, buf);

        // Create 1x4 horizontal grid for stats
        let horizontal_areas = Layout::horizontal([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ]).split(stats_area);
        let first: Rect = horizontal_areas[0];
        let second = horizontal_areas[1];
        let third = horizontal_areas[2];
        let fourth = horizontal_areas[3];

        // Render each stats block
        self.render_stat_block("Statistics", self.has_stats.has_stats, first, buf);
        self.render_stat_block("Dict Page", self.has_stats.has_dictionary_page, second, buf);
        self.render_stat_block("Bloom Filter", self.has_stats.has_bloom_filter, third, buf);
        self.render_stat_block("Page Stats", self.has_stats.has_page_encoding_stats, fourth, buf);
    }
}

impl RowGroupColumnMetadata {
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