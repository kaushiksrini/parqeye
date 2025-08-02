use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::basic::{PageType, Encoding};
use std::fs::File;
use ratatui::{
    buffer::Buffer, layout::{Constraint, Layout, Rect}, prelude::Color, style::Stylize, symbols::border, text::Line, widgets::{Block, Borders, Cell, Row, Table, Widget}
};

use crate::utils::{human_readable_bytes, commas};

pub struct RowGroupPageStats {
    pub page_stats: Vec<PageStats>,
}

pub struct PageStats {
    pub page_type: String,
    pub size: usize,
    pub rows: usize,
    pub encoding: String,
}

pub struct HasStats {
    pub has_stats: bool,
    pub has_dictionary_page: bool,
    pub has_bloom_filter: bool,
    pub has_page_encoding_stats: bool
}

pub struct RowGroupStats {
    pub row_group_idx: usize,
    pub rows: i64,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
    pub compression_ratio: String,
}

pub struct RowGroupColumnMetadata {
    pub file_offset: i64,
    // pub physical_type: String,
    pub column_path: String,
    pub has_stats: HasStats,
    // pub statistics: Option<Statistics>,
    pub total_compressed_size: i64,
    pub total_uncompressed_size: i64,
    pub compression_type: String,
}

impl RowGroupPageStats {
    pub fn from_parquet_file(file_path: &str, _metadata: &ParquetMetaData, row_group_idx: usize, column_idx: usize) -> Result<Self, Box<dyn std::error::Error>> {
        // TODO: make async and reduce file opening to once. 
        let file = File::open(file_path)?;
        let parquet_reader = SerializedFileReader::new(file)?;
        
        // Get the page reader for this column
        let mut page_reader = parquet_reader.get_row_group(row_group_idx)?
            .get_column_page_reader(column_idx)?;
        
        let mut page_stats = Vec::new();
        
        // Iterate through pages and collect stats
        while let Ok(page) = page_reader.get_next_page() {
            if let Some(page) = page {
                let page_type = match page.page_type() {
                    PageType::DATA_PAGE => "Data Page".to_string(),
                    PageType::INDEX_PAGE => "Index Page".to_string(),
                    PageType::DICTIONARY_PAGE => "Dictionary Page".to_string(),
                    PageType::DATA_PAGE_V2 => "Data Page V2".to_string(),
                };
                
                let encoding = match page.encoding() {
                    Encoding::PLAIN => "Plain".to_string(),
                    Encoding::PLAIN_DICTIONARY => "Plain Dictionary".to_string(),
                    Encoding::RLE => "RLE".to_string(),
                    Encoding::DELTA_BINARY_PACKED => "Delta Binary Packed".to_string(),
                    Encoding::DELTA_LENGTH_BYTE_ARRAY => "Delta Length Byte Array".to_string(),
                    Encoding::DELTA_BYTE_ARRAY => "Delta Byte Array".to_string(),
                    Encoding::RLE_DICTIONARY => "RLE Dictionary".to_string(),
                    Encoding::BYTE_STREAM_SPLIT => "Byte Stream Split".to_string(),
                    _ => format!("{:?}", page.encoding()), // Handle any other encoding types
                };
                
                page_stats.push(PageStats {
                    page_type,
                    size: page.buffer().len(),
                    rows: page.num_values() as usize,
                    encoding,
                });
            } else {
                break;
            }
        }
        
        Ok(RowGroupPageStats { page_stats })
    }
}

impl Widget for RowGroupPageStats {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title(" Page Information ")
            .border_style(ratatui::style::Style::default().fg(Color::Rgb(128, 128, 128)))
            .title_style(ratatui::style::Style::default().fg(Color::LightBlue).bold());
        
        let inner_area = block.inner(area);
        block.render(area, buf);
        
        if self.page_stats.is_empty() {
            let no_data = Table::new(
                vec![Row::new(vec![Cell::from("No page data available")])],
                vec![Constraint::Fill(1)]
            );
            no_data.render(inner_area, buf);
            return;
        }
        
        // Create table headers
        let header = Row::new(vec![
            Cell::from("#").bold().fg(Color::Blue),
            Cell::from("Page Type").bold().fg(Color::Blue),
            Cell::from("Size").bold().fg(Color::Blue),
            Cell::from("Rows").bold().fg(Color::Blue),
            Cell::from("Encoding").bold().fg(Color::Blue),
        ]);
        
        // Create table rows from page stats
        let rows: Vec<Row> = self.page_stats
            .into_iter()
            .enumerate()
            .map(|(i, page)| {
                Row::new(vec![
                    Cell::from(format!("{}", i)),
                    Cell::from(page.page_type),
                    Cell::from(human_readable_bytes(page.size as u64)),
                    Cell::from(commas(page.rows as u64)),
                    Cell::from(page.encoding),
                ])
            })
            .collect();
        
        let table = Table::new(rows, vec![
            Constraint::Max(3),  // Page Number
            Constraint::Fill(1),  // Page Type
            Constraint::Fill(1),  // Size
            Constraint::Fill(1),  // Rows
            Constraint::Fill(1),     // Encoding
        ])
        .header(header);
        
        table.render(inner_area, buf);
    }
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
            column_path: column_chunk.column_descr().path().string(),
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
            row_group_idx: row_group_idx,
            rows: row_group.num_rows(),
            compressed_size: compressed_size,
            uncompressed_size: uncompressed_size,
            compression_ratio: format!("{:.2}x", (uncompressed_size as f64 / compressed_size as f64))
        }
    }
}

impl Widget for RowGroupStats {
    fn render(self, area: Rect, buf: &mut Buffer) {

        let title = vec![" Row Group: ".into(), format!("{}", self.row_group_idx).light_blue().bold(), " ".into()];
        let block = Block::bordered()
            .title(Line::from(title).centered())
            .borders(Borders::TOP)
            .border_set(border::DOUBLE);
        
        let inner_area: Rect = block.inner(area);
        block.render(area, buf);
        
        // Create 1x4 horizontal grid for stats
        let horizontal_areas = Layout::horizontal([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ]).split(inner_area);
        
        // Render each stat block
        self.render_stat_block("Rows", &commas(self.rows as u64), horizontal_areas[0], buf);
        self.render_stat_block("Compressed", &human_readable_bytes(self.compressed_size as u64), horizontal_areas[1], buf);
        self.render_stat_block("Uncompressed", &human_readable_bytes(self.uncompressed_size as u64), horizontal_areas[2], buf);
        self.render_stat_block("Ratio", &self.compression_ratio, horizontal_areas[3], buf);
    }
}

impl RowGroupStats {
    fn render_stat_block(&self, title: &str, value: &str, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title(title)
            .border_style(ratatui::style::Style::default().fg(Color::Rgb(128, 128, 128))) // Gray color
            .title_style(ratatui::style::Style::default().fg(Color::Rgb(211, 211, 211)));

        let inner = block.inner(area);
        block.render(area, buf);

        // Center the value in the block
        if inner.width > 0 && inner.height > 0 {
            let value_x = inner.x + (inner.width.saturating_sub(value.len() as u16)) / 2;
            let value_y = inner.y + inner.height / 2;
            
            if value_y < inner.y + inner.height {
                for (i, ch) in value.chars().enumerate() {
                    let x = value_x + i as u16;
                    if x < inner.x + inner.width {
                        buf.get_mut(x, value_y)
                            .set_symbol(&ch.to_string())
                            .set_style(ratatui::style::Style::default().fg(Color::LightMagenta).bold());
                    }
                }
            }
        }
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

        let table = Table::new(rows, vec![Constraint::Length(18), Constraint::Length(20)]);
        
        let title = vec![" Column: ".into(), self.column_path.clone().yellow().bold(), " ".into()];

        // First, create and render the outer block
        let block = Block::bordered()
            .title(Line::from(title).centered())
            .borders(Borders::TOP)
            .border_set(border::DOUBLE);
        
        let inner_area = block.inner(area);
        block.render(area, buf);

        // Now subdivide the inner area for stats, table
        let [stats_area, table_area] = Layout::vertical([
            Constraint::Length(3),   // Stats area (1x4 grid)
            Constraint::Fill(2),     // Table area (column metadata)
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