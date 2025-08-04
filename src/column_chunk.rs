use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::basic::{PageType, Encoding};
use ratatui::widgets::{Axis, Chart, Dataset};
use std::fs::File;
use ratatui::{
    buffer::Buffer, layout::{Constraint, Layout, Rect}, prelude::Color, style::{Style, Stylize}, symbols::{border, self}, text::Line, widgets::{Block, Borders, Cell, Row, Table, Widget}
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

pub fn calculate_row_group_stats(file_path: &str) -> Result<Vec<RowGroupStats>, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let parquet_reader = SerializedFileReader::new(file)?;
    let metadata = parquet_reader.metadata();
    let row_group_stats = metadata.row_groups().iter().enumerate().map(|(i, _)| RowGroupStats::from_parquet_file(metadata, i)).collect();
    Ok(row_group_stats)
}

pub fn render_row_group_charts(row_group_stats: &Vec<RowGroupStats>, area: Rect, buf: &mut Buffer) {
    // Split the area into two charts vertically
    let chart_areas = Layout::vertical([
        Constraint::Percentage(50),
        Constraint::Percentage(50),
    ]).split(area);

    // Chart 1: Scatter plot of compressed vs uncompressed sizes per row group
    render_size_comparison_chart(row_group_stats, chart_areas[0], buf);
    
    // Chart 2: Compression ratios vs row group number
    render_compression_ratio_chart(row_group_stats, chart_areas[1], buf);
}

fn render_size_comparison_chart(row_group_stats: &Vec<RowGroupStats>, area: Rect, buf: &mut Buffer) {
    // Prepare data: (row_group_index, size) pairs
    let compressed_data: Vec<(f64, f64)> = row_group_stats
        .iter()
        .map(|rg| (rg.row_group_idx as f64, rg.compressed_size as f64))
        .collect();
    
    let uncompressed_data: Vec<(f64, f64)> = row_group_stats
        .iter()
        .map(|rg| (rg.row_group_idx as f64, rg.uncompressed_size as f64))
        .collect();

    // Find max size for y-axis bounds
    let max_compressed = compressed_data.iter().map(|(_, size)| *size).fold(0.0, f64::max);
    let max_uncompressed = uncompressed_data.iter().map(|(_, size)| *size).fold(0.0, f64::max);
    let max_size = max_compressed.max(max_uncompressed);
    
    // Find x-axis bounds
    let max_row_group = row_group_stats.len() as f64 - 1.0;

    let datasets = vec![
        Dataset::default()
            .name("Compressed")
            .marker(symbols::Marker::Dot)
            .style(Style::default().fg(Color::Blue))
            .data(&compressed_data),
        Dataset::default()
            .name("Uncompressed")
            .marker(symbols::Marker::Dot)
            .style(Style::default().fg(Color::Red))
            .data(&uncompressed_data),
    ];

    // Create x-axis labels (row group indices)
    let x_labels: Vec<String> = (0..row_group_stats.len())
        .step_by((row_group_stats.len() / 5).max(1)) // Show ~5 labels
        .map(|i| i.to_string())
        .collect();
    
    // Create y-axis labels (size values)
    let y_step = (max_size * 1.1) / 4.0; // 4 intervals
    let y_labels: Vec<String> = (0..4)
        .map(|i| {
            let value = i as f64 * y_step;
            if value >= 1_000_000.0 {
                format!("{:.1}M", value / (1_024.0 * 1_024.0))
            } else if value >= 1_000.0 {
                format!("{:.1}K", value / 1_024.0)
            } else {
                format!("{:.0}", value)
            }
        })
        .collect();
    
    let title = vec!["Compressed".light_blue().bold(), " vs ".into(), "Uncompressed".light_red().bold(), " sizes (B)".into()];
    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(Line::from(title).centered())  // Y-axis label at top
                .title_bottom("Row Group".dark_gray())  // X-axis label at bottom
                .borders(Borders::NONE)
        )
        .x_axis(
            Axis::default()
                .style(Style::default().fg(Color::White))
                .bounds([0.0, max_row_group])
                .labels(x_labels)
        )
        .y_axis(
            Axis::default()
                .style(Style::default().fg(Color::White))
                .bounds([0.0, max_size * 1.1])
                .labels(y_labels)
        );

    chart.render(area, buf);
}

fn render_compression_ratio_chart(row_group_stats: &Vec<RowGroupStats>, area: Rect, buf: &mut Buffer) {
    // Prepare compression ratio data: (row_group_index, compression_ratio)
    let ratio_data: Vec<(f64, f64)> = row_group_stats
        .iter()
        .map(|rg| {
            let ratio = if rg.compressed_size > 0 {
                rg.uncompressed_size as f64 / rg.compressed_size as f64
            } else {
                1.0
            };
            (rg.row_group_idx as f64, ratio)
        })
        .collect();

    // Find bounds
    let max_ratio = ratio_data.iter().map(|(_, ratio)| *ratio).fold(0.0, f64::max);
    let max_row_group = row_group_stats.len() as f64 - 1.0;

    let datasets = vec![
        Dataset::default()
            .name("Compression Ratio")
            .marker(symbols::Marker::Dot)
            .style(Style::default().fg(Color::Green))
            .data(&ratio_data),
    ];

    // Create x-axis labels (row group indices)
    let x_labels: Vec<String> = (0..row_group_stats.len())
        .step_by((row_group_stats.len() / 4).max(1)) // Show ~5 labels
        .map(|i| i.to_string())
        .collect();
    
    // Create y-axis labels (compression ratio values)
    let y_range = max_ratio * 1.1 - 1.0;
    let y_step = y_range / 4.0; // 4 intervals
    let y_labels: Vec<String> = (0..4)
        .map(|i| {
            let value = 1.0 + (i as f64 * y_step);
            format!("{:.1}x", value)
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title("Compression Ratio")  // Y-axis label at top
                .title_bottom("Row Group".dark_gray())   // X-axis label at bottom
                .borders(Borders::NONE)
        )
        .x_axis(
            Axis::default()
                .style(Style::default().fg(Color::White))
                .bounds([0.0, max_row_group])
                .labels(x_labels)
        )
        .y_axis(
            Axis::default()
                .style(Style::default().fg(Color::White))
                .bounds([1.0, max_ratio * 1.1])
                .labels(y_labels)
        );

    chart.render(area, buf);
}