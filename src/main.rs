mod utils;

use clap::{Parser, Subcommand};
use parquet::{basic::{LogicalType, TimeUnit}, file::reader::{FileReader, SerializedFileReader}};
use std::{collections::{HashMap, HashSet}, fs::File, io, path::Path};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    buffer::Buffer,
    crossterm::style::Color,
    layout::{Constraint, Layout, Rect},
    style::{Stylize, Style, Modifier},
    symbols::border,
    text::{Line, Text},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Tabs, Widget},
    DefaultTerminal, Frame,
};

use parquet::schema::types::{Type as ParquetType};
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::ParquetMetaData;

#[derive(Parser)]
#[command(author, version, about="Tool to visualize parquet files")]
pub struct Opts {
    #[command(subcommand)] pub cmd: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Print the metadata summary table
    // Meta { path: String },
    // Schema {
    //     path: String,
    //     #[arg(long, help = "Include per-column statistics in the schema table")]
    //     show_stats: bool,
    // },
    // Stats {
    //     path: String,
    //     #[arg(long, help = "Only show a single row group (0-based index)")]
    //     row_group: Option<usize>,
    //     #[arg(long, help = "Include page-level breakdown")] 
    //     page: bool,
    // },

    Tui { path: String },
    // … your other commands …
    // Cat { path: String }
    
}

fn main() -> io::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Tui { path } => tui(&path)?,
        // Command::Meta { path } => print_metadata_table(&path)?,
        // Command::Schema { path, show_stats } => print_schema_table(&path, show_stats),
        // Command::Stats { path, row_group, page } => print_stats(&path, row_group, page)?,

        // …
    }
    Ok(())
}

fn tui(path: &str) -> io::Result<()> {
    let mut terminal = ratatui::init();
    let app_result: Result<(), io::Error> = App::default().run(&mut terminal, path);
    ratatui::restore();
    app_result
}

/// Convert a byte count into a human-readable string (e.g. "2.3 MB").
pub fn human_readable_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{:.0} {}", size, UNITS[unit])
    } else {
        format!("{:.2} {}", size, UNITS[unit])
    }
}

/// Convert a plain count into a human-readable string with K / M / B suffixes.
fn human_readable_count(n: u64) -> String {
    const UNITS: [&str; 4] = ["", "K", "M", "B"]; // up to billions
    let mut unit = 0;
    let mut value = n as f64;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}", n)
    } else {
        format!("{:.1} {}", value, UNITS[unit])
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
            format!("{:.4}", v)
        }
        PhysicalType::DOUBLE if bytes.len() == 8 => {
            let v = f64::from_le_bytes(bytes.try_into().unwrap());
            format!("{:.4}", v)
        }
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            match std::str::from_utf8(bytes) {
                Ok(s) => s.to_string(),
                Err(_) => bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(""),
            }
        }
        _ => bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(""),
    }
}

/// Aggregate statistics (total values, min, max, nulls, distinct) for the given column across all row groups.
fn aggregate_column_stats(md: &ParquetMetaData, col_idx: usize, physical: PhysicalType) -> ColumnStats {
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
                nulls += n as u64;
            }
            if let Some(d) = stats.distinct_count_opt() {
                distinct = Some(distinct.unwrap_or(0) + d as u64);
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

/// Extract dictionary values from dictionary pages for dictionary-encoded columns.
/// This implementation manually parses the dictionary page format for common types.
fn extract_dictionary_values(reader: &SerializedFileReader<File>, col_idx: usize, max_items: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    use parquet::basic::PageType;
    
    let mut dictionary_values = Vec::new();
    let metadata = reader.metadata();
    let schema_descr = metadata.file_metadata().schema_descr();
    let column_descr = schema_descr.column(col_idx);
    let physical_type = column_descr.physical_type();

    // Iterate through all row groups to find dictionary pages
    for rg_idx in 0..reader.num_row_groups() {
        let rg_reader = reader.get_row_group(rg_idx)?;
        let mut page_reader = rg_reader.get_column_page_reader(col_idx)?;
        
        // Read pages to find dictionary page first
        while let Some(page) = page_reader.get_next_page()? {
            if page.page_type() == PageType::DICTIONARY_PAGE {
                let page_buffer = page.buffer();
                let num_values = page.num_values() as usize;
                
                // Manual parsing based on physical type and PLAIN encoding (most common for dictionary pages)
                match physical_type {
                    parquet::basic::Type::BYTE_ARRAY => {
                        // BYTE_ARRAY PLAIN encoding: [length:4][data:length][length:4][data:length]...
                        let mut offset = 0;
                        let buffer_slice = &page_buffer[..];
                        
                        for _ in 0..num_values.min(max_items) {
                            if offset + 4 > buffer_slice.len() {
                                break;
                            }
                            
                            // Read length (little-endian u32)
                            let length = u32::from_le_bytes([
                                buffer_slice[offset],
                                buffer_slice[offset + 1],
                                buffer_slice[offset + 2],
                                buffer_slice[offset + 3],
                            ]) as usize;
                            offset += 4;
                            
                            if offset + length > buffer_slice.len() {
                                break;
                            }
                            
                            // Read string data
                            let string_data = &buffer_slice[offset..offset + length];
                            match std::str::from_utf8(string_data) {
                                Ok(s) => dictionary_values.push(s.to_string()),
                                Err(_) => {
                                    // If not valid UTF-8, show as hex
                                    let hex = string_data.iter()
                                        .take(8)
                                        .map(|b| format!("{:02X}", b))
                                        .collect::<Vec<_>>()
                                        .join("");
                                    dictionary_values.push(format!("0x{}", hex));
                                }
                            }
                            offset += length;
                            
                            if dictionary_values.len() >= max_items {
                                break;
                            }
                        }
                    }
                    parquet::basic::Type::INT32 => {
                        // INT32 PLAIN encoding: [value:4][value:4]...
                        let buffer_slice = &page_buffer[..];
                        let max_vals = (buffer_slice.len() / 4).min(num_values).min(max_items);
                        
                        for i in 0..max_vals {
                            let offset = i * 4;
                            if offset + 4 <= buffer_slice.len() {
                                let value = i32::from_le_bytes([
                                    buffer_slice[offset],
                                    buffer_slice[offset + 1],
                                    buffer_slice[offset + 2],
                                    buffer_slice[offset + 3],
                                ]);
                                dictionary_values.push(value.to_string());
                            }
                        }
                    }
                    parquet::basic::Type::INT64 => {
                        // INT64 PLAIN encoding: [value:8][value:8]...
                        let buffer_slice = &page_buffer[..];
                        let max_vals = (buffer_slice.len() / 8).min(num_values).min(max_items);
                        
                        for i in 0..max_vals {
                            let offset = i * 8;
                            if offset + 8 <= buffer_slice.len() {
                                let value = i64::from_le_bytes([
                                    buffer_slice[offset],
                                    buffer_slice[offset + 1],
                                    buffer_slice[offset + 2],
                                    buffer_slice[offset + 3],
                                    buffer_slice[offset + 4],
                                    buffer_slice[offset + 5],
                                    buffer_slice[offset + 6],
                                    buffer_slice[offset + 7],
                                ]);
                                dictionary_values.push(value.to_string());
                            }
                        }
                    }
                    parquet::basic::Type::INT96 => {
                        // INT96 PLAIN encoding: [value:12][value:12]...
                        // INT96 is typically used for timestamps: 8 bytes nanoseconds + 4 bytes Julian day
                        let buffer_slice = &page_buffer[..];
                        let max_vals = (buffer_slice.len() / 12).min(num_values).min(max_items);
                        
                        for i in 0..max_vals {
                            let offset = i * 12;
                            if offset + 12 <= buffer_slice.len() {
                                // Read as little-endian: first 8 bytes are nanoseconds, last 4 bytes are Julian day
                                let nanos = u64::from_le_bytes([
                                    buffer_slice[offset],
                                    buffer_slice[offset + 1],
                                    buffer_slice[offset + 2],
                                    buffer_slice[offset + 3],
                                    buffer_slice[offset + 4],
                                    buffer_slice[offset + 5],
                                    buffer_slice[offset + 6],
                                    buffer_slice[offset + 7],
                                ]);
                                let julian_day = u32::from_le_bytes([
                                    buffer_slice[offset + 8],
                                    buffer_slice[offset + 9],
                                    buffer_slice[offset + 10],
                                    buffer_slice[offset + 11],
                                ]);
                                
                                // Convert Julian day to Unix epoch (Jan 1, 1970 = Julian day 2440588)
                                const JULIAN_DAY_OF_EPOCH: i64 = 2440588;
                                let days_since_epoch = julian_day as i64 - JULIAN_DAY_OF_EPOCH;
                                let seconds_since_epoch = days_since_epoch * 24 * 60 * 60;
                                let total_nanos = seconds_since_epoch * 1_000_000_000 + nanos as i64;
                                
                                // Convert to readable timestamp
                                let timestamp_secs = total_nanos / 1_000_000_000;
                                let timestamp_nanos = total_nanos % 1_000_000_000;
                                
                                // Format as ISO 8601 timestamp
                                if let Some(datetime) = chrono::DateTime::from_timestamp(timestamp_secs, timestamp_nanos as u32) {
                                    dictionary_values.push(datetime.format("%Y-%m-%d %H:%M:%S%.9f UTC").to_string());
                                } else {
                                    // Fallback: show raw values
                                    dictionary_values.push(format!("INT96(nanos={}, julian_day={})", nanos, julian_day));
                                }
                            }
                        }
                    }
                    parquet::basic::Type::FLOAT => {
                        // FLOAT PLAIN encoding: [value:4][value:4]...
                        let buffer_slice = &page_buffer[..];
                        let max_vals = (buffer_slice.len() / 4).min(num_values).min(max_items);
                        
                        for i in 0..max_vals {
                            let offset = i * 4;
                            if offset + 4 <= buffer_slice.len() {
                                let bytes = [
                                    buffer_slice[offset],
                                    buffer_slice[offset + 1],
                                    buffer_slice[offset + 2],
                                    buffer_slice[offset + 3],
                                ];
                                let value = f32::from_le_bytes(bytes);
                                dictionary_values.push(format!("{:.6}", value));
                            }
                        }
                    }
                    parquet::basic::Type::DOUBLE => {
                        // DOUBLE PLAIN encoding: [value:8][value:8]...
                        let buffer_slice = &page_buffer[..];
                        let max_vals = (buffer_slice.len() / 8).min(num_values).min(max_items);
                        
                        for i in 0..max_vals {
                            let offset = i * 8;
                            if offset + 8 <= buffer_slice.len() {
                                let bytes = [
                                    buffer_slice[offset],
                                    buffer_slice[offset + 1],
                                    buffer_slice[offset + 2],
                                    buffer_slice[offset + 3],
                                    buffer_slice[offset + 4],
                                    buffer_slice[offset + 5],
                                    buffer_slice[offset + 6],
                                    buffer_slice[offset + 7],
                                ];
                                let value = f64::from_le_bytes(bytes);
                                dictionary_values.push(format!("{:.6}", value));
                            }
                        }
                    }
                    _ => {
                        // For other types, show hex representation
                        let hex_preview = page_buffer
                            .iter()
                            .take(32)
                            .map(|b| format!("{:02X}", b))
                            .collect::<Vec<_>>()
                            .join(" ");
                        dictionary_values.push(format!("Binary({}): {}", physical_type, hex_preview));
                    }
                }
                
                if dictionary_values.len() >= max_items {
                    break;
                }
            }
        }
        
        if dictionary_values.len() >= max_items {
            break;
        }
    }

    Ok(dictionary_values)
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() > max {
        let truncated: String = s.chars().take(max - 1).collect();
        format!("{}…", truncated)
    } else {
        s.to_string()
    }
}

fn commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len()/3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

#[derive(Debug)]
pub struct ParquetFileMetadata {
    file_name: String,
    format_version: String,
    created_by: String,
    rows: u64,
    columns: u64,
    row_groups: u64,
    size_raw: u64,
    size_compressed: u64,
    compression_ratio: f64,
    codecs: Vec<String>,
    encodings: String,
    avg_row_size: u64,
}

#[derive(Debug)]
pub struct ColumnSchemaInfo {
    pub name: String,
    pub repetition: String,
    pub physical: String,
    pub logical: String,
    pub codec: String,
    pub converted_type: String,
    pub encoding: String,
}

#[derive(Debug)]
pub enum ColumnType {
    Primitive(ColumnSchemaInfo),
    Group(String),
}

#[derive(Debug, Default)]
pub struct App {
    file_name: String,
    exit: bool,
    tabs: Vec<&'static str>,
    active_tab: usize,
    column_selected: Option<usize>,
    // to be populated by the schema_tree_lines function for schema name and tree
    schema_columns: Vec<SchemaColumnType>,
    // to be populated by the schema_tree_lines function for column information
    schema_map: HashMap<String, ColumnType>,
}

#[derive(Debug)]
pub enum SchemaColumnType {
    // Just the root
    Root {name: String, display: String },
    // name, then display string
    Primitive {name: String, display: String },
    // name, then display string
    Group {name: String, display: String }
}

#[derive(Debug)]
pub struct ColumnStats {
    pub min: Option<String>,
    pub max: Option<String>,
    pub nulls: u64,
    pub distinct: Option<u64>,
    pub total_compressed_size: u64,
    pub total_uncompressed_size: u64,
}

impl App {
    /// runs the application's main loop until the user quits
    pub fn run(&mut self, terminal: &mut DefaultTerminal, path: &str) -> io::Result<()> {
        self.file_name = path.to_string();
        self.tabs = vec!["Schema", "Row Groups"];
        self.active_tab = 0; // Schema selected by default
        self.column_selected = None;
        // TODO: handle errors
        let (schema_columns, schema_map) = self.schema_tree_lines().unwrap();
        self.schema_columns = schema_columns;
        self.schema_map = schema_map;
        while !self.exit {
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn draw(&self, frame: &mut Frame) {
        frame.render_widget(self, frame.area());
    }

    /// updates the application's state based on user input
    fn handle_events(&mut self) -> io::Result<()> {
        match event::read()? {
            // it's important to check that the event is a key press event as
            // crossterm also emits key release and repeat events on Windows.
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                self.handle_key_event(key_event)
            }
            _ => {}
        };
        Ok(())
    }

    // ANCHOR: handle_key_event fn
    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            KeyCode::Right => {
                if self.active_tab + 1 < self.tabs.len() {
                    self.active_tab += 1;
                }
            }
            KeyCode::Left => {
                if self.active_tab > 0 {
                    self.active_tab -= 1;
                }
            }
            KeyCode::Down => {
                if self.active_tab == 0 {
                    // schema tab: move selection down within leaf count
                    let total_columns: usize = self.schema_columns.len();
                    if let Some(idx) = self.column_selected {
                        if idx + 1 < total_columns {
                            self.column_selected = Some(idx + 1);
                        }
                    } else {
                        self.column_selected = Some(1);
                    }
                }
            }
            KeyCode::Up => {
                if self.active_tab == 0 {
                    if let Some(idx) = self.column_selected {
                        if idx > 1 {
                            self.column_selected = Some(idx - 1);
                        } else {
                            self.column_selected = None
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }

    fn parquet_file_metadata(&self) -> Result<ParquetFileMetadata, Box<dyn std::error::Error>> {
        let file = match File::open(&Path::new(self.file_name.as_str())) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to open file: {}", e);
                return Err(Box::new(e));
            }
        };
    
        // Create a Parquet file reader
        let reader: SerializedFileReader<File> = match SerializedFileReader::new(file) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Failed to read Parquet file: {}", e);
                return Err(Box::new(e));
            }
        };
    
        let md = reader.metadata();
    
        let binding = md.file_metadata().created_by();
        let version        = md.file_metadata().version();
        let created_by     = binding.as_deref().unwrap_or("—");
        let row_groups     = md.num_row_groups();
        let total_rows: i64 = md.row_groups().iter().map(|rg| rg.num_rows()).sum();
        let num_cols       = md.file_metadata().schema_descr().num_columns();
    
        // --------------------------------------------------
        // Column-chunk level aggregation
        // --------------------------------------------------
        let mut raw_size: u64 = 0;
        let mut compressed_size: u64 = 0;
        let mut encodings_seen: HashSet<String> = HashSet::new();
        let mut codec_counts: HashMap<String, usize> = HashMap::new();
    
        let mut rows_per_rg: Vec<i64> = Vec::with_capacity(row_groups as usize);
    
        for rg in md.row_groups() {
            rows_per_rg.push(rg.num_rows());
            for col in rg.columns() {
                raw_size += col.uncompressed_size() as u64;
                compressed_size += col.compressed_size() as u64;
    
                let codec_name = format!("{:?}", col.compression());
                *codec_counts.entry(codec_name).or_insert(0) += 1;
    
                for enc in col.encodings() {
                    encodings_seen.insert(format!("{:?}", enc));
                }
            }
        }
    
        // Row-group stats
        let avg_rows = (total_rows as f64) / (row_groups as f64);
        let min_rows = rows_per_rg.iter().min().copied().unwrap_or(0);
        let max_rows = rows_per_rg.iter().max().copied().unwrap_or(0);
    
        // Size & compression ratio
        let compression_ratio  = if compressed_size > 0 { raw_size as f64 / compressed_size as f64 } else { 0.0 };
    
        // Codec summary with counts
        let mut codec_vec: Vec<String> = codec_counts.iter()
            .map(|(c, n)| format!("{}({})", c, n))
            .collect();
        codec_vec.sort();
        let codec_summary = codec_vec.join("  ");
    
        // Encoding summary
        let mut encodings: Vec<String> = encodings_seen.into_iter().collect();
        encodings.sort();
        let encodings_summary = encodings.join(", ");
    
        // Average row size
        let avg_row_size = if total_rows > 0 { raw_size as f64 / total_rows as f64 } else { 0.0 };

        /// just get the file name without the path
        let file_name = self.file_name.split("/").last().unwrap().to_string();
        Ok(ParquetFileMetadata {
            file_name,
            format_version: version.to_string(),
            created_by: created_by.to_string(),
            rows: total_rows as u64,
            columns: num_cols as u64,
            row_groups: row_groups as u64,
            size_raw: raw_size,
            size_compressed: compressed_size,
            compression_ratio: compression_ratio,
            codecs: codec_vec,
            encodings: encodings_summary,
            avg_row_size: avg_row_size as u64,
        })
    }

    /// Build a tree representation of the Parquet schema as a Vec of lines
    fn schema_tree_lines(&self) -> Result<(Vec<SchemaColumnType>, HashMap<String, ColumnType>), Box<dyn std::error::Error>> {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        // Open file
        let file = File::open(&Path::new(self.file_name.as_str()))?;
        let reader: SerializedFileReader<File> = SerializedFileReader::new(file)?;
        let md = reader.metadata();
        let schema_descr = md.file_metadata().schema_descr();
        let root = schema_descr.root_schema();

        // ------------------------------------------------------------------
        // Pre-compute codec + encoding summary for every leaf column
        // ------------------------------------------------------------------
        let mut leaf_summaries: Vec<(String, String)> = Vec::new(); // (codec_summary, enc_summary)
        for (col_idx, _) in schema_descr.columns().iter().enumerate() {
            use std::collections::HashSet;
            let mut codecs: HashSet<String> = HashSet::new();
            let mut encs: HashSet<String> = HashSet::new();

            // expensive operation, since it iterates over all row groups
            for rg in md.row_groups() {
                let col_chunk = rg.column(col_idx);
                codecs.insert(format!("{:?}", col_chunk.compression()));
                for enc in col_chunk.encodings() {
                    encs.insert(format!("{:?}", enc));
                }
            }
            let mut codec_vec: Vec<String> = codecs.into_iter().collect();
            codec_vec.sort();
            let codec_summary = codec_vec.join(", ");

            let mut enc_vec: Vec<String> = encs.into_iter().collect();
            enc_vec.sort();
            let enc_summary = enc_vec.join(", ");

            leaf_summaries.push((codec_summary, enc_summary));
        }

        // Recursive traversal helper
        fn traverse(node: &ParquetType, prefix: String, is_last: bool, lines: &mut Vec<SchemaColumnType>, map: &mut HashMap<String, ColumnType>, leaf_idx: &mut usize, summaries: &Vec<(String,String)>) {
            let connector: &'static str = if is_last { "└─" } else { "├─" };
            let line = format!("{}{} {}", prefix, connector, node.name());

            // Determine details for the node
            if node.is_primitive() {
                // Leaf column
                let repetition = format!("{:?}", node.get_basic_info().repetition());
                let physical = format!("{:?}", node.get_physical_type());
                let logical = match node.get_basic_info().logical_type() {
                    Some(logical) => match logical {
                        LogicalType::Decimal { scale, precision } => format!("Decimal({},{})", scale, precision),
                        LogicalType::Integer { bit_width, is_signed } => format!("Integer({},{})", bit_width, if is_signed { "sign" } else { "unsign" }),
                        LogicalType::Time { is_adjusted_to_u_t_c, unit } => match unit {
                            TimeUnit::MILLIS(_) => format!("Time({}, millis)", if is_adjusted_to_u_t_c { "utc" } else { "local" }),
                            TimeUnit::MICROS(_) => format!("Time({}, micros)", if is_adjusted_to_u_t_c { "utc" } else { "local" }),
                            TimeUnit::NANOS(_) => format!("Time({}, nanos)", if is_adjusted_to_u_t_c { "utc" } else { "local" }),
                        },
                        LogicalType::Timestamp { is_adjusted_to_u_t_c, unit } => match unit {
                            TimeUnit::MILLIS(_) => format!("Timestamp({}, millis)", if is_adjusted_to_u_t_c { "utc" } else { "local" }),
                            TimeUnit::MICROS(_) => format!("Timestamp({}, micros)", if is_adjusted_to_u_t_c { "utc" } else { "local" }),
                            TimeUnit::NANOS(_) => format!("Timestamp({}, nanos)", if is_adjusted_to_u_t_c { "utc" } else { "local" }),
                        },
                        _ => format!("{:?}", logical),
                    },
                    None => String::new(),
                };

                // codec & encoding summary from pre-computed vector
                let (codec_sum, enc_sum) = &summaries[*leaf_idx];
                let column_info = ColumnSchemaInfo {
                    name: node.name().to_string(),
                    repetition: repetition.clone(),
                    physical: physical.clone(),
                    logical: logical.clone(),
                    codec: codec_sum.clone(),
                    encoding: enc_sum.clone(),
                    converted_type: node.get_basic_info().converted_type().to_string(),
                };
                map.insert(line.clone(), ColumnType::Primitive(column_info));
                lines.push(SchemaColumnType::Primitive {name: node.name().to_string(), display: line});
                
                *leaf_idx += 1;
            } else {
                // Group / map etc.
                let repetition = format!("{:?}", node.get_basic_info().repetition());
                // line.push_str(&format!(" {} group", repetition));
                map.insert(line.clone(), ColumnType::Group(repetition));
                lines.push(SchemaColumnType::Group {name: node.name().to_string(), display: line});
            }

            if node.is_group() {
                let fields = node.get_fields();
                let count = fields.len();
                for (idx, child) in fields.iter().enumerate() {
                    let next_prefix = format!("{}{}", prefix, if is_last { "   " } else { "│  " });
                    traverse(child.as_ref(), next_prefix, idx == count - 1, lines, map, leaf_idx, summaries);
                }
            }

        }

        let mut lines: Vec<SchemaColumnType> = Vec::new();
        // Root line
        lines.push(SchemaColumnType::Root {name: "root".to_string(), display: "└─ root (message)".to_string()});

        let mut column_to_type: HashMap<String, ColumnType> = HashMap::new();

        let children = root.get_fields();
        let count = children.len();
        let mut leaf_idx: usize = 0;
        for (idx, child) in children.iter().enumerate() {
            traverse(child.as_ref(), "   ".to_string(), idx == count - 1, &mut lines, &mut column_to_type, &mut leaf_idx, &leaf_summaries);
        }

        Ok((lines, column_to_type))
    }
}


#[derive(Default)]
pub enum TableTab {
    #[default]
    Schema,
    RowGroups,
}

#[derive(Default)]
pub struct TableView {
    pub selected_tab: TableTab,
}


impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Build the surrounding block with title and instructions
        let title: Line<'_> = Line::from(" datatools ".bold().fg(Color::Green));
        let block: Block<'_> = Block::bordered()
            .title(title.centered())
            .border_set(border::THICK);

        // Compute the inner area of the outer block (the space inside borders)
        let inner_area = block.inner(area);

        block.render(area, buf);

        let [left_area, right_area] = Layout::horizontal([Constraint::Fill(2), Constraint::Fill(5)])
            .margin(1)
            .areas(inner_area);

        let [nav_area, margin_area ] = Layout::horizontal([Constraint::Fill(1), Constraint::Length(3)])
            .areas(left_area);

        let vertical_separator = Block::default()
            .borders(Borders::RIGHT)
            .fg(Color::Yellow);

        vertical_separator.render(margin_area, buf);

        // Fetch parquet metadata once per render. If it fails, show an error row instead.
        let metadata_result = self.parquet_file_metadata();

        // Separate file name and remaining metadata
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
                    ("Size (compressed)".into(), human_readable_bytes(md.size_compressed)),
                    ("Compression ratio".into(), format!("{:.2}x", md.compression_ratio)),
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
        let file_name_para = Paragraph::new(file_name_display.green())
            .block(
                Block::bordered()
                    .title(Line::from("File Name".yellow().bold()).centered())
                    .border_set(border::ROUNDED),
            );

        let metadata_block = Block::bordered()
            .title(Line::from("File Metadata".yellow().bold()).centered())
            .border_set(border::ROUNDED);

        let kv_len = kv_pairs.len();

        let max_key_len = kv_pairs
            .iter()
            .map(|(k, _)| k.len())
            .max()
            .unwrap_or(0);

        // Calculate width required for second column (fallback to inner_area width on overflow)
        let max_val_len = kv_pairs
            .iter()
            .map(|(_, v)| v.len())
            .max()
            .unwrap_or(0);

        let table_width_est = max_key_len + 1 /*spacing*/ + max_val_len + 2 /*borders*/;

        // Prepare rows for the widget
        let rows: Vec<Row> = kv_pairs
            .into_iter()
            .map(|(k, v)| {
                Row::new(vec![
                    Cell::from(k).bold().fg(Color::Blue),
                    Cell::from(v),
                ])
            })
            .collect();

        // Split left pane vertically: file name block on top, metadata table below
        let [file_name_area, table_container_area, _spacer] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Max(kv_len as u16 + 2),
            Constraint::Fill(1),
        ])
        .areas(nav_area);

        // Render the file name block
        file_name_para.render(file_name_area, buf);


        // Build the table widget
        let desired_height = (rows.len() as u16 + 2).min(table_container_area.height);
        // The table should span the full horizontal space available
        let table_full_width = table_container_area.width;
        let table = Table::new(rows, vec![Constraint::Length(max_key_len as u16), Constraint::Min(max_key_len as u16)])
            .column_spacing(1)
            .block(metadata_block);

        // Compute area sized to table (but not exceeding available area)
        let table_area = ratatui::layout::Rect {
            x: table_container_area.x,
            y: table_container_area.y,
            width: table_full_width,
            height: desired_height,
        };

        table.render(table_area, buf);

        // --------------------------------------------------
        // Right area: Tabs and content
        // --------------------------------------------------

        let [tabs_bar_area, content_area] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Fill(1),
        ])
        .areas(right_area);

        // Build Tabs widget
        let tab_titles: Vec<Line> = self.tabs.iter().map(|t| Line::from(*t)).collect();
        let tabs_widget = Tabs::new(tab_titles)
            .select(self.active_tab)
            .block(Block::bordered().title("Tabs"));

        tabs_widget.render(tabs_bar_area, buf);

        // Render content based on selected tab
        match self.active_tab {
            0 => { // Schema view split (tree | table)
                let (lines, col_map) = match self.schema_tree_lines() {
                    Ok(res) => res,
                    Err(e) => {
                        let err_para = Paragraph::new(format!("Error reading schema: {}", e))
                            .block(Block::bordered().title("Schema").border_set(border::ROUNDED));
                        err_para.render(content_area, buf);
                        return;
                    }
                };

                let tree_width = lines.iter().map(|line| {
                    match line {
                        SchemaColumnType::Root {name: _, display: ref d} => d.len(),
                        SchemaColumnType::Primitive {name: _, display: ref d} => d.len(),
                        SchemaColumnType::Group {name: _, display: ref d} => d.len(),
                    }
                }).max().unwrap_or(0);

                // Build rows from primitives in order of appearance
                let mut table_rows: Vec<Row> = Vec::new();
                for (idx, line) in lines.iter().enumerate() {
                    let display = match line {
                        SchemaColumnType::Root {name: _, display: ref d} => {continue;},
                        SchemaColumnType::Primitive {name: _, display: ref d} => d,
                        SchemaColumnType::Group {name: _, display: ref d} => d,
                    };
                    if let Some(column_type) = col_map.get(display) {
                        match column_type {
                            ColumnType::Primitive(info) => {
                                let mut row = Row::new(vec![
                                    Cell::from(info.repetition.clone()),
                                    Cell::from(info.physical.clone()),
                                    Cell::from(info.logical.clone()),
                                    Cell::from(info.converted_type.clone()),
                                    Cell::from(info.codec.clone()),
                                    Cell::from(info.encoding.clone()),
                                ]);
                                if let Some(selected_index) = self.column_selected {
                                    if idx == selected_index {
                                        row = row.style(Style::default().bg(ratatui::prelude::Color::DarkGray));
                                    }
                                }
                                table_rows.push(row);
                            }
                            ColumnType::Group(repetition) => {
                                let mut row = Row::new(vec![
                                    Cell::from(repetition.clone().green()),
                                    Cell::from("group".green()),
                                ]);
                                
                                if let Some(selected_index) = self.column_selected {
                                    if idx == selected_index {
                                        row = row.style(Style::default().bg(ratatui::prelude::Color::DarkGray));
                                    }
                                } 
                                table_rows.push(row);
                            }

                        }
                    }
                }

                // Layout: tree | separator | table
                let [tree_area, sep_area, table_area] = Layout::horizontal([
                    Constraint::Length(tree_width as u16),
                    Constraint::Length(1),
                    Constraint::Fill(1),
                ])
                .areas(content_area);
                
                let mut table_area = table_area;
                let mut stats_area = table_area;
                let mut table_stats_sep = sep_area;
                if self.column_selected.is_some() {
                    let [t1, t2, t3] = Layout::horizontal([
                        Constraint::Fill(2),
                        Constraint::Length(1),
                        Constraint::Fill(1),
                    ])
                    .areas(table_area);
                    table_area = t1;
                    table_stats_sep = t2;
                    stats_area = t3;
                }

                // Render tree text
                let mut tree_vec = vec![
                    "Leaf".blue(),
                    ", ".into(),
                    "Group".green(),
                ];
                
                if let Some(_) = self.column_selected {
                    tree_vec.extend(vec![", ".into(), "Selected".bold().yellow()]);
                }
                let tree_info = Line::from(tree_vec);

                let list = List::new(
                    lines.iter().enumerate().map(|(idx, line)| {
                        match line {
                            SchemaColumnType::Root {name: _, display: ref d} => {
                                ListItem::new(d.clone()).dark_gray()
                            },
                            SchemaColumnType::Primitive {name: _, display: ref d} => {
                                let mut item = ListItem::new(d.clone()).blue();
                                if let Some(selected_index) = self.column_selected {
                                    if idx == selected_index {
                                        item = item.fg(Color::Yellow).bold();
                                    }
                                }
                                item
                            },
                            SchemaColumnType::Group {name: _, display: ref d} => {
                                let mut item: ListItem<'_> = ListItem::new(d.clone()).green();
                                if let Some(selected_index) = self.column_selected {
                                    if idx == selected_index {
                                        item = item.fg(Color::Yellow).bold();
                                    }
                                }
                                item
                            }
                        }
                    }).collect::<Vec<ListItem>>()
                ).block(Block::bordered().title(Line::from("Schema Tree").centered()).title_bottom(tree_info.centered()).border_set(border::ROUNDED));
                list.render(tree_area, buf);

                // Vertical separator
                let sep_block = Block::default().borders(Borders::RIGHT).fg(Color::Yellow);
                sep_block.render(sep_area, buf);

                // vertical separator between table and stats
                if let Some(selected_idx) = self.column_selected {
                    let table_stats_sep_block = Block::default().borders(Borders::RIGHT).fg(Color::Yellow);
                    table_stats_sep_block.render(table_stats_sep, buf);

                    // Determine column index among leaf columns
                    let mut leaf_counter: usize = 0;
                    let mut selected_col_idx: Option<usize> = None;
                    for (i, l) in lines.iter().enumerate() {
                        if let SchemaColumnType::Primitive { .. } = l {
                            if i == selected_idx {
                                selected_col_idx = Some(leaf_counter);
                                break;
                            }
                            leaf_counter += 1;
                        } else if i == selected_idx {
                            // selected is a Group/root – no per-column stats
                            selected_col_idx = None;
                            break;
                        }
                    }

                    if let Some(col_idx) = selected_col_idx {
                        // Open file and gather metadata
                        if let Ok(file) = File::open(&Path::new(self.file_name.as_str())) {
                            if let Ok(reader) = SerializedFileReader::new(file) {
                                let md = reader.metadata();
                                let schema_descr = md.file_metadata().schema_descr();
                                let physical = schema_descr.column(col_idx).physical_type();

                                let column_stats = aggregate_column_stats(&md, col_idx, physical);

                                let mut kv_stats: Vec<(String, String)> = vec![
                                    ("Null count".into(), commas(column_stats.nulls)),
                                ];
                                if let Some(ref min_val) = column_stats.min {
                                    kv_stats.push(("Min".into(), min_val.clone()));
                                }
                                if let Some(ref max_val) = column_stats.max {
                                    kv_stats.push(("Max".into(), max_val.clone()));
                                }
                                if let Some(dist) = column_stats.distinct {
                                    kv_stats.push(("Distinct".into(), commas(dist as u64)));
                                }
                                kv_stats.push(("Total compressed size".into(), human_readable_bytes(column_stats.total_compressed_size)));
                                kv_stats.push(("Total uncompressed size".into(), human_readable_bytes(column_stats.total_uncompressed_size)));
                                kv_stats.push(("Compression ratio".into(), format!("{:.2}x", column_stats.total_uncompressed_size as f64 / column_stats.total_compressed_size as f64)));
                                
                                // Check for dictionary encoding and extract values
                                let encodings_str = {
                                    // Retrieve encoding summary from ColumnSchemaInfo where possible
                                    let display_key = &lines[selected_idx];
                                    if let SchemaColumnType::Primitive { display: ref d, .. } = display_key {
                                        if let Some(ColumnType::Primitive(info)) = col_map.get(d) {
                                            info.encoding.clone()
                                        } else { String::new() }
                                    } else { String::new() }
                                };

                                let dictionary_sample: Option<Vec<String>> = if encodings_str.contains("DICTIONARY") {
                                    match extract_dictionary_values(&reader, col_idx, 10) {
                                        Ok(sample_vals) if !sample_vals.is_empty() => Some(sample_vals),
                                        _ => None
                                    }
                                } else {
                                    None
                                };

                                // Determine layout for key/value table
                                let max_key_len = kv_stats.iter().map(|(k, _)| k.len()).max().unwrap_or(0);

                                let rows: Vec<Row> = kv_stats.into_iter()
                                    .map(|(k, v)| {
                                        Row::new(vec![
                                            Cell::from(k).bold().fg(Color::Blue),
                                            Cell::from(v),
                                        ])
                                    })
                                    .collect();

                                // Split stats area if we have dictionary samples to show
                                if let Some(ref dict_vals) = dictionary_sample {
                                    let [table_area, dict_area] = Layout::vertical([
                                        Constraint::Fill(1),
                                        Constraint::Length(3 + (dict_vals.len() as u16 / 3).max(1)), // Estimate height needed
                                    ])
                                    .areas(stats_area);

                                    let table_widget = Table::new(rows, vec![Constraint::Length(max_key_len as u16), Constraint::Min(5)])
                                        .column_spacing(1)
                                        .block(Block::bordered().title(Line::from("Stats").centered()).border_set(border::ROUNDED));
                                    table_widget.render(table_area, buf);

                                    // Render dictionary sample paragraph
                                    let dict_text = format!("{}", dict_vals.join(", "));
                                    let dict_paragraph = Paragraph::new(dict_text)
                                        .wrap(ratatui::widgets::Wrap { trim: true })
                                        .block(Block::bordered().title(Line::from(format!("Dictionary Sample ({})", dict_vals.len())).centered()).border_set(border::ROUNDED));
                                    dict_paragraph.render(dict_area, buf);
                                } else {
                                    let table_widget = Table::new(rows, vec![Constraint::Length(max_key_len as u16), Constraint::Min(5)])
                                        .column_spacing(1)
                                        .block(Block::bordered().title(Line::from("Stats").centered()).border_set(border::ROUNDED));
                                    table_widget.render(stats_area, buf);
                                }
                            } else {
                                // reader error
                                Paragraph::new("Error reading file stats").render(stats_area, buf);
                            }
                        } else {
                            Paragraph::new("Error opening file").render(stats_area, buf);
                        }
                    } else {
                        Paragraph::new("(No stats available for group)")
                            .block(Block::bordered().title(Line::from("Stats").centered()).border_set(border::ROUNDED))
                            .render(stats_area, buf);
                    }
                }

                // Table of columns
                let header = vec!["Rep", "Physical", "Logical", "Converted Type", "Codec", "Encoding"];
                let col_constraints = vec![
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(18),
                    Constraint::Length(10),
                    Constraint::Length(8),
                    Constraint::Min(10),
                ];
                let table_widget = Table::new(table_rows, col_constraints)
                    .header(Row::new(header.into_iter().map(|h| Cell::from(h).bold().fg(Color::DarkRed))))
                    .column_spacing(1)
                    .block(Block::bordered().title(Line::from("Columns").centered()).border_set(border::ROUNDED));

                table_widget.render(table_area, buf);
            }
            _ => {
                let placeholder = Paragraph::new("Coming soon...")
                    .block(Block::bordered().title(Line::from(self.tabs[self.active_tab]).centered()).border_set(border::ROUNDED));
                placeholder.render(content_area, buf);
            }
        }
        
    }
}