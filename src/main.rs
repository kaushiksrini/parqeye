// mod schema;
// mod stats;
mod utils;

use clap::{Parser, Subcommand};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{collections::{HashMap, HashSet}, fs::File, io, path::Path};
// use schema::print_schema_table;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    buffer::Buffer,
    crossterm::style::Color,
    layout::{Constraint, Layout, Rect},
    style::Stylize,
    symbols::border,
    text::{Line, Text},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Widget},
    DefaultTerminal, Frame,
};

use parquet::schema::types::{Type as ParquetType};

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
    rows: u64,
    columns: u64,
    row_groups: u64,
    size_raw: u64,
    size_compressed: u64,
    codecs: Vec<String>,
    encodings: String,
    avg_row_size: u64,
}

#[derive(Debug, Default)]
pub struct App {
    file_name: String,
    exit: bool,
}

impl App {
    /// runs the application's main loop until the user quits
    pub fn run(&mut self, terminal: &mut DefaultTerminal, path: &str) -> io::Result<()> {
        self.file_name = path.to_string();
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
            _ => {}
        }
    }
    // ANCHOR_END: handle_key_event fn

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
        let raw_size_hr        = human_readable_bytes(raw_size);
        let compressed_size_hr = human_readable_bytes(compressed_size);
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

        Ok(ParquetFileMetadata {
            file_name: self.file_name.clone(),
            format_version: format!("{}  ({})", version, created_by),
            rows: total_rows as u64,
            columns: num_cols as u64,
            row_groups: row_groups as u64,
            size_raw: raw_size,
            size_compressed: compressed_size,
            codecs: codec_vec,
            encodings: encodings_summary,
            avg_row_size: avg_row_size as u64,
        })
    }

    /// Build a tree representation of the Parquet schema as a Vec of lines
    fn schema_tree_lines(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        // Open file
        let file = File::open(&Path::new(self.file_name.as_str()))?;
        let reader: SerializedFileReader<File> = SerializedFileReader::new(file)?;
        let schema_descr = reader.metadata().file_metadata().schema_descr();
        let root = schema_descr.root_schema();

        // Recursive traversal helper
        fn traverse(node: &ParquetType, prefix: String, is_last: bool, lines: &mut Vec<String>, map: &mut HashMap<String, String>) {
            let connector: &'static str = if is_last { "└─" } else { "├─" };
            let line = format!("{}{} {}", prefix, connector, node.name());

            // Determine details for the node
            if node.is_primitive() {
                // Leaf column
                let repetition = format!("{:?}", node.get_basic_info().repetition());
                let physical = format!("{:?}", node.get_physical_type());
                let logical = match node.get_basic_info().logical_type() {
                    Some(logical) => format!("[{:?}]", logical),
                    None => "".to_string(),
                };
                let codecs = 

                // line.push_str(&format!(" {} | {}", repetition, physical));
                map.insert(line.clone(), format!("{} | {} {}", repetition, physical, logical));
            } else {
                // Group / map etc.
                let repetition = format!("{:?}", node.get_basic_info().repetition());
                // line.push_str(&format!(" {} group", repetition));
                map.insert(line.clone(), format!("{} group", repetition));
            }

            lines.push(line);

            if node.is_group() {
                let fields = node.get_fields();
                let count = fields.len();
                for (idx, child) in fields.iter().enumerate() {
                    let next_prefix = format!("{}{}", prefix, if is_last { "   " } else { "│  " });
                    traverse(child.as_ref(), next_prefix, idx == count - 1, lines, map);
                }
            }

        }

        let mut lines: Vec<String> = Vec::new();
        // Root line
        lines.push("└─ root (message)".to_string());

        let mut column_to_type: HashMap<String, String> = HashMap::new();

        let children = root.get_fields();
        let count = children.len();
        for (idx, child) in children.iter().enumerate() {
            traverse(child.as_ref(), "   ".to_string(), idx == count - 1, &mut lines, &mut column_to_type);
        }

        let max_len = lines.iter().map(|line| line.len()).max().unwrap_or(0);

        for line in lines.iter_mut() {
            if let Some(column_type) = column_to_type.get(line) {
                let line_len = max_len + 3 - line.len();
                line.push_str(&format!("{} {}", " ".repeat(line_len), column_type));
            }
        }

        Ok(lines)
    }
}
// ANCHOR_END: impl App



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

// impl Widget for TableView {

// }

// ANCHOR: impl Widget
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

        let [left_area, right_area] = Layout::horizontal([Constraint::Fill(1), Constraint::Fill(4)])
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
                    ("Rows".into(), commas(md.rows)),
                    ("Columns".into(), md.columns.to_string()),
                    ("Row groups".into(), md.row_groups.to_string()),
                    ("Size (raw)".into(), human_readable_bytes(md.size_raw)),
                    ("Size (compressed)".into(), human_readable_bytes(md.size_compressed)),
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
        let [file_name_area, table_container_area, nav_tab] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Fill(1),
            Constraint::Length(3),
        ])
        .areas(nav_area);

        // Render the file name block
        file_name_para.render(file_name_area, buf);

        // Render the selection widget for the tabs (placeholder)
        let tab_selector = Paragraph::new(
            Line::from("Schema").bold().fg(Color::Blue)
        );
        tab_selector.render(nav_tab, buf);

        // Build the table widget
        let desired_height = (rows.len() as u16 + 2).min(table_container_area.height);
        let desired_width = table_width_est.min(table_container_area.width as usize) as u16;
        let table = Table::new(rows, vec![Constraint::Length(max_key_len as u16), Constraint::Min(10)])
            .column_spacing(1)
            .block(metadata_block);

        // Compute area sized to table (but not exceeding available area)
        let table_area = ratatui::layout::Rect {
            x: table_container_area.x,
            y: table_container_area.y,
            width: desired_width,
            height: desired_height,
        };

        table.render(table_area, buf);

        // --------------------------------------------------
        // Right area: Parquet schema tree view
        // --------------------------------------------------
        let schema_text = match self.schema_tree_lines() {
            Ok(lines) => lines.join("\n"),
            Err(e) => format!("Error reading schema: {}", e),
        };

        let schema_para = Paragraph::new(schema_text);

        schema_para.render(right_area, buf);
    }
}