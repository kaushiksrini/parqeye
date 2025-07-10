mod utils;

use clap::{Parser, Subcommand};
use parquet::{basic::{LogicalType, TimeUnit}, file::reader::{FileReader, SerializedFileReader}};
use std::{collections::{HashMap, HashSet}, fs::File, io, path::Path};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    buffer::Buffer,
    crossterm::style::Color,
    layout::{Constraint, Layout, Rect},
    style::Stylize,
    symbols::border,
    text::{Line, Text},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Tabs, Widget},
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

#[derive(Debug)]
pub struct ColumnSchemaInfo {
    pub name: String,
    pub repetition: String,
    pub physical: String,
    pub logical: String,
    pub codec: String,
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
}

pub enum SchemaColumnType {
    // Just the root
    Root {name: String, display: String },
    // name, then display string
    Primitive {name: String, display: String },
    // name, then display string
    Group {name: String, display: String }
}

impl App {
    /// runs the application's main loop until the user quits
    pub fn run(&mut self, terminal: &mut DefaultTerminal, path: &str) -> io::Result<()> {
        self.file_name = path.to_string();
        self.tabs = vec!["Schema", "Row Groups", "Stats", "Meta"];
        self.active_tab = 0; // Schema selected by default
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
        for (col_idx, _col_descr) in schema_descr.columns().iter().enumerate() {
            use std::collections::HashSet;
            let mut codecs: HashSet<String> = HashSet::new();
            let mut encs: HashSet<String> = HashSet::new();
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
                        LogicalType::Decimal { scale, precision } => format!("Decimal (scale={}, precision={})", scale, precision),
                        LogicalType::Integer { bit_width, is_signed } => format!("Integer (bit_width={}, is_signed={})", bit_width, is_signed),
                        LogicalType::Time { is_adjusted_to_u_t_c, unit } => match unit {
                            TimeUnit::MILLIS(_) => format!("Time (utc={}, unit=millis)", is_adjusted_to_u_t_c),
                            TimeUnit::MICROS(_) => format!("Time (utc={}, unit=micros)", is_adjusted_to_u_t_c),
                            TimeUnit::NANOS(_) => format!("Time (utc={}, unit=nanos)", is_adjusted_to_u_t_c),
                        },
                        LogicalType::Timestamp { is_adjusted_to_u_t_c, unit } => match unit {
                            TimeUnit::MILLIS(_) => format!("Timestamp (utc={}, unit=millis)", is_adjusted_to_u_t_c),
                            TimeUnit::MICROS(_) => format!("Timestamp (utc={}, unit=micros)", is_adjusted_to_u_t_c),
                            TimeUnit::NANOS(_) => format!("Timestamp (utc={}, unit=nanos)", is_adjusted_to_u_t_c),
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
        let [file_name_area, table_container_area, _spacer] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Max(11),
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
                for line in &lines {
                    let display = match line {
                        SchemaColumnType::Root {name: _, display: ref d} => {continue;},
                        SchemaColumnType::Primitive {name: _, display: ref d} => d,
                        SchemaColumnType::Group {name: _, display: ref d} => d,
                    };
                    if let Some(column_type) = col_map.get(display) {
                        match column_type {
                            ColumnType::Primitive(info) => {
                                table_rows.push(Row::new(vec![
                                    Cell::from(info.repetition.clone()),
                                    Cell::from(info.physical.clone()),
                                    Cell::from(info.logical.clone()),
                                    Cell::from(info.codec.clone()),
                                    Cell::from(info.encoding.clone()),
                                ]));
                            }
                            ColumnType::Group(repetition) => {
                                table_rows.push(Row::new(vec![
                                    Cell::from(repetition.clone()),
                                    Cell::from("group"),
                                ]));
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

                // Render tree text
                let tree_info = Line::from(vec![
                    "Leaf".blue(),
                    ", ".into(),
                    "Group".green(),
                ]);

                let list = List::new(
                    lines.iter().map(|line| {
                        match line {
                            SchemaColumnType::Root {name: _, display: ref d} => ListItem::new(d.clone()).dark_gray(),
                            SchemaColumnType::Primitive {name: _, display: ref d} => ListItem::new(d.clone()).blue(),
                            SchemaColumnType::Group {name: _, display: ref d} => ListItem::new(d.clone()).green(),
                        }
                    }).collect::<Vec<ListItem>>()
                ).block(Block::bordered().title(Line::from("Schema Tree").centered()).title_bottom(tree_info.centered()).border_set(border::ROUNDED));
                list.render(tree_area, buf);

                // Vertical separator
                let sep_block = Block::default().borders(Borders::RIGHT).fg(Color::Yellow);
                sep_block.render(sep_area, buf);

                // Table of columns
                let header = vec!["Rep", "Physical", "Logical", "Codec", "Encoding"];
                let col_constraints = vec![
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(18),
                    Constraint::Length(8),
                    Constraint::Min(10),
                ];
                let table_widget = Table::new(table_rows, col_constraints)
                    .header(Row::new(header.into_iter().map(|h| Cell::from(h).bold().fg(Color::Green))))
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