use std::fs::File;
use std::path::Path;
use ratatui::{
    buffer::Buffer,
    prelude::Color,
    layout::{Constraint, Layout, Rect},
    style::{Stylize, Style},
    symbols::border,
    text::Line,
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Tabs, Widget},
    Frame,
};
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::app::App;
use crate::metadata::extract_parquet_file_metadata;
use crate::schema::{SchemaColumnType, ColumnType};
use crate::stats::aggregate_column_stats;
use crate::dictionary::extract_dictionary_values;
use crate::utils::{human_readable_bytes, commas};

pub fn render_app(app: &App, frame: &mut Frame) {
    frame.render_widget(AppWidget(app), frame.area());
}

struct AppWidget<'a>(&'a App);

impl<'a> Widget for AppWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let app = self.0;
        
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

        // Render left panel (metadata)
        render_metadata_panel(app, nav_area, buf);

        // Render right panel (tabs and content)
        render_right_panel(app, right_area, buf);
    }
}

fn render_metadata_panel(app: &App, area: Rect, buf: &mut Buffer) {
    // Fetch parquet metadata once per render
    let metadata_result = extract_parquet_file_metadata(&app.file_name);

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
            app.file_name.clone(),
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
    let max_key_len = kv_pairs.iter().map(|(k, _)| k.len()).max().unwrap_or(0);

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
    .areas(area);

    // Render the file name block
    file_name_para.render(file_name_area, buf);

    // Build the table widget
    let desired_height = (rows.len() as u16 + 2).min(table_container_area.height);
    let table_full_width = table_container_area.width;
    let table = Table::new(rows, vec![Constraint::Length(max_key_len as u16), Constraint::Min(max_key_len as u16)])
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

fn render_right_panel(app: &App, area: Rect, buf: &mut Buffer) {
    let [tabs_bar_area, content_area] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Fill(1),
    ])
    .areas(area);

    // Build Tabs widget
    let tab_titles: Vec<Line> = app.tabs.iter().map(|t| Line::from(*t)).collect();
    let tabs_widget = Tabs::new(tab_titles)
        .select(app.active_tab)
        .block(Block::bordered().title("Tabs"));

    tabs_widget.render(tabs_bar_area, buf);

    // Render content based on selected tab
    match app.active_tab {
        0 => render_schema_tab(app, content_area, buf),
        _ => {
            let placeholder = Paragraph::new("Coming soon...")
                .block(Block::bordered().title(Line::from(app.tabs[app.active_tab]).centered()).border_set(border::ROUNDED));
            placeholder.render(content_area, buf);
        }
    }
}

fn render_schema_tab(app: &App, area: Rect, buf: &mut Buffer) {
    let tree_width = app.schema_columns.iter().map(|line| {
        match line {
            SchemaColumnType::Root {display: ref d, ..} => d.len(),
            SchemaColumnType::Primitive {display: ref d, ..} => d.len(),
            SchemaColumnType::Group {display: ref d, ..} => d.len(),
        }
    }).max().unwrap_or(0);

    // Build rows from primitives in order of appearance
    let mut table_rows: Vec<Row> = Vec::new();
    for (idx, line) in app.schema_columns.iter().enumerate() {
        let display = match line {
            SchemaColumnType::Root {display: _, ..} => {continue;},
            SchemaColumnType::Primitive {display: ref d, ..} => d,
            SchemaColumnType::Group {display: ref d, ..} => d,
        };
        if let Some(column_type) = app.schema_map.get(display) {
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
                    if let Some(selected_index) = app.column_selected {
                        if idx == selected_index {
                            row = row.style(Style::default().bg(Color::DarkGray));
                        }
                    }
                    table_rows.push(row);
                }
                ColumnType::Group(repetition) => {
                    let mut row = Row::new(vec![
                        Cell::from(repetition.clone().green()),
                        Cell::from("group".green()),
                    ]);
                    
                    if let Some(selected_index) = app.column_selected {
                        if idx == selected_index {
                            row = row.style(Style::default().bg(Color::DarkGray));
                        }
                    } 
                    table_rows.push(row);
                }
            }
        }
    }

    // Layout: tree | separator | table | separator | stats (if selected)
    let areas = if app.column_selected.is_some() {
        let [tree_area, sep_area, table_stats_area] = Layout::horizontal([
            Constraint::Length(tree_width as u16),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .areas(area);
        
        let [table_area, table_stats_sep, stats_area] = Layout::horizontal([
            Constraint::Fill(2),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .areas(table_stats_area);
        
        (tree_area, sep_area, table_area, Some(table_stats_sep), Some(stats_area))
    } else {
        let [tree_area, sep_area, table_area] = Layout::horizontal([
            Constraint::Length(tree_width as u16),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .areas(area);
        
        (tree_area, sep_area, table_area, None, None)
    };

    // Render tree
    render_schema_tree(app, areas.0, buf);
    
    // Render separator
    let sep_block = Block::default().borders(Borders::RIGHT).fg(Color::Yellow);
    sep_block.render(areas.1, buf);
    
    // Render columns table
    render_columns_table(table_rows, areas.2, buf);
    
    // Render stats if column is selected
    if let (Some(sep_area), Some(stats_area)) = (areas.3, areas.4) {
        let table_stats_sep_block = Block::default().borders(Borders::RIGHT).fg(Color::Yellow);
        table_stats_sep_block.render(sep_area, buf);
        
        render_column_stats(app, stats_area, buf);
    }
}

fn render_schema_tree(app: &App, area: Rect, buf: &mut Buffer) {
    let mut tree_vec = vec![
        "Leaf".blue(),
        ", ".into(),
        "Group".green(),
    ];
    
    if app.column_selected.is_some() {
        tree_vec.extend(vec![", ".into(), "Selected".bold().yellow()]);
    }
    let tree_info = Line::from(tree_vec);

    let list = List::new(
        app.schema_columns.iter().enumerate().map(|(idx, line)| {
            match line {
                SchemaColumnType::Root {display: ref d, ..} => {
                    ListItem::new(d.clone()).dark_gray()
                },
                SchemaColumnType::Primitive {display: ref d, ..} => {
                    let mut item = ListItem::new(d.clone()).blue();
                    if let Some(selected_index) = app.column_selected {
                        if idx == selected_index {
                            item = item.fg(Color::Yellow).bold();
                        }
                    }
                    item
                },
                SchemaColumnType::Group {display: ref d, ..} => {
                    let mut item: ListItem<'_> = ListItem::new(d.clone()).green();
                    if let Some(selected_index) = app.column_selected {
                        if idx == selected_index {
                            item = item.fg(Color::Yellow).bold();
                        }
                    }
                    item
                }
            }
        }).collect::<Vec<ListItem>>()
    ).block(Block::bordered().title(Line::from("Schema Tree").centered()).title_bottom(tree_info.centered()).border_set(border::ROUNDED));
    
    list.render(area, buf);
}

fn render_columns_table(table_rows: Vec<Row>, area: Rect, buf: &mut Buffer) {
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
        .header(Row::new(header.into_iter().map(|h| Cell::from(h).bold().fg(Color::Red))))
        .column_spacing(1)
        .block(Block::bordered().title(Line::from("Columns").centered()).border_set(border::ROUNDED));

    table_widget.render(area, buf);
}

fn render_column_stats(app: &App, area: Rect, buf: &mut Buffer) {
    if let Some(selected_idx) = app.column_selected {
        // Determine column index among leaf columns
        let mut leaf_counter: usize = 0;
        let mut selected_col_idx: Option<usize> = None;
        for (i, l) in app.schema_columns.iter().enumerate() {
            if let SchemaColumnType::Primitive { .. } = l {
                if i == selected_idx {
                    selected_col_idx = Some(leaf_counter);
                    break;
                }
                leaf_counter += 1;
            } else if i == selected_idx {
                selected_col_idx = None;
                break;
            }
        }

        if let Some(col_idx) = selected_col_idx {
            // Open file and gather metadata
            if let Ok(file) = File::open(&Path::new(app.file_name.as_str())) {
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
                        let display_key = &app.schema_columns[selected_idx];
                        if let SchemaColumnType::Primitive { display: ref d, .. } = display_key {
                            if let Some(ColumnType::Primitive(info)) = app.schema_map.get(d) {
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
                            Constraint::Length(3 + (dict_vals.len() as u16 / 3).max(1)),
                        ])
                        .areas(area);

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
                        table_widget.render(area, buf);
                    }
                } else {
                    Paragraph::new("Error reading file stats").render(area, buf);
                }
            } else {
                Paragraph::new("Error opening file").render(area, buf);
            }
        } else {
            Paragraph::new("(No stats available for group)")
                .block(Block::bordered().title(Line::from("Stats").centered()).border_set(border::ROUNDED))
                .render(area, buf);
        }
    }
}