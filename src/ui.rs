use parquet::file::{
    metadata::ParquetMetaData,
    reader::{FileReader, SerializedFileReader},
};
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    prelude::Color,
    style::{Style, Stylize},
    symbols::border,
    text::Line,
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Widget},
    Frame,
};
use std::path::Path;
use std::{fs::File, io};

use crate::{components::{MetadataComponent, ScrollbarComponent, TabsComponent}, row_groups::RowGroupsTab, schema::SchemaTab, visualize::VisualizeTab};
use crate::dictionary::extract_dictionary_values;
use crate::schema::{ColumnType, SchemaColumnType};
use crate::stats::aggregate_column_stats;
use crate::utils::{commas, human_readable_bytes};
use crate::{app::App, metadata::extract_file_metadata};

use crate::column_chunk::{
    render_row_group_charts, RowGroupColumnMetadata, RowGroupPageStats, RowGroupStats,
};

pub fn render_app(app: &mut App, frame: &mut Frame) {
    frame.render_widget(AppWidget(app), frame.area());
}

struct AppWidget<'a>(&'a mut App);

impl<'a> Widget for AppWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let app = self.0;

        // Build the surrounding block with title and instructions
        let title: Line<'_> = Line::from(" parqeye ".bold().fg(Color::Green));
        let block: Block<'_> = Block::bordered()
            .title(title.centered())
            .border_set(border::THICK);

        // Compute the inner area of the outer block (the space inside borders)
        let inner_area = block.inner(area);
        block.render(area, buf);

        let [metadata_area, right_area] =
            Layout::horizontal([Constraint::Fill(2), Constraint::Fill(5)])
                .margin(1)
                .areas(inner_area);

        let [nav_area, margin_area] =
            Layout::horizontal([Constraint::Fill(1), Constraint::Length(1)]).areas(metadata_area);

        Block::default()
            .borders(Borders::RIGHT)
            .fg(Color::Yellow)
            .render(margin_area, buf);

        // Render left panel (metadata)
        let metadata_component =
            MetadataComponent::new(app.file_name.clone()).with_title("File Metadata".to_string());
        metadata_component.render(nav_area, buf);

        // Render right panel (tabs and content)
        render_right_panel(app, right_area, buf);
    }
}

fn render_right_panel(app: &mut App, area: Rect, buf: &mut Buffer) {
    let [tabs_bar_area, content_area] =
        Layout::vertical([Constraint::Length(3), Constraint::Fill(1)]).areas(area);

    // Build Tabs widget
    let tabs_widget = TabsComponent::new(app.active_tab.all_tab_names())
        .with_selected_tab(app.active_tab)
        .with_title("Tabs".to_string());

    tabs_widget.render(tabs_bar_area, buf);

    // Render content based on selected tab

    let id = app.active_tab;
    if let Some(mut tab) = app.tabs.remove(&id) {
        tab.render(app, area, buf); // now we can borrow &mut self safely
        app.tabs.insert(id, tab);
    } else {
        let placeholder = Paragraph::new("Coming soon...").block(
            Block::bordered()
                .title(Line::from("Missing tab").centered())
                .border_set(border::ROUNDED),
        );
        placeholder.render(content_area, buf);
    }
}

pub fn render_row_groups_tab(app: &mut App, area: Rect, buf: &mut Buffer, tab: &RowGroupsTab) {
    let tree_width = app
        .schema_columns
        .iter()
        .map(|line| match line {
            SchemaColumnType::Root { display: ref d, .. } => d.len(),
            SchemaColumnType::Primitive { display: ref d, .. } => d.len(),
            SchemaColumnType::Group { display: ref d, .. } => d.len(),
        })
        .max()
        .unwrap_or(0)
        .max(24); // max for the bottom of the chart

    let [tree_area, central_area] =
        Layout::horizontal([Constraint::Length(tree_width as u16), Constraint::Fill(1)])
            .areas(area);

    render_schema_tree(app, tree_area, buf, tab.column_selected, tab.scroll_offset);

    // now we render the stats for that row group
    // split the area into 3 parts with majority in the center and others in the side

    let [main_stats_area, _, column_stats_area] = Layout::horizontal([
        Constraint::Percentage(40),
        Constraint::Length(1),
        Constraint::Percentage(60),
    ])
    .areas(central_area);

    let md: ParquetMetaData = extract_file_metadata(&app.file_name)
        .map_err(|e| io::Error::other(e.to_string()))
        .unwrap();

    // get a ParquetObjectReader for a file
    // let reader = ParquetObjectReader::new(app.file_name.clone());

    let [row_group_stats_area, charts_area] =
        Layout::vertical([Constraint::Length(4), Constraint::Fill(1)]).areas(main_stats_area);

    let max_row_group_idx = md.row_groups().len() - 1;
    app.row_group_selected = app.row_group_selected.min(max_row_group_idx);

    RowGroupStats::from_parquet_file(&md, app.row_group_selected).render(row_group_stats_area, buf);
    render_row_group_charts(&app.row_group_stats, charts_area, buf);

    let [row_group_column_metadata_area, row_group_page_stats_area] =
        Layout::vertical([Constraint::Length(10), Constraint::Fill(1)]).areas(column_stats_area);

    if let Some(column_selected) = tab.column_selected {
        match app.schema_columns[column_selected] {
            SchemaColumnType::Primitive { ref name, .. } => {
                let column_idx = app.primitive_columns_idx[name];
                RowGroupColumnMetadata::from_parquet_file(&md, app.row_group_selected, column_idx)
                    .render(row_group_column_metadata_area, buf);
                RowGroupPageStats::from_parquet_file(
                    &app.file_name,
                    &md,
                    app.row_group_selected,
                    column_idx,
                )
                .unwrap()
                .render(row_group_page_stats_area, buf);
            }
            SchemaColumnType::Group { .. } => {
                let placeholder = Paragraph::new("Column stats unavailable for groups");
                placeholder.render(row_group_column_metadata_area, buf);
            }
            _ => {
                let placeholder = Paragraph::new("Coming soon...").block(
                    Block::bordered()
                        .title(Line::from(app.active_tab.name()).centered())
                        .border_set(border::ROUNDED),
                );
                placeholder.render(column_stats_area, buf);
            }
        }
    }
}

pub fn render_schema_tab(app: &mut App, area: Rect, buf: &mut Buffer, tab: &SchemaTab) {
    let tree_width = app
        .schema_columns
        .iter()
        .map(|line| match line {
            SchemaColumnType::Root { display: ref d, .. } => d.len(),
            SchemaColumnType::Primitive { display: ref d, .. } => d.len(),
            SchemaColumnType::Group { display: ref d, .. } => d.len(),
        })
        .max()
        .unwrap_or(0);

    // Build rows from primitives in order of appearance
    let mut table_rows: Vec<Row> = Vec::new();
    for (idx, line) in app.schema_columns.iter().enumerate() {
        let display = match line {
            SchemaColumnType::Root { display: _, .. } => {
                continue;
            }
            SchemaColumnType::Primitive { display: ref d, .. } => d,
            SchemaColumnType::Group { display: ref d, .. } => d,
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
                    if let Some(selected_index) = tab.column_selected {
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

                    if let Some(selected_index) = tab.column_selected {
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
    let areas = if tab.column_selected.is_some() {
        let [tree_area, table_stats_area] =
            Layout::horizontal([Constraint::Length(tree_width as u16), Constraint::Fill(1)])
                .areas(area);

        let [table_area, table_stats_sep, stats_area] = Layout::horizontal([
            Constraint::Fill(2),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .areas(table_stats_area);

        (
            tree_area,
            table_area,
            Some(table_stats_sep),
            Some(stats_area),
        )
    } else {
        let [tree_area, table_area] =
            Layout::horizontal([Constraint::Length(tree_width as u16), Constraint::Fill(1)])
                .areas(area);

        (tree_area, table_area, None, None)
    };

    render_schema_tree(app, areas.0, buf, tab.column_selected, tab.scroll_offset);

    // Render columns table
    render_columns_table(table_rows, areas.1, buf, app, tab);

    // Render stats if column is selected
    if let (Some(sep_area), Some(stats_area)) = (areas.2, areas.3) {
        let table_stats_sep_block = Block::default().borders(Borders::RIGHT).fg(Color::Yellow);
        table_stats_sep_block.render(sep_area, buf);

        render_column_stats(app, stats_area, buf, tab);
    }
}

pub fn render_visualize_tab(app: &mut App, area: Rect, buf: &mut Buffer, tab: &VisualizeTab) {
    use crate::visualize::load_rows_window;

    // Determine how many rows to show based on area height (leave 3 for header/borders)
    let viewport_rows: usize = area.height.saturating_sub(3) as usize;
    let page_size = viewport_rows.max(1);

    // Load a window of rows lazily
    let (rows, column_names) = match load_rows_window(&app.file_name, tab.row_offset, page_size)
    {
        Ok(v) => v,
        Err(_) => {
            let p = Paragraph::new("Error loading rows").block(
                Block::bordered()
                    .title(Line::from("Visualize").centered())
                    .border_set(border::ROUNDED),
            );
            p.render(area, buf);
            return;
        }
    };

    // Determine total columns and names
    let total_cols: usize = if !column_names.is_empty() {
        column_names.len()
    } else {
        rows.first().map(|r| r.len()).unwrap_or(0)
    };
    let all_col_names: Vec<String> = if !column_names.is_empty() {
        column_names.clone()
    } else {
        (0..total_cols).map(|i| format!("c{i}")).collect()
    };

    // Compute per-column minimum widths (column name length + 2)
    let col_min_widths: Vec<u16> = all_col_names
        .iter()
        .map(|name| (name.len() as u16).saturating_add(2))
        .collect();

    // Available content width (approx) and spacing per column
    let available_width: u16 = area.width.saturating_sub(2); // leave margin for borders
    let spacing: u16 = 1; // matches .column_spacing(1)
    let rownum_width: u16 = 6;

    // Determine how many columns can fit if the window is anchored at the end (for clamping)
    let mut required: u16 = rownum_width;
    let mut cols_fit_from_end: usize = 0;
    for w in col_min_widths.iter().rev() {
        let next = required.saturating_add(spacing).saturating_add(*w);
        if next <= available_width {
            required = next;
            cols_fit_from_end += 1;
        } else {
            break;
        }
    }
    let last_valid_start: usize = total_cols.saturating_sub(cols_fit_from_end.max(1));

    // Clamp requested start so the right-most can reach the last column
    let start_col: usize = tab.col_offset.min(last_valid_start);

    // Compute visible window [start_col, end_col) based on available width
    let mut end_col: usize = start_col;
    required = rownum_width;
    for i in start_col..total_cols {
        let w = col_min_widths[i];
        let next = required.saturating_add(spacing).saturating_add(w);
        if next <= available_width {
            required = next;
            end_col = i + 1;
        } else {
            break;
        }
    }
    if end_col == start_col && start_col < total_cols {
        end_col = start_col + 1; // ensure we show at least one column
    }

    // Build header for the visible window
    let mut header_titles: Vec<String> = Vec::with_capacity(1 + (end_col - start_col));
    header_titles.push("#".to_string());
    header_titles.extend(all_col_names[start_col..end_col].iter().cloned());
    let header_cells = header_titles
        .into_iter()
        .map(|h| Cell::from(h).bold().fg(Color::Red));

    let mut table_rows: Vec<Row> = Vec::with_capacity(rows.len());
    for (i, r) in rows.iter().enumerate() {
        let mut row_columns: Vec<Cell> = Vec::with_capacity(r.len());
        for (_, col) in r.get_column_iter() {
            row_columns.push(Cell::from(col.to_json_value().to_string()));
        }
        let row_num = (tab.row_offset + i + 1).to_string();
        let slice_end = end_col.min(row_columns.len());
        let slice_start = start_col.min(slice_end);
        let visible_cells_iter = row_columns
            .into_iter()
            .skip(slice_start)
            .take(slice_end.saturating_sub(slice_start));
        let cells: Vec<Cell> = std::iter::once(Cell::from(row_num)).chain(visible_cells_iter).collect();
        table_rows.push(Row::new(cells));
    }

    let mut col_constraints: Vec<Constraint> = Vec::with_capacity(1 + (end_col - start_col));
    col_constraints.push(Constraint::Length(rownum_width)); // row number
    for i in start_col..end_col {
        col_constraints.push(Constraint::Min(col_min_widths[i]));
    }

    let table_widget = Table::new(table_rows, col_constraints)
        .header(Row::new(header_cells))
        .column_spacing(1)
        .block(
            Block::bordered()
                .title(Line::from("Visualize").centered())
                .border_set(border::ROUNDED),
        );

    table_widget.render(area, buf);
}

fn render_schema_tree(app: &mut App, area: Rect, buf: &mut Buffer, column_selected: Option<usize>, scroll_offset: usize) {
    // Calculate viewport height (subtract 2 for borders)
    let viewport_height = area.height.saturating_sub(2) as usize;
    app.set_schema_tree_height(viewport_height);

    let [tree_area, line_area] =
        Layout::horizontal([Constraint::Fill(1), Constraint::Length(1)]).areas(area);

    // Check if we need a scrollbar
    let needs_scrollbar = app.needs_scrollbar(viewport_height);

    let mut tree_vec = vec!["Leaf".blue(), ", ".into(), "Group".green()];

    if column_selected.is_some() {
        tree_vec.extend(vec![", ".into(), "Selected".bold().yellow()]);
    }
    let tree_info = Line::from(tree_vec);

    // Get visible items based on scroll
    let (visible_items, _) = app.get_visible_schema_items(viewport_height, scroll_offset);

    let list_items: Vec<ListItem> = visible_items
        .iter()
        .enumerate()
        .map(|(visible_idx, line)| {
            // Calculate actual index in the schema_columns array
            let actual_idx = if visible_idx == 0 {
                // First visible item is always the root (index 0)
                0
            } else {
                // Other items start from index 1 + scroll_offset
                // visible_idx + app.scroll_offset
                visible_idx
            };

            match line {
                SchemaColumnType::Root { display: ref d, .. } => {
                    let mut item = ListItem::new(d.clone()).dark_gray();
                    if let Some(selected_index) = column_selected {
                        if actual_idx == selected_index {
                            item = item.fg(Color::Yellow).bold();
                        }
                    }
                    item
                }
                SchemaColumnType::Primitive { display: ref d, .. } => {
                    let mut item = ListItem::new(d.clone()).blue();
                    if let Some(selected_index) = column_selected {
                        if actual_idx == selected_index {
                            item = item.fg(Color::Yellow).bold();
                        }
                    }
                    item
                }
                SchemaColumnType::Group { display: ref d, .. } => {
                    let mut item: ListItem<'_> = ListItem::new(d.clone()).green();
                    if let Some(selected_index) = column_selected {
                        if actual_idx == selected_index {
                            item = item.fg(Color::Yellow).bold();
                        }
                    }
                    item
                }
            }
        })
        .collect();

    let list = List::new(list_items).block(
        Block::bordered()
            .title(Line::from("Schema Tree").centered())
            .title_bottom(tree_info.centered())
            .border_set(border::ROUNDED),
    );

    list.render(tree_area, buf);

    // Render scrollbar if needed
    if needs_scrollbar {
        // Calculate scrollbar parameters for the new scrolling logic
        let total_items = app.schema_columns.len();
        let scrollable_items = total_items.saturating_sub(1); // Exclude always-visible root
        let effective_viewport = viewport_height.saturating_sub(1); // Account for root

        // Clamp scroll offset to valid range for scrollbar calculation
        let max_scroll_offset = scrollable_items.saturating_sub(effective_viewport);
        // let clamped_scroll_offset = app.scroll_offset.min(max_scroll_offset);
        let clamped_scroll_offset = 0;

        let scrollbar = ScrollbarComponent::vertical(
            scrollable_items,
            effective_viewport,
            clamped_scroll_offset,
        )
        .with_colors(Color::Yellow, Color::White);

        scrollbar.render(line_area, buf);
    } else {
        Block::default()
            .borders(Borders::RIGHT)
            .fg(Color::Yellow)
            .render(line_area, buf);
    }
}

fn render_columns_table(table_rows: Vec<Row>, area: Rect, buf: &mut Buffer, app: &App, tab: &SchemaTab) {
    // Calculate viewport height for table (subtract 3 for borders and header)
    let viewport_height = area.height.saturating_sub(3) as usize;

    let header = vec![
        "Rep",
        "Physical",
        "Logical",
        "Converted Type",
        "Codec",
        "Encoding",
    ];

    // Get visible rows based on scroll offset
    let visible_rows: Vec<Row> = table_rows
        .into_iter()
        .skip(tab.scroll_offset)
        .take(viewport_height)
        .collect();

    let col_constraints = vec![
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(18),
        Constraint::Length(10),
        Constraint::Length(8),
        Constraint::Min(10),
    ];

    let table_widget = Table::new(visible_rows, col_constraints)
        .header(Row::new(
            header
                .into_iter()
                .map(|h| Cell::from(h).bold().fg(Color::Red)),
        ))
        .column_spacing(1)
        .block(
            Block::bordered()
                .title(Line::from("Columns").centered())
                .border_set(border::ROUNDED),
        );

    table_widget.render(area, buf);
}

fn render_column_stats(app: &App, area: Rect, buf: &mut Buffer, tab: &SchemaTab) {
    if let Some(selected_idx) = tab.column_selected {
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
            if let Ok(file) = File::open(Path::new(&app.file_name.as_str())) {
                if let Ok(reader) = SerializedFileReader::new(file) {
                    let md = reader.metadata();
                    let schema_descr = md.file_metadata().schema_descr();
                    let physical = schema_descr.column(col_idx).physical_type();

                    let column_stats = aggregate_column_stats(md, col_idx, physical);

                    let mut kv_stats: Vec<(String, String)> =
                        vec![("Null count".into(), commas(column_stats.nulls))];
                    if let Some(ref min_val) = column_stats.min {
                        kv_stats.push(("Min".into(), min_val.clone()));
                    }
                    if let Some(ref max_val) = column_stats.max {
                        kv_stats.push(("Max".into(), max_val.clone()));
                    }
                    if let Some(dist) = column_stats.distinct {
                        kv_stats.push(("Distinct".into(), commas(dist)));
                    }
                    kv_stats.push((
                        "Total compressed size".into(),
                        human_readable_bytes(column_stats.total_compressed_size),
                    ));
                    kv_stats.push((
                        "Total uncompressed size".into(),
                        human_readable_bytes(column_stats.total_uncompressed_size),
                    ));
                    kv_stats.push((
                        "Compression ratio".into(),
                        format!(
                            "{:.2}x",
                            column_stats.total_uncompressed_size as f64
                                / column_stats.total_compressed_size as f64
                        ),
                    ));

                    // Check for dictionary encoding and extract values
                    let encodings_str = {
                        let display_key = &app.schema_columns[selected_idx];
                        if let SchemaColumnType::Primitive { display: ref d, .. } = display_key {
                            if let Some(ColumnType::Primitive(info)) = app.schema_map.get(d) {
                                info.encoding.clone()
                            } else {
                                String::new()
                            }
                        } else {
                            String::new()
                        }
                    };

                    let dictionary_sample: Option<Vec<String>> =
                        if encodings_str.contains("DICTIONARY") {
                            match extract_dictionary_values(&reader, col_idx, 10) {
                                Ok(sample_vals) if !sample_vals.is_empty() => Some(sample_vals),
                                _ => None,
                            }
                        } else {
                            None
                        };

                    // Determine layout for key/value table
                    let max_key_len = kv_stats.iter().map(|(k, _)| k.len()).max().unwrap_or(0);

                    let rows: Vec<Row> = kv_stats
                        .into_iter()
                        .map(|(k, v)| {
                            Row::new(vec![Cell::from(k).bold().fg(Color::Blue), Cell::from(v)])
                        })
                        .collect();

                    // Split stats area if we have dictionary samples to show
                    if let Some(ref dict_vals) = dictionary_sample {
                        let [table_area, dict_area] = Layout::vertical([
                            Constraint::Fill(1),
                            Constraint::Length(3 + (dict_vals.len() as u16 / 3).max(1)),
                        ])
                        .areas(area);

                        let table_widget = Table::new(
                            rows,
                            vec![Constraint::Length(max_key_len as u16), Constraint::Min(5)],
                        )
                        .column_spacing(1)
                        .block(
                            Block::bordered()
                                .title(Line::from("Stats").centered())
                                .border_set(border::ROUNDED),
                        );
                        table_widget.render(table_area, buf);

                        // Render dictionary sample paragraph
                        let dict_text = dict_vals.join(", ");
                        let dict_paragraph = Paragraph::new(dict_text)
                            .wrap(ratatui::widgets::Wrap { trim: true })
                            .block(
                                Block::bordered()
                                    .title(
                                        Line::from(format!(
                                            "Dictionary Sample ({})",
                                            dict_vals.len()
                                        ))
                                        .centered(),
                                    )
                                    .border_set(border::ROUNDED),
                            );
                        dict_paragraph.render(dict_area, buf);
                    } else {
                        let table_widget = Table::new(
                            rows,
                            vec![Constraint::Length(max_key_len as u16), Constraint::Min(5)],
                        )
                        .column_spacing(1)
                        .block(
                            Block::bordered()
                                .title(Line::from("Stats").centered())
                                .border_set(border::ROUNDED),
                        );
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
                .block(
                    Block::bordered()
                        .title(Line::from("Stats").centered())
                        .border_set(border::ROUNDED),
                )
                .render(area, buf);
        }
    }
}
