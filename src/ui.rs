use ratatui::{
    Frame,
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    prelude::{Color, Position},
    style::{Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph, Widget, Wrap},
};

use crate::app::AppRenderView;
use crate::components::data_table::truncate_cell;
use crate::components::{
    DataTable, FileSchemaTable, RowGroupColumnMetadataComponent, RowGroupMetadata,
    RowGroupProgressBar, SchemaTreeComponent, ScrollbarComponent,
};
use crate::file::Renderable;
use crate::tabs::{
    TabManager,
    visualize::{CommandPrompt, PromptKind, SelectionMode},
};

fn prompt_prefix(prompt: &CommandPrompt, width: u16) -> &'static str {
    match prompt.kind {
        PromptKind::Columns if width >= 72 => "* Columns (regex, ignore case): ",
        PromptKind::Columns if width >= 40 => "* regex: ",
        PromptKind::Columns => "*",
        PromptKind::GoToRow => ":",
    }
}

struct PromptLayout<'a> {
    prefix: &'static str,
    input: &'a str,
    edit_area: Rect,
    input_area: Rect,
    status: Line<'a>,
    status_area: Rect,
    cursor: Position,
}

/// Rendering and cursor placement must use the same input width after reserving
/// space for feedback on the existing single footer row.
fn prompt_layout(prompt: &CommandPrompt, total_columns: usize, area: Rect) -> PromptLayout<'_> {
    let status = if let Some(error) = &prompt.error {
        Line::from(error.as_str()).red()
    } else if prompt.kind == PromptKind::Columns {
        let count = prompt.column_matches.as_ref().map_or(0, Vec::len);
        let message = if prompt.buffer.is_empty() {
            format!("{count}/{total_columns} | a|b, ^a$")
        } else {
            format!("{count}/{total_columns} columns")
        };
        Line::from(message).cyan()
    } else {
        Line::default()
    };
    let status_width = if status.width() == 0 {
        0
    } else {
        usize::from(area.width / 3).min(status.width() + 3) as u16
    };
    let status = if status_width >= 4 {
        let style = status.style;
        let mut spans = vec![" | ".dim()];
        spans.extend(status.spans);
        Line::from(spans).style(style)
    } else {
        status
    };
    let prefix = prompt_prefix(prompt, area.width);
    let edit_area = Rect::new(
        area.x,
        area.y,
        area.width - status_width,
        area.height.min(1),
    );
    let prefix_width = (prefix.len() as u16).min(edit_area.width);
    let input_area = Rect::new(
        edit_area.x + prefix_width,
        area.y,
        edit_area.width - prefix_width,
        edit_area.height,
    );
    let (input, offset) = prompt.visible_input(input_area.width);
    let cursor = Position::new(
        (input_area.x + offset).min(area.right().saturating_sub(1)),
        area.y,
    );
    PromptLayout {
        prefix,
        input,
        edit_area,
        input_area,
        status,
        cursor,
        status_area: Rect::new(edit_area.right(), area.y, status_width, edit_area.height),
    }
}

pub fn render_app<'a, 'b>(app: &'b AppRenderView<'a>, frame: &mut Frame)
where
    'b: 'a,
{
    let area = frame.area();
    frame.render_widget(AppWidget(app), area);

    if area.width > 0
        && area.height > 0
        && let Some(prompt) = app.state().visualize().prompt()
    {
        let footer = Rect::new(area.x, area.bottom() - 1, area.width, 1);
        frame.set_cursor_position(
            prompt_layout(prompt, app.parquet_ctx.sample_data.total_columns, footer).cursor,
        );
    }
}

struct AppWidget<'a>(&'a AppRenderView<'a>);

impl<'a> AppWidget<'a> {
    // Helper function to calculate the tree index of the selected primitive column
    fn calculate_selected_tree_index(&self, vertical_offset: usize) -> Option<usize> {
        if vertical_offset == 0 {
            return None;
        }

        let primitive_to_schema_map: Vec<usize> = self
            .0
            .parquet_ctx
            .schema
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| {
                matches!(line, crate::file::schema::SchemaInfo::Primitive { .. }).then_some(idx)
            })
            .collect();

        primitive_to_schema_map.get(vertical_offset - 1).copied()
    }

    // Helper function to calculate adjusted scroll offset to keep selected item visible
    fn calculate_scroll_to_show_item(
        &self,
        selected_tree_idx: Option<usize>,
        current_scroll: usize,
        visible_items: usize,
    ) -> usize {
        match selected_tree_idx {
            Some(idx) => {
                // Ensure selected item is visible
                if idx < current_scroll {
                    idx
                } else if idx >= current_scroll + visible_items {
                    idx.saturating_sub(visible_items - 1)
                } else {
                    current_scroll
                }
            }
            None => current_scroll,
        }
    }

    // Calculate the adjusted scroll offset for the schema tree
    fn calculate_adjusted_scroll_offset(&self, visible_tree_items: usize) -> usize {
        let selected_tree_idx =
            self.calculate_selected_tree_index(self.0.state().vertical_offset());
        self.calculate_scroll_to_show_item(
            selected_tree_idx,
            self.0.state().tree_scroll_offset(),
            visible_tree_items,
        )
    }

    // Calculate the total width needed for the tree section (including scrollbar if needed)
    fn calculate_tree_width(&self, tree_width: u16, needs_scrollbar: bool) -> u16 {
        if needs_scrollbar {
            tree_width + 2 // +1 for scrollbar, +1 for spacing
        } else {
            tree_width + 1
        }
    }

    // Calculate tree width for row groups view (slightly different spacing)
    fn calculate_tree_width_for_row_groups(&self, tree_width: u16, needs_scrollbar: bool) -> u16 {
        if needs_scrollbar {
            tree_width + 2 // +1 for scrollbar, +1 for spacing
        } else {
            tree_width
        }
    }

    // Render the schema tree section (tree + optional scrollbar
    #[allow(clippy::too_many_arguments)]
    fn render_schema_tree_section(
        &self,
        area: Rect,
        tree_width: u16,
        needs_scrollbar: bool,
        total_tree_items: usize,
        visible_tree_items: usize,
        adjusted_scroll: usize,
        buf: &mut Buffer,
    ) {
        if needs_scrollbar {
            let [tree_area, scrollbar_area] =
                Layout::horizontal([Constraint::Length(tree_width + 1), Constraint::Length(1)])
                    .areas(area);

            self.render_schema_tree_with_scroll(tree_area, adjusted_scroll, buf);

            ScrollbarComponent::vertical(total_tree_items, visible_tree_items, adjusted_scroll)
                .render(scrollbar_area, buf);
        } else {
            self.render_schema_tree_with_scroll(area, adjusted_scroll, buf);
        }
    }

    // Render the schema table
    fn render_schema_table(&self, area: Rect, adjusted_scroll: usize, buf: &mut Buffer) {
        FileSchemaTable::new(&self.0.parquet_ctx.schema)
            .with_selected_index(self.0.state().vertical_offset())
            .with_horizontal_scroll(self.0.state().horizontal_offset())
            .with_vertical_scroll(adjusted_scroll)
            .render(area, buf);
    }

    fn render_tabs_view(&self, area: Rect, buf: &mut Buffer) {
        render_tabs_header(self.0.tabs(), self.0.file_name(), area, buf);
    }

    fn render_footer_view(&self, area: Rect, buf: &mut Buffer) {
        if let Some(prompt) = self.0.state().visualize().prompt() {
            let layout = prompt_layout(prompt, self.0.parquet_ctx.sample_data.total_columns, area);
            layout.prefix.green().render(layout.edit_area, buf);
            Line::from(layout.input).render(layout.input_area, buf);
            layout.status.render(layout.status_area, buf);
            return;
        }

        if let Some(toast) = self.0.state().toast() {
            let color = if toast.is_error {
                Color::Red
            } else {
                Color::Green
            };
            Line::from(toast.message.as_str())
                .fg(color)
                .render(area, buf);
            return;
        }

        if self.0.tabs().active_tab().to_string() == "Visualize"
            && let Some(context) = self.visualize_footer(area.width)
        {
            context.render(area, buf);
            return;
        }

        let title = self.0.title;
        let title_width = Line::from(title).width() as u16;
        let [title_area, footer_area] =
            Layout::horizontal([Constraint::Length(title_width), Constraint::Fill(1)]).areas(area);
        title.bold().fg(Color::Green).render(title_area, buf);
        self.0.tabs().render_instructions(footer_area, buf);
    }

    fn visualize_footer(&self, width: u16) -> Option<Line<'_>> {
        let visualize = self.0.state().visualize();
        let data = &self.0.parquet_ctx.sample_data;
        let name = (visualize.selection_mode != SelectionMode::Row)
            .then(|| visualize.source_column(visualize.selected_column))
            .flatten()
            .and_then(|index| data.flattened_columns.get(index));
        if visualize.column_filter.is_none() && name.is_none() {
            return None;
        }
        let mut remaining = width;
        let mut spans = Vec::new();
        if visualize.column_filter.is_some() {
            let count = format!(
                "Columns: {}/{}",
                visualize.column_count(data.total_columns),
                data.total_columns
            );
            remaining = remaining.saturating_sub(Span::raw(&count).width() as u16);
            spans.push(count.cyan());
        }
        if let Some(name) = name {
            if !spans.is_empty() {
                spans.push(" | ".into());
                remaining = remaining.saturating_sub(3);
            }
            let start = name
                .char_indices()
                .nth(visualize.column_name_scroll)
                .map_or(name.len(), |(index, _)| index);
            // Leave room for the expression when both it and the name fit.
            let expression_width = if visualize.column_filter.is_some() && remaining >= 24 {
                remaining / 3
            } else {
                0
            };
            let text = truncate_cell(
                &name[start..],
                remaining.saturating_sub(expression_width + 5),
            );
            remaining = remaining.saturating_sub(5 + Span::raw(text.as_ref()).width() as u16);
            spans.extend([
                "Col: ".yellow(),
                Span::styled(text, Style::default().fg(Color::Yellow)),
            ]);
            if expression_width == 0 {
                return Some(Line::from(spans));
            }
        }
        if let Some(expression) = &visualize.column_filter {
            spans.extend([
                " | * ".cyan(),
                Span::styled(
                    truncate_cell(expression, remaining.saturating_sub(5)),
                    Style::default().fg(Color::Cyan),
                ),
            ]);
        }
        Some(Line::from(spans))
    }

    fn render_metadata_view(&self, area: Rect, buf: &mut Buffer) {
        self.0
            .parquet_ctx
            .metadata
            .render_with_scroll(area, buf, self.0.state().vertical_offset());
    }

    fn render_schema_view(&self, area: Rect, buf: &mut Buffer) {
        let tree_width = self.0.parquet_ctx.schema.tree_width() as u16;
        let total_tree_items = self.0.parquet_ctx.schema.columns.len();
        let visible_tree_items = area.height.saturating_sub(2) as usize;

        let needs_scrollbar = total_tree_items > visible_tree_items;
        let adjusted_scroll = self.calculate_adjusted_scroll_offset(visible_tree_items);
        let tree_total_width = self.calculate_tree_width(tree_width, needs_scrollbar);

        let [tree_container_area, central_area] =
            Layout::horizontal([Constraint::Length(tree_total_width), Constraint::Fill(1)])
                .areas(area);

        self.render_schema_tree_section(
            tree_container_area,
            tree_width,
            needs_scrollbar,
            total_tree_items,
            visible_tree_items,
            adjusted_scroll,
            buf,
        );
        self.render_schema_table(central_area, adjusted_scroll, buf);
    }

    fn render_schema_tree_with_scroll(&self, area: Rect, scroll_offset: usize, buf: &mut Buffer) {
        SchemaTreeComponent::new(&self.0.parquet_ctx.schema.columns)
            .with_title("Schema Tree".to_string())
            .with_selected_index(self.0.state().vertical_offset())
            .with_scroll_offset(scroll_offset)
            .render(area, buf);
    }

    fn render_row_groups_view(&self, area: Rect, buf: &mut Buffer) {
        let tree_width = self.0.parquet_ctx.schema.tree_width() as u16;
        let total_tree_items = self.0.parquet_ctx.schema.columns.len();
        let visible_tree_items = area.height.saturating_sub(2) as usize;

        let needs_scrollbar = total_tree_items > visible_tree_items;
        let adjusted_scroll = self.calculate_adjusted_scroll_offset(visible_tree_items);
        let tree_total_width =
            self.calculate_tree_width_for_row_groups(tree_width, needs_scrollbar);

        let [tree_container_area, main_area] =
            Layout::horizontal([Constraint::Length(tree_total_width), Constraint::Fill(1)])
                .areas(area);

        self.render_schema_tree_section(
            tree_container_area,
            tree_width,
            needs_scrollbar,
            total_tree_items,
            visible_tree_items,
            adjusted_scroll,
            buf,
        );

        let [rg_progress, central_area] =
            Layout::vertical([Constraint::Length(3), Constraint::Fill(1)]).areas(main_area);

        RowGroupProgressBar::new(
            &self.0.parquet_ctx.row_groups.row_groups,
            self.0.state().horizontal_offset(),
        )
        .render(rg_progress, buf);

        if self.0.state().vertical_offset() > 0 {
            let rg_idx = self.0.state().horizontal_offset();
            let col_idx = self.0.state().vertical_offset() - 1;
            // Read this column's pages lazily (and cached) — only the selected column.
            let pages = self.0.parquet_ctx.page_info(rg_idx, col_idx);
            RowGroupColumnMetadataComponent::new(
                &self.0.parquet_ctx.row_groups.row_groups[rg_idx].column_metadata[col_idx],
                &pages,
            )
            .render(central_area, buf);
        } else {
            // Display row group level statistics and charts when no column is selected
            RowGroupMetadata::new(
                &self.0.parquet_ctx.row_groups.row_groups,
                &self.0.parquet_ctx.row_groups.avg_median_stats,
                self.0.state().horizontal_offset(),
            )
            .render(central_area, buf);
        }
    }

    fn render_visualize_view(&self, area: Rect, buf: &mut Buffer) {
        // if polars fails to read the file, return error
        if let Some(error) = &self.0.parquet_ctx.sample_data_error {
            return render_sample_data_error(error, area, buf);
        }

        let visualize = self.0.state().visualize();
        let data = &self.0.parquet_ctx.sample_data;
        DataTable::new(data)
            .with_width_cache(&visualize.column_widths)
            .with_projection(visualize.projection())
            .with_horizontal_scroll(self.0.state().horizontal_offset())
            .with_vertical_scroll(self.0.state().data_vertical_scroll())
            .with_selected_row(Some(self.0.state().vertical_offset()))
            .with_selection(
                self.0.state().visualize().selection_mode,
                self.0.state().visualize().selected_column,
            )
            .render(area, buf)
    }
}

fn render_tabs_header(tabs: &TabManager, file_name: &str, area: Rect, buf: &mut Buffer) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::LightYellow));
    let inner_area = block.inner(area);
    block.render(area, buf);

    // Reserve the tabs and a gap before giving the path any remaining space.
    let available_width = usize::from(inner_area.width).saturating_sub(tabs.content_width() + 1);
    let file_name_line = Line::from(file_name).green().right_aligned();
    let file_name_width = file_name_line.width().min(available_width) as u16;
    let [tabs_area, file_name_area] =
        Layout::horizontal([Constraint::Fill(1), Constraint::Length(file_name_width)])
            .areas(inner_area);
    tabs.render_content(tabs_area, buf);

    if file_name_line.width() > usize::from(file_name_width) && file_name_width > 0 {
        let [ellipsis_area, suffix_area] =
            Layout::horizontal([Constraint::Length(1), Constraint::Fill(1)]).areas(file_name_area);
        "…".green().render(ellipsis_area, buf);
        file_name_line.render(suffix_area, buf);
    } else {
        file_name_line.render(file_name_area, buf);
    }
}

/// show error screen and error when failing
fn render_sample_data_error(error: &str, area: Rect, buf: &mut Buffer) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Yellow))
        .title(" Preview unavailable ".yellow().bold());

    Paragraph::new(vec![
        Line::from("This file's data could not be read by polars.".yellow()),
        Line::from(""),
        Line::from(error.to_string()),
        Line::from(""),
        Line::from("The Metadata, Schema and Row Groups tabs still work.".dim()),
    ])
    .block(block)
    .wrap(Wrap { trim: true })
    .render(area, buf);
}

impl<'a> Widget for AppWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let app = self.0;

        let vertical = Layout::vertical([
            Constraint::Length(3),
            Constraint::Fill(1),
            Constraint::Length(1),
        ]);
        let [header_area, inner_area, footer_area] = vertical.areas(area);

        self.render_tabs_view(header_area, buf);
        self.render_footer_view(footer_area, buf);

        match app.tabs().active_tab().to_string().as_str() {
            "Metadata" => self.render_metadata_view(inner_area, buf),
            "Schema" => self.render_schema_view(inner_area, buf),
            "Row Groups" => self.render_row_groups_view(inner_area, buf),
            "Visualize" => self.render_visualize_view(inner_area, buf),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_feedback_leaves_space_for_input_and_its_cursor() {
        for kind in [PromptKind::Columns, PromptKind::GoToRow] {
            let mut prompt = CommandPrompt::new(kind);
            prompt.buffer = "資料_customer_".repeat(30);
            prompt.column_matches = Some(vec![0, 2]);
            for error in [None, Some("Invalid regex: unclosed group".to_string())] {
                prompt.error = error;
                for cursor in [0, "資料_".len(), prompt.buffer.len()] {
                    prompt.cursor = cursor;
                    for width in 0..=100 {
                        let area = Rect::new(2, 9, width, 1);
                        let layout = prompt_layout(&prompt, 50_000, area);
                        assert!(layout.status_area.width <= width / 3);
                        assert_eq!(layout.edit_area.right(), layout.status_area.x);
                        assert_eq!(layout.status_area.right(), area.right());
                        assert_eq!(layout.cursor.y, area.y);
                        if layout.input_area.width > 0 {
                            assert!(layout.cursor.x >= layout.input_area.x);
                            assert!(layout.cursor.x < layout.input_area.right());
                            let start = prompt.buffer.len() - layout.input.len();
                            assert_eq!(
                                Span::raw(&prompt.buffer[start..cursor]).width(),
                                usize::from(layout.cursor.x - layout.input_area.x),
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn long_file_paths_leave_all_tabs_visible() {
        let tabs = TabManager::new(1, 1, 1, 1, &["id".into()]);
        let paths = [
            format!("{}/data.parquet", "/many/nested/directories".repeat(10)),
            format!("{}/data.parquet", "/資料/📊".repeat(10)),
            format!("{}/data.parquet", "a".repeat(65_536)),
        ];
        for path in paths {
            let area = Rect::new(0, 0, 80, 3);
            let mut buf = Buffer::empty(area);
            render_tabs_header(&tabs, &path, area, &mut buf);

            let header: String = (1..79).map(|x| buf[(x, 1)].symbol()).collect();
            assert!(header.starts_with(" Visualize   Metadata   Schema   Row Groups "));
            assert!(header.contains('…'));
            assert!(header.ends_with("/data.parquet"));
            assert_eq!(buf[(79, 1)].symbol(), "│");
        }
    }

    #[test]
    fn file_paths_that_fit_are_shown_in_full_using_display_width() {
        let tabs = TabManager::new(1, 1, 1, 1, &["id".into()]);
        for path in ["data.parquet", "/資料/e\u{301}📊.parquet"] {
            let header = format!(" Visualize   Metadata   Schema   Row Groups  {path}");
            let border = "─".repeat(Line::from(header.as_str()).width());
            let expected = Buffer::with_lines([
                format!("╭{border}╮"),
                format!("│{header}│"),
                format!("╰{border}╯"),
            ]);
            let mut actual = Buffer::empty(expected.area);
            render_tabs_header(&tabs, path, expected.area, &mut actual);

            for (actual, expected) in actual.content().iter().zip(expected.content()) {
                assert_eq!(actual.symbol(), expected.symbol());
            }
        }
    }

    #[test]
    fn narrow_headers_prioritize_tabs_and_do_not_panic() {
        let tabs = TabManager::new(1, 1, 1, 1, &["id".into()]);
        for width in 0..=50 {
            let area = Rect::new(0, 0, width, 3);
            let mut with_path = Buffer::empty(area);
            let mut without_path = Buffer::empty(area);
            render_tabs_header(&tabs, "/long/path/to/data.parquet", area, &mut with_path);
            render_tabs_header(&tabs, "", area, &mut without_path);

            // The file path must never change the cells occupied by tab labels.
            for x in 1..width.saturating_sub(1).min(tabs.content_width() as u16 + 1) {
                assert_eq!(with_path[(x, 1)], without_path[(x, 1)]);
            }
        }
    }

    #[test]
    fn sample_data_error_is_rendered_instead_of_the_table() {
        let area = Rect::new(0, 0, 60, 10);
        let mut buf = Buffer::empty(area);

        render_sample_data_error("LogicalType union has no variant set", area, &mut buf);

        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(text.contains("Preview unavailable"), "got: {text}");
        assert!(text.contains("LogicalType union has no variant set"));
    }
}
