use crate::file::schema::FileSchema;
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Rect},
    prelude::Color,
    style::Stylize,
    symbols::border,
    text::Line,
    widgets::{Block, Cell, Row, Table, Widget},
};
use std::cmp::min;

use crate::file::Renderable;

pub struct FileSchemaTable<'a> {
    pub schema: &'a FileSchema,
    pub selected_index: usize,
    pub title: String,
    pub title_color: Color,
    pub selected_color: Color,
    pub border_style: border::Set,
    pub horizontal_scroll: usize,
    pub vertical_scroll: usize,
}

impl<'a> FileSchemaTable<'a> {
    pub fn new(schema: &'a FileSchema) -> Self {
        Self {
            schema,
            selected_index: 0,
            title: "Column Statistics".to_string(),
            title_color: Color::Green,
            selected_color: Color::Yellow,
            border_style: border::ROUNDED,
            horizontal_scroll: 0,
            vertical_scroll: 0,
        }
    }

    pub fn with_selected_index(mut self, index: usize) -> Self {
        self.selected_index = index;
        self
    }

    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }

    pub fn with_colors(mut self, title: Color, selected: Color) -> Self {
        self.title_color = title;
        self.selected_color = selected;
        self
    }

    pub fn with_border_style(mut self, border_style: border::Set) -> Self {
        self.border_style = border_style;
        self
    }

    pub fn with_horizontal_scroll(mut self, offset: usize) -> Self {
        self.horizontal_scroll = offset;
        self
    }

    pub fn with_vertical_scroll(mut self, offset: usize) -> Self {
        self.vertical_scroll = offset;
        self
    }

    pub fn scroll_left(&mut self) {
        if self.horizontal_scroll > 0 {
            self.horizontal_scroll -= 1;
        }
    }

    pub fn scroll_right(&mut self) {
        self.horizontal_scroll += 1;
    }

    pub fn get_max_scroll(&self) -> usize {
        // Calculate how many columns we can show at full width
        let available_width = 80; // Assume 80 characters available
        let min_column_width = 12; // Minimum width for readability
        let max_visible_columns = available_width / min_column_width;

        // Total columns minus visible columns
        let total_columns = 10usize; // We have 10 columns
        total_columns.saturating_sub(max_visible_columns as usize)
    }
}

impl<'a> Widget for FileSchemaTable<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let all_headers = [
            "Repetition",
            "Physical",
            "Compressed",
            "Uncompressed",
            "Ratio",
            "Encodings",
            "Compression",
            "Min",
            "Max",
            "Nulls",
        ];

        // Calculate how many columns we can show at full width
        let available_width = area.width.saturating_sub(4); // Account for borders and spacing
        let min_column_width = 12;
        let max_visible_columns = (available_width / min_column_width).max(1);

        // Clamp scroll offset to valid range
        let max_scroll = all_headers
            .len()
            .saturating_sub(max_visible_columns as usize);
        let horizontal_scroll = self.horizontal_scroll.min(max_scroll);

        // Calculate visible rows based on vertical scroll and available height
        let visible_rows_count = area.height.saturating_sub(1) as usize;

        // Generate table data with only visible columns and rows
        let (visible_rows, column_widths) = self.schema.generate_table_rows_with_scroll(
            self.selected_index,
            horizontal_scroll,
            max_visible_columns as usize,
            self.vertical_scroll,
            visible_rows_count,
        );

        // Get visible columns
        let visible_headers: Vec<_> = all_headers
            .iter()
            .skip(horizontal_scroll)
            .take(max_visible_columns as usize)
            .collect();

        // Include header widths in the calculation and create constraints
        let col_constraints: Vec<_> = visible_headers
            .iter()
            .enumerate()
            .map(|(i, header)| {
                let content_width = column_widths.get(i).cloned().unwrap_or(0);
                let header_width = header.len();
                // Use maximum of 30 for readability
                Constraint::Length(min(content_width.max(header_width), 30) as u16 + 1)
            })
            .collect();

        let table_widget = Table::new(visible_rows, col_constraints)
            .header(Row::new(
                visible_headers
                    .into_iter()
                    .map(|h| Cell::from(*h).bold().fg(Color::Yellow)),
            ))
            .column_spacing(1)
            .block(
                Block::bordered()
                    .title(
                        Line::from(self.title.clone())
                            .centered()
                            .bold()
                            .fg(self.title_color),
                    )
                    .border_set(self.border_style),
            );

        table_widget.render(area, buf);
    }
}

impl Renderable for FileSchema {
    fn render_content(&self, area: Rect, buf: &mut Buffer) {
        // Default implementation without selection highlighting
        let table_component = FileSchemaTable::new(self);
        table_component.render(area, buf);
    }
}
