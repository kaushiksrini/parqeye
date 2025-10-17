use crate::file::sample_data::ParquetSampleData;
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    prelude::Color,
    style::Stylize,
    symbols::border,
    text::Line,
    widgets::{Block, Borders, Cell, Row, Table, Widget},
};
use std::cmp::min;

use crate::file::Renderable;

pub struct DataTable<'a> {
    pub data: &'a ParquetSampleData,
    pub title: String,
    pub title_color: Color,
    pub border_style: border::Set,
    pub horizontal_scroll: usize,
    pub selected_row: Option<usize>,
    pub selected_color: Color,
}

impl<'a> DataTable<'a> {
    pub fn new(data: &'a ParquetSampleData) -> Self {
        Self {
            data,
            title: "Data Preview".to_string(),
            title_color: Color::Cyan,
            border_style: border::ROUNDED,
            horizontal_scroll: 0,
            selected_row: None,
            selected_color: Color::Yellow,
        }
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

    pub fn with_selected_row(mut self, row: Option<usize>) -> Self {
        self.selected_row = row;
        self
    }

    pub fn scroll_left(&mut self) {
        if self.horizontal_scroll > 0 {
            self.horizontal_scroll -= 1;
        }
    }

    pub fn scroll_right(&mut self) {
        let max_scroll = self.get_max_scroll();
        if self.horizontal_scroll < max_scroll {
            self.horizontal_scroll += 1;
        }
    }

    pub fn get_max_scroll(&self) -> usize {
        // Calculate how many columns we can show at reasonable width
        let available_width = 120; // Assume reasonable terminal width
        let min_column_width = 12; // Minimum width for readability
        let max_visible_columns = available_width / min_column_width;

        // Total columns minus visible columns
        self.data.total_columns.saturating_sub(max_visible_columns)
    }

    fn calculate_column_widths(
        &self,
        headers: &[String],
        visible_rows: &[Vec<String>],
    ) -> Vec<usize> {
        let mut widths = Vec::new();

        for (col_idx, header) in headers.iter().enumerate() {
            let mut max_width = header.len();

            // Check content width for this column
            for row in visible_rows {
                if let Some(cell) = row.get(col_idx) {
                    max_width = max_width.max(cell.len());
                }
            }

            // Use minimum width of 8 and maximum of 25 for readability
            widths.push(min(max_width.max(8), 25));
        }

        widths
    }
}

impl<'a> Widget for DataTable<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Calculate how many columns we can show (reserve space for row numbers)
        let row_num_width = 6; // Width for row numbers
        let available_width = area.width.saturating_sub(row_num_width + 6); // Account for borders and row numbers
        let min_column_width = 12;
        let max_visible_columns = (available_width / min_column_width).max(1) as usize;

        // Clamp scroll offset to valid range
        let max_scroll = self.data.total_columns.saturating_sub(max_visible_columns);
        let horizontal_scroll = self.horizontal_scroll.min(max_scroll);

        // Get visible columns
        let visible_headers: Vec<String> = self
            .data
            .flattened_columns
            .iter()
            .skip(horizontal_scroll)
            .take(max_visible_columns)
            .cloned()
            .collect();

        // Get visible data for each row
        let visible_rows: Vec<Vec<String>> = self
            .data
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .skip(horizontal_scroll)
                    .take(max_visible_columns)
                    .cloned()
                    .collect()
            })
            .collect();

        // Calculate column widths
        let column_widths = self.calculate_column_widths(&visible_headers, &visible_rows);

        // Create constraints with row number column first
        let mut col_constraints: Vec<Constraint> = vec![Constraint::Length(row_num_width)];
        col_constraints.extend(
            column_widths
                .iter()
                .map(|&width| Constraint::Length(width as u16)),
        );

        // Create table rows with row numbers
        let table_rows: Vec<Row> = visible_rows
            .into_iter()
            .enumerate()
            .map(|(row_idx, row_data)| {
                // Create cells with row number first
                let mut cells: Vec<Cell> =
                    vec![Cell::from(format!("{:>4}", row_idx + 1)).fg(Color::DarkGray)];

                // Add data cells
                cells.extend(row_data.into_iter().map(|cell_data| {
                    // Truncate cell data if too long (Unicode-safe)
                    let truncated = if cell_data.chars().count() > 23 {
                        let truncated_chars: String = cell_data.chars().take(20).collect();
                        format!("{truncated_chars}...")
                    } else {
                        cell_data
                    };
                    Cell::from(format!(" {truncated}")) // Add space for padding
                }));

                let mut row = Row::new(cells);

                // Highlight selected row
                if let Some(selected) = self.selected_row {
                    if row_idx == selected {
                        row = row.style(
                            ratatui::style::Style::default()
                                .bg(self.selected_color)
                                .fg(Color::Black),
                        );
                    }
                }

                row
            })
            .collect();

        // Create header row with empty cell for row number column
        let mut header_cells: Vec<Cell> = vec![Cell::from("    ").fg(Color::DarkGray)];

        header_cells.extend(visible_headers.into_iter().map(|header| {
            let truncated = if header.len() > 20 {
                format!("{}...", &header[..17])
            } else {
                header
            };
            Cell::from(format!(" {truncated}")).bold().fg(Color::Yellow)
        }));

        let scroll_indicator = if max_scroll > 0 {
            format!(" (←→ scroll {horizontal_scroll}/{max_scroll})")
        } else {
            "".to_string()
        };

        let [header_area, content_area] =
            Layout::vertical([Constraint::Length(3), Constraint::Fill(1)]).areas(area);

        // Create header as a single-row table
        let header_widget = Table::new(vec![Row::new(header_cells)], col_constraints.clone())
            .column_spacing(2)
            .block(
                Block::bordered()
                    .borders(Borders::BOTTOM | Borders::TOP)
                    .border_set(self.border_style)
                    .title(
                        Line::from(format!("{}{}", self.title, scroll_indicator))
                            .centered()
                            .bold()
                            .fg(self.title_color),
                    ),
            );

        // Create data table without header
        let table_widget = Table::new(table_rows, col_constraints).column_spacing(2);

        header_widget.render(header_area, buf);
        table_widget.render(content_area, buf);
    }
}

impl Renderable for ParquetSampleData {
    fn render_content(&self, area: Rect, buf: &mut Buffer) {
        let table_component = DataTable::new(self);
        table_component.render(area, buf);
    }
}
