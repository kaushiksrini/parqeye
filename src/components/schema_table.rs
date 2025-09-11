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

use crate::file::Renderable;

pub struct FileSchemaTable<'a> {
    pub schema: &'a FileSchema,
    pub selected_index: Option<usize>,
    pub title: String,
    pub title_color: Color,
    pub selected_color: Color,
    pub border_style: border::Set,
}

impl<'a> FileSchemaTable<'a> {
    pub fn new(schema: &'a FileSchema) -> Self {
        Self {
            schema,
            selected_index: None,
            title: "Column Statistics".to_string(),
            title_color: Color::Green,
            selected_color: Color::Yellow,
            border_style: border::ROUNDED,
        }
    }

    pub fn with_selected_index(mut self, index: Option<usize>) -> Self {
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
}

impl<'a> Widget for FileSchemaTable<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let header = vec![
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

        let table_rows = self.schema.generate_table_rows(self.selected_index);

        let col_constraints = vec![
            Constraint::Length(10), // Repetition
            Constraint::Length(15), // Physical type
            Constraint::Length(12), // Compressed size
            Constraint::Length(12), // Uncompressed size
            Constraint::Length(8),  // Compression ratio
            Constraint::Length(25), // Encodings
            Constraint::Length(13), // Compression
            Constraint::Length(12), // Min value
            Constraint::Length(12), // Max value
            Constraint::Length(8),  // Null count
        ];

        let table_widget = Table::new(table_rows, col_constraints)
            .header(Row::new(
                header
                    .into_iter()
                    .map(|h| Cell::from(h).bold().fg(Color::Yellow)),
            ))
            .column_spacing(1)
            .block(
                Block::bordered()
                    .title(
                        Line::from(self.title)
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
