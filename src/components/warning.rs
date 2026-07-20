use ratatui::{
    buffer::Buffer,
    layout::Rect,
    prelude::Color,
    style::{Style, Stylize},
    text::Line,
    widgets::{Block, BorderType, Borders, Paragraph, Widget, Wrap},
};

/// A bordered warning panel: a highlighted headline with an optional body note
/// and optional error detail. Used when a view can't render its normal content
/// but the rest of the app is still usable, so it composes into any `Rect` the
/// same way the other components do.
pub struct WarningView {
    title: String,
    headline: String,
    note: Option<String>,
    detail: Option<String>,
    color: Color,
}

impl WarningView {
    pub fn new(headline: impl Into<String>) -> Self {
        Self {
            title: " Warning ".to_string(),
            headline: headline.into(),
            note: None,
            detail: None,
            color: Color::Yellow,
        }
    }

    /// Text shown in the border title (defaults to " Warning ").
    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
    }

    /// A secondary explanatory line rendered below the headline.
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }

    /// Underlying error detail, rendered last in a muted style.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Accent color for the border, title, and headline (defaults to yellow).
    pub fn with_color(mut self, color: Color) -> Self {
        self.color = color;
        self
    }
}

impl Widget for WarningView {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(self.color))
            .title(self.title.clone());
        let inner_area = block.inner(area);
        block.render(area, buf);

        let mut lines = vec![Line::from(vec![
            "⚠  ".fg(self.color).bold(),
            self.headline.clone().fg(self.color).bold(),
        ])];

        if let Some(note) = &self.note {
            lines.push(Line::from(""));
            lines.push(Line::from(note.clone().fg(Color::Gray)));
        }

        if let Some(detail) = &self.detail {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                "Details: ".fg(Color::DarkGray),
                detail.clone().fg(Color::DarkGray),
            ]));
        }

        Paragraph::new(lines)
            .wrap(Wrap { trim: true })
            .render(inner_area, buf);
    }
}
