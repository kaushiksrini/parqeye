use crate::file::row_groups::RowGroupStats;
use ratatui::{
    buffer::Buffer,
    layout::{Position, Rect},
    prelude::{Color, Span},
    style::Stylize,
    text::Line,
    widgets::{Block, Widget},
};

pub struct RowGroupProgressBar<'a> {
    pub row_group_stats: &'a [RowGroupStats],
    pub selected_idx: usize,
}

impl<'a> RowGroupProgressBar<'a> {
    pub fn new(row_group_stats: &'a [RowGroupStats], selected_idx: usize) -> Self {
        Self {
            row_group_stats,
            selected_idx,
        }
    }
}

impl<'a> Widget for RowGroupProgressBar<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let total_row_groups = self.row_group_stats.len();
        let title: Vec<Span<'static>> = vec![
            " Row Group: ".into(),
            format!("{}", self.selected_idx + 1).into(),
            " / ".into(),
            format!("{total_row_groups}").into(),
            " ".into(),
        ];

        let block = Block::bordered()
            .title(Line::from(title))
            .border_style(ratatui::style::Style::default().fg(Color::Gray))
            .title_style(ratatui::style::Style::default().fg(Color::Gray).bold());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.width > 0 && inner.height > 0 && total_row_groups > 0 {
            // Calculate the width of each segment
            let segment_width = inner.width as f64 / total_row_groups as f64;

            // Find the center line to draw the thin progress bar
            let center_y = inner.y + inner.height / 2;

            // First, draw the thin white line across the entire width
            for x in inner.x..inner.x + inner.width {
                if let Some(cell) = buf.cell_mut(Position::new(x, center_y)) {
                    cell.set_symbol("─")
                        .set_style(ratatui::style::Style::default().fg(Color::White));
                }
            }

            // Then, draw the thick filled section for the selected row group
            let selected_start_x = inner.x + (self.selected_idx as f64 * segment_width) as u16;
            let selected_end_x = inner.x + ((self.selected_idx + 1) as f64 * segment_width) as u16;

            // Fill the selected section with solid blocks (single line, centered)
            for x in selected_start_x..selected_end_x.min(inner.x + inner.width) {
                if let Some(cell) = buf.cell_mut(Position::new(x, center_y)) {
                    cell.set_symbol("█")
                        .set_style(ratatui::style::Style::default().fg(Color::Blue));
                }
            }
        }
    }
}
