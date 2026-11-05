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
            // A row group's segment can be narrower than one terminal cell.
            let selected_end_x = (inner.x
                + ((self.selected_idx + 1) as f64 * segment_width) as u16)
                .max(selected_start_x + 1);

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

#[cfg(test)]
mod tests {
    use super::*;

    fn row_groups(count: usize) -> Vec<RowGroupStats> {
        (0..count)
            .map(|idx| RowGroupStats {
                idx,
                rows: 0,
                compressed_size: 0,
                uncompressed_size: 0,
                compression_ratio: 0.0,
                column_metadata: Vec::new(),
            })
            .collect()
    }

    fn cursor_positions(row_groups: &[RowGroupStats], selected_idx: usize, width: u16) -> Vec<u16> {
        let area = Rect::new(3, 2, width + 2, 3);
        let mut buf = Buffer::empty(area);
        RowGroupProgressBar::new(row_groups, selected_idx).render(area, &mut buf);

        (0..width)
            .filter(|&x| {
                let cell = &buf[(area.x + 1 + x, area.y + 1)];
                cell.symbol() == "█" && cell.fg == Color::Blue
            })
            .collect()
    }

    #[test]
    fn cursor_stays_visible_for_every_row_group() {
        for count in [1, 3, 10, 1000] {
            let row_groups = row_groups(count);
            for width in [1, 2, 7, 20, 80] {
                let mut previous_start = 0;
                for selected_idx in 0..count {
                    let positions = cursor_positions(&row_groups, selected_idx, width);
                    assert!(
                        !positions.is_empty(),
                        "cursor disappeared: count={count}, width={width}, selection={selected_idx}"
                    );
                    assert!(positions[0] >= previous_start);
                    previous_start = positions[0];
                    if selected_idx == 0 {
                        assert_eq!(positions[0], 0);
                    }
                    if selected_idx == count - 1 {
                        assert_eq!(positions.last(), Some(&(width - 1)));
                    }
                }
            }
        }
    }

    #[test]
    fn cursor_keeps_proportional_width_when_row_groups_fit() {
        let row_groups = row_groups(3);
        assert_eq!(cursor_positions(&row_groups, 0, 8), vec![0, 1]);
        assert_eq!(cursor_positions(&row_groups, 1, 8), vec![2, 3, 4]);
        assert_eq!(cursor_positions(&row_groups, 2, 8), vec![5, 6, 7]);
    }
}
