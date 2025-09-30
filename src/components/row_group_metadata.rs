use ratatui::{
    buffer::Buffer, layout::{Constraint, Layout, Position, Rect}, prelude::{Color, Span}, style::Stylize, text::Line, widgets::{Block, Widget}
};
use crate::file::row_groups::RowGroupStats;

use crate::file::utils::{human_readable_bytes, commas};

/// Component to display row group level statistics
pub struct RowGroupMetadata<'a> {
    row_group_stats: &'a RowGroupStats,
    total_row_groups: usize,
}

impl<'a> RowGroupMetadata<'a> {
    pub fn new(row_group_stats: &'a RowGroupStats, total_row_groups: usize) -> Self {
        Self { row_group_stats, total_row_groups }
    }
}

impl<'a> Widget for RowGroupMetadata<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        

        let vertical_areas = Layout::vertical([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Fill(1),
        ]).split(area);

        // Render the progress bar
        self.render_progress_bar(vertical_areas[0], buf);

        // Create 1x4 horizontal grid for stats
        let horizontal_areas = Layout::horizontal([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(vertical_areas[1]);

        // Render each stat block
        self.render_stat_block(
            "Rows", 
            &commas(self.row_group_stats.rows as u64), 
            horizontal_areas[0], 
            buf
        );
        self.render_stat_block(
            "Compressed",
            &human_readable_bytes(self.row_group_stats.compressed_size as u64),
            horizontal_areas[1],
            buf,
        );
        self.render_stat_block(
            "Uncompressed",
            &human_readable_bytes(self.row_group_stats.uncompressed_size as u64),
            horizontal_areas[2],
            buf,
        );
        self.render_stat_block(
            "Ratio", 
            &self.row_group_stats.compression_ratio, 
            horizontal_areas[3], 
            buf
        );
    }
}

impl<'a> RowGroupMetadata<'a> {
    fn render_progress_bar(&self, area: Rect, buf: &mut Buffer) {
        let title: Vec<Span<'static>> = vec![
            " Row Group: ".into(),
            format!("{}", self.row_group_stats.idx + 1).into(),
            " / ".into(),
            format!("{}", self.total_row_groups).into(),
            " ".into(),
        ];
        
        let block = Block::bordered()
            .title(Line::from(title))
            .border_style(ratatui::style::Style::default().fg(Color::Gray))
            .title_style(ratatui::style::Style::default().fg(Color::Gray).bold());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.width > 0 && inner.height > 0 && self.total_row_groups > 0 {
            // Calculate the width of each segment
            let segment_width = inner.width as f64 / self.total_row_groups as f64;
            
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
            let selected_start_x = inner.x + (self.row_group_stats.idx as f64 * segment_width) as u16;
            let selected_end_x = inner.x + ((self.row_group_stats.idx + 1) as f64 * segment_width) as u16;
            
            // Fill the selected section with solid blocks (single line, centered)
            for x in selected_start_x..selected_end_x.min(inner.x + inner.width) {
                if let Some(cell) = buf.cell_mut(Position::new(x, center_y)) {
                    cell.set_symbol("█")
                        .set_style(ratatui::style::Style::default().fg(Color::Blue));
                }
            }
        }
    }

    fn render_stat_block(&self, title: &str, value: &str, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title(title)
            .border_style(ratatui::style::Style::default().fg(Color::Blue))
            .title_style(ratatui::style::Style::default().fg(Color::Blue).bold());

        let inner = block.inner(area);
        block.render(area, buf);

        // Center the value in the block
        if inner.width > 0 && inner.height > 0 {
            let lines: Vec<&str> = value.lines().collect();
            let start_y = inner.y + (inner.height.saturating_sub(lines.len() as u16)) / 2;
            
            for (i, line) in lines.iter().enumerate() {
                let y = start_y + i as u16;
                if y < inner.y + inner.height {
                    let x = inner.x + (inner.width.saturating_sub(line.len() as u16)) / 2;
                    if x < inner.x + inner.width {
                        line.bold().yellow().render(
                            Rect::new(x, y, line.len() as u16, 1),
                            buf
                        );
                    }
                }
            }
        }
    }
}
