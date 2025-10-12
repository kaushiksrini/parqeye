use ratatui::{
    buffer::Buffer, 
    layout::{Constraint, Layout, Position, Rect}, 
    prelude::{Color, Span}, 
    style::{Style, Stylize}, 
    symbols::Marker,
    text::Line, 
    widgets::{Axis, Block, Borders, Chart, Dataset, Widget}
};
use crate::file::row_groups::RowGroupStats;

use crate::file::utils::{human_readable_bytes, commas};

/// Component to display row group level statistics
pub struct RowGroupMetadata<'a> {
    row_group_stats: &'a [RowGroupStats],
    selected_idx: usize,
}

impl<'a> RowGroupMetadata<'a> {
    pub fn new(row_group_stats: &'a [RowGroupStats], selected_idx: usize) -> Self {
        Self { row_group_stats, selected_idx }
    }
}

impl<'a> Widget for RowGroupMetadata<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let selected_stats = &self.row_group_stats[self.selected_idx];
        
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
            &commas(selected_stats.rows as u64), 
            horizontal_areas[0], 
            buf
        );
        self.render_stat_block(
            "Compressed",
            &human_readable_bytes(selected_stats.compressed_size as u64),
            horizontal_areas[1],
            buf,
        );
        self.render_stat_block(
            "Uncompressed",
            &human_readable_bytes(selected_stats.uncompressed_size as u64),
            horizontal_areas[2],
            buf,
        );
        self.render_stat_block(
            "Ratio", 
            &selected_stats.compression_ratio, 
            horizontal_areas[3], 
            buf
        );

        let central_area = Layout::horizontal([
            Constraint::Fill(1),
            Constraint::Fill(1),
        ]).split(vertical_areas[2]);

        // Render charts in the remaining area
        self.render_charts(central_area[0], buf);
    }
}

impl<'a> RowGroupMetadata<'a> {
    fn render_progress_bar(&self, area: Rect, buf: &mut Buffer) {
        let total_row_groups = self.row_group_stats.len();
        let title: Vec<Span<'static>> = vec![
            " Row Group: ".into(),
            format!("{}", self.selected_idx + 1).into(),
            " / ".into(),
            format!("{}", total_row_groups).into(),
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

    fn render_charts(&self, area: Rect, buf: &mut Buffer) {
        // Split area into two charts horizontally
        let chart_areas = Layout::vertical([
            Constraint::Percentage(50),
            Constraint::Percentage(50),
        ])
        .split(area);

        self.render_size_comparison_chart(chart_areas[0], buf);
        self.render_compression_ratio_chart(chart_areas[1], buf);
    }

    fn normalized_x_positions(&self) -> Vec<f64> {
        let num_points = self.row_group_stats.len();
        if num_points == 0 {
            return vec![];
        }
        (0..num_points)
            .map(|i| (i as f64 + 0.5) / num_points as f64)
            .collect()
    }

    fn make_x_labels(&self) -> Vec<String> {
        let num_points = self.row_group_stats.len();
        match num_points {
            0 => vec![],
            1 => vec!["1".to_string()],
            2 => vec!["1".to_string(), "2".to_string()],
            3 => vec!["1".to_string(), "2".to_string(), "3".to_string()],
            n => {
                let a: usize = 1usize;
                let d = n;
                let b = 1 + (n.saturating_sub(1)) / 3;
                let c = 1 + (n.saturating_sub(1)) * 2 / 3;
                let mut labels = vec![a, b, c, d];
                labels.sort_unstable();
                labels.dedup();
                labels.into_iter().map(|v| v.to_string()).collect()
            }
        }
    }

    fn render_size_comparison_chart(&self, area: Rect, buf: &mut Buffer) {
        let n = self.row_group_stats.len();
        if n == 0 {
            return;
        }

        let x_positions = self.normalized_x_positions();
        let compressed_data: Vec<(f64, f64)> = self.row_group_stats
            .iter()
            .enumerate()
            .map(|(i, rg)| (x_positions[i], rg.compressed_size as f64))
            .collect();

        let uncompressed_data: Vec<(f64, f64)> = self.row_group_stats
            .iter()
            .enumerate()
            .map(|(i, rg)| (x_positions[i], rg.uncompressed_size as f64))
            .collect();

        let max_compressed = compressed_data
            .iter()
            .map(|(_, size)| *size)
            .fold(0.0, f64::max);
        let max_uncompressed = uncompressed_data
            .iter()
            .map(|(_, size)| *size)
            .fold(0.0, f64::max);
        let max_size = max_compressed.max(max_uncompressed);

        let datasets = vec![
            Dataset::default()
                .name("Compressed")
                .marker(Marker::Dot)
                .style(Style::default().fg(Color::Blue))
                .data(&compressed_data),
            Dataset::default()
                .name("Uncompressed")
                .marker(Marker::Dot)
                .style(Style::default().fg(Color::Red))
                .data(&uncompressed_data),
        ];

        let x_labels = self.make_x_labels();

        let y_step = (max_size * 1.5) / 4.0;
        let y_labels: Vec<String> = (0..4)
            .map(|i| {
                let value = i as f64 * y_step;
                if value >= 1_000_000.0 {
                    format!("{:.1}M", value / (1_024.0 * 1_024.0))
                } else if value >= 1_000.0 {
                    format!("{:.1}K", value / 1_024.0)
                } else {
                    format!("{value:.0}")
                }
            })
            .collect();

        let title = vec![
            "Compressed".light_blue().bold(),
            " vs ".into(),
            "Uncompressed".light_red().bold(),
            " (B)".into(),
        ];

        let chart = Chart::new(datasets)
            .block(
                Block::default()
                    .title(Line::from(title).centered())
                    .title_bottom("Row Group".dark_gray())
                    .borders(Borders::NONE),
            )
            .x_axis(
                Axis::default()
                    .style(Style::default().fg(Color::White))
                    .bounds([0.0, 1.0])
                    .labels(x_labels),
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().fg(Color::White))
                    .bounds([0.0, max_size * 1.5])
                    .labels(y_labels),
            );

        chart.render(area, buf);
    }

    fn render_compression_ratio_chart(&self, area: Rect, buf: &mut Buffer) {
        let n = self.row_group_stats.len();
        if n == 0 {
            return;
        }

        let x_positions = self.normalized_x_positions();
        let ratio_data: Vec<(f64, f64)> = self.row_group_stats
            .iter()
            .enumerate()
            .map(|(i, rg)| {
                let ratio = if rg.compressed_size > 0 {
                    rg.uncompressed_size as f64 / rg.compressed_size as f64
                } else {
                    1.0
                };
                (x_positions[i], ratio)
            })
            .collect();

        let max_ratio = ratio_data
            .iter()
            .map(|(_, ratio)| *ratio)
            .fold(0.0, f64::max);

        let datasets = vec![Dataset::default()
            .name("Compression Ratio")
            .marker(Marker::Dot)
            .style(Style::default().fg(Color::Yellow))
            .data(&ratio_data)];

        let x_labels = self.make_x_labels();

        let y_range = max_ratio * 1.1 - 1.0;
        let y_step = y_range / 4.0;

        let y_labels = vec![
            "1.0x".to_string(),
            format!("{:.1}x", 1.0 + y_step),
            format!("{:.1}x", 1.0 + y_step * 2.0),
            format!("{:.1}x", 1.0 + y_step * 3.0),
        ];

        let chart = Chart::new(datasets)
            .block(
                Block::default()
                    .title("Compression Ratio".yellow())
                    .title_bottom("Row Group".dark_gray())
                    .borders(Borders::NONE),
            )
            .x_axis(
                Axis::default()
                    .style(Style::default().fg(Color::White))
                    .bounds([0.0, 1.0])
                    .labels(x_labels),
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().fg(Color::White))
                    .bounds([1.0, max_ratio * 1.1])
                    .labels(y_labels),
            );

        chart.render(area, buf);
    }
}
