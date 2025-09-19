use ratatui::{
    buffer::Buffer,
    layout::{Position, Rect},
    style::{Color, Style, Stylize},
    text::Line,
    widgets::{Block, Borders, Widget},
};

use crate::file::schema::{FileSchema, SchemaInfo};
use crate::file::utils::human_readable_bytes;

/// Lightweight butterfly chart widget that directly references schema data
pub struct ColumnSizesButterflyChart<'a> {
    schema: &'a FileSchema,
    max_compressed_size: u64,
    max_uncompressed_size: u64,
}

impl<'a> ColumnSizesButterflyChart<'a> {
    pub fn new(schema: &'a FileSchema) -> Self {
        // Calculate max sizes directly from schema without copying data
        let (max_compressed_size, max_uncompressed_size) = schema
            .columns
            .iter()
            .filter_map(|col| match col {
                SchemaInfo::Primitive { stats, .. } => {
                    Some((stats.total_compressed_size, stats.total_uncompressed_size))
                }
                _ => None,
            })
            .fold((0u64, 0u64), |(max_comp, max_uncomp), (comp, uncomp)| {
                (max_comp.max(comp), max_uncomp.max(uncomp))
            });

        Self {
            schema,
            max_compressed_size,
            max_uncompressed_size,
        }
    }

    fn render_butterfly_chart(&self, area: Rect, buf: &mut Buffer) {
        // Get all schema items to match line-by-line with schema tree
        let all_schema_items: Vec<_> = self.schema.columns.iter().collect();
        if all_schema_items.is_empty() {
            return;
        }

        // Add padding for size labels (11 characters on each side)
        const LABEL_PADDING: u16 = 11;
        let padded_area = Rect {
            x: area.x + LABEL_PADDING,
            y: area.y,
            width: area.width.saturating_sub(LABEL_PADDING * 2),
            height: area.height,
        };

        // Calculate center line position within padded area
        let center_x = padded_area.x + padded_area.width / 2;
        let left_width = center_x - padded_area.x;
        let right_width = padded_area.x + padded_area.width - center_x;

        // Draw center vertical line
        for y in area.y..area.y + area.height {
            if let Some(cell) = buf.cell_mut(Position::new(center_x, y)) {
                cell.set_symbol("│")
                    .set_style(Style::default().fg(Color::DarkGray));
            }
        }

        // Render bars line-by-line to match schema tree, skip groups
        for (i, schema_item) in all_schema_items.iter().enumerate() {
            let row_y = area.y + i as u16;
            if row_y >= area.y + area.height {
                break;
            }

            // Only draw bars for primitive columns, skip groups and root
            if let SchemaInfo::Primitive { stats, .. } = schema_item {
                // Calculate bar lengths as percentage of available width
                let uncompressed_ratio = if self.max_uncompressed_size > 0 {
                    stats.total_uncompressed_size as f64 / self.max_uncompressed_size as f64
                } else {
                    0.0
                };

                let compressed_ratio = if self.max_compressed_size > 0 {
                    stats.total_compressed_size as f64 / self.max_compressed_size as f64
                } else {
                    0.0
                };

                let uncompressed_width = (left_width as f64 * uncompressed_ratio * 0.9) as u16;
                let compressed_width = (right_width as f64 * compressed_ratio * 0.9) as u16;

                // Draw thin left bar (uncompressed) using horizontal line
                let left_start = center_x.saturating_sub(uncompressed_width);
                for x in left_start..center_x {
                    if let Some(cell) = buf.cell_mut(Position::new(x, row_y)) {
                        cell.set_symbol("─")
                            .set_style(Style::default().fg(Color::LightBlue));
                    }
                }

                // Draw thin right bar (compressed) using horizontal line
                let right_end =
                    (center_x + compressed_width).min(padded_area.x + padded_area.width);
                for x in center_x + 1..right_end {
                    if let Some(cell) = buf.cell_mut(Position::new(x, row_y)) {
                        cell.set_symbol("─")
                            .set_style(Style::default().fg(Color::LightGreen));
                    }
                }

                // Add size labels on either side of the bars (within the padding areas)
                // Left side: uncompressed size (in the left padding area)
                let left_label = human_readable_bytes(stats.total_uncompressed_size);
                let left_label_start =
                    area.x + LABEL_PADDING.saturating_sub(left_label.len() as u16);
                for (j, ch) in left_label.chars().enumerate() {
                    let x = left_label_start + j as u16;
                    if x >= area.x && x < padded_area.x {
                        if let Some(cell) = buf.cell_mut(Position::new(x, row_y)) {
                            cell.set_symbol(&ch.to_string())
                                .set_style(Style::default().fg(Color::LightBlue).dim());
                        }
                    }
                }

                // Right side: compressed size (in the right padding area)
                let right_label = human_readable_bytes(stats.total_compressed_size);
                let right_label_start = padded_area.x + padded_area.width + 1;
                for (j, ch) in right_label.chars().enumerate() {
                    let x = right_label_start + j as u16;
                    if x < area.x + area.width {
                        if let Some(cell) = buf.cell_mut(Position::new(x, row_y)) {
                            cell.set_symbol(&ch.to_string())
                                .set_style(Style::default().fg(Color::LightGreen).dim());
                        }
                    }
                }
            }
            // For Group and Root types, we simply skip drawing anything on this line
        }
    }

    /// Create with block title
    pub fn render_with_block(&self, area: Rect, buf: &mut Buffer) {
        if area.height < 3 {
            return;
        }

        // Create block with title that includes compressed/uncompressed labels
        let block = Block::default()
            .title(
                Line::from(vec![
                    "Uncompressed".light_blue().bold(),
                    "│".dark_gray(),
                    "Compressed ".light_green().bold(),
                ])
                .centered(),
            )
            .borders(Borders::ALL);

        // Render the block
        let inner_area = block.inner(area);
        block.render(area, buf);

        // Draw chart in the inner area
        self.render_butterfly_chart(inner_area, buf);
    }
}

impl<'a> Widget for ColumnSizesButterflyChart<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        self.render_with_block(area, buf);
    }
}
