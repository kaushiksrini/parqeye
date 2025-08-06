use ratatui::{buffer::Buffer, layout::Rect, style::Color, widgets::Widget};

pub struct ScrollbarComponent {
    pub orientation: ScrollbarOrientation,
    pub total_items: usize,
    pub visible_items: usize,
    pub position: usize,
    pub track_color: Color,
    pub thumb_color: Color,
    pub track_symbol: &'static str,
    pub thumb_symbol: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub enum ScrollbarOrientation {
    Vertical,
    Horizontal,
}

impl ScrollbarComponent {
    pub fn vertical(total_items: usize, visible_items: usize, position: usize) -> Self {
        Self {
            orientation: ScrollbarOrientation::Vertical,
            total_items,
            visible_items,
            position,
            track_color: Color::Yellow,
            thumb_color: Color::Gray,
            track_symbol: "│",
            thumb_symbol: "█",
        }
    }

    pub fn horizontal(total_items: usize, visible_items: usize, position: usize) -> Self {
        Self {
            orientation: ScrollbarOrientation::Horizontal,
            total_items,
            visible_items,
            position,
            track_color: Color::DarkGray,
            thumb_color: Color::Gray,
            track_symbol: "─",
            thumb_symbol: "█",
        }
    }

    pub fn with_colors(mut self, track_color: Color, thumb_color: Color) -> Self {
        self.track_color = track_color;
        self.thumb_color = thumb_color;
        self
    }

    pub fn with_symbols(mut self, track_symbol: &'static str, thumb_symbol: &'static str) -> Self {
        self.track_symbol = track_symbol;
        self.thumb_symbol = thumb_symbol;
        self
    }

    fn calculate_thumb_info(&self, track_length: usize) -> (usize, usize) {
        if self.total_items <= self.visible_items {
            return (track_length, 0);
        }

        let thumb_size = ((self.visible_items * track_length) / self.total_items).max(1);
        let max_position = self.total_items.saturating_sub(self.visible_items);
        let thumb_position = if max_position == 0 {
            0
        } else {
            (self.position * (track_length - thumb_size)) / max_position
        };

        (thumb_size, thumb_position)
    }
}

impl Widget for ScrollbarComponent {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.width == 0 || area.height == 0 {
            return;
        }

        match self.orientation {
            ScrollbarOrientation::Vertical => {
                if area.width < 1 || area.height < 2 {
                    return;
                }

                let track_length = area.height as usize;
                let (thumb_size, thumb_position) = self.calculate_thumb_info(track_length);

                // Render the track
                for y in 0..area.height {
                    buf[(area.x, area.y + y)]
                        .set_symbol(self.track_symbol)
                        .set_fg(self.track_color);
                }

                // Render the thumb
                for i in 0..thumb_size {
                    let y = area.y + thumb_position as u16 + i as u16;
                    if y < area.y + area.height {
                        buf[(area.x, y)]
                            .set_symbol(self.thumb_symbol)
                            .set_fg(self.thumb_color);
                    }
                }
            }
            ScrollbarOrientation::Horizontal => {
                if area.width < 2 || area.height < 1 {
                    return;
                }

                let track_length = area.width as usize;
                let (thumb_size, thumb_position) = self.calculate_thumb_info(track_length);

                // Render the track
                for x in 0..area.width {
                    buf[(area.x + x, area.y)]
                        .set_symbol(self.track_symbol)
                        .set_fg(self.track_color);
                }

                // Render the thumb
                for i in 0..thumb_size {
                    let x = area.x + thumb_position as u16 + i as u16;
                    if x < area.x + area.width {
                        buf[(x, area.y)]
                            .set_symbol(self.thumb_symbol)
                            .set_fg(self.thumb_color);
                    }
                }
            }
        }
    }
}
