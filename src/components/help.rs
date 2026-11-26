use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Stylize},
    text::Line,
    widgets::{Block, BorderType, Borders, Clear, Paragraph, Widget},
};

use crate::tabs::Tab;

pub struct HelpSection {
    pub title: &'static str,
    pub commands: &'static [(&'static str, &'static str)],
}

#[derive(Default)]
pub struct HelpState {
    pub(crate) scroll: usize,
    pub(crate) visible_rows: usize,
    pub(crate) max_scroll: usize,
}

const COMMON: HelpSection = HelpSection {
    title: "Across all tabs",
    commands: &[
        ("Tab / Shift+Tab", "Switch to the next or previous tab."),
        ("?", "Open or close help for this tab."),
        ("Esc", "Reset navigation while browsing."),
        ("q / Q", "Quit parqeye."),
    ],
};

pub struct HelpModal<'a> {
    tab: &'a dyn Tab,
    scroll: usize,
}

struct HelpLayout {
    area: Rect,
    content: Rect,
    lines: Vec<Line<'static>>,
}

impl<'a> HelpModal<'a> {
    pub fn new(tab: &'a dyn Tab, scroll: usize) -> Self {
        Self { tab, scroll }
    }

    fn layout(&self, area: Rect) -> HelpLayout {
        // Leave the application footer visible, including the quit hint.
        let available_height = area.height.saturating_sub(1);
        let width = area
            .width
            .saturating_sub(if area.width >= 40 { 4 } else { 0 })
            .min(88);
        let content_width = width.saturating_sub(4);
        let mut lines = Vec::new();
        for section in self.tab.help().iter().chain(std::iter::once(&COMMON)) {
            if !lines.is_empty() {
                lines.push(Line::default());
            }
            lines.extend(
                wrap_text(section.title, content_width)
                    .into_iter()
                    .map(|line| Line::from(line).yellow().bold()),
            );
            for &(key, description) in section.commands {
                // Stack keys above descriptions on narrow terminals.
                let key_width = 23;
                if content_width >= 54 {
                    for (index, line) in wrap_text(description, content_width - key_width)
                        .into_iter()
                        .enumerate()
                    {
                        let key = if index == 0 { key } else { "" };
                        lines.push(Line::from(vec![format!("{key:<23}").cyan(), line.into()]));
                    }
                } else {
                    lines.extend(
                        wrap_text(key, content_width)
                            .into_iter()
                            .map(|line| Line::from(line).cyan()),
                    );
                    lines.extend(
                        wrap_text(description, content_width)
                            .into_iter()
                            .map(Line::from),
                    );
                }
            }
        }
        let height =
            (lines.len().saturating_add(4).min(u16::MAX as usize) as u16).min(available_height);
        let area = Rect::new(
            area.x + (area.width - width) / 2,
            area.y + (available_height - height) / 2,
            width,
            height,
        );
        HelpLayout {
            area,
            content: Rect::new(
                area.x + width.min(2),
                area.y + height.min(1),
                content_width,
                height.saturating_sub(4),
            ),
            lines,
        }
    }

    pub fn scroll_bounds(&self, area: Rect) -> (usize, usize) {
        let layout = self.layout(area);
        let visible = usize::from(layout.content.height);
        (visible, layout.lines.len().saturating_sub(visible))
    }
}

// Help copy is ASCII apart from arrow key labels, each of which is one cell.
// Wrap before rendering so scrolling uses the actual number of displayed lines.
fn wrap_text(text: &str, width: u16) -> Vec<String> {
    let width = usize::from(width.max(1));
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() && line.chars().count() + 1 + word.chars().count() > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        for character in word.chars() {
            if line.chars().count() == width {
                lines.push(std::mem::take(&mut line));
            }
            line.push(character);
        }
    }
    if !line.is_empty() {
        lines.push(line.trim_end().to_string());
    }
    lines
}

impl Widget for HelpModal<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let layout = self.layout(area);
        Clear.render(layout.area, buf);
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Color::Yellow)
            .title(format!(" {} - Help ", self.tab.to_string()).bold());
        block.render(layout.area, buf);

        let visible = usize::from(layout.content.height);
        let max_scroll = layout.lines.len().saturating_sub(visible);
        let scroll = self.scroll.min(max_scroll);
        let total = layout.lines.len();
        Paragraph::new(
            layout
                .lines
                .into_iter()
                .skip(scroll)
                .take(visible)
                .collect::<Vec<_>>(),
        )
        .render(layout.content, buf);

        if layout.area.height >= 4 {
            let controls = Rect::new(
                layout.content.x,
                layout.area.bottom() - 3,
                layout.content.width,
                1,
            );
            let status = Rect::new(controls.x, controls.y + 1, controls.width, 1);
            "[?] Close | [q]uit".dim().render(controls, buf);
            if max_scroll > 0 {
                format!(
                    "↑/↓ PgUp/PgDn scroll | Home/End | {}-{}/{total}",
                    scroll + 1,
                    (scroll + visible).min(total)
                )
                .dim()
                .render(status, buf);
            }
        }
    }
}
