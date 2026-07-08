use crate::file::Renderable;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Stylize;
use ratatui::text::Line;
use ratatui::widgets::Tabs;
use ratatui::widgets::Widget;

use crate::tabs::Tab;
use crate::tabs::metadata::MetadataTab;
use crate::tabs::row_groups::RowGroupsTab;
use crate::tabs::schema::SchemaTab;
use crate::tabs::visualize::VisualizeTab;

pub struct TabManager {
    pub tabs: Vec<Box<dyn Tab>>,
    pub active_tab: usize,
    pub title: String,
}

impl TabManager {
    pub fn new(
        num_columns: usize,
        num_row_groups: usize,
        sample_data_rows: usize,
        num_properties: usize,
    ) -> Self {
        Self {
            tabs: vec![
                Box::new(
                    VisualizeTab::new()
                        .with_max_horizontal_scroll(num_columns)
                        .with_max_rows(sample_data_rows),
                ),
                Box::new(
                    MetadataTab::new().with_max_vertical_scroll(num_properties.saturating_sub(1)),
                ),
                Box::new(SchemaTab::new().with_max_vertical_scroll(num_columns)),
                Box::new(
                    RowGroupsTab::new()
                        .with_max_horizontal_scroll(num_row_groups - 1)
                        .with_max_vertical_scroll(num_columns),
                ),
            ],
            active_tab: 0,
            title: "Tabs".to_string(),
        }
    }

    pub fn next(&mut self) {
        self.active_tab = (self.active_tab + 1) % self.tabs.len();
    }

    pub fn prev(&mut self) {
        if self.active_tab == 0 {
            self.active_tab = self.tabs.len() - 1;
        } else {
            self.active_tab = (self.active_tab.saturating_sub(1)) % self.tabs.len();
        }
    }

    #[allow(clippy::borrowed_box)]
    pub fn active_tab(&self) -> &Box<dyn Tab> {
        &self.tabs[self.active_tab]
    }

    pub fn render_instructions(&self, area: Rect, buf: &mut Buffer) {
        let mut span = self.active_tab().instructions();
        if !span.is_empty() {
            span.push(" - ".into());
        }
        span.extend(vec![
            "[Tab]".green(),
            " Next Tab".into(),
            ", ".into(),
            "[Q]".blue(),
            "uit".into(),
        ]);
        let line = Line::from(span);

        // Calculate the width of the instruction text
        let instruction_width = line.width() as u16;

        // Create a layout that positions the instructions on the right
        use ratatui::layout::{Constraint, Layout};
        let [_, instruction_area] =
            Layout::horizontal([Constraint::Fill(1), Constraint::Length(instruction_width)])
                .areas(area);

        line.render(instruction_area, buf);
    }
}

impl Renderable for TabManager {
    fn render_content(&self, area: Rect, buf: &mut Buffer) {
        if area.width == 0 {
            return;
        }

        let titles: Vec<String> = self.tabs.iter().map(|t| t.to_string()).collect();

        // Each tab occupies its title plus one space of padding on each side
        // (matching `.padding(" ", " ")`); tabs are separated by a single divider.
        const PADDING: u16 = 2;
        const DIVIDER: u16 = 1;
        let tab_widths: Vec<u16> = titles
            .iter()
            .map(|t| t.chars().count() as u16 + PADDING)
            .collect();

        // The `Tabs` widget clips overflow on the right without scrolling, so
        // find the smallest starting offset that keeps the active tab visible.
        let mut offset = 0usize;
        while offset < self.active_tab {
            let mut used = 0u16;
            let mut fits = true;
            for (i, width) in tab_widths.iter().enumerate().take(self.active_tab + 1).skip(offset) {
                if i > offset {
                    used = used.saturating_add(DIVIDER);
                }
                used = used.saturating_add(*width);
                if used > area.width {
                    fits = false;
                    break;
                }
            }
            if fits {
                break;
            }
            offset += 1;
        }

        let tab_titles: Vec<Line> = titles[offset..]
            .iter()
            .map(|t| Line::from(t.clone()))
            .collect();
        let tabs_widget: Tabs<'_> = Tabs::new(tab_titles)
            .select(self.active_tab - offset)
            .padding(" ", " ")
            .divider(" ");

        tabs_widget.render(area, buf);
    }
}
