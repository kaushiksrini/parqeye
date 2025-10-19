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
    pub fn new(num_columns: usize, num_row_groups: usize) -> Self {
        Self {
            tabs: vec![
                Box::new(
                    VisualizeTab::new()
                        .with_max_horizontal_scroll(num_columns)
                        .with_max_vertical_scroll(num_row_groups),
                ),
                Box::new(
                    MetadataTab::new()
                        .with_max_horizontal_scroll(num_columns)
                        .with_max_vertical_scroll(num_row_groups),
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
        let tab_titles: Vec<Line> = self
            .tabs
            .iter()
            .map(|t| Line::from(t.to_string()))
            .collect();
        let tabs_widget: Tabs<'_> = Tabs::new(tab_titles)
            .select(self.active_tab)
            .padding(" ", " ")
            .divider(" ");

        tabs_widget.render(area, buf);
    }
}
