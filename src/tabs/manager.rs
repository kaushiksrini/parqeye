use crate::file::Renderable;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::Tabs;
use ratatui::widgets::Widget;

use crate::tabs::visualize::VisualizeTab;
use crate::tabs::metadata::MetadataTab;
use crate::tabs::schema::SchemaTab;
use crate::tabs::row_groups::RowGroupsTab;
use crate::tabs::Tab;


pub struct TabManager {
    pub tabs: Vec<Box<dyn Tab>>,
    pub active_tab: usize,
    pub title: String,
}

impl TabManager {
    pub fn new(num_columns: usize, num_row_groups: usize) -> Self {
        Self {
            tabs: vec![
                Box::new(VisualizeTab::new()
                    .with_max_horizontal_scroll(num_columns)
                    .with_max_vertical_scroll(num_row_groups)),
                Box::new(MetadataTab::new()
                    .with_max_horizontal_scroll(num_columns)
                    .with_max_vertical_scroll(num_row_groups)),
                Box::new(SchemaTab::new()
                    .with_max_horizontal_scroll(num_row_groups)
                    .with_max_vertical_scroll(num_columns)),
                Box::new(RowGroupsTab::new()
                    .with_max_horizontal_scroll(num_row_groups)
                    .with_max_vertical_scroll(num_columns)),
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

    pub fn active_tab(&self) -> &Box<dyn Tab> {
        &self.tabs[self.active_tab]
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
