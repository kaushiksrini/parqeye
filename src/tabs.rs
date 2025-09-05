use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::Widget;
use ratatui::widgets::Tabs;

use crate::file::Renderable;

pub enum TabType {
    Visualize,
    Metadata,
    Schema,
    RowGroups,
}

impl std::fmt::Display for TabType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TabType::Visualize => write!(f, "Visualize"),
            TabType::Metadata => write!(f, "Metadata"),
            TabType::Schema => write!(f, "Schema"),
            TabType::RowGroups => write!(f, "Row Groups"),
        }
    }
}


pub struct TabManager {
    pub tabs: Vec<TabType>,
    pub active_tab: usize,
    pub title: String,
}

impl TabManager {
    pub fn new() -> Self {
        Self {
            tabs: vec![TabType::Visualize, TabType::Metadata, TabType::Schema, TabType::RowGroups],
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

    pub fn active_tab(&self) -> &TabType {
        &self.tabs[self.active_tab]
    }

    pub fn with_selected_tab(mut self, active_tab: usize) -> Self {
        self.active_tab = active_tab;
        self
    }
}

impl Renderable for TabManager {
    fn render_content(&self, area: Rect, buf: &mut Buffer) {
        let tab_titles: Vec<Line> = self.tabs.iter().map(|t| Line::from(t.to_string())).collect();
        let tabs_widget: Tabs<'_> = Tabs::new(tab_titles)
            .select(self.active_tab)
            .padding(" ", " ")
            .divider(" ");

        tabs_widget.render(area, buf);
    }
}