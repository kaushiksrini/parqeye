use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::Widget;
use ratatui::widgets::{Block, Tabs};

pub enum TabType {
    Schema,
    RowGroups,
    Visualize,
}

impl std::fmt::Display for TabType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TabType::Schema => write!(f, "Schema"),
            TabType::RowGroups => write!(f, "Row Groups"),
            TabType::Visualize => write!(f, "Visualize"),
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
            tabs: vec![TabType::Schema, TabType::RowGroups, TabType::Visualize],
            active_tab: 0,
            title: "Tabs".to_string(),
        }
    }

    pub fn with_selected_tab(mut self, active_tab: usize) -> Self {
        self.active_tab = active_tab;
        self
    }

    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }
}

impl Widget for TabManager {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let tab_titles: Vec<Line> = self.tabs.iter().map(|t| Line::from(t.to_string())).collect();
        let tabs_widget = Tabs::new(tab_titles)
            .select(self.active_tab)
            .block(Block::bordered().title(self.title));

        tabs_widget.render(area, buf);
    }
}