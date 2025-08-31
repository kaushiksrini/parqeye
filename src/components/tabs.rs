use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::Widget;
use ratatui::widgets::{Block, Tabs};

use crate::tab::TabId;

pub struct TabsComponent<'a> {
    pub tabs: Vec<&'a str>,
    pub active_tab: TabId,
    pub title: String,
}

impl<'a> TabsComponent<'a> {
    pub fn new(tabs: Vec<&'a str>) -> Self {
        Self {
            tabs,
            active_tab: TabId::RowGroups,
            title: "Tabs".to_string(),
        }
    }

    pub fn with_selected_tab(mut self, active_tab: TabId) -> Self {
        self.active_tab = active_tab;
        self
    }

    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }
}

impl<'a> Widget for TabsComponent<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let tab_titles: Vec<Line> = self.tabs.iter().map(|t| Line::from(*t)).collect();
        let tabs_widget = Tabs::new(tab_titles)
            .select(self.active_tab.index())
            .block(Block::bordered().title(self.title));

        tabs_widget.render(area, buf);
    }
}
