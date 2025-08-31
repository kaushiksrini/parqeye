use crossterm::event::{KeyEvent};
use ratatui::{buffer::Buffer, layout::Rect};

use crate::App;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TabId {
    #[default]
    Schema = 0,
    RowGroups = 1,
    Visualize = 2,
}

impl TabId {
    const ALL: [TabId; 3] = [TabId::Schema, TabId::RowGroups, TabId::Visualize];
    const NAMES: [&str; 3] = ["Schema", "Row Groups", "Visualize"];
    
    fn from_index(i: usize) -> Self {
        Self::ALL[i % Self::ALL.len()]
    }
    
    pub fn name(&self) -> &str {
        Self::NAMES[self.index()]
    }

    pub fn index(self) -> usize {
        self as usize
    }

    pub fn next(self) -> Self {
        Self::from_index(self.index() + 1)
    }

    pub fn prev(self) -> Self {
        Self::from_index(self.index().wrapping_sub(1))
    }

    pub fn all_tab_names(&self) -> Vec<&str> {
        Self::NAMES.to_vec()
    }
}

pub trait Tab {
    fn on_focus(&mut self);
    fn on_event(&mut self, key_event: KeyEvent, app: &mut App);
    fn render(&mut self, app: &mut App, area: Rect, buf: &mut Buffer);
}