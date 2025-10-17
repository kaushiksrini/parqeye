use crossterm::event::KeyEvent;
use std::io;

use crate::{app::AppState, tabs::Tab};
use ratatui::text::Span;

pub struct MetadataTab {
    pub max_horizontal_scroll: Option<usize>,
    pub max_vertical_scroll: Option<usize>,
}

impl Default for MetadataTab {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataTab {
    pub fn new() -> Self {
        Self {
            max_horizontal_scroll: None,
            max_vertical_scroll: None,
        }
    }

    pub fn with_max_horizontal_scroll(mut self, max_horizontal_scroll: usize) -> Self {
        self.max_horizontal_scroll = Some(max_horizontal_scroll);
        self
    }

    pub fn with_max_vertical_scroll(mut self, max_vertical_scroll: usize) -> Self {
        self.max_vertical_scroll = Some(max_vertical_scroll);
        self
    }
}

impl Tab for MetadataTab {
    #[allow(unused_variables)]
    fn on_event(&self, key_event: KeyEvent, state: &mut AppState) -> Result<(), io::Error> {
        Ok(())
    }

    fn instructions(&self) -> Vec<Span<'static>> {
        vec![]
    }

    fn to_string(&self) -> String {
        "Metadata".to_string()
    }
}
