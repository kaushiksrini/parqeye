use crossterm::event::{KeyCode, KeyEvent};
use ratatui::style::Stylize;
use ratatui::text::Span;
use std::io;

use crate::{app::AppState, tabs::Tab};

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
    fn on_event(&self, key_event: KeyEvent, state: &mut AppState) -> Result<(), io::Error> {
        if self.max_vertical_scroll.is_some() {
            match key_event.code {
                KeyCode::Up if state.vertical_offset() > 0 => state.up(),
                KeyCode::Down
                    if state.vertical_offset()
                        < self.max_vertical_scroll.unwrap_or(usize::MAX) =>
                {
                    state.down()
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn instructions(&self) -> Vec<Span<'static>> {
        if self.max_vertical_scroll.map(|n| n > 0).unwrap_or(false) {
            vec![
                "↑".green(),
                "/".white(),
                "↓".blue(),
                " : ".into(),
                "Scroll properties".into(),
            ]
        } else {
            vec![]
        }
    }

    fn to_string(&self) -> String {
        "Metadata".to_string()
    }
}
