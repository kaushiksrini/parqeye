use crossterm::event::{KeyCode, KeyEvent};
use std::io;

use crate::{app::AppState, tabs::Tab};

pub struct SchemaTab {
    pub max_horizontal_scroll: Option<usize>,
    pub max_vertical_scroll: Option<usize>,
}

impl Default for SchemaTab {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaTab {
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

impl Tab for SchemaTab {
    fn on_event(&self, key_event: KeyEvent, state: &mut AppState) -> Result<(), io::Error> {
        match key_event.code {
            KeyCode::Up if state.vertical_offset() > 0 => state.up(),
            KeyCode::Down
                if state.vertical_offset() < self.max_vertical_scroll.unwrap_or(usize::MAX) =>
            {
                state.down()
            }
            KeyCode::Left if state.horizontal_offset() > 0 => state.left(),
            KeyCode::Right
                if state.horizontal_offset() < self.max_horizontal_scroll.unwrap_or(usize::MAX) =>
            {
                state.right()
            }
            _ => {}
        }
        Ok(())
    }

    fn instructions(&self) -> String {
        todo!()
    }

    fn to_string(&self) -> String {
        "Schema".to_string()
    }
}
