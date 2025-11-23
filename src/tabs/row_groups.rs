use crate::config::Action;
use crate::{app::AppState, tabs::Tab};
use crossterm::event::KeyEvent;
use ratatui::style::Stylize;
use ratatui::text::Span;
use std::io;

pub struct RowGroupsTab {
    pub max_horizontal_scroll: Option<usize>,
    pub max_vertical_scroll: Option<usize>,
}

impl Default for RowGroupsTab {
    fn default() -> Self {
        Self::new()
    }
}

impl RowGroupsTab {
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

impl Tab for RowGroupsTab {
    fn on_event(&self, key_event: KeyEvent, state: &mut AppState) -> Result<(), io::Error> {
        if let Some(action) = state.config.keymap.get_action(key_event.code) {
            match action {
                Action::Up => {
                    if state.vertical_offset() > 0 {
                        state.up();
                    }
                }
                Action::Down => {
                    if state.vertical_offset() < self.max_vertical_scroll.unwrap_or(usize::MAX) {
                        state.down();
                    }
                }
                Action::Left => {
                    if state.horizontal_offset() > 0 {
                        state.left();
                    }
                }
                Action::Right => {
                    if state.horizontal_offset() < self.max_horizontal_scroll.unwrap_or(usize::MAX)
                    {
                        state.right();
                    }
                }
                _ => {}
            }
            return Ok(());
        }
        Ok(())
    }

    fn instructions(&self) -> Vec<Span<'static>> {
        vec![
            "→".green(),
            "/".white(),
            "←".blue(),
            " : ".into(),
            "Iterate Row Groups".into(),
            ", ".into(),
            "↑".green(),
            "/".white(),
            "↓".blue(),
            " : ".into(),
            "Schema".into(),
        ]
    }

    fn to_string(&self) -> String {
        "Row Groups".to_string()
    }
}
