use crossterm::event::{KeyCode, KeyEvent};
use ratatui::style::Stylize;
use ratatui::text::Span;
use std::io;

use crate::{app::AppState, tabs::Tab};

pub struct VisualizeTab {
    pub max_horizontal_scroll: Option<usize>,
    pub max_vertical_scroll: Option<usize>,
}

impl Default for VisualizeTab {
    fn default() -> Self {
        Self::new()
    }
}

impl VisualizeTab {
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

impl Tab for VisualizeTab {
    fn on_event(&self, key_event: KeyEvent, state: &mut AppState) -> Result<(), io::Error> {
        match key_event.code {
            KeyCode::Up if state.vertical_offset() > 0 => state.up(),
            KeyCode::Down
                if state.vertical_offset() < self.max_vertical_scroll.unwrap_or(usize::MAX) - 1 =>
            {
                state.down()
            }
            KeyCode::Left if state.horizontal_offset() > 0 => state.left(),
            KeyCode::Right
                if state.horizontal_offset()
                    < self.max_horizontal_scroll.unwrap_or(usize::MAX) - 1 =>
            {
                state.right()
            }
            _ => {}
        }
        Ok(())
    }

    fn instructions(&self) -> Vec<Span<'static>> {
        vec![
            "→".green(),
            "/".white(),
            "←".blue(),
            " : ".into(),
            "Navigate".into(),
        ]
    }

    fn to_string(&self) -> String {
        "Visualize".to_string()
    }
}

// impl Tab for VisualizeTab {
//     fn on_event(&mut self, event: Event, state: &mut TabState) -> Result<(), io::Error> {
//         match event {
//             Event::Key(key_event) => {
//                 match key_event.code {
//                     KeyCode::Left => {
//                         state.horizontal_scroll -= 1;
//                     }
//                     KeyCode::Right => {
//                         state.horizontal_scroll += 1;
//                     }
//                     KeyCode::Up => {
//                         state.vertical_scroll -= 1;
//                     }
//                     KeyCode::Down => {
//                         state.vertical_scroll += 1;
//                 }
//             }
//         }
//         Ok(())
//     }
// }
