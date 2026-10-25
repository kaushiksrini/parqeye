use crossterm::event::{KeyCode, KeyEvent};
use ratatui::style::Stylize;
use ratatui::text::Span;
use std::io;

use crate::{
    app::AppState,
    tabs::{EventOutcome, Tab},
};

mod command;

pub use command::{CommandPrompt, PromptKind};
use command::{PromptEvent, VisualizeCommand, parse_command};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VisualizeState {
    mode: VisualizeMode,
}

impl VisualizeState {
    pub fn begin_prompt(&mut self, kind: PromptKind) {
        self.mode = VisualizeMode::Prompt(CommandPrompt::new(kind));
    }

    pub fn cancel_prompt(&mut self) {
        self.mode = VisualizeMode::Normal;
    }

    pub fn prompt(&self) -> Option<&CommandPrompt> {
        match &self.mode {
            VisualizeMode::Normal => None,
            VisualizeMode::Prompt(prompt) => Some(prompt),
        }
    }

    fn prompt_mut(&mut self) -> Option<&mut CommandPrompt> {
        match &mut self.mode {
            VisualizeMode::Normal => None,
            VisualizeMode::Prompt(prompt) => Some(prompt),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
enum VisualizeMode {
    #[default]
    Normal,
    Prompt(CommandPrompt),
}

pub struct VisualizeTab {
    pub max_horizontal_scroll: Option<usize>,
    pub max_rows: Option<usize>,
    pub visible_rows: Option<usize>,
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
            max_rows: None,
            visible_rows: None,
        }
    }

    pub fn with_max_horizontal_scroll(mut self, max_horizontal_scroll: usize) -> Self {
        self.max_horizontal_scroll = Some(max_horizontal_scroll);
        self
    }

    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = Some(max_rows);
        self
    }

    pub fn with_visible_rows(mut self, visible_rows: usize) -> Self {
        self.visible_rows = Some(visible_rows);
        self
    }

    fn execute_command(
        &self,
        command: VisualizeCommand,
        state: &mut AppState,
    ) -> Result<(), String> {
        match command {
            VisualizeCommand::GoToRow(row) => {
                let max_rows = self.max_rows.unwrap_or(0);
                if row > max_rows {
                    return Err(if max_rows == 0 {
                        "there are no rows to navigate".to_string()
                    } else {
                        format!("row must be between 1 and {max_rows}")
                    });
                }

                state.jump_to_row(row - 1, state.visible_data_rows(), max_rows);
                Ok(())
            }
        }
    }

    fn handle_prompt_event(&self, key_event: KeyEvent, state: &mut AppState) -> EventOutcome {
        let prompt_event = state
            .visualize_mut()
            .prompt_mut()
            .expect("prompt mode must contain a prompt")
            .handle_key_event(key_event);

        match prompt_event {
            PromptEvent::Continue => {}
            PromptEvent::Cancel => state.visualize_mut().cancel_prompt(),
            PromptEvent::Submit { kind, input } => {
                let result = parse_command(kind, &input)
                    .map_err(|error| error.to_string())
                    .and_then(|command| self.execute_command(command, state));

                match result {
                    Ok(()) => state.visualize_mut().cancel_prompt(),
                    Err(error) => {
                        if let Some(prompt) = state.visualize_mut().prompt_mut() {
                            prompt.error = Some(error);
                        }
                    }
                }
            }
        }

        EventOutcome::Consumed
    }
}

impl Tab for VisualizeTab {
    fn on_event(
        &self,
        key_event: KeyEvent,
        state: &mut AppState,
    ) -> Result<EventOutcome, io::Error> {
        if state.visualize().prompt().is_some() {
            return Ok(self.handle_prompt_event(key_event, state));
        }

        let max_rows = self.max_rows.unwrap_or(0);
        let visible_rows = state.visible_data_rows();

        let outcome = match key_event.code {
            KeyCode::Char(':') => {
                state.visualize_mut().begin_prompt(PromptKind::GoToRow);
                EventOutcome::Consumed
            }
            // Row navigation (Up/Down arrows)
            KeyCode::Up => {
                if state.vertical_offset() > 0 {
                    state.up();
                    state.adjust_scroll_to_selection(visible_rows, max_rows);
                }
                EventOutcome::Consumed
            }
            KeyCode::Down => {
                if state.vertical_offset() < max_rows.saturating_sub(1) {
                    state.down();
                    state.adjust_scroll_to_selection(visible_rows, max_rows);
                }
                EventOutcome::Consumed
            }
            // Page navigation (u/d keys)
            KeyCode::Char('u') | KeyCode::Char('U') => {
                state.page_up(visible_rows, max_rows);
                EventOutcome::Consumed
            }
            KeyCode::Char('d') | KeyCode::Char('D') => {
                state.page_down(visible_rows, max_rows);
                EventOutcome::Consumed
            }
            // Jump to first / last row (vim-style g/G, or Home/End)
            KeyCode::Char('g') | KeyCode::Home => {
                state.jump_to_top();
                EventOutcome::Consumed
            }
            KeyCode::Char('G') | KeyCode::End => {
                state.jump_to_bottom(visible_rows, max_rows);
                EventOutcome::Consumed
            }
            // Column navigation (Left/Right arrows)
            KeyCode::Left => {
                if state.horizontal_offset() > 0 {
                    state.left();
                }
                EventOutcome::Consumed
            }
            KeyCode::Right => {
                state.right();
                EventOutcome::Consumed
            }
            _ => EventOutcome::Bubble,
        };
        Ok(outcome)
    }

    fn instructions(&self) -> Vec<Span<'static>> {
        vec![
            "↑".green(),
            "/".white(),
            "↓".blue(),
            " : ".into(),
            "Row".into(),
            " | ".white(),
            "→".green(),
            "/".white(),
            "←".blue(),
            " : ".into(),
            "Column".into(),
            " | ".white(),
            "u".green(),
            "/".white(),
            "d".blue(),
            " : ".into(),
            "Page".into(),
            " | ".white(),
            "g".green(),
            "/".white(),
            "G".blue(),
            " : ".into(),
            "Top/Bottom".into(),
            " | ".white(),
            ":".green(),
            " : ".into(),
            "Go to row".into(),
        ]
    }

    fn to_string(&self) -> String {
        "Visualize".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn row_prompt_executes_a_one_based_jump() {
        let tab = VisualizeTab::new().with_max_rows(100);
        let mut state = AppState::new();
        state.set_visible_data_rows(20);

        assert_eq!(
            tab.on_event(key(KeyCode::Char(':')), &mut state).unwrap(),
            EventOutcome::Consumed
        );
        tab.on_event(key(KeyCode::Char('4')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Char('2')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Enter), &mut state).unwrap();

        assert_eq!(state.vertical_offset(), 41);
        assert_eq!(state.data_vertical_scroll(), 22);
        assert!(state.visualize().prompt().is_none());
    }

    #[test]
    fn prompt_consumes_q_instead_of_bubbling_it_to_quit() {
        let tab = VisualizeTab::new().with_max_rows(100);
        let mut state = AppState::new();

        tab.on_event(key(KeyCode::Char(':')), &mut state).unwrap();
        let outcome = tab.on_event(key(KeyCode::Char('q')), &mut state).unwrap();

        assert_eq!(outcome, EventOutcome::Consumed);
        assert_eq!(state.visualize().prompt().unwrap().buffer, "q");
    }

    #[test]
    fn escape_cancels_the_prompt_without_resetting_the_view() {
        let tab = VisualizeTab::new().with_max_rows(100);
        let mut state = AppState::new();
        state.jump_to_row(50, 20, 100);

        tab.on_event(key(KeyCode::Char(':')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Esc), &mut state).unwrap();

        assert_eq!(state.vertical_offset(), 50);
        assert!(state.visualize().prompt().is_none());
    }

    #[test]
    fn invalid_row_keeps_the_prompt_open_with_an_error() {
        let tab = VisualizeTab::new().with_max_rows(10);
        let mut state = AppState::new();

        tab.on_event(key(KeyCode::Char(':')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Char('9')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Char('9')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Enter), &mut state).unwrap();

        let prompt = state.visualize().prompt().unwrap();
        assert_eq!(
            prompt.error.as_deref(),
            Some("row must be between 1 and 10")
        );
        assert_eq!(state.vertical_offset(), 0);
    }
}
