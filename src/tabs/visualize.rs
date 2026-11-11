use crate::components::data_table::ColumnWidthCache;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::style::Stylize;
use ratatui::text::Span;
use std::cell::RefCell;
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
    pub selection_mode: SelectionMode,
    pub selected_column: usize,
    pub column_filter: Option<String>,
    projection: Option<Vec<usize>>,
    pub column_name_scroll: usize,
    pub(crate) column_widths: RefCell<ColumnWidthCache>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SelectionMode {
    #[default]
    Row,
    Column,
    Cell,
}

impl SelectionMode {
    fn next(self) -> Self {
        match self {
            Self::Row => Self::Column,
            Self::Column => Self::Cell,
            Self::Cell => Self::Row,
        }
    }
}

impl VisualizeState {
    pub fn begin_prompt(&mut self, kind: PromptKind) {
        let mut prompt = CommandPrompt::new(kind);
        if kind == PromptKind::Columns {
            prompt.buffer = self.column_filter.clone().unwrap_or_default();
            prompt.cursor = prompt.buffer.len();
        }
        self.mode = VisualizeMode::Prompt(prompt);
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub fn column_count(&self, total: usize) -> usize {
        self.projection.as_ref().map_or(total, Vec::len)
    }

    pub fn source_column(&self, visible: usize) -> Option<usize> {
        self.projection
            .as_ref()
            .map_or(Some(visible), |indices| indices.get(visible).copied())
    }

    pub fn reset_navigation(&mut self) {
        self.mode = VisualizeMode::Normal;
        self.selection_mode = SelectionMode::Row;
        self.selected_column = 0;
        self.column_name_scroll = 0;
    }

    /// Lines above the table shared by rendering and viewport calculations.
    pub fn header_lines(&self) -> u16 {
        u16::from(self.column_filter.is_some())
            + u16::from(self.selection_mode != SelectionMode::Row)
            + u16::from(
                self.prompt()
                    .is_some_and(|prompt| prompt.kind == PromptKind::Columns),
            )
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
    pub max_columns: Option<usize>,
    pub max_rows: Option<usize>,
    pub visible_rows: Option<usize>,
    column_names: Vec<String>,
}

impl Default for VisualizeTab {
    fn default() -> Self {
        Self::new()
    }
}

impl VisualizeTab {
    pub fn new() -> Self {
        Self {
            max_columns: None,
            max_rows: None,
            visible_rows: None,
            column_names: Vec::new(),
        }
    }

    pub fn with_max_columns(mut self, max_columns: usize) -> Self {
        self.max_columns = Some(max_columns);
        self
    }

    pub fn with_column_names(mut self, names: &[String]) -> Self {
        self.max_columns = Some(names.len());
        self.column_names = names.to_vec();
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
            VisualizeCommand::FilterColumns(expression) => {
                let prompt = state
                    .visualize()
                    .prompt()
                    .expect("column prompt must be open");
                if let Some(error) = &prompt.error {
                    return Err(error.clone());
                }
                let indices = prompt
                    .column_matches
                    .clone()
                    .ok_or("Column filter has no matches")?;
                let source = state
                    .visualize()
                    .source_column(state.visualize().selected_column);
                let selected = source
                    .and_then(|source| indices.iter().position(|&index| index == source))
                    .unwrap_or(0);
                let visualize = state.visualize_mut();
                visualize.selected_column = selected;
                visualize.column_name_scroll = 0;
                if expression.is_empty() {
                    visualize.column_filter = None;
                    visualize.projection = None;
                } else {
                    visualize.column_filter = Some(expression);
                    visualize.projection = Some(indices);
                }
                // The viewport is recalculated before the next render.
                state.set_max_horizontal_offset(usize::MAX);
                state.set_horizontal_offset(selected);
                Ok(())
            }
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
        let old_buffer = state.visualize().prompt().unwrap().buffer.clone();
        let prompt_event = state
            .visualize_mut()
            .prompt_mut()
            .expect("prompt mode must contain a prompt")
            .handle_key_event(key_event);

        match prompt_event {
            PromptEvent::Continue => {
                let prompt = state.visualize_mut().prompt_mut().unwrap();
                if prompt.kind == PromptKind::Columns && prompt.buffer != old_buffer {
                    prompt.resolve_columns(&self.column_names);
                }
            }
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
        let max_columns = state
            .visualize()
            .column_count(self.max_columns.unwrap_or(0));
        let visible_rows = state.visible_data_rows();
        let selection_mode = state.visualize().selection_mode;

        let outcome = match key_event.code {
            KeyCode::Char('*') => {
                state.visualize_mut().begin_prompt(PromptKind::Columns);
                state
                    .visualize_mut()
                    .prompt_mut()
                    .unwrap()
                    .resolve_columns(&self.column_names);
                EventOutcome::Consumed
            }
            KeyCode::Left | KeyCode::Right
                if key_event.modifiers == KeyModifiers::ALT
                    && selection_mode != SelectionMode::Row =>
            {
                let visualize = state.visualize_mut();
                let name_len = visualize
                    .source_column(visualize.selected_column)
                    .and_then(|index| self.column_names.get(index))
                    .map_or(0, |name| name.chars().count());
                visualize.column_name_scroll = if key_event.code == KeyCode::Left {
                    visualize.column_name_scroll.saturating_sub(1)
                } else {
                    visualize
                        .column_name_scroll
                        .saturating_add(1)
                        .min(name_len.saturating_sub(1))
                };
                EventOutcome::Consumed
            }
            KeyCode::Char(':') => {
                state.visualize_mut().begin_prompt(PromptKind::GoToRow);
                EventOutcome::Consumed
            }
            KeyCode::Char('y') if key_event.modifiers == KeyModifiers::NONE => {
                if selection_mode != SelectionMode::Cell && max_columns != 1 {
                    state.show_toast("Yank requires cell mode (press v)", true);
                    EventOutcome::Consumed
                } else if max_rows == 0 || max_columns == 0 {
                    state.show_toast("No cell selected", true);
                    EventOutcome::Consumed
                } else {
                    EventOutcome::CopyCell
                }
            }
            KeyCode::Char('v') if key_event.modifiers == KeyModifiers::NONE => {
                if max_columns == 1 {
                    return Ok(EventOutcome::Consumed);
                }
                let next = selection_mode.next();
                state.visualize_mut().selection_mode = next;
                if next == SelectionMode::Cell {
                    // Column mode scrolls independently of the row cursor. Bring
                    // that cursor into the current viewport without jumping back.
                    let first = state.data_vertical_scroll();
                    let last = first
                        .saturating_add(visible_rows.saturating_sub(1))
                        .min(max_rows.saturating_sub(1));
                    let row = state.vertical_offset().max(first).min(last);
                    state.jump_to_row(row, visible_rows, max_rows);
                }
                EventOutcome::Consumed
            }
            // Row navigation (Up/Down arrows)
            KeyCode::Up if selection_mode == SelectionMode::Column => {
                state.set_data_vertical_scroll(state.data_vertical_scroll().saturating_sub(1));
                EventOutcome::Consumed
            }
            KeyCode::Down if selection_mode == SelectionMode::Column => {
                state.set_data_vertical_scroll(
                    state
                        .data_vertical_scroll()
                        .saturating_add(1)
                        .min(max_rows.saturating_sub(visible_rows)),
                );
                EventOutcome::Consumed
            }
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
            // Page navigation (Ctrl-u/Ctrl-d or PageUp/PageDown)
            KeyCode::Char('u') | KeyCode::PageUp
                if key_event.code == KeyCode::PageUp
                    || key_event.modifiers == KeyModifiers::CONTROL =>
            {
                if selection_mode == SelectionMode::Column {
                    state.set_data_vertical_scroll(
                        state.data_vertical_scroll().saturating_sub(visible_rows),
                    );
                } else {
                    state.page_up(visible_rows, max_rows);
                }
                EventOutcome::Consumed
            }
            KeyCode::Char('d') | KeyCode::PageDown
                if key_event.code == KeyCode::PageDown
                    || key_event.modifiers == KeyModifiers::CONTROL =>
            {
                if selection_mode == SelectionMode::Column {
                    state.set_data_vertical_scroll(
                        state
                            .data_vertical_scroll()
                            .saturating_add(visible_rows)
                            .min(max_rows.saturating_sub(visible_rows)),
                    );
                } else {
                    state.page_down(visible_rows, max_rows);
                }
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
                state.visualize_mut().column_name_scroll = 0;
                if selection_mode == SelectionMode::Row {
                    state.left();
                    state.visualize_mut().selected_column = state.horizontal_offset();
                } else {
                    let column = state.visualize().selected_column.saturating_sub(1);
                    state.visualize_mut().selected_column = column;
                }
                EventOutcome::Consumed
            }
            KeyCode::Right => {
                state.visualize_mut().column_name_scroll = 0;
                if selection_mode == SelectionMode::Row {
                    state.right();
                    state.visualize_mut().selected_column = state.horizontal_offset();
                } else {
                    let column = state
                        .visualize()
                        .selected_column
                        .saturating_add(1)
                        .min(max_columns.saturating_sub(1));
                    state.visualize_mut().selected_column = column;
                }
                EventOutcome::Consumed
            }
            _ => EventOutcome::Bubble,
        };
        Ok(outcome)
    }

    fn instructions(&self) -> Vec<Span<'static>> {
        vec![
            "*".green(),
            " : Columns | ".into(),
            "Alt-←/→".green(),
            " : Scroll name | ".into(),
            "y".green(),
            " : Yank cell | ".into(),
            "v".green(),
            " : Selection | ".into(),
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
            "Ctrl-u".green(),
            "/".white(),
            "Ctrl-d".blue(),
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

    fn enter_filter(tab: &VisualizeTab, state: &mut AppState, expression: &str) {
        tab.on_event(key(KeyCode::Char('*')), state).unwrap();
        let prompt = state.visualize_mut().prompt_mut().unwrap();
        prompt.buffer = expression.into();
        prompt.cursor = expression.len();
        prompt.resolve_columns(&tab.column_names);
        tab.on_event(key(KeyCode::Enter), state).unwrap();
    }

    #[test]
    fn filters_replace_preserve_source_selection_and_clear_without_changing_rows() {
        let names: Vec<_> = ["a", "b", "c", "d"].into_iter().map(String::from).collect();
        let tab = VisualizeTab::new()
            .with_column_names(&names)
            .with_max_rows(20);
        let mut state = AppState::new();
        state.jump_to_row(7, 3, 20);
        state.visualize_mut().selection_mode = SelectionMode::Cell;
        state.visualize_mut().selected_column = 2;
        let scroll = state.data_vertical_scroll();
        enter_filter(&tab, &mut state, "^(a|c)$");
        assert_eq!(state.visualize().projection(), Some([0, 2].as_slice()));
        assert_eq!(state.visualize().selected_column, 1);
        assert_eq!(state.visualize().selection_mode, SelectionMode::Cell);
        assert_eq!(state.vertical_offset(), 7);
        assert_eq!(state.data_vertical_scroll(), scroll);
        tab.on_event(key(KeyCode::Right), &mut state).unwrap();
        assert_eq!(state.visualize().selected_column, 1);
        tab.on_event(key(KeyCode::Char('*')), &mut state).unwrap();
        assert_eq!(state.visualize().prompt().unwrap().buffer, "^(a|c)$");
        tab.on_event(key(KeyCode::Esc), &mut state).unwrap();
        enter_filter(&tab, &mut state, "^d$");
        assert_eq!(state.visualize().projection(), Some([3].as_slice()));
        assert_eq!(state.visualize().selected_column, 0);
        enter_filter(&tab, &mut state, "");
        assert_eq!(state.visualize().projection(), None);
        assert_eq!(state.visualize().column_filter, None);
        assert_eq!(state.visualize().selected_column, 3);
    }

    #[test]
    fn invalid_input_cancellation_and_navigation_resets_preserve_applied_filter() {
        let names = vec!["a".into(), "b".into()];
        let tab = VisualizeTab::new()
            .with_column_names(&names)
            .with_max_rows(20);
        let mut state = AppState::new();
        enter_filter(&tab, &mut state, "^b$");
        for invalid in ["[", "missing"] {
            enter_filter(&tab, &mut state, invalid);
            assert!(state.visualize().prompt().unwrap().error.is_some());
            assert_eq!(state.visualize().projection(), Some([1].as_slice()));
            tab.on_event(key(KeyCode::Esc), &mut state).unwrap();
        }
        state.reset();
        assert_eq!(state.visualize().column_filter.as_deref(), Some("^b$"));
        assert_eq!(state.visualize().projection(), Some([1].as_slice()));
        assert_eq!(
            tab.on_event(key(KeyCode::Char('y')), &mut state).unwrap(),
            EventOutcome::CopyCell
        );
        tab.on_event(key(KeyCode::Char('v')), &mut state).unwrap();
        assert_eq!(state.visualize().selection_mode, SelectionMode::Row);
        enter_filter(&tab, &mut state, "");
        tab.on_event(key(KeyCode::Char('*')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Char('B')), &mut state).unwrap();
        assert_eq!(
            state
                .visualize()
                .prompt()
                .unwrap()
                .column_matches
                .as_deref(),
            Some([1].as_slice())
        );
        assert_eq!(
            tab.on_event(key(KeyCode::Tab), &mut state).unwrap(),
            EventOutcome::Consumed
        );
        tab.on_event(
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            &mut state,
        )
        .unwrap();
        assert!(state.visualize().prompt().is_none());
        assert!(state.visualize().projection().is_none());
    }

    #[test]
    fn yank_requires_a_single_cell_and_leaves_prompt_input_alone() {
        let tab = VisualizeTab::new().with_max_rows(20).with_max_columns(4);
        let mut state = AppState::new();
        for mode in [SelectionMode::Row, SelectionMode::Column] {
            state.visualize_mut().selection_mode = mode;
            assert_eq!(
                tab.on_event(key(KeyCode::Char('y')), &mut state).unwrap(),
                EventOutcome::Consumed
            );
            let toast = state.toast().unwrap();
            assert!(toast.is_error);
            assert_eq!(toast.message, "Yank requires cell mode (press v)");
        }
        state.visualize_mut().selection_mode = SelectionMode::Cell;
        assert_eq!(
            tab.on_event(key(KeyCode::Char('y')), &mut state).unwrap(),
            EventOutcome::CopyCell
        );
        tab.on_event(key(KeyCode::Char(':')), &mut state).unwrap();
        assert_eq!(
            tab.on_event(key(KeyCode::Char('y')), &mut state).unwrap(),
            EventOutcome::Consumed
        );
        assert_eq!(state.visualize().prompt().unwrap().buffer, "y");

        state.visualize_mut().cancel_prompt();
        let empty_tab = VisualizeTab::new();
        assert_eq!(
            empty_tab
                .on_event(key(KeyCode::Char('y')), &mut state)
                .unwrap(),
            EventOutcome::Consumed
        );
        assert_eq!(state.toast().unwrap().message, "No cell selected");
    }

    #[test]
    fn single_column_allows_yank_in_every_mode_and_selection_toggle_is_a_noop() {
        let tab = VisualizeTab::new().with_max_rows(20).with_max_columns(1);
        let mut state = AppState::new();
        state.set_visible_data_rows(3);
        state.jump_to_row(7, 3, 20);
        for mode in [
            SelectionMode::Row,
            SelectionMode::Column,
            SelectionMode::Cell,
        ] {
            state.visualize_mut().selection_mode = mode;
            let before = state.visualize().clone();
            let scroll = state.data_vertical_scroll();
            assert_eq!(
                tab.on_event(key(KeyCode::Char('v')), &mut state).unwrap(),
                EventOutcome::Consumed
            );
            assert_eq!(state.visualize(), &before);
            assert_eq!(state.vertical_offset(), 7);
            assert_eq!(state.data_vertical_scroll(), scroll);
            assert_eq!(
                tab.on_event(key(KeyCode::Char('y')), &mut state).unwrap(),
                EventOutcome::CopyCell
            );
            assert!(state.toast().is_none());
        }

        let empty = VisualizeTab::new().with_max_rows(0).with_max_columns(1);
        state.visualize_mut().selection_mode = SelectionMode::Row;
        assert_eq!(
            empty.on_event(key(KeyCode::Char('y')), &mut state).unwrap(),
            EventOutcome::Consumed
        );
        assert_eq!(state.toast().unwrap().message, "No cell selected");
    }

    #[test]
    fn selection_cycles_without_losing_the_cursor() {
        let tab = VisualizeTab::new().with_max_rows(20).with_max_columns(4);
        let mut state = AppState::new();
        state.jump_to_row(7, 20, 20);

        for expected in [
            SelectionMode::Column,
            SelectionMode::Cell,
            SelectionMode::Row,
        ] {
            assert_eq!(
                tab.on_event(key(KeyCode::Char('v')), &mut state).unwrap(),
                EventOutcome::Consumed
            );
            assert_eq!(state.visualize().selection_mode, expected);
            if expected == SelectionMode::Column {
                tab.on_event(key(KeyCode::Right), &mut state).unwrap();
            }
            assert_eq!(state.visualize().selected_column, 1);
            assert_eq!(state.vertical_offset(), 7);
            assert_eq!(state.horizontal_offset(), 0);
        }
    }

    #[test]
    fn column_navigation_scrolls_rows_and_cell_mode_enters_the_current_view() {
        let tab = VisualizeTab::new().with_max_rows(10).with_max_columns(3);
        let mut state = AppState::new();
        state.set_visible_data_rows(3);
        tab.on_event(key(KeyCode::Char('v')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Right), &mut state).unwrap();
        tab.on_event(key(KeyCode::Down), &mut state).unwrap();
        assert_eq!(state.data_vertical_scroll(), 1);
        assert_eq!(state.vertical_offset(), 0);
        tab.on_event(key(KeyCode::PageDown), &mut state).unwrap();
        assert_eq!(state.data_vertical_scroll(), 4);
        tab.on_event(key(KeyCode::PageDown), &mut state).unwrap();
        tab.on_event(key(KeyCode::Down), &mut state).unwrap();
        assert_eq!(state.data_vertical_scroll(), 7);
        tab.on_event(key(KeyCode::Up), &mut state).unwrap();
        assert_eq!(state.data_vertical_scroll(), 6);

        tab.on_event(key(KeyCode::Char('v')), &mut state).unwrap();
        assert_eq!(state.visualize().selection_mode, SelectionMode::Cell);
        assert_eq!(state.vertical_offset(), 6);
        assert_eq!(state.data_vertical_scroll(), 6);
        assert_eq!(state.visualize().selected_column, 1);
        tab.on_event(key(KeyCode::Down), &mut state).unwrap();
        assert_eq!(state.vertical_offset(), 7);
        assert_eq!(state.data_vertical_scroll(), 6);
    }

    #[test]
    fn cell_navigation_clamps_at_data_boundaries_including_empty_tables() {
        for (rows, columns) in [(2, 3), (0, 0)] {
            let tab = VisualizeTab::new()
                .with_max_rows(rows)
                .with_max_columns(columns);
            let mut state = AppState::new();
            for _ in 0..2 {
                tab.on_event(key(KeyCode::Char('v')), &mut state).unwrap();
            }
            for _ in 0..5 {
                tab.on_event(key(KeyCode::Right), &mut state).unwrap();
                tab.on_event(key(KeyCode::Down), &mut state).unwrap();
            }
            assert_eq!(state.visualize().selected_column, columns.saturating_sub(1));
            assert_eq!(state.vertical_offset(), rows.saturating_sub(1));
            for _ in 0..5 {
                tab.on_event(key(KeyCode::Left), &mut state).unwrap();
                tab.on_event(key(KeyCode::Up), &mut state).unwrap();
            }
            assert_eq!(state.visualize().selected_column, 0);
            assert_eq!(state.vertical_offset(), 0);
        }
    }

    #[test]
    fn row_mode_scrolls_columns_and_selects_a_visible_column_on_toggle() {
        let tab = VisualizeTab::new().with_max_rows(10).with_max_columns(4);
        let mut state = AppState::new();
        state.set_max_horizontal_offset(2);
        for _ in 0..5 {
            tab.on_event(key(KeyCode::Right), &mut state).unwrap();
        }
        assert_eq!(state.horizontal_offset(), 2);
        tab.on_event(key(KeyCode::Char('v')), &mut state).unwrap();
        assert_eq!(state.visualize().selected_column, 2);
        tab.on_event(key(KeyCode::Right), &mut state).unwrap();
        assert_eq!(state.visualize().selected_column, 3);
        assert_eq!(state.horizontal_offset(), 2);
    }

    #[test]
    fn prompt_input_and_row_jumps_preserve_selection_mode_and_column() {
        let tab = VisualizeTab::new().with_max_rows(100).with_max_columns(3);
        let mut state = AppState::new();
        for _ in 0..2 {
            tab.on_event(key(KeyCode::Char('v')), &mut state).unwrap();
        }
        tab.on_event(key(KeyCode::Right), &mut state).unwrap();
        tab.on_event(key(KeyCode::Char(':')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Char('v')), &mut state).unwrap();
        assert_eq!(state.visualize().prompt().unwrap().buffer, "v");
        assert_eq!(state.visualize().selection_mode, SelectionMode::Cell);
        tab.on_event(key(KeyCode::Esc), &mut state).unwrap();
        tab.on_event(key(KeyCode::Char(':')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Char('4')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Char('2')), &mut state).unwrap();
        tab.on_event(key(KeyCode::Enter), &mut state).unwrap();
        assert_eq!(state.vertical_offset(), 41);
        assert_eq!(state.visualize().selection_mode, SelectionMode::Cell);
        assert_eq!(state.visualize().selected_column, 1);
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
