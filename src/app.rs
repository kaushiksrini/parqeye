use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::DefaultTerminal;
use std::io;

use crate::file::parquet_ctx::ParquetCtx;
use crate::search::get_filtered_primitive_indices;
use crate::tabs::TabManager;

pub struct AppRenderView<'a> {
    pub title: &'a str,
    pub parquet_ctx: &'a ParquetCtx,
    file_name: &'a str,
    tabs: &'a TabManager,
    pub state: &'a AppState,
}

impl<'a> AppRenderView<'a> {
    fn from_app(app: &'a App) -> Self {
        Self {
            title: "parqeye",
            parquet_ctx: app.parquet_ctx,
            file_name: &app.file_name,
            tabs: &app.tabs,
            state: &app.state,
        }
    }

    pub fn tabs(&self) -> &TabManager {
        self.tabs
    }

    pub fn file_name(&self) -> &str {
        self.file_name
    }

    pub fn state(&self) -> &AppState {
        self.state
    }
}

pub struct App<'a> {
    pub parquet_ctx: &'a ParquetCtx,
    pub file_name: String,
    pub exit: bool,
    pub tabs: TabManager,
    pub state: AppState,
}

#[derive(Debug, Default, Clone)]
pub struct SearchState {
    pub active: bool,
    pub query: String,
    pub cursor_pos: usize,
    pub confirmed: bool,
}

impl SearchState {
    pub fn activate(&mut self) {
        self.active = true;
        self.confirmed = false;
    }

    pub fn deactivate(&mut self) {
        self.active = false;
        self.confirmed = false;
        self.query.clear();
        self.cursor_pos = 0;
    }

    pub fn confirm(&mut self) {
        self.active = false;
        self.confirmed = true;
    }

    pub fn push_char(&mut self, c: char) {
        self.query.insert(self.cursor_pos, c);
        self.cursor_pos += c.len_utf8();
    }

    pub fn backspace(&mut self) {
        if self.cursor_pos > 0 {
            let prev_char_boundary = self.query[..self.cursor_pos]
                .char_indices()
                .next_back()
                .map(|(idx, _)| idx)
                .unwrap_or(0);
            self.query.remove(prev_char_boundary);
            self.cursor_pos = prev_char_boundary;
        }
    }

    pub fn move_cursor_left(&mut self) {
        if self.cursor_pos > 0 {
            self.cursor_pos = self.query[..self.cursor_pos]
                .char_indices()
                .next_back()
                .map(|(idx, _)| idx)
                .unwrap_or(0);
        }
    }

    pub fn move_cursor_right(&mut self) {
        if self.cursor_pos < self.query.len() {
            self.cursor_pos = self.query[self.cursor_pos..]
                .char_indices()
                .nth(1)
                .map(|(idx, _)| self.cursor_pos + idx)
                .unwrap_or(self.query.len());
        }
    }
}

pub struct AppState {
    horizontal_offset: usize,
    vertical_offset: usize,
    tree_scroll_offset: usize,
    data_vertical_scroll: usize,
    visible_data_rows: usize,
    pub search: SearchState,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    pub fn new() -> Self {
        Self {
            horizontal_offset: 0,
            vertical_offset: 0,
            tree_scroll_offset: 0,
            data_vertical_scroll: 0,
            visible_data_rows: 20, // Default fallback
            search: SearchState::default(),
        }
    }

    pub fn reset(&mut self) {
        self.horizontal_offset = 0;
        self.vertical_offset = 0;
        self.tree_scroll_offset = 0;
        self.data_vertical_scroll = 0;
        self.search.deactivate();
    }

    pub fn horizontal_offset(&self) -> usize {
        self.horizontal_offset
    }

    pub fn vertical_offset(&self) -> usize {
        self.vertical_offset
    }

    pub fn down(&mut self) {
        self.vertical_offset += 1;
    }

    pub fn up(&mut self) {
        self.vertical_offset = self.vertical_offset.saturating_sub(1);
    }

    /// Navigate down through filtered primitive indices only.
    /// filtered_indices should be 1-based primitive column indices.
    pub fn down_filtered(&mut self, filtered_indices: &[usize]) {
        if filtered_indices.is_empty() {
            return;
        }
        let current = self.vertical_offset;
        // Find the next index in filtered list that is > current
        if let Some(&next) = filtered_indices.iter().find(|&&idx| idx > current) {
            self.vertical_offset = next;
        }
    }

    /// Navigate up through filtered primitive indices only.
    /// filtered_indices should be 1-based primitive column indices.
    pub fn up_filtered(&mut self, filtered_indices: &[usize]) {
        if filtered_indices.is_empty() {
            return;
        }
        let current = self.vertical_offset;
        if current == 0 {
            return;
        }
        // Find the previous index in filtered list that is < current
        if let Some(&prev) = filtered_indices.iter().rev().find(|&&idx| idx < current) {
            self.vertical_offset = prev;
        } else {
            // No previous filtered item, go to 0 (deselect)
            self.vertical_offset = 0;
        }
    }

    pub fn right(&mut self) {
        self.horizontal_offset += 1;
    }

    pub fn left(&mut self) {
        self.horizontal_offset = self.horizontal_offset.saturating_sub(1);
    }

    pub fn tree_scroll_offset(&self) -> usize {
        self.tree_scroll_offset
    }

    pub fn tree_scroll_up(&mut self) {
        self.tree_scroll_offset = self.tree_scroll_offset.saturating_sub(1);
    }

    pub fn tree_scroll_down(&mut self) {
        self.tree_scroll_offset += 1;
    }

    pub fn data_vertical_scroll(&self) -> usize {
        self.data_vertical_scroll
    }

    pub fn set_data_vertical_scroll(&mut self, scroll: usize) {
        self.data_vertical_scroll = scroll;
    }

    pub fn visible_data_rows(&self) -> usize {
        self.visible_data_rows
    }

    pub fn set_visible_data_rows(&mut self, rows: usize) {
        self.visible_data_rows = rows;
    }

    pub fn page_up(&mut self, visible_rows: usize, max_rows: usize) {
        // Move selection up by visible_rows
        self.vertical_offset = self.vertical_offset.saturating_sub(visible_rows);
        // Adjust scroll to keep selection visible
        self.adjust_scroll_to_selection(visible_rows, max_rows);
    }

    pub fn page_down(&mut self, visible_rows: usize, max_rows: usize) {
        // Move selection down by visible_rows, clamped to max_rows - 1
        self.vertical_offset =
            (self.vertical_offset + visible_rows).min(max_rows.saturating_sub(1));
        // Adjust scroll to keep selection visible
        self.adjust_scroll_to_selection(visible_rows, max_rows);
    }

    pub fn adjust_scroll_to_selection(&mut self, visible_rows: usize, max_rows: usize) {
        // Ensure selected row is visible in viewport
        if self.vertical_offset < self.data_vertical_scroll {
            // Selection is above viewport, scroll up
            self.data_vertical_scroll = self.vertical_offset;
        } else if self.vertical_offset >= self.data_vertical_scroll + visible_rows {
            // Selection is below viewport, scroll down
            self.data_vertical_scroll = self.vertical_offset.saturating_sub(visible_rows - 1);
        }

        // Clamp scroll to valid range
        let max_scroll = max_rows.saturating_sub(visible_rows);
        self.data_vertical_scroll = self.data_vertical_scroll.min(max_scroll);
    }
}

impl<'a> App<'a> {
    pub fn new(file_info: &'a ParquetCtx) -> Self {
        let sample_data_rows = file_info.sample_data.total_rows;

        let tab_manager = TabManager::new(
            file_info.schema.column_size(),
            file_info.row_groups.num_row_groups(),
            sample_data_rows,
        );

        Self {
            parquet_ctx: file_info,
            file_name: file_info.file_path.clone(),
            exit: false,
            tabs: tab_manager,
            state: AppState::new(),
        }
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        while !self.exit {
            // Calculate visible data rows based on terminal size
            let terminal_size = terminal.size()?;
            // Account for: header (3 lines), footer (1 line), table header (3 lines) = 7 lines total
            let visible_data_rows = (terminal_size.height.saturating_sub(7) as usize).max(1);
            self.state.set_visible_data_rows(visible_data_rows);

            let render_view = AppRenderView::from_app(self);
            terminal.draw(|frame| crate::ui::render_app(&render_view, frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn handle_events(&mut self) -> io::Result<()> {
        match event::read()? {
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                self.handle_key_event(key_event)
            }
            _ => {}
        };
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        // Handle search mode input first
        if self.state.search.active {
            match key_event.code {
                KeyCode::Esc => self.state.search.deactivate(),
                KeyCode::Enter => self.state.search.confirm(),
                KeyCode::Backspace => self.state.search.backspace(),
                KeyCode::Left => self.state.search.move_cursor_left(),
                KeyCode::Right => self.state.search.move_cursor_right(),
                KeyCode::Char(c) => self.state.search.push_char(c),
                KeyCode::Up | KeyCode::Down => {
                    // Use filtered navigation for Schema and Row Groups tabs
                    self.handle_filtered_navigation(key_event);
                }
                _ => {}
            }
            return;
        }

        match key_event.code {
            KeyCode::Char('q') | KeyCode::Char('Q') => self.exit(),
            KeyCode::Esc => self.state.reset(),
            KeyCode::Tab => {
                self.tabs.next();
                self.state.reset();
            }
            KeyCode::BackTab => {
                self.tabs.prev();
                self.state.reset();
            }
            KeyCode::Up | KeyCode::Down
                if self.state.search.confirmed && !self.state.search.query.is_empty() =>
            {
                // Use filtered navigation when search is confirmed
                self.handle_filtered_navigation(key_event);
            }
            _ => {
                self.tabs
                    .active_tab()
                    .on_event(key_event, &mut self.state)
                    .unwrap();
            }
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }

    /// Handle Up/Down navigation with filtering for Schema and Row Groups tabs
    fn handle_filtered_navigation(&mut self, key_event: KeyEvent) {
        let active_tab = self.tabs.active_tab().to_string();

        // Only apply filtered navigation for Schema and Row Groups tabs
        if active_tab == "Schema" || active_tab == "Row Groups" {
            let filtered_indices = get_filtered_primitive_indices(
                &self.parquet_ctx.schema.columns,
                &self.state.search.query,
            );

            match key_event.code {
                KeyCode::Down => self.state.down_filtered(&filtered_indices),
                KeyCode::Up => self.state.up_filtered(&filtered_indices),
                _ => {}
            }
        } else {
            // For other tabs, use regular navigation
            self.tabs
                .active_tab()
                .on_event(key_event, &mut self.state)
                .unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for AppState filtered navigation

    #[test]
    fn test_down_filtered_from_zero() {
        let mut state = AppState::new();
        assert_eq!(state.vertical_offset(), 0);

        // Navigate down with filtered indices [1, 3, 5]
        let filtered = vec![1, 3, 5];
        state.down_filtered(&filtered);

        // Should move to first filtered index
        assert_eq!(state.vertical_offset(), 1);
    }

    #[test]
    fn test_down_filtered_skips_non_filtered() {
        let mut state = AppState::new();
        state.vertical_offset = 1;

        // Filtered indices are [1, 3, 5] - index 2 is not in the list
        let filtered = vec![1, 3, 5];
        state.down_filtered(&filtered);

        // Should skip 2 and go to 3
        assert_eq!(state.vertical_offset(), 3);
    }

    #[test]
    fn test_down_filtered_at_last() {
        let mut state = AppState::new();
        state.vertical_offset = 5;

        // At last filtered index, should not change
        let filtered = vec![1, 3, 5];
        state.down_filtered(&filtered);

        assert_eq!(state.vertical_offset(), 5);
    }

    #[test]
    fn test_down_filtered_empty_list() {
        let mut state = AppState::new();
        state.vertical_offset = 2;

        // Empty filtered list - should not change
        let filtered: Vec<usize> = vec![];
        state.down_filtered(&filtered);

        assert_eq!(state.vertical_offset(), 2);
    }

    #[test]
    fn test_up_filtered_to_zero() {
        let mut state = AppState::new();
        state.vertical_offset = 1;

        // At first filtered index, up should go to 0
        let filtered = vec![1, 3, 5];
        state.up_filtered(&filtered);

        assert_eq!(state.vertical_offset(), 0);
    }

    #[test]
    fn test_up_filtered_skips_non_filtered() {
        let mut state = AppState::new();
        state.vertical_offset = 5;

        // Filtered indices are [1, 3, 5] - should skip 4, 2 and go to 3
        let filtered = vec![1, 3, 5];
        state.up_filtered(&filtered);

        assert_eq!(state.vertical_offset(), 3);
    }

    #[test]
    fn test_up_filtered_from_middle() {
        let mut state = AppState::new();
        state.vertical_offset = 3;

        let filtered = vec![1, 3, 5];
        state.up_filtered(&filtered);

        assert_eq!(state.vertical_offset(), 1);
    }

    #[test]
    fn test_up_filtered_at_zero() {
        let mut state = AppState::new();
        assert_eq!(state.vertical_offset(), 0);

        // At 0, up should stay at 0
        let filtered = vec![1, 3, 5];
        state.up_filtered(&filtered);

        assert_eq!(state.vertical_offset(), 0);
    }

    #[test]
    fn test_up_filtered_empty_list() {
        let mut state = AppState::new();
        state.vertical_offset = 3;

        // Empty filtered list - should not change
        let filtered: Vec<usize> = vec![];
        state.up_filtered(&filtered);

        assert_eq!(state.vertical_offset(), 3);
    }

    #[test]
    fn test_filtered_navigation_round_trip() {
        let mut state = AppState::new();
        let filtered = vec![2, 5, 8];

        // Start at 0, navigate down through all filtered indices
        state.down_filtered(&filtered);
        assert_eq!(state.vertical_offset(), 2);

        state.down_filtered(&filtered);
        assert_eq!(state.vertical_offset(), 5);

        state.down_filtered(&filtered);
        assert_eq!(state.vertical_offset(), 8);

        // At last, down should not change
        state.down_filtered(&filtered);
        assert_eq!(state.vertical_offset(), 8);

        // Navigate back up
        state.up_filtered(&filtered);
        assert_eq!(state.vertical_offset(), 5);

        state.up_filtered(&filtered);
        assert_eq!(state.vertical_offset(), 2);

        state.up_filtered(&filtered);
        assert_eq!(state.vertical_offset(), 0);
    }

    #[test]
    fn test_filtered_navigation_single_item() {
        let mut state = AppState::new();
        let filtered = vec![3];

        // Navigate to the single item
        state.down_filtered(&filtered);
        assert_eq!(state.vertical_offset(), 3);

        // Can't go further down
        state.down_filtered(&filtered);
        assert_eq!(state.vertical_offset(), 3);

        // Go back to 0
        state.up_filtered(&filtered);
        assert_eq!(state.vertical_offset(), 0);
    }

    // Tests for SearchState

    #[test]
    fn test_search_state_activate_deactivate() {
        let mut search = SearchState::default();
        assert!(!search.active);
        assert!(!search.confirmed);

        search.activate();
        assert!(search.active);
        assert!(!search.confirmed);

        search.push_char('t');
        search.push_char('e');
        search.push_char('s');
        search.push_char('t');
        assert_eq!(search.query, "test");

        search.deactivate();
        assert!(!search.active);
        assert!(!search.confirmed);
        assert!(search.query.is_empty());
    }

    #[test]
    fn test_search_state_confirm() {
        let mut search = SearchState::default();
        search.activate();
        search.push_char('a');
        search.push_char('b');

        search.confirm();
        assert!(!search.active);
        assert!(search.confirmed);
        assert_eq!(search.query, "ab"); // Query preserved after confirm
    }

    #[test]
    fn test_search_state_cursor_movement() {
        let mut search = SearchState::default();
        search.activate();
        search.push_char('h');
        search.push_char('e');
        search.push_char('l');
        search.push_char('l');
        search.push_char('o');

        assert_eq!(search.cursor_pos, 5);

        search.move_cursor_left();
        assert_eq!(search.cursor_pos, 4);

        search.move_cursor_left();
        search.move_cursor_left();
        assert_eq!(search.cursor_pos, 2);

        search.move_cursor_right();
        assert_eq!(search.cursor_pos, 3);

        // Move to start
        search.move_cursor_left();
        search.move_cursor_left();
        search.move_cursor_left();
        assert_eq!(search.cursor_pos, 0);

        // Can't go past start
        search.move_cursor_left();
        assert_eq!(search.cursor_pos, 0);
    }

    #[test]
    fn test_search_state_backspace() {
        let mut search = SearchState::default();
        search.activate();
        search.push_char('a');
        search.push_char('b');
        search.push_char('c');

        assert_eq!(search.query, "abc");
        assert_eq!(search.cursor_pos, 3);

        search.backspace();
        assert_eq!(search.query, "ab");
        assert_eq!(search.cursor_pos, 2);

        // Move cursor to middle and backspace
        search.move_cursor_left();
        search.backspace();
        assert_eq!(search.query, "b");
        assert_eq!(search.cursor_pos, 0);

        // Backspace at start should do nothing
        search.backspace();
        assert_eq!(search.query, "b");
        assert_eq!(search.cursor_pos, 0);
    }
}
