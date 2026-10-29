use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::DefaultTerminal;
use std::io;

use crate::file::parquet_ctx::ParquetCtx;
use crate::tabs::TabManager;

fn visible_data_rows(terminal_height: u16) -> usize {
    // Remove the tab header (3 lines) and footer (1 line), then let the table
    // account for its own header so navigation matches the rendered rows.
    usize::from(crate::components::DataTable::visible_rows(
        terminal_height.saturating_sub(4),
    ))
    .max(1)
}

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

pub struct AppState {
    horizontal_offset: usize,
    vertical_offset: usize,
    tree_scroll_offset: usize,
    data_vertical_scroll: usize,
    visible_data_rows: usize,
    // Upper bound for `horizontal_offset`, recomputed each frame from the
    // on-screen column count. Prevents scrolling past the last visible column.
    max_horizontal_offset: usize,
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
            max_horizontal_offset: usize::MAX,
        }
    }

    pub fn reset(&mut self) {
        self.horizontal_offset = 0;
        self.vertical_offset = 0;
        self.tree_scroll_offset = 0;
        self.data_vertical_scroll = 0;
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

    pub fn right(&mut self) {
        self.horizontal_offset = (self.horizontal_offset + 1).min(self.max_horizontal_offset);
    }

    pub fn left(&mut self) {
        self.horizontal_offset = self.horizontal_offset.saturating_sub(1);
    }

    /// Set the upper bound for horizontal scrolling and clamp the current offset
    /// to it (handles overshoot from a previous frame and terminal resizes).
    pub fn set_max_horizontal_offset(&mut self, max: usize) {
        self.max_horizontal_offset = max;
        self.horizontal_offset = self.horizontal_offset.min(max);
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

    /// Jump the selection (and viewport) to the first row.
    pub fn jump_to_top(&mut self) {
        self.vertical_offset = 0;
        self.data_vertical_scroll = 0;
    }

    /// Jump the selection (and viewport) to the last row.
    pub fn jump_to_bottom(&mut self, visible_rows: usize, max_rows: usize) {
        self.vertical_offset = max_rows.saturating_sub(1);
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
            file_info.metadata.total_property_display_lines(),
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
            let visible_data_rows = visible_data_rows(terminal_size.height);
            self.state.set_visible_data_rows(visible_data_rows);

            // Lazily (re)load the row window backing the Visualize table so only a
            // bounded slice of a large file is ever held in memory.
            if self.tabs.active_tab().to_string() == "Visualize" {
                self.parquet_ctx
                    .sample_data
                    .ensure_loaded(self.state.data_vertical_scroll(), visible_data_rows);
            }

            // Bound visualize horizontal scrolling to what fits.
            // terminal minus the tab header (3 lines) and footer (1 line).
            let max_horizontal_offset = if self.tabs.active_tab().to_string() == "Visualize" {
                crate::components::DataTable::new(&self.parquet_ctx.sample_data)
                    .with_vertical_scroll(self.state.data_vertical_scroll())
                    .max_horizontal_scroll(
                        terminal_size.width,
                        terminal_size.height.saturating_sub(4),
                    )
            } else {
                usize::MAX
            };
            self.state.set_max_horizontal_offset(max_horizontal_offset);

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::schema::{SchemaInfo, tests::scroll_test_schema};
    use crossterm::event::KeyModifiers;
    use ratatui::{
        Terminal,
        backend::TestBackend,
        style::{Color, Modifier},
    };

    fn assert_last_data_line_is_selected(app: &App<'_>, terminal_height: u16, row_number: usize) {
        let mut terminal = Terminal::new(TestBackend::new(80, terminal_height)).unwrap();
        let view = AppRenderView::from_app(app);
        terminal
            .draw(|frame| crate::ui::render_app(&view, frame))
            .unwrap();

        // The last data line is immediately above the one-line footer.
        let row_number_cell = &terminal.backend().buffer()[(0, terminal_height - 2)];
        assert_eq!(row_number_cell.symbol(), row_number.to_string());
        assert!(
            row_number_cell.modifier.contains(Modifier::UNDERLINED),
            "last data line should be selected at terminal height {terminal_height}"
        );
    }

    #[test]
    fn test_visualize_selection_reaches_last_screen_line() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        assert!(ctx.sample_data_error.is_none());

        for terminal_height in [8, 10, 13] {
            let mut app = App::new(&ctx);
            app.state
                .set_visible_data_rows(visible_data_rows(terminal_height));
            // Three tab-header lines, two table-header lines, one footer line.
            let rows_on_screen = (terminal_height - 6) as usize;
            let down = KeyEvent::from(KeyCode::Down);

            for _ in 1..rows_on_screen {
                app.handle_key_event(down);
            }
            assert_last_data_line_is_selected(&app, terminal_height, rows_on_screen);
            assert_eq!(app.state.data_vertical_scroll(), 0);

            app.handle_key_event(down);
            assert_last_data_line_is_selected(&app, terminal_height, rows_on_screen + 1);
            assert_eq!(app.state.data_vertical_scroll(), 1);

            // Arrow, page and jump navigation must all reach the final file row.
            for key in [
                KeyCode::Down,
                KeyCode::PageDown,
                KeyCode::Char('d'),
                KeyCode::End,
                KeyCode::Char('G'),
            ] {
                app.state.reset();
                for _ in 0..ctx.sample_data.total_rows {
                    app.handle_key_event(KeyEvent::from(key));
                }
                assert_eq!(app.state.vertical_offset(), ctx.sample_data.total_rows - 1);
                assert_last_data_line_is_selected(
                    &app,
                    terminal_height,
                    ctx.sample_data.total_rows,
                );
                assert_eq!(
                    app.state.data_vertical_scroll(),
                    ctx.sample_data.total_rows - rows_on_screen
                );
            }
        }
    }

    fn assert_schema_highlights_align(nested: bool) {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let mut context = ParquetCtx::from_file(&path).unwrap();
        context.schema = scroll_test_schema(nested);
        let names = context.schema.primitive_column_names();

        for height in [8, 12, 24] {
            for horizontal_scroll in [0, 7, 9] {
                let mut app = App::new(&context);
                app.tabs.active_tab = 2;
                for _ in 0..horizontal_scroll {
                    app.state.right();
                }
                let table_x = context.schema.tree_width() as u16 + 3;
                // Leave room for one statistics column so every horizontal offset applies.
                let width = table_x + 15;
                let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();

                for key in [KeyCode::Down, KeyCode::Up] {
                    for _ in 0..names.len() {
                        app.handle_key_event(KeyEvent::new(key, KeyModifiers::NONE));
                        terminal
                            .draw(|frame| {
                                crate::ui::render_app(&AppRenderView::from_app(&app), frame)
                            })
                            .unwrap();
                        let buffer = terminal.backend().buffer();
                        let highlighted_rows = |x| {
                            (0..height)
                                .filter(|&y| buffer[(x, y)].bg == Color::Yellow)
                                .collect::<Vec<_>>()
                        };
                        let tree_rows = highlighted_rows(1);
                        let table_rows = highlighted_rows(table_x);
                        let selected = app.state.vertical_offset();
                        assert_eq!(
                            tree_rows, table_rows,
                            "nested {nested}, height {height}, horizontal {horizontal_scroll}, selection {selected}"
                        );
                        if selected == 0 {
                            assert!(tree_rows.is_empty());
                            continue;
                        }
                        assert_eq!(tree_rows.len(), 1, "selection must stay visible");
                        let y = tree_rows[0];
                        let tree_text: String =
                            (1..table_x - 2).map(|x| buffer[(x, y)].symbol()).collect();
                        assert!(tree_text.contains(&names[selected - 1]));

                        let table_text: String = (table_x..width - 1)
                            .map(|x| buffer[(x, y)].symbol())
                            .collect();
                        let expected = match horizontal_scroll {
                            0 => context
                                .schema
                                .columns
                                .iter()
                                .filter_map(|column| match column {
                                    SchemaInfo::Primitive { info, .. } => {
                                        Some(info.repetition.as_str())
                                    }
                                    _ => None,
                                })
                                .nth(selected - 1)
                                .unwrap(),
                            7 => &names[selected - 1],
                            9 => "0",
                            _ => unreachable!(),
                        };
                        assert!(table_text.starts_with(expected), "{table_text:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn test_schema_highlights_align_when_scrolling_nested_groups() {
        assert_schema_highlights_align(true);
    }

    #[test]
    fn test_schema_highlights_align_when_scrolling_flat_schema() {
        assert_schema_highlights_align(false);
    }

    #[test]
    fn test_right_is_clamped_to_max_horizontal_offset() {
        let mut state = AppState::new();
        state.set_max_horizontal_offset(3);
        for _ in 0..10 {
            state.right();
        }
        // Without the clamp the offset would run past the visible range,
        // producing the "empty clicks" seen when scrolling back.
        assert_eq!(state.horizontal_offset(), 3);
    }

    #[test]
    fn test_left_saturates_at_zero() {
        let mut state = AppState::new();
        state.set_max_horizontal_offset(5);
        state.right();
        state.right();
        state.left();
        state.left();
        state.left();
        assert_eq!(state.horizontal_offset(), 0);
    }

    #[test]
    fn test_jump_to_bottom_then_top() {
        let mut state = AppState::new();

        state.jump_to_bottom(20, 7300);
        assert_eq!(state.vertical_offset(), 7299);
        assert_eq!(state.data_vertical_scroll(), 7300 - 20);

        state.jump_to_top();
        assert_eq!(state.vertical_offset(), 0);
        assert_eq!(state.data_vertical_scroll(), 0);
    }

    #[test]
    fn test_page_navigation_moves_by_visible_rows_and_clamps() {
        let mut state = AppState::new();

        state.page_down(5, 12);
        assert_eq!(state.vertical_offset(), 5);
        assert_eq!(state.data_vertical_scroll(), 1);

        state.page_down(5, 12);
        assert_eq!(state.vertical_offset(), 10);
        assert_eq!(state.data_vertical_scroll(), 6);

        state.page_down(5, 12);
        assert_eq!(state.vertical_offset(), 11);
        assert_eq!(state.data_vertical_scroll(), 7);

        state.page_up(5, 12);
        assert_eq!(state.vertical_offset(), 6);
        assert_eq!(state.data_vertical_scroll(), 6);
    }

    #[test]
    fn test_page_up_clamps_at_first_row() {
        let mut state = AppState::new();
        state.jump_to_bottom(5, 12);

        for (selected, scroll) in [(6, 6), (1, 1), (0, 0), (0, 0)] {
            state.page_up(5, 12);
            assert_eq!(state.vertical_offset(), selected);
            assert_eq!(state.data_vertical_scroll(), scroll);
        }
    }

    #[test]
    fn test_page_navigation_with_fewer_rows_than_a_page() {
        for max_rows in [0, 1, 4, 5] {
            let mut state = AppState::new();

            for _ in 0..2 {
                state.page_down(5, max_rows);
                assert_eq!(state.vertical_offset(), max_rows.saturating_sub(1));
                assert_eq!(state.data_vertical_scroll(), 0);
            }

            for _ in 0..2 {
                state.page_up(5, max_rows);
                assert_eq!(state.vertical_offset(), 0);
                assert_eq!(state.data_vertical_scroll(), 0);
            }
        }
    }

    #[test]
    fn test_page_navigation_with_one_visible_row() {
        let mut state = AppState::new();

        for selected in [1, 2, 2] {
            state.page_down(1, 3);
            assert_eq!(state.vertical_offset(), selected);
            assert_eq!(state.data_vertical_scroll(), selected);
        }

        for selected in [1, 0, 0] {
            state.page_up(1, 3);
            assert_eq!(state.vertical_offset(), selected);
            assert_eq!(state.data_vertical_scroll(), selected);
        }
    }

    #[test]
    fn test_page_navigation_keeps_selection_visible_after_resize() {
        let mut state = AppState::new();
        state.jump_to_bottom(5, 30);

        state.page_up(10, 30);
        assert_eq!(state.vertical_offset(), 19);
        assert_eq!(state.data_vertical_scroll(), 19);

        state.page_down(3, 30);
        assert_eq!(state.vertical_offset(), 22);
        assert_eq!(state.data_vertical_scroll(), 20);
    }

    #[test]
    fn test_shrinking_the_max_clamps_the_current_offset() {
        let mut state = AppState::new();
        state.set_max_horizontal_offset(10);
        for _ in 0..10 {
            state.right();
        }
        assert_eq!(state.horizontal_offset(), 10);
        // e.g. the terminal was widened, so fewer columns need scrolling.
        state.set_max_horizontal_offset(4);
        assert_eq!(state.horizontal_offset(), 4);
    }
}
