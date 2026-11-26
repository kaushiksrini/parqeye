use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{DefaultTerminal, layout::Size};
use std::io;
use std::time::{Duration, Instant};

use crate::components::help::{HelpModal, HelpState};
use crate::file::parquet_ctx::ParquetCtx;
use crate::tabs::visualize::search::{SearchRequest, SearchWorker};
use crate::tabs::visualize::{SelectionMode, VisualizeState};
use crate::tabs::{EventOutcome, TabManager};

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
    search_worker: SearchWorker,
    search_started_at: Instant,
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
    visualize: VisualizeState,
    toast: Option<Toast>,
    help: Option<HelpState>,
}

pub struct Toast {
    pub message: String,
    pub is_error: bool,
    expires_at: Instant,
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
            visualize: VisualizeState::default(),
            toast: None,
            help: None,
        }
    }

    pub fn reset(&mut self) {
        self.horizontal_offset = 0;
        self.vertical_offset = 0;
        self.tree_scroll_offset = 0;
        self.data_vertical_scroll = 0;
        self.visualize.reset_navigation();
        self.toast = None;
        self.help = None;
    }

    pub fn help(&self) -> Option<&HelpState> {
        self.help.as_ref()
    }

    pub fn show_toast(&mut self, message: impl Into<String>, is_error: bool) {
        self.toast = Some(Toast {
            message: message.into(),
            is_error,
            expires_at: Instant::now() + Duration::from_secs(2),
        });
    }

    pub fn toast(&self) -> Option<&Toast> {
        self.toast
            .as_ref()
            .filter(|toast| toast.expires_at > Instant::now())
    }

    pub fn horizontal_offset(&self) -> usize {
        self.horizontal_offset
    }

    pub fn set_horizontal_offset(&mut self, offset: usize) {
        self.horizontal_offset = offset.min(self.max_horizontal_offset);
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

    pub fn visualize(&self) -> &VisualizeState {
        &self.visualize
    }

    pub fn visualize_mut(&mut self) -> &mut VisualizeState {
        &mut self.visualize
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

    /// Jump to an absolute, zero-based row and keep it visible.
    pub fn jump_to_row(&mut self, row: usize, visible_rows: usize, max_rows: usize) {
        self.vertical_offset = row.min(max_rows.saturating_sub(1));
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
            &file_info.sample_data.flattened_columns,
        );

        Self {
            parquet_ctx: file_info,
            file_name: file_info.file_path.clone(),
            exit: false,
            tabs: tab_manager,
            state: AppState::new(),
            search_worker: SearchWorker::default(),
            search_started_at: Instant::now(),
        }
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        while !self.exit {
            self.poll_search();
            self.prepare_viewport(terminal.size()?);

            let render_view = AppRenderView::from_app(self);
            terminal.draw(|frame| crate::ui::render_app(&render_view, frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn prepare_viewport(&mut self, terminal_size: Size) {
        if let Some(help) = &mut self.state.help {
            let area = ratatui::layout::Rect::new(0, 0, terminal_size.width, terminal_size.height);
            (help.visible_rows, help.max_scroll) =
                HelpModal::new(self.tabs.active_tab().as_ref(), help.scroll).scroll_bounds(area);
            help.scroll = help.scroll.min(help.max_scroll);
        }
        let is_visualize = self.tabs.active_tab().to_string() == "Visualize";
        let visible_rows = visible_data_rows(terminal_size.height);
        self.state.set_visible_data_rows(visible_rows);

        if is_visualize {
            let data = &self.parquet_ctx.sample_data;
            if self.state.visualize().selection_mode == SelectionMode::Column {
                self.state.data_vertical_scroll = self
                    .state
                    .data_vertical_scroll
                    .min(data.total_rows.saturating_sub(visible_rows));
            } else {
                self.state
                    .adjust_scroll_to_selection(visible_rows, data.total_rows);
            }
            data.ensure_loaded(self.state.data_vertical_scroll(), visible_rows);

            let table = crate::components::DataTable::new(data)
                .with_width_cache(&self.state.visualize().column_widths)
                .with_projection(self.state.visualize().projection())
                .with_vertical_scroll(self.state.data_vertical_scroll())
                .with_horizontal_scroll(self.state.horizontal_offset());
            let area_height = terminal_size.height.saturating_sub(4);
            let max_scroll = table.max_horizontal_scroll(terminal_size.width, area_height);
            let selected_column = self.state.visualize().selected_column.min(
                self.state
                    .visualize()
                    .column_count(data.total_columns)
                    .saturating_sub(1),
            );
            let horizontal_scroll = if self.state.visualize().selection_mode != SelectionMode::Row {
                table.horizontal_scroll_for_selection(
                    terminal_size.width,
                    area_height,
                    selected_column,
                )
            } else {
                self.state.horizontal_offset().min(max_scroll)
            };
            self.state.set_max_horizontal_offset(max_scroll);
            self.state.visualize_mut().selected_column = selected_column;
            self.state.set_horizontal_offset(horizontal_scroll);
        } else {
            self.state.set_max_horizontal_offset(usize::MAX);
        }
    }

    fn handle_events(&mut self) -> io::Result<()> {
        // Poll only while searching or waiting for a toast to expire. Idle reads
        // still block, so adding background search does not introduce busy work.
        let timeout = self
            .state
            .toast
            .as_ref()
            .map(|toast| toast.expires_at.saturating_duration_since(Instant::now()));
        let timeout = if self.search_worker.is_running() {
            Some(timeout.map_or(Duration::from_millis(50), |timeout| {
                timeout.min(Duration::from_millis(50))
            }))
        } else {
            timeout
        };
        if let Some(timeout) = timeout
            && !event::poll(timeout)?
        {
            if self.state.toast().is_none() {
                self.state.toast = None;
            }
            return Ok(());
        }
        match event::read()? {
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                self.handle_key_event(key_event)
            }
            _ => {}
        };
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        self.handle_key_event_with_clipboard(key_event, &mut io::stdout().lock());
    }

    fn handle_key_event_with_clipboard(
        &mut self,
        key_event: KeyEvent,
        clipboard: &mut impl io::Write,
    ) {
        if self.state.help.is_some() {
            self.handle_help_key(key_event);
            return;
        }
        // Prompts own printable characters, including regex '?' and literal 'q'.
        // Opening help otherwise leaves the tab's selection and search intact.
        if key_event.code == KeyCode::Char('?') && self.state.visualize().prompt().is_none() {
            self.state.help = Some(HelpState::default());
            return;
        }
        self.state.toast = None;
        let repeating_find = self.state.visualize().prompt().is_none()
            && matches!(key_event.code, KeyCode::Char('n' | 'N'))
            && !key_event
                .modifiers
                .intersects(event::KeyModifiers::CONTROL | event::KeyModifiers::ALT);
        if repeating_find {
            // Apply a completed hit before starting the next step. Key repeat
            // during an unfinished scan must not continually restart that scan.
            self.poll_search();
            if self.search_worker.is_running() {
                return;
            }
        }
        // A fresh command or navigation must never be overwritten by a late
        // search result. The pattern/highlights survive cancellation.
        if self.search_worker.is_running() {
            self.search_worker.cancel();
            self.state.visualize_mut().search_status = Some("Search cancelled".into());
            if key_event.code == KeyCode::Esc {
                return;
            }
        }
        let outcome = self
            .tabs
            .active_tab()
            .on_event(key_event, &mut self.state)
            .unwrap();
        match outcome {
            EventOutcome::Consumed => return,
            EventOutcome::FindCell {
                backwards,
                include_current,
            } => {
                self.start_search(backwards, include_current);
                return;
            }
            EventOutcome::CopyCell => {
                let result = self.parquet_ctx.sample_data.clipboard_value(
                    self.state.vertical_offset(),
                    self.state
                        .visualize()
                        .source_column(self.state.visualize().selected_column)
                        .unwrap_or(self.parquet_ctx.sample_data.total_columns),
                );
                match result {
                    Ok(value) => match crate::clipboard::copy(&value, clipboard) {
                        Ok(()) => self
                            .state
                            .show_toast(format!("Copied {} chars", value.chars().count()), false),
                        Err(error) => self.state.show_toast(format!("Copy failed: {error}"), true),
                    },
                    Err(_) => self.state.show_toast("Unable to read selected cell", true),
                }
                return;
            }
            EventOutcome::Bubble => {}
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
            _ => {}
        }
    }

    fn handle_help_key(&mut self, key_event: KeyEvent) {
        let help = self.state.help.as_mut().expect("help must be open");
        match key_event.code {
            KeyCode::Char('?') => self.state.help = None,
            KeyCode::Char('q' | 'Q') => self.exit(),
            KeyCode::Up => help.scroll = help.scroll.saturating_sub(1),
            KeyCode::Down => help.scroll = help.scroll.saturating_add(1).min(help.max_scroll),
            KeyCode::PageUp => help.scroll = help.scroll.saturating_sub(help.visible_rows.max(1)),
            KeyCode::PageDown => {
                help.scroll = help
                    .scroll
                    .saturating_add(help.visible_rows.max(1))
                    .min(help.max_scroll);
            }
            KeyCode::Home => help.scroll = 0,
            KeyCode::End => help.scroll = help.max_scroll,
            KeyCode::Tab | KeyCode::BackTab => {
                if key_event.code == KeyCode::Tab {
                    self.tabs.next();
                } else {
                    self.tabs.prev();
                }
                self.state.reset();
                self.state.help = Some(HelpState::default());
            }
            _ => {}
        }
    }

    fn exit(&mut self) {
        self.search_worker.cancel();
        self.exit = true;
    }

    fn start_search(&mut self, backwards: bool, include_current: bool) {
        let Some(pattern) = self.state.visualize().search.clone() else {
            return;
        };
        if self.parquet_ctx.sample_data_error.is_some() {
            self.state.visualize_mut().search_status =
                Some("Search unavailable: preview could not be read".into());
            return;
        }
        let data = &self.parquet_ctx.sample_data;
        let visualize = self.state.visualize();
        let columns = (0..visualize.column_count(data.total_columns))
            .filter_map(|index| visualize.source_column(index))
            .filter_map(|index| data.flattened_columns.get(index).cloned())
            .collect();
        let request = SearchRequest {
            path: self.parquet_ctx.file_path.clone(),
            columns,
            total_rows: data.total_rows,
            pattern,
            row: self.state.vertical_offset(),
            column: visualize.selected_column,
            backwards,
            include_current,
        };
        match self.search_worker.start(request) {
            Ok(()) => {
                self.search_started_at = Instant::now();
                self.update_search_progress();
            }
            Err(error) => {
                self.state.visualize_mut().search_status = Some(format!("Search failed: {error}"))
            }
        }
    }

    fn poll_search(&mut self) {
        if let Some(result) = self.search_worker.poll() {
            let status = match result {
                Ok(Some(hit)) => {
                    self.state.jump_to_row(
                        hit.row,
                        self.state.visible_data_rows(),
                        self.parquet_ctx.sample_data.total_rows,
                    );
                    let visualize = self.state.visualize_mut();
                    visualize.selected_column = hit.column;
                    visualize.selection_mode = SelectionMode::Cell;
                    hit.wrapped.then(|| "Search wrapped".to_string())
                }
                Ok(None) => Some("No matches".into()),
                Err(error) => Some(format!("Search failed: {error}")),
            };
            self.state.visualize_mut().search_status = status;
        } else if self.search_worker.is_running() {
            self.update_search_progress();
        }
    }

    fn update_search_progress(&mut self) {
        const FRAMES: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        let frame =
            (self.search_started_at.elapsed().as_millis() / 100 % FRAMES.len() as u128) as usize;
        let spinner = FRAMES[frame];
        let total = self.parquet_ctx.sample_data.total_rows;
        let row = self.search_worker.current_row().min(total);
        self.state.visualize_mut().search_status =
            Some(format!("{spinner} Row {row}/{total} · Esc cancels"));
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

    fn set_search(app: &mut App<'_>, expression: &str) {
        app.handle_key_event(KeyCode::Char('/').into());
        let count = app
            .state
            .visualize()
            .prompt()
            .unwrap()
            .buffer
            .chars()
            .count();
        app.handle_key_event(KeyCode::Home.into());
        for _ in 0..count {
            app.handle_key_event(KeyCode::Delete.into());
        }
        for character in expression.chars() {
            app.handle_key_event(KeyCode::Char(character).into());
        }
        app.handle_key_event(KeyCode::Enter.into());
    }

    fn finish_search(app: &mut App<'_>) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while app.search_worker.is_running() {
            assert!(Instant::now() < deadline, "search did not finish");
            app.poll_search();
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn find_navigates_all_columns_then_respects_reordered_projection_and_keeps_rows() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let expected: Vec<_> =
            ctx.sample_data
                .loaded()
                .rows
                .iter()
                .enumerate()
                .flat_map(|(row, cells)| {
                    cells.iter().enumerate().filter_map(move |(column, value)| {
                        (value == "true").then_some((row, column))
                    })
                })
                .collect();
        assert!(expected.len() > 1);
        let mut app = App::new(&ctx);
        set_search(&mut app, "^true$");
        finish_search(&mut app);
        let cursor = |app: &App<'_>| {
            (
                app.state.vertical_offset(),
                app.state.visualize().selected_column,
            )
        };
        assert_eq!(cursor(&app), expected[0]);
        assert_eq!(app.state.visualize().selection_mode, SelectionMode::Cell);
        assert_eq!(app.state.visualize().projection(), None);
        app.handle_key_event(KeyCode::Char('n').into());
        finish_search(&mut app);
        assert_eq!(cursor(&app), expected[1]);
        app.handle_key_event(KeyCode::Char('N').into());
        finish_search(&mut app);
        assert_eq!(cursor(&app), expected[0]);
        app.handle_key_event(KeyCode::Char('N').into());
        finish_search(&mut app);
        assert_eq!(cursor(&app), *expected.last().unwrap());
        assert_eq!(
            app.state.visualize().search_status.as_deref(),
            Some("Search wrapped")
        );
        let mut clipboard = Vec::new();
        app.handle_key_event_with_clipboard(KeyCode::Char('y').into(), &mut clipboard);
        assert!(!clipboard.is_empty());

        let names = &ctx.sample_data.flattened_columns;
        set_column_filter(
            &mut app,
            &format!(
                "^{}$|^{}$",
                regex::escape(&names[expected[0].1]),
                regex::escape(&names[0])
            ),
        );
        app.state.jump_to_top();
        app.state.visualize_mut().selected_column = 0;
        set_search(&mut app, "^true$");
        finish_search(&mut app);
        assert_eq!(cursor(&app), (expected[0].0, 0));
        app.prepare_viewport(Size::new(30, 9));
        let mut terminal = Terminal::new(TestBackend::new(30, 9)).unwrap();
        terminal
            .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(&app), frame))
            .unwrap();
        assert!((9..30).any(|x| terminal.backend().buffer()[(x, 5)].fg == Color::Red));
        for key in [KeyCode::Tab, KeyCode::BackTab] {
            app.handle_key_event(key.into());
            assert!(app.state.visualize().search.is_some());
        }
        set_column_filter(&mut app, &format!("^{}$", regex::escape(&names[0])));
        let before = cursor(&app);
        set_search(&mut app, "^true$");
        finish_search(&mut app);
        assert_eq!(cursor(&app), before);
        assert_eq!(
            app.state.visualize().search_status.as_deref(),
            Some("No matches")
        );
        assert_eq!(ctx.sample_data.total_rows, 8);
        set_search(&mut app, "");
        assert!(app.state.visualize().search.is_none());
        assert!(!app.search_worker.is_running());
    }

    #[test]
    fn search_cancellation_keeps_cursor_and_drops_results_after_navigation() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        app.state.jump_to_row(3, 4, 8);
        set_search(&mut app, ".");
        assert!(app.search_worker.is_running());
        assert!(
            app.state
                .visualize()
                .search_status
                .as_deref()
                .unwrap()
                .contains("Row 4/8")
        );
        app.handle_key_event(KeyCode::Esc.into());
        assert!(!app.search_worker.is_running());
        assert_eq!(app.state.vertical_offset(), 3);
        assert!(app.state.visualize().search.is_some());
        app.poll_search();
        assert_eq!(app.state.vertical_offset(), 3);
        app.handle_key_event(KeyCode::Char('n').into());
        app.handle_key_event(KeyCode::Down.into());
        assert!(!app.search_worker.is_running());
        assert_eq!(app.state.vertical_offset(), 4);
        app.poll_search();
        assert_eq!(app.state.vertical_offset(), 4);
        app.handle_key_event(KeyCode::Char('n').into());
        app.handle_key_event(KeyCode::Tab.into());
        assert!(!app.search_worker.is_running());
        assert_eq!(app.tabs.active_tab().to_string(), "Metadata");
        assert!(app.state.visualize().search.is_some());
    }

    #[test]
    fn clearing_search_restores_row_selection_and_footer_at_the_current_position() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        for filtered in [false, true] {
            let mut app = App::new(&ctx);
            if filtered {
                set_column_filter(&mut app, "^id$|^bool_col$");
            }
            set_search(&mut app, "^true$");
            finish_search(&mut app);
            app.handle_key_event(KeyCode::Char('n').into());
            finish_search(&mut app);
            app.prepare_viewport(Size::new(80, 8));
            assert_eq!(app.state.visualize().selection_mode, SelectionMode::Cell);
            let row = app.state.vertical_offset();
            let vertical_scroll = app.state.data_vertical_scroll();
            let horizontal_scroll = app.state.horizontal_offset();
            let projection = app.state.visualize().projection().map(<[usize]>::to_vec);

            set_search(&mut app, "");
            app.prepare_viewport(Size::new(80, 8));

            assert!(app.state.visualize().search.is_none());
            assert!(app.state.visualize().search_status.is_none());
            assert_eq!(app.state.visualize().selection_mode, SelectionMode::Row);
            assert_eq!(app.state.visualize().selected_column, horizontal_scroll);
            assert_eq!(app.state.vertical_offset(), row);
            assert_eq!(app.state.data_vertical_scroll(), vertical_scroll);
            assert_eq!(app.state.horizontal_offset(), horizontal_scroll);
            assert_eq!(app.state.visualize().projection(), projection.as_deref());
            let footer = footer(&app).0;
            assert!(!footer.contains("Col:"), "{footer}");
            assert!(
                footer.starts_with(if filtered { "* " } else { "parqeye" }),
                "{footer}"
            );
        }
    }

    #[test]
    fn repeated_find_keys_apply_completed_hits_and_do_not_starve_the_worker() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        set_search(&mut app, "^true$");
        let deadline = Instant::now() + Duration::from_secs(10);
        // Deliberately do not call poll_search: each repeated key must accept a
        // completed result before stepping again, even faster than the UI tick.
        while app.state.vertical_offset() == 0 && app.state.visualize().selected_column == 0 {
            assert!(
                Instant::now() < deadline,
                "repeated n prevented search progress"
            );
            app.handle_key_event(KeyCode::Char('n').into());
            std::thread::yield_now();
        }
        finish_search(&mut app);
        let source = app
            .state
            .visualize()
            .source_column(app.state.visualize().selected_column)
            .unwrap();
        assert_eq!(
            ctx.sample_data
                .clipboard_value(app.state.vertical_offset(), source)
                .unwrap(),
            "true"
        );
    }

    fn set_column_filter(app: &mut App<'_>, expression: &str) {
        app.handle_key_event(KeyCode::Char('*').into());
        let count = app
            .state
            .visualize()
            .prompt()
            .unwrap()
            .buffer
            .chars()
            .count();
        app.handle_key_event(KeyCode::Home.into());
        for _ in 0..count {
            app.handle_key_event(KeyCode::Delete.into());
        }
        for character in expression.chars() {
            app.handle_key_event(KeyCode::Char(character).into());
        }
        app.handle_key_event(KeyCode::Enter.into());
    }

    #[test]
    #[ignore = "manual navigation timing; set PARQEYE_BENCH_FILE to benchmark another file"]
    fn benchmark_visualize_navigation() {
        let path = std::env::var("PARQEYE_BENCH_FILE").unwrap_or_else(|_| {
            format!(
                "{}/alltypes_tiny_pages.parquet",
                crate::file::parquet_test_data()
            )
        });
        let ctx = ParquetCtx::from_file(&path).unwrap();
        assert!(
            ctx.sample_data_error.is_none(),
            "{:?}",
            ctx.sample_data_error
        );
        let expression = ctx
            .sample_data
            .flattened_columns
            .iter()
            .step_by(2)
            .rev()
            .map(|name| format!("^{}$", regex::escape(name)))
            .collect::<Vec<_>>()
            .join("|");
        let size = Size::new(120, 35);
        for filtered in [false, true] {
            for mode in [SelectionMode::Row, SelectionMode::Cell] {
                let mut app = App::new(&ctx);
                if filtered {
                    set_column_filter(&mut app, &expression);
                    app.state.set_horizontal_offset(0);
                    app.state.visualize_mut().selected_column = 0;
                }
                app.state.visualize_mut().selection_mode = mode;
                let mut terminal =
                    Terminal::new(TestBackend::new(size.width, size.height)).unwrap();
                let started = Instant::now();
                for _ in 0..100 {
                    app.handle_key_event(KeyCode::Down.into());
                    app.prepare_viewport(size);
                    terminal
                        .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(&app), frame))
                        .unwrap();
                    std::hint::black_box(terminal.backend().buffer());
                }
                eprintln!(
                    "filtered={filtered}, mode={mode:?}: {:.3} ms/frame",
                    started.elapsed().as_secs_f64() * 10.0
                );
            }
        }
    }

    #[test]
    fn reordered_filters_preserve_selection_expression_navigation_and_clipboard_values() {
        use base64::{Engine, engine::general_purpose::STANDARD};
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        let names = &ctx.sample_data.flattened_columns;
        let exact = |index: usize| format!("^{}$", regex::escape(&names[index]));
        set_column_filter(&mut app, &[exact(0), exact(1), exact(4)].join("|"));
        app.state.visualize_mut().selection_mode = SelectionMode::Cell;
        app.handle_key_event(KeyCode::Down.into());
        app.prepare_viewport(Size::new(80, 12));
        let expression = [exact(4), exact(1), exact(0), exact(4)].join("|");
        set_column_filter(&mut app, &expression);
        assert_eq!(
            app.state.visualize().projection(),
            Some([4, 1, 0].as_slice())
        );
        assert_eq!(app.state.visualize().selected_column, 2);
        assert_eq!(app.state.vertical_offset(), 1);

        app.prepare_viewport(Size::new(80, 12));
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        terminal
            .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(&app), frame))
            .unwrap();
        let header_y = 3;
        let header: String = (0..80)
            .map(|x| terminal.backend().buffer()[(x, header_y)].symbol())
            .collect();
        assert!(header.find(&names[4]).unwrap() < header.find(&names[1]).unwrap());
        assert!(header.find(&names[1]).unwrap() < header.find(&names[0]).unwrap());
        let mut output = Vec::new();
        app.handle_key_event_with_clipboard(KeyCode::Char('y').into(), &mut output);
        assert_eq!(
            output,
            format!(
                "\x1b]52;c;{}\x07",
                STANDARD.encode(ctx.sample_data.clipboard_value(1, 0).unwrap())
            )
            .into_bytes()
        );

        app.handle_key_event(KeyCode::Char('*').into());
        let prompt = app.state.visualize().prompt().unwrap();
        assert_eq!(prompt.buffer, expression);
        assert_eq!(prompt.column_matches.as_deref(), Some([4, 1, 0].as_slice()));
        app.handle_key_event(KeyCode::Esc.into());
        for key in [KeyCode::Tab, KeyCode::BackTab] {
            app.handle_key_event(key.into());
            assert_eq!(
                app.state.visualize().projection(),
                Some([4, 1, 0].as_slice())
            );
        }
        app.state.visualize_mut().selection_mode = SelectionMode::Cell;
        for source in [4, 1, 0] {
            app.prepare_viewport(Size::new(25, 10));
            let mut output = Vec::new();
            app.handle_key_event_with_clipboard(KeyCode::Char('y').into(), &mut output);
            assert_eq!(
                output,
                format!(
                    "\x1b]52;c;{}\x07",
                    STANDARD.encode(ctx.sample_data.clipboard_value(0, source).unwrap())
                )
                .into_bytes()
            );
            app.handle_key_event(KeyCode::Right.into());
        }
        set_column_filter(&mut app, "");
        assert!(app.state.visualize().projection().is_none());
        assert_eq!(app.state.visualize().selected_column, 0);
    }

    #[test]
    fn filtered_navigation_copy_and_tab_switches_use_source_columns() {
        use base64::{Engine, engine::general_purpose::STANDARD};
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        let names = &ctx.sample_data.flattened_columns;
        set_column_filter(
            &mut app,
            &format!(
                "^({}|{})$",
                regex::escape(&names[1]),
                regex::escape(&names[4])
            ),
        );
        app.handle_key_event(KeyCode::Char('v').into());
        app.handle_key_event(KeyCode::Char('v').into());
        app.handle_key_event(KeyCode::Right.into());
        app.handle_key_event(KeyCode::Down.into());
        assert_eq!(
            app.state
                .visualize()
                .source_column(app.state.visualize().selected_column),
            Some(4)
        );
        for (width, height) in [(80, 12), (25, 9), (100, 13)] {
            app.prepare_viewport(Size::new(width, height));
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal
                .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(&app), frame))
                .unwrap();
            let buf = terminal.backend().buffer();
            let first_row = 5;
            let selected_y =
                first_row + (app.state.vertical_offset() - app.state.data_vertical_scroll()) as u16;
            assert!((9..width).any(|x| buf[(x, selected_y)].bg == Color::Rgb(60, 60, 60)));
            let mut output = Vec::new();
            app.handle_key_event_with_clipboard(KeyCode::Char('y').into(), &mut output);
            assert_eq!(
                output,
                format!(
                    "\x1b]52;c;{}\x07",
                    STANDARD.encode(ctx.sample_data.clipboard_value(1, 4).unwrap())
                )
                .into_bytes()
            );
        }
        for key in [KeyCode::Tab, KeyCode::BackTab, KeyCode::Esc] {
            app.handle_key_event(key.into());
            app.prepare_viewport(Size::new(80, 12));
            assert_eq!(app.state.visualize().projection(), Some([1, 4].as_slice()));
            if key == KeyCode::Tab {
                assert_eq!(app.state.visible_data_rows(), visible_data_rows(12));
            }
        }
        set_column_filter(&mut app, &format!("^{}$", regex::escape(&names[4])));
        let mut output = Vec::new();
        app.handle_key_event_with_clipboard(KeyCode::Char('y').into(), &mut output);
        assert!(!output.is_empty());
        assert_eq!(app.state.visualize().selection_mode, SelectionMode::Row);
        for code in [KeyCode::Char(':'), KeyCode::Char('8'), KeyCode::Enter] {
            app.handle_key_event(code.into());
        }
        assert_eq!(app.state.vertical_offset(), 7);
        assert_eq!(app.state.visualize().source_column(0), Some(4));
        app.handle_key_event(KeyCode::PageUp.into());
        assert!(app.state.vertical_offset() < 7);
        set_column_filter(&mut app, "");
        assert!(app.state.visualize().projection().is_none());
        assert_eq!(app.state.visualize().selected_column, 4);
    }

    #[test]
    fn column_prompt_scrolls_long_unicode_input_and_renders_in_tiny_terminals() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let mut ctx = ParquetCtx::from_file(&path).unwrap();
        let long_name = format!("{}suffix", "資料_customer_".repeat(20));
        ctx.sample_data.flattened_columns[0] = long_name.clone();
        let mut app = App::new(&ctx);
        set_column_filter(&mut app, "suffix$");
        assert_eq!(app.state.visualize().projection(), Some([0].as_slice()));
        app.state.visualize_mut().selection_mode = SelectionMode::Cell;
        app.handle_key_event(KeyCode::Char('*').into());
        for character in long_name.chars() {
            app.handle_key_event(KeyCode::Char(character).into());
        }
        for (width, height) in [(0, 0), (1, 1), (12, 6), (35, 10), (80, 12)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            app.prepare_viewport(Size::new(width, height));
            terminal
                .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(&app), frame))
                .unwrap();
            if width >= 35 {
                let cursor = terminal.get_cursor_position().unwrap();
                let buf = terminal.backend().buffer();
                let line: String = (0..width).map(|x| buf[(x, height - 1)].symbol()).collect();
                assert!(line.contains("suffix"), "{line}");
                assert_eq!(cursor.y, height - 1);
                assert_eq!(buf[cursor].symbol(), " ", "cursor must follow the input");
                app.handle_key_event(KeyCode::Home.into());
                app.prepare_viewport(Size::new(width, height));
                terminal
                    .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(&app), frame))
                    .unwrap();
                let line: String = (0..width)
                    .map(|x| terminal.backend().buffer()[(x, height - 1)].symbol())
                    .collect();
                assert!(line.contains("suffix$"), "{line}");
                let cursor = terminal.get_cursor_position().unwrap();
                assert_eq!(cursor.y, height - 1);
                assert_eq!(terminal.backend().buffer()[cursor].symbol(), "s");
                app.handle_key_event(KeyCode::End.into());
            }
        }
    }

    fn footer(app: &App<'_>) -> (String, Color) {
        footer_at(app, 80)
    }

    fn footer_at(app: &App<'_>, width: u16) -> (String, Color) {
        let mut terminal = Terminal::new(TestBackend::new(width, 10)).unwrap();
        terminal
            .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(app), frame))
            .unwrap();
        let buffer = terminal.backend().buffer();
        (
            (0..width).map(|x| buffer[(x, 9)].symbol()).collect(),
            buffer[(0, 9)].fg,
        )
    }

    #[test]
    fn footer_shortcuts_stay_fixed_while_functions_prompts_and_messages_compose() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        for width in [0, 1, 12, 24, 40, 80, 120, 200] {
            let mut app = App::new(&ctx);
            let mut terminal = Terminal::new(TestBackend::new(width, 10)).unwrap();
            let context_width =
                crate::ui::footer_areas(ratatui::layout::Rect::new(0, 9, width, 1))[0].width;
            let mut render = |app: &mut App<'_>| {
                app.prepare_viewport(Size::new(width, 10));
                assert_eq!(app.state.visible_data_rows(), 4);
                terminal
                    .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(app), frame))
                    .unwrap();
                let buf = terminal.backend().buffer();
                let context: String = (0..context_width).map(|x| buf[(x, 9)].symbol()).collect();
                assert!(!context.contains("Col:"), "{context}");
                let shortcuts: Vec<_> = (context_width..width)
                    .map(|x| buf[(x, 9)].clone())
                    .collect();
                if context_width > 0 && app.state.visualize().prompt().is_some() {
                    let cursor = terminal.get_cursor_position().unwrap();
                    assert_eq!(cursor.y, 9);
                    assert!(cursor.x < context_width.max(1));
                }
                (context, shortcuts)
            };
            let (_, expected_shortcuts) = render(&mut app);
            if width >= 80 {
                let text: String = expected_shortcuts
                    .iter()
                    .map(|cell| cell.symbol())
                    .collect();
                assert!(
                    text.contains("[Tab] Toggle Mode")
                        && text.contains("[?] Help")
                        && text.contains("[Q]uit"),
                    "{text}"
                );
            }

            app.handle_key_event(KeyCode::Char('*').into());
            assert_eq!(render(&mut app).1, expected_shortcuts);
            app.handle_key_event(KeyCode::Char('[').into());
            app.handle_key_event(KeyCode::Enter.into());
            assert_eq!(render(&mut app).1, expected_shortcuts);
            app.handle_key_event(KeyCode::Esc.into());
            set_column_filter(&mut app, "^bool_col$|^id$");
            assert_eq!(render(&mut app).1, expected_shortcuts);
            app.handle_key_event(KeyCode::Char('/').into());
            assert_eq!(render(&mut app).1, expected_shortcuts);
            app.handle_key_event(KeyCode::Esc.into());
            set_search(&mut app, "^true$");
            assert_eq!(render(&mut app).1, expected_shortcuts);
            finish_search(&mut app);
            let (context, shortcuts) = render(&mut app);
            assert_eq!(shortcuts, expected_shortcuts);
            if width >= 80 {
                assert!(
                    context.contains("* 2/") && context.contains("/ ^true$"),
                    "{context}"
                );
            }
            app.state.show_toast("Copied 4 chars", false);
            assert_eq!(render(&mut app).1, expected_shortcuts);
            app.state.toast.as_mut().unwrap().expires_at = Instant::now();
            assert_eq!(render(&mut app).0, context);

            // Clear each function independently. The other function and all
            // shortcut cells keep their positions and styling.
            set_search(&mut app, "");
            let (context, shortcuts) = render(&mut app);
            assert_eq!(shortcuts, expected_shortcuts);
            if width >= 40 {
                assert!(context.starts_with("* "), "{context}");
            }
            set_search(&mut app, "^true$");
            finish_search(&mut app);
            set_column_filter(&mut app, "");
            let (context, shortcuts) = render(&mut app);
            assert_eq!(shortcuts, expected_shortcuts);
            if width >= 40 {
                assert!(
                    context.starts_with("/ ") && !context.contains("* "),
                    "{context}"
                );
            }
            set_search(&mut app, "");
            assert_eq!(render(&mut app).1, expected_shortcuts);
        }
    }

    #[test]
    fn help_owns_keys_preserves_navigation_and_switches_context_with_tabs() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        for tab in 0..app.tabs.tabs.len() {
            app.tabs.active_tab = tab;
            app.state.reset();
            app.handle_key_event(KeyCode::Down.into());
            app.handle_key_event(KeyCode::Right.into());
            app.prepare_viewport(Size::new(80, 12));
            let row = app.state.vertical_offset();
            let column = app.state.horizontal_offset();
            let selection = app.state.visualize().clone();
            app.handle_key_event(KeyCode::Char('?').into());
            app.prepare_viewport(Size::new(80, 12));
            assert!(app.state.help().is_some());
            let mut clipboard = Vec::new();
            for key in [
                KeyCode::Down,
                KeyCode::Right,
                KeyCode::Char('v'),
                KeyCode::Char('y'),
                KeyCode::Char('/'),
                KeyCode::Char('*'),
                KeyCode::Char(':'),
            ] {
                app.handle_key_event_with_clipboard(key.into(), &mut clipboard);
            }
            assert!(clipboard.is_empty());
            assert_eq!(app.state.vertical_offset(), row);
            assert_eq!(app.state.horizontal_offset(), column);
            assert_eq!(app.state.visualize(), &selection);
            let help_scroll = app.state.help().unwrap().scroll;
            app.handle_key_event(KeyCode::Esc.into());
            assert_eq!(app.state.help().unwrap().scroll, help_scroll);
            assert_eq!(app.state.vertical_offset(), row);
            assert_eq!(app.state.horizontal_offset(), column);
            assert_eq!(app.state.visualize(), &selection);
            app.handle_key_event(KeyCode::Char('?').into());
            assert!(app.state.help().is_none());
        }

        app.handle_key_event(KeyCode::Char('?').into());
        let tab = app.tabs.active_tab;
        app.handle_key_event(KeyCode::Tab.into());
        assert_eq!(app.tabs.active_tab, (tab + 1) % app.tabs.tabs.len());
        assert_eq!(app.state.help().unwrap().scroll, 0);
        app.handle_key_event(KeyCode::BackTab.into());
        assert_eq!(app.tabs.active_tab, tab);
        assert!(app.state.help().is_some());
        app.handle_key_event(KeyCode::Char('q').into());
        assert!(app.exit);
    }

    #[test]
    fn help_scrolls_without_moving_the_table_and_clamps_after_resize() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        app.handle_key_event(KeyCode::Char('?').into());
        app.prepare_viewport(Size::new(60, 12));
        let help = app.state.help().unwrap();
        let page = help.visible_rows;
        assert!(help.max_scroll > page && page > 0);
        app.handle_key_event(KeyCode::PageDown.into());
        assert_eq!(app.state.help().unwrap().scroll, page);
        app.handle_key_event(KeyCode::Down.into());
        assert_eq!(app.state.help().unwrap().scroll, page + 1);
        app.handle_key_event(KeyCode::Up.into());
        app.handle_key_event(KeyCode::PageUp.into());
        assert_eq!(app.state.help().unwrap().scroll, 0);
        app.handle_key_event(KeyCode::End.into());
        assert_eq!(
            app.state.help().unwrap().scroll,
            app.state.help().unwrap().max_scroll
        );
        app.handle_key_event(KeyCode::Down.into());
        app.prepare_viewport(Size::new(120, 100));
        assert_eq!(app.state.help().unwrap().scroll, 0);
        assert_eq!(app.state.vertical_offset(), 0);
        app.prepare_viewport(Size::new(40, 10));
        app.handle_key_event(KeyCode::End.into());
        app.handle_key_event(KeyCode::Home.into());
        assert_eq!(app.state.help().unwrap().scroll, 0);
    }

    #[test]
    fn help_and_quit_characters_remain_text_in_command_prompts() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        for command in ['/', '*', ':'] {
            app.handle_key_event(KeyCode::Char(command).into());
            for character in "q?".chars() {
                app.handle_key_event(KeyCode::Char(character).into());
            }
            assert_eq!(app.state.visualize().prompt().unwrap().buffer, "q?");
            assert!(app.state.help().is_none());
            assert!(!app.exit);
            assert!(footer(&app).0.contains("[Q]uit"));
            app.handle_key_event(KeyCode::Esc.into());
        }
    }

    #[test]
    fn help_shows_tab_commands_and_keeps_quit_visible_at_small_sizes() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        let mut terminal = Terminal::new(TestBackend::new(100, 100)).unwrap();
        let expected_sections = [
            "Navigation and selection",
            "Browse metadata",
            "Explore the schema",
            "Inspect row groups",
        ];
        for (tab, section) in expected_sections.into_iter().enumerate() {
            app.tabs.active_tab = tab;
            app.state.reset();
            app.handle_key_event(KeyCode::Char('?').into());
            app.prepare_viewport(Size::new(100, 100));
            terminal
                .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(&app), frame))
                .unwrap();
            let buf = terminal.backend().buffer();
            let text: String = buf.content().iter().map(|cell| cell.symbol()).collect();
            assert!(text.contains(&format!("{} - Help", app.tabs.active_tab().to_string())));
            assert!(text.contains(section), "{text}");
            assert!(text.contains("Across all tabs"));
            assert_eq!(text.contains("Find, columns and row commands"), tab == 0);
            assert!(text.contains("[?] Close"));

            for (width, height) in [
                (0, 0),
                (1, 1),
                (6, 1),
                (6, 4),
                (12, 6),
                (24, 8),
                (40, 12),
                (80, 24),
            ] {
                let mut small_terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
                app.prepare_viewport(Size::new(width, height));
                app.handle_key_event(KeyCode::End.into());
                small_terminal
                    .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(&app), frame))
                    .unwrap();
                if width >= 6 && height > 0 {
                    let buf = small_terminal.backend().buffer();
                    let footer: String =
                        (0..width).map(|x| buf[(x, height - 1)].symbol()).collect();
                    assert!(footer.contains("[Q]uit"), "{width}x{height}: {footer}");
                    if width >= 17 {
                        assert!(footer.contains("[?] Help"), "{footer}");
                    }
                }
            }
        }
    }

    #[test]
    fn visualize_context_and_prompts_keep_the_table_geometry_fixed() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let mut ctx = ParquetCtx::from_file(&path).unwrap();
        for (index, name) in ["a", "b", "c"].iter().enumerate() {
            ctx.sample_data.flattened_columns[index] = (*name).into();
        }
        for height in [8, 10, 13] {
            let mut app = App::new(&ctx);
            let mut terminal = Terminal::new(TestBackend::new(80, height)).unwrap();
            let mut render_table = |app: &mut App<'_>, expected_footer: &str| {
                app.prepare_viewport(Size::new(80, height));
                assert_eq!(app.state.visible_data_rows(), usize::from(height - 6));
                terminal
                    .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(app), frame))
                    .unwrap();
                let buffer = terminal.backend().buffer();
                let line = |y| -> String { (0..80).map(|x| buffer[(x, y)].symbol()).collect() };
                assert!(line(3).contains('a'), "header must stay on row 3");
                assert_eq!(buffer[(0, 5)].symbol(), "1");
                assert_eq!(buffer[(0, height - 2)].symbol(), (height - 6).to_string());
                let footer = line(height - 1);
                assert!(
                    footer.contains(expected_footer),
                    "expected {expected_footer:?} in {footer:?}"
                );
                (3..height - 1).map(line).collect::<Vec<_>>()
            };

            let table = render_table(&mut app, "parqeye");
            for _ in 0..3 {
                app.handle_key_event(KeyCode::Char('v').into());
                assert_eq!(render_table(&mut app, ""), table);
            }

            app.handle_key_event(KeyCode::Char('*').into());
            assert_eq!(render_table(&mut app, "e.g."), table);
            app.handle_key_event(KeyCode::Char('[').into());
            assert_eq!(render_table(&mut app, "Invalid"), table);
            app.handle_key_event(KeyCode::Esc.into());
            set_column_filter(&mut app, "^missing$");
            assert_eq!(render_table(&mut app, "No col"), table);
            app.handle_key_event(KeyCode::Esc.into());

            set_column_filter(&mut app, "^a$|^c$|^b$");
            let filtered_table = render_table(&mut app, "* 3/");
            assert!(filtered_table[0].find('c') < filtered_table[0].find('b'));
            for mode in [
                SelectionMode::Column,
                SelectionMode::Cell,
                SelectionMode::Row,
            ] {
                app.state.visualize_mut().selection_mode = mode;
                assert_eq!(render_table(&mut app, "* 3/"), filtered_table);
            }
            app.handle_key_event(KeyCode::Char('*').into());
            assert_eq!(render_table(&mut app, "3/"), filtered_table);
            app.handle_key_event(KeyCode::Esc.into());
            app.handle_key_event(KeyCode::Char(':').into());
            app.handle_key_event(KeyCode::Char('0').into());
            app.handle_key_event(KeyCode::Enter.into());
            assert_eq!(render_table(&mut app, "row"), filtered_table);
            app.handle_key_event(KeyCode::Esc.into());
            app.state.show_toast("Copied 1 chars", false);
            assert_eq!(render_table(&mut app, "Copied 1 chars"), filtered_table);
            app.state.toast.as_mut().unwrap().expires_at = Instant::now();
            assert_eq!(render_table(&mut app, "* 3/"), filtered_table);

            for _ in 0..3 {
                app.handle_key_event(KeyCode::Tab.into());
                assert!(footer(&app).0.starts_with("parqeye"));
            }
        }
    }

    #[test]
    fn filtered_cell_navigation_uses_the_last_data_line_after_resizing() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        set_column_filter(&mut app, "^id$");
        app.state.visualize_mut().selection_mode = SelectionMode::Cell;
        for height in [8, 13, 10] {
            app.handle_key_event(KeyCode::Home.into());
            app.prepare_viewport(Size::new(80, height));
            let rows_on_screen = usize::from(height - 6);
            for _ in 1..rows_on_screen {
                app.handle_key_event(KeyCode::Down.into());
            }
            assert_last_data_line_is_selected(&app, height, rows_on_screen);
            app.handle_key_event(KeyCode::PageDown.into());
            app.prepare_viewport(Size::new(80, height));
            assert_last_data_line_is_selected(&app, height, (2 * rows_on_screen).min(8));
            app.handle_key_event(KeyCode::Char('G').into());
            app.prepare_viewport(Size::new(80, height));
            assert_last_data_line_is_selected(&app, height, ctx.sample_data.total_rows);
        }
    }

    #[test]
    fn yank_copies_the_cursor_cell_and_renders_a_temporary_confirmation() {
        use base64::{Engine, engine::general_purpose::STANDARD};

        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        for code in [
            KeyCode::Char('v'),
            KeyCode::Char('v'),
            KeyCode::Down,
            KeyCode::Right,
        ] {
            app.handle_key_event(KeyEvent::from(code));
        }
        let value = &ctx.sample_data.loaded().rows[1][1];
        let mut output = Vec::new();
        app.handle_key_event_with_clipboard(KeyEvent::from(KeyCode::Char('y')), &mut output);
        assert_eq!(
            output,
            format!("\x1b]52;c;{}\x07", STANDARD.encode(value)).into_bytes()
        );
        let (text, color) = footer(&app);
        assert!(text.starts_with(&format!("Copied {} chars", value.chars().count())));
        assert_eq!(color, Color::Green);
        assert_eq!(app.state.vertical_offset(), 1);
        assert_eq!(app.state.visualize().selected_column, 1);

        app.state.toast.as_mut().unwrap().expires_at = Instant::now();
        assert!(app.state.toast().is_none());
        assert!(footer(&app).0.starts_with("parqeye"));
    }

    #[test]
    fn invalid_yanks_never_write_and_errors_clear_on_navigation() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        let mut output = Vec::new();
        for mode in [SelectionMode::Row, SelectionMode::Column] {
            app.state.visualize_mut().selection_mode = mode;
            app.handle_key_event_with_clipboard(KeyEvent::from(KeyCode::Char('y')), &mut output);
            assert!(output.is_empty());
            let (text, color) = footer(&app);
            assert!(text.starts_with("Yank requires cell mode"));
            assert_eq!(color, Color::Red);
            app.handle_key_event(KeyEvent::from(KeyCode::Down));
            assert!(app.state.toast().is_none());
        }

        app.state.visualize_mut().selection_mode = SelectionMode::Cell;
        app.handle_key_event(KeyEvent::from(KeyCode::Char(':')));
        app.handle_key_event_with_clipboard(KeyEvent::from(KeyCode::Char('y')), &mut output);
        assert!(output.is_empty());
        assert_eq!(app.state.visualize().prompt().unwrap().buffer, "y");
        app.handle_key_event(KeyEvent::from(KeyCode::Esc));
        app.handle_key_event(KeyEvent::from(KeyCode::Tab));
        app.handle_key_event_with_clipboard(KeyEvent::from(KeyCode::Char('y')), &mut output);
        assert!(output.is_empty());
        assert!(app.state.toast().is_none());
    }

    #[test]
    fn clipboard_write_failures_are_shown_without_exiting() {
        struct BrokenClipboard;
        impl io::Write for BrokenClipboard {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("clipboard unavailable"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        app.state.visualize_mut().selection_mode = SelectionMode::Cell;
        app.handle_key_event_with_clipboard(
            KeyEvent::from(KeyCode::Char('y')),
            &mut BrokenClipboard,
        );
        let toast = app.state.toast().unwrap();
        assert!(toast.is_error);
        assert_eq!(toast.message, "Copy failed: clipboard unavailable");
        assert!(!app.exit);
    }

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
                KeyEvent::from(KeyCode::Down),
                KeyEvent::from(KeyCode::PageDown),
                KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL),
                KeyEvent::from(KeyCode::End),
                KeyEvent::from(KeyCode::Char('G')),
            ] {
                app.state.reset();
                for _ in 0..ctx.sample_data.total_rows {
                    app.handle_key_event(key);
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

    #[test]
    fn visualize_cell_navigation_follows_selection_and_survives_resizing() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let mut ctx = ParquetCtx::from_file(&path).unwrap();
        // The schema tab can have more entries than the data table (e.g. nested fields).
        ctx.schema = scroll_test_schema(true);
        assert_ne!(ctx.schema.column_size(), ctx.sample_data.total_columns);
        let mut app = App::new(&ctx);
        let mut terminal = Terminal::new(TestBackend::new(60, 10)).unwrap();
        app.prepare_viewport(Size::new(60, 10));
        for _ in 0..2 {
            app.handle_key_event(KeyEvent::from(KeyCode::Char('v')));
        }
        for _ in 0..ctx.sample_data.total_columns + 3 {
            app.handle_key_event(KeyEvent::from(KeyCode::Right));
            app.prepare_viewport(Size::new(60, 10));
        }
        assert_eq!(
            app.state.visualize().selected_column,
            ctx.sample_data.total_columns - 1
        );
        assert!(app.state.horizontal_offset() > 0);
        for _ in 0..6 {
            app.handle_key_event(KeyEvent::from(KeyCode::Down));
        }

        for (width, height) in [(60, 10), (30, 8), (100, 13)] {
            app.prepare_viewport(Size::new(width, height));
            terminal.backend_mut().resize(width, height);
            terminal
                .resize(ratatui::layout::Rect::new(0, 0, width, height))
                .unwrap();
            let view = AppRenderView::from_app(&app);
            terminal
                .draw(|frame| crate::ui::render_app(&view, frame))
                .unwrap();
            let buf = terminal.backend().buffer();
            let selected_y =
                5 + (app.state.vertical_offset() - app.state.data_vertical_scroll()) as u16;
            assert!(
                (9..width).any(|x| buf[(x, selected_y)].bg == Color::Rgb(60, 60, 60)),
                "selected cell must be visible at {width}x{height}, row {}, scroll {}, column {}, horizontal {}",
                app.state.vertical_offset(),
                app.state.data_vertical_scroll(),
                app.state.visualize().selected_column,
                app.state.horizontal_offset(),
            );
            let footer: String = (0..width).map(|x| buf[(x, height - 1)].symbol()).collect();
            assert!(footer.starts_with("parqeye"), "{footer}");
        }
        for _ in 0..ctx.sample_data.total_columns {
            app.handle_key_event(KeyEvent::from(KeyCode::Left));
            app.prepare_viewport(Size::new(30, 8));
        }
        assert_eq!(app.state.visualize().selected_column, 0);
        assert_eq!(app.state.horizontal_offset(), 0);
    }

    #[test]
    fn tab_switch_and_escape_reset_selection_consistently_with_the_viewport() {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        let ctx = ParquetCtx::from_file(&path).unwrap();
        let mut app = App::new(&ctx);
        for reset_key in [KeyCode::Esc, KeyCode::Tab] {
            app.handle_key_event(KeyEvent::from(KeyCode::Char('v')));
            app.handle_key_event(KeyEvent::from(KeyCode::Right));
            app.handle_key_event(KeyEvent::from(reset_key));
            assert_eq!(app.state.visualize().selection_mode, SelectionMode::Row);
            assert_eq!(app.state.visualize().selected_column, 0);
            assert_eq!(app.state.horizontal_offset(), 0);
            assert_eq!(app.state.data_vertical_scroll(), 0);
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
