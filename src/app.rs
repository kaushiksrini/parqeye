use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{DefaultTerminal, layout::Size};
use std::io;
use std::time::{Duration, Instant};

use crate::file::parquet_ctx::ParquetCtx;
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
        }
    }

    pub fn reset(&mut self) {
        self.horizontal_offset = 0;
        self.vertical_offset = 0;
        self.tree_scroll_offset = 0;
        self.data_vertical_scroll = 0;
        self.visualize.reset_navigation();
        self.toast = None;
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
        }
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        while !self.exit {
            self.prepare_viewport(terminal.size()?);

            let render_view = AppRenderView::from_app(self);
            terminal.draw(|frame| crate::ui::render_app(&render_view, frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn prepare_viewport(&mut self, terminal_size: Size) {
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
        // Wake once to clear the toast, then return to blocking reads while idle.
        if let Some(toast) = &self.state.toast
            && !event::poll(toast.expires_at.saturating_duration_since(Instant::now()))?
        {
            self.state.toast = None;
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
        self.state.toast = None;
        let outcome = self
            .tabs
            .active_tab()
            .on_event(key_event, &mut self.state)
            .unwrap();
        match outcome {
            EventOutcome::Consumed => return,
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
        app.handle_key_event(KeyEvent::new(KeyCode::Right, KeyModifiers::ALT));
        assert_eq!(app.state.visualize().column_name_scroll, 1);
        assert!(footer(&app).0.replace(' ', "").contains("Col:料_customer_"));
        app.handle_key_event(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT));
        assert_eq!(app.state.visualize().column_name_scroll, 0);
        assert!(
            footer(&app)
                .0
                .replace(' ', "")
                .contains("Col:資料_customer_")
        );
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
            if width >= 12 {
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
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        terminal
            .draw(|frame| crate::ui::render_app(&AppRenderView::from_app(app), frame))
            .unwrap();
        let buffer = terminal.backend().buffer();
        (
            (0..80).map(|x| buffer[(x, 9)].symbol()).collect(),
            buffer[(0, 9)].fg,
        )
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
                assert!(line(height - 1).contains(expected_footer));
                (3..height - 1).map(line).collect::<Vec<_>>()
            };

            let table = render_table(&mut app, "parqeye");
            for _ in 0..3 {
                app.handle_key_event(KeyCode::Char('v').into());
                assert_eq!(render_table(&mut app, ""), table);
            }

            app.handle_key_event(KeyCode::Char('*').into());
            assert_eq!(render_table(&mut app, "a|b"), table);
            app.handle_key_event(KeyCode::Char('[').into());
            assert_eq!(render_table(&mut app, "Invalid regex"), table);
            app.handle_key_event(KeyCode::Esc.into());
            set_column_filter(&mut app, "^missing$");
            assert_eq!(render_table(&mut app, "No columns match"), table);
            app.handle_key_event(KeyCode::Esc.into());

            set_column_filter(&mut app, "^a$|^c$|^b$");
            let filtered_table = render_table(&mut app, "Columns: 3/");
            assert!(filtered_table[0].find('c') < filtered_table[0].find('b'));
            for mode in [
                SelectionMode::Column,
                SelectionMode::Cell,
                SelectionMode::Row,
            ] {
                app.state.visualize_mut().selection_mode = mode;
                assert_eq!(render_table(&mut app, "Columns: 3/"), filtered_table);
            }
            app.handle_key_event(KeyCode::Char('*').into());
            assert_eq!(render_table(&mut app, "3/"), filtered_table);
            app.handle_key_event(KeyCode::Esc.into());
            app.handle_key_event(KeyCode::Char(':').into());
            app.handle_key_event(KeyCode::Char('0').into());
            app.handle_key_event(KeyCode::Enter.into());
            assert_eq!(render_table(&mut app, "row must be"), filtered_table);
            app.handle_key_event(KeyCode::Esc.into());
            app.state.show_toast("Copied 1 chars", false);
            assert_eq!(render_table(&mut app, "Copied 1 chars"), filtered_table);
            app.state.toast.as_mut().unwrap().expires_at = Instant::now();
            assert_eq!(render_table(&mut app, "Columns: 3/"), filtered_table);

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
        assert!(footer(&app).0.starts_with("Col: "));
        assert!(
            footer(&app)
                .0
                .contains(&ctx.sample_data.flattened_columns[1])
        );
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
            assert!(text.starts_with("Yank requires cell mode (press v)"));
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
            assert!(footer.starts_with("Col: "), "{footer}");
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
