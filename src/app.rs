use crossterm::event::{self, Event, KeyEvent, KeyEventKind};
use ratatui::DefaultTerminal;
use std::io;

use crate::config;
use crate::file::parquet_ctx::ParquetCtx;
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

pub struct AppState {
    horizontal_offset: usize,
    vertical_offset: usize,
    tree_scroll_offset: usize,
    data_vertical_scroll: usize,
    visible_data_rows: usize,
    pub config: config::AppConfig,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new(config::AppConfig::default())
    }
}

impl AppState {
    pub fn new(config: config::AppConfig) -> Self {
        Self {
            horizontal_offset: 0,
            vertical_offset: 0,
            tree_scroll_offset: 0,
            data_vertical_scroll: 0,
            visible_data_rows: 20, // Default fallback
            config: config,
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
    pub fn new(file_info: &'a ParquetCtx, config: config::AppConfig) -> Self {
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
            state: AppState::new(config),
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
        if let Some(action) = self.state.config.keymap.get_action(key_event.code) {
            match action {
                config::Action::Quit => self.exit(),
                config::Action::Reset => self.state.reset(),
                config::Action::NextTab => {
                    self.tabs.next();
                    self.state.reset();
                }
                config::Action::PrevTab => {
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
    }

    fn exit(&mut self) {
        self.exit = true;
    }
}
