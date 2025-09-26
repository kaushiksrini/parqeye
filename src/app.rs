use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::DefaultTerminal;
use std::io;

use crate::file::parquet_ctx::ParquetCtx;
use crate::tabs::{TabManager, TabType};
use std::cmp::max;

pub struct AppRenderView<'a> {
    pub title: &'a str,
    pub parquet_ctx: &'a ParquetCtx,
    file_name: &'a str,
    tabs: &'a TabManager,
    column_selected: Option<usize>,
    row_group_selected: usize,
    // schema_tree_height: usize,
    visualize_col_offset: usize,
    pub horizontal_scroll: usize,
}

impl<'a> AppRenderView<'a> {
    fn from_app(app: &'a App) -> Self {
        Self {
            title: "parqeye",
            parquet_ctx: app.parquet_ctx,
            file_name: &app.file_name,
            tabs: &app.tabs,
            column_selected: app.column_selected,
            row_group_selected: app.row_group_selected,
            // schema_tree_height: app.schema_tree_height,
            visualize_col_offset: app.visualize_col_offset,
            horizontal_scroll: app.horizontal_scroll,
        }
    }

    pub fn tabs(&self) -> &TabManager {
        &self.tabs
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn column_selected(&self) -> &Option<usize> {
        &self.column_selected
    }
}

pub struct App<'a> {
    pub parquet_ctx: &'a ParquetCtx,
    pub file_name: String,
    pub exit: bool,
    pub tabs: TabManager,
    pub column_selected: Option<usize>,
    pub scroll_offset: usize,
    pub row_group_selected: usize,
    // pub schema_tree_height: usize,
    // Visualize tab state
    pub visualize_col_offset: usize,
    pub horizontal_scroll: usize,
}

impl<'a> App<'a> {
    pub fn new(file_info: &'a ParquetCtx) -> Self {
        Self {
            parquet_ctx: file_info,
            file_name: file_info.file_path.clone(),
            exit: false,
            tabs: TabManager::new(),
            column_selected: None,
            scroll_offset: 0,
            row_group_selected: 0,
            visualize_col_offset: 0,
            horizontal_scroll: 0,
        }
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        while !self.exit {
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
            KeyCode::Char('q') => self.exit(),
            KeyCode::Tab => {
                self.tabs.next();
            }
            KeyCode::BackTab => {
                self.tabs.prev();
            }
            KeyCode::Down => {
                let total_columns: usize = self.parquet_ctx.schema.column_size();
                match self.tabs.active_tab() {
                    TabType::Schema | TabType::RowGroups => {
                        if let Some(idx) = self.column_selected {
                            if idx + 1 <= total_columns {
                                self.column_selected = Some(idx + 1);
                            }
                        } else {
                            self.column_selected = Some(1);
                        }
                    }
                    _ => {}
                }
            }
            KeyCode::Up => match self.tabs.active_tab() {
                TabType::Schema | TabType::RowGroups => {
                    if let Some(idx) = self.column_selected {
                        if idx > 1 {
                            self.column_selected = Some(idx - 1);
                        } else {
                            self.column_selected = None;
                        }
                    }
                }
                _ => {}
            },
            KeyCode::Right => match self.tabs.active_tab() {
                TabType::Schema | TabType::Visualize => {
                    self.horizontal_scroll += 1;
                }
                _ => {}
            },
            KeyCode::Left => match self.tabs.active_tab() {
                TabType::Schema | TabType::Visualize => {
                    self.horizontal_scroll = max(0, self.horizontal_scroll.saturating_sub(1));
                }
                _ => {}
            },
            _ => {}
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }
}
