use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{buffer::Buffer, layout::Rect};

use crate::tab::Tab;
use crate::ui::render_row_groups_tab;
use crate::utils::adjust_scroll_for_selection;

pub struct RowGroupsTab {
    pub row_offset: usize,
    pub col_offset: usize,
    pub scroll_offset: usize,
    pub column_selected: Option<usize>,
    pub row_group_selected: usize,
}

impl RowGroupsTab {
    pub fn new() -> Self {
        Self {
            row_offset: 0,
            col_offset: 0,
            scroll_offset: 0,
            column_selected: None,
            row_group_selected: 0,
        }
    }

    fn scroll_down(&mut self, amount: usize, app: &crate::App) {
        // Max scroll should account for the fact that root is always visible
        // So we can scroll through items 1 to end
        let scrollable_items = app.schema_columns.len().saturating_sub(1);
        self.scroll_offset = (self.scroll_offset + amount).min(scrollable_items);
    }

    fn scroll_up(&mut self, amount: usize) {
        self.scroll_offset = self.scroll_offset.saturating_sub(amount);
    }
}

impl Tab for RowGroupsTab {
    fn on_focus(&mut self) {
        self.col_offset = 0;
        self.row_offset = 0;
        self.scroll_offset = 0;
        self.column_selected = None;
        self.row_group_selected = 0;
    }
    
    fn on_event(&mut self, key_event: KeyEvent, app: &mut crate::App) {
        match key_event.code {
            KeyCode::Left => {
                if self.column_selected.is_none() {
                    self.column_selected = Some(1);
                }
            }
            KeyCode::Right => {
                if self.column_selected.is_none() {
                    self.column_selected = Some(1);
                }
            }
            KeyCode::Down => {
                let total_columns: usize = app.schema_columns.len();
                if let Some(idx) = self.column_selected {
                    if idx + 1 < total_columns {
                        self.column_selected = Some(idx + 1);
                    }
                } else {
                    self.column_selected = Some(1);
                }
                self.scroll_offset = adjust_scroll_for_selection(self.column_selected, app.schema_tree_height);
            } 
            KeyCode::Up => {
                if let Some(idx) = self.column_selected {
                    if idx > 0 {
                        self.column_selected = Some(idx - 1);
                    }
                } else {
                    self.column_selected = Some(1);
                }
                self.scroll_offset = adjust_scroll_for_selection(self.column_selected, app.schema_tree_height);
            }
            KeyCode::PageDown => {
                self.scroll_down(2, app);
            }
            KeyCode::PageUp => {
                self.scroll_up(2);
            }
            KeyCode::Char('j') => {
                self.row_group_selected += 1;
            }
            KeyCode::Char('k') => {
                if self.row_group_selected > 0 {
                    self.row_group_selected -= 1;
                }
            }
            _ => {}
        }
    }
    
    fn render(&mut self, app: &mut crate::App, area: Rect, buf: &mut Buffer) {
        render_row_groups_tab(app, area, buf, self);
    }
}