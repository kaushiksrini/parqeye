use std::collections::HashMap;
use std::io;
use ratatui::DefaultTerminal;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};

use crate::schema::{SchemaColumnType, ColumnType};
use crate::column_chunk::RowGroupStats;

#[derive(Default)]
pub struct App {
    pub file_name: String,
    pub exit: bool,
    pub tabs: Vec<&'static str>,
    pub active_tab: usize,
    pub column_selected: Option<usize>,
    pub schema_columns: Vec<SchemaColumnType>,
    pub schema_map: HashMap<String, ColumnType>,
    pub scroll_offset: usize,
    pub row_group_selected: usize,
    pub schema_tree_height: usize,
    pub row_group_stats: Vec<RowGroupStats>,
    pub primitive_columns_idx: HashMap<String, usize>,
}

impl App {
    pub fn run(&mut self, terminal: &mut DefaultTerminal, path: &str) -> io::Result<()> {
        self.file_name = path.to_string();
        self.tabs = vec!["Schema", "Row Groups", "Visualize"];
        self.active_tab = 0;
        self.column_selected = None;
        self.scroll_offset = 0;
        self.row_group_selected = 0;
        
        let (schema_columns, schema_map) = crate::schema::build_schema_tree_lines(path)
            .map_err(| e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        self.schema_columns = schema_columns;
        self.schema_map = schema_map;

        for (idx, c) in self.schema_columns.iter().filter(|c| matches!(c, SchemaColumnType::Primitive { .. })).enumerate() {
            if let SchemaColumnType::Primitive { name, .. } = c {
                self.primitive_columns_idx.insert(name.clone(), idx);
            }
        }

        // calculate row group stats
        self.row_group_stats = crate::column_chunk::calculate_row_group_stats(path).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        while !self.exit {
            terminal.draw(|frame| crate::ui::render_app(self, frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn handle_events(&mut self, ) -> io::Result<()> {
        match event::read()? {
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                self.handle_key_event(key_event)
            }
            _ => {}
        };
        Ok(())
    }

    pub fn set_schema_tree_height(&mut self, new_height: usize) {
        self.schema_tree_height = new_height;
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            KeyCode::Right => {
                if self.active_tab + 1 < self.tabs.len() {
                    self.active_tab += 1;
                }
                if self.active_tab == 1 && self.column_selected.is_none() {
                    self.column_selected = Some(1);
                }
            }
            KeyCode::Left => {
                if self.active_tab > 0 {
                    self.active_tab -= 1;
                }
                if self.active_tab == 1 && self.column_selected.is_none() {
                    self.column_selected = Some(1);
                }
            }
            KeyCode::Down => {
                if self.active_tab == 0 || self.active_tab == 1 {
                    let total_columns: usize = self.schema_columns.len();
                    if let Some(idx) = self.column_selected {
                        if idx + 1 < total_columns {
                            self.column_selected = Some(idx + 1);
                        }
                    } else {
                        self.column_selected = Some(1);
                    }
                    self.adjust_scroll_for_selection();
                }
            }
            KeyCode::Up => {
                if self.active_tab == 0 || self.active_tab == 1 {
                    if let Some(idx) = self.column_selected {
                        if idx > 1 {
                            self.column_selected = Some(idx - 1);
                        } else {
                            if self.active_tab != 1 {
                                self.column_selected = None;
                            }
                        }
                    }
                    self.adjust_scroll_for_selection();
                }
            }
            KeyCode::PageDown => {
                if self.active_tab == 0 {
                    self.scroll_down(2);
                }
            }
            KeyCode::PageUp => {
                if self.active_tab == 0 {
                    self.scroll_up(2);
                }
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

    fn exit(&mut self) {
        self.exit = true;
    }

    fn scroll_down(&mut self, amount: usize) {
        // Max scroll should account for the fact that root is always visible
        // So we can scroll through items 1 to end
        let scrollable_items = self.schema_columns.len().saturating_sub(1);
        self.scroll_offset = (self.scroll_offset + amount).min(scrollable_items);
    }

    fn scroll_up(&mut self, amount: usize) {
        self.scroll_offset = self.scroll_offset.saturating_sub(amount);
    }

    fn adjust_scroll_for_selection(&mut self) {
        if let Some(_selected_idx) = self.column_selected {
            // Set the viewport height from the schema tree height
            let viewport_height = self.schema_tree_height;
            self.adjust_scroll_for_viewport(viewport_height);
        }
    }

    pub fn adjust_scroll_for_viewport(&mut self, viewport_height: usize) {
        if let Some(selected_idx) = self.column_selected {
            // If root (index 0) is selected, no scrolling needed
            if selected_idx == 0 {
                self.scroll_offset = 0;
                return;
            }
            
            // For items after root, adjust scroll considering root is always visible
            let effective_viewport = viewport_height.saturating_sub(1); // Account for root
            let relative_selected = selected_idx - 1; // Relative to items after root
            
            // Check if selection is above visible area (scroll up to show it)
            if relative_selected < self.scroll_offset {
                self.scroll_offset = relative_selected;
            } 
            // Check if selection is at or below the last visible position (scroll down)
            // Only scroll when selection goes beyond the last visible item
            else if relative_selected > self.scroll_offset + effective_viewport - 1 {
                self.scroll_offset = relative_selected.saturating_sub(effective_viewport - 1);
            }
        }
    }

    pub fn get_visible_schema_items(&self, viewport_height: usize) -> (Vec<&SchemaColumnType>, usize) {
        if self.schema_columns.is_empty() {
            return (vec![], 0);
        }

        let mut visible_items = Vec::new();
        
        // Always include the first item (root) at the top
        visible_items.push(&self.schema_columns[0]);
        
        if viewport_height <= 1 {
            return (visible_items, 0);
        }
        
        // Calculate how many more items we can show after the root
        let remaining_viewport = viewport_height - 1;
        
        // Start from item 1 (after root) and apply scroll offset
        let start_idx = 1 + self.scroll_offset;
        let end_idx = (start_idx + remaining_viewport).min(self.schema_columns.len());
        
        // Add the scrolled items after the root
        for i in start_idx..end_idx {
            visible_items.push(&self.schema_columns[i]);
        }
        
        (visible_items, 0) // Return 0 as offset since we're managing display differently
    }

    pub fn needs_scrollbar(&self, viewport_height: usize) -> bool {
        // Need scrollbar if we have more items than can fit, considering root is always visible
        self.schema_columns.len() > viewport_height
    }

    pub fn get_scrollbar_info(&self, viewport_height: usize) -> (usize, usize, usize) {
        let total_items = self.schema_columns.len();
        
        if total_items <= viewport_height {
            return (viewport_height, 0, viewport_height);
        }
        
        // Calculate scrollbar based on scrollable content (excluding always-visible root)
        let scrollable_items = total_items.saturating_sub(1);
        let effective_viewport = viewport_height.saturating_sub(1);
        
        let scrollbar_height = if scrollable_items == 0 { 
            viewport_height 
        } else { 
            (effective_viewport * viewport_height) / scrollable_items 
        }.max(1);
        
        let max_scroll_offset = scrollable_items.saturating_sub(effective_viewport);
        let scrollbar_position = if max_scroll_offset == 0 {
            0
        } else {
            (self.scroll_offset * (viewport_height - scrollbar_height)) / max_scroll_offset
        };
        
        (scrollbar_height, scrollbar_position, viewport_height)
    }
}