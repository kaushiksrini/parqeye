use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::DefaultTerminal;
use std::collections::HashMap;
use std::io;

use crate::column_chunk::RowGroupStats;
use crate::schema::{ColumnType, SchemaColumnType};
use crate::tab::{TabId, Tab};

#[derive(Default)]
pub struct App {
    pub file_name: String,
    pub exit: bool,
    pub tabs: HashMap<TabId, Box<dyn Tab>>,
    pub active_tab: TabId,
    pub schema_columns: Vec<SchemaColumnType>,
    pub schema_map: HashMap<String, ColumnType>,
    pub row_group_selected: usize,
    pub schema_tree_height: usize,
    pub row_group_stats: Vec<RowGroupStats>,
    pub primitive_columns_idx: HashMap<String, usize>,
}

impl App {
    pub fn run(&mut self, terminal: &mut DefaultTerminal, path: &str) -> io::Result<()> {
        self.file_name = path.to_string();
        self.active_tab = TabId::Schema;
        self.row_group_selected = 0;

        let (schema_columns, schema_map) = crate::schema::build_schema_tree_lines(path)
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.schema_columns = schema_columns;
        self.schema_map = schema_map;

        for (idx, c) in self
            .schema_columns
            .iter()
            .filter(|c| matches!(c, SchemaColumnType::Primitive { .. }))
            .enumerate()
        {
            if let SchemaColumnType::Primitive { name, .. } = c {
                self.primitive_columns_idx.insert(name.clone(), idx);
            }
        }

        // calculate row group stats
        self.row_group_stats = crate::column_chunk::calculate_row_group_stats(path)
            .map_err(|e| io::Error::other(e.to_string()))?;

        // Initialize tabs
        self.tabs.insert(TabId::Schema, Box::new(crate::schema::SchemaTab {
            row_offset: 0,
            col_offset: 0,
            scroll_offset: 0,
            column_selected: None,
            schema_columns: self.schema_columns.clone(),
        }));
        self.tabs.insert(TabId::RowGroups, Box::new(crate::row_groups::RowGroupsTab::new()));
        self.tabs.insert(TabId::Visualize, Box::new(crate::visualize::VisualizeTab::new(50)));

        while !self.exit {
            terminal.draw(|frame| crate::ui::render_app(self, frame))?;
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

    pub fn set_schema_tree_height(&mut self, new_height: usize) {
        self.schema_tree_height = new_height;
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            _ => {}
        }
        
        let id = self.active_tab;
        // println!("DEBUG: Active tab: {:?}", id);
        // println!("DEBUG: Available tabs: {:?}", self.tabs.keys().collect::<Vec<_>>());
        
        if let Some(mut tab) = self.tabs.remove(&id) {
            tab.on_event(key_event, self);
            self.tabs.insert(id, tab);
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }

    pub fn get_visible_schema_items(
        &self,
        viewport_height: usize,
        scroll_offset: usize
    ) -> (Vec<&SchemaColumnType>, usize) {
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
        let start_idx = 1 + scroll_offset;
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

    pub fn get_scrollbar_info(&self, viewport_height: usize, scroll_offset: usize) -> (usize, usize, usize) {
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
        }
        .max(1);

        let max_scroll_offset = scrollable_items.saturating_sub(effective_viewport);
        let scrollbar_position = if max_scroll_offset == 0 {
            0
        } else {
            (scroll_offset * (viewport_height - scrollbar_height)) / max_scroll_offset
        };

        (scrollbar_height, scrollbar_position, viewport_height)
    }
}
