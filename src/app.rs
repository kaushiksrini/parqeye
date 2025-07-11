use std::collections::HashMap;
use std::io;
use ratatui::DefaultTerminal;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};

use crate::schema::{SchemaColumnType, ColumnType};

#[derive(Debug, Default)]
pub struct App {
    pub file_name: String,
    pub exit: bool,
    pub tabs: Vec<&'static str>,
    pub active_tab: usize,
    pub column_selected: Option<usize>,
    pub schema_columns: Vec<SchemaColumnType>,
    pub schema_map: HashMap<String, ColumnType>,
}

impl App {
    pub fn run(&mut self, terminal: &mut DefaultTerminal, path: &str) -> io::Result<()> {
        self.file_name = path.to_string();
        self.tabs = vec!["Schema"];
        self.active_tab = 0;
        self.column_selected = None;
        
        let (schema_columns, schema_map) = crate::schema::build_schema_tree_lines(path)
            .map_err(| e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        self.schema_columns = schema_columns;
        self.schema_map = schema_map;
        
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

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            KeyCode::Right => {
                if self.active_tab + 1 < self.tabs.len() {
                    self.active_tab += 1;
                }
            }
            KeyCode::Left => {
                if self.active_tab > 0 {
                    self.active_tab -= 1;
                }
            }
            KeyCode::Down => {
                if self.active_tab == 0 {
                    let total_columns: usize = self.schema_columns.len();
                    if let Some(idx) = self.column_selected {
                        if idx + 1 < total_columns {
                            self.column_selected = Some(idx + 1);
                        }
                    } else {
                        self.column_selected = Some(1);
                    }
                }
            }
            KeyCode::Up => {
                if self.active_tab == 0 {
                    if let Some(idx) = self.column_selected {
                        if idx > 1 {
                            self.column_selected = Some(idx - 1);
                        } else {
                            self.column_selected = None
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }
}