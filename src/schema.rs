use crossterm::event::{KeyCode, KeyEvent};
use parquet::basic::{LogicalType, TimeUnit};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::Type as ParquetType;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use crate::tab::Tab;
use crate::ui::render_schema_tab;
use crate::utils::adjust_scroll_for_selection;

pub struct SchemaTab {
    pub row_offset: usize,
    pub col_offset: usize,
    pub scroll_offset: usize,

    pub column_selected: Option<usize>,
    pub schema_columns: Vec<SchemaColumnType>,
}

impl SchemaTab {
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

impl Tab for SchemaTab {
    fn on_focus(&mut self) {
        self.col_offset = 0;
        self.row_offset = 0;
        self.scroll_offset = 0;
        self.column_selected = None;
    }
    
    fn on_event(&mut self, key_event: KeyEvent, app: &mut crate::App) {
        match key_event.code {
            KeyCode::Down => {
                let total_columns: usize = self.schema_columns.len();
                if let Some(idx) = self.column_selected {
                    if idx + 1 < total_columns {
                        self.column_selected = Some(idx + 1);
                    }
                } else {
                    self.column_selected = Some(1);
                }
                adjust_scroll_for_selection(self.column_selected, app.schema_tree_height);
            } 
            KeyCode::Up => {
                let total_columns: usize = self.schema_columns.len();
                if let Some(idx) = self.column_selected {
                    if idx + 1 < total_columns {
                        self.column_selected = Some(idx - 1);
                    }
                } else {
                    self.column_selected = Some(1);
                }
                adjust_scroll_for_selection(self.column_selected, app.schema_tree_height);
            }
            KeyCode::PageDown => {
                self.scroll_down(2, app);
            }
            KeyCode::PageUp => {
                self.scroll_up(2);
            }
            _ => {}
        }
    }
    
    fn render(&mut self, app: &mut crate::App, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer) {
        render_schema_tab(app, area, buf, self);
    }
    
}

#[derive(Debug, Clone)]
pub struct ColumnSchemaInfo {
    pub name: String,
    pub repetition: String,
    pub physical: String,
    pub logical: String,
    pub codec: String,
    pub converted_type: String,
    pub encoding: String,
}

#[derive(Debug, Clone)]
pub enum ColumnType {
    Primitive(ColumnSchemaInfo),
    Group(String),
}

#[derive(Debug, Clone)]
pub enum SchemaColumnType {
    Root { name: String, display: String },
    Primitive { name: String, display: String },
    Group { name: String, display: String },
}

pub fn build_schema_tree_lines(
    file_name: &str,
) -> Result<(Vec<SchemaColumnType>, HashMap<String, ColumnType>), Box<dyn std::error::Error>> {
    let file = File::open(Path::new(file_name))?;
    let reader: SerializedFileReader<File> = SerializedFileReader::new(file)?;
    let md = reader.metadata();
    let schema_descr = md.file_metadata().schema_descr();
    let root = schema_descr.root_schema();

    // Pre-compute codec + encoding summary for every leaf column
    let mut leaf_summaries: Vec<(String, String)> = Vec::new();
    for (col_idx, _) in schema_descr.columns().iter().enumerate() {
        use std::collections::HashSet;
        let mut codecs: HashSet<String> = HashSet::new();
        let mut encs: HashSet<String> = HashSet::new();

        for rg in md.row_groups() {
            let col_chunk = rg.column(col_idx);
            codecs.insert(format!("{:?}", col_chunk.compression()));
            for enc in col_chunk.encodings() {
                encs.insert(format!("{enc:?}"));
            }
        }
        let mut codec_vec: Vec<String> = codecs.into_iter().collect();
        codec_vec.sort();
        let codec_summary = codec_vec.join(", ");

        let mut enc_vec: Vec<String> = encs.into_iter().collect();
        enc_vec.sort();
        let enc_summary = enc_vec.join(", ");

        leaf_summaries.push((codec_summary, enc_summary));
    }

    fn traverse(
        node: &ParquetType,
        prefix: String,
        is_last: bool,
        lines: &mut Vec<SchemaColumnType>,
        map: &mut HashMap<String, ColumnType>,
        leaf_idx: &mut usize,
        summaries: &Vec<(String, String)>,
    ) {
        let connector: &'static str = if is_last { "└─" } else { "├─" };
        let line = format!("{}{} {}", prefix, connector, node.name());

        if node.is_primitive() {
            let repetition = format!("{:?}", node.get_basic_info().repetition());
            let physical = format!("{:?}", node.get_physical_type());
            let logical = match node.get_basic_info().logical_type() {
                Some(logical) => match logical {
                    LogicalType::Decimal { scale, precision } => {
                        format!("Decimal({scale},{precision})")
                    }
                    LogicalType::Integer {
                        bit_width,
                        is_signed,
                    } => format!(
                        "Integer({bit_width},{})",
                        if is_signed { "sign" } else { "unsign" }
                    ),
                    LogicalType::Time {
                        is_adjusted_to_u_t_c,
                        unit,
                    } => match unit {
                        TimeUnit::MILLIS(_) => format!(
                            "Time({}, millis)",
                            if is_adjusted_to_u_t_c { "utc" } else { "local" }
                        ),
                        TimeUnit::MICROS(_) => format!(
                            "Time({}, micros)",
                            if is_adjusted_to_u_t_c { "utc" } else { "local" }
                        ),
                        TimeUnit::NANOS(_) => format!(
                            "Time({}, nanos)",
                            if is_adjusted_to_u_t_c { "utc" } else { "local" }
                        ),
                    },
                    LogicalType::Timestamp {
                        is_adjusted_to_u_t_c,
                        unit,
                    } => match unit {
                        TimeUnit::MILLIS(_) => format!(
                            "Timestamp({}, millis)",
                            if is_adjusted_to_u_t_c { "utc" } else { "local" }
                        ),
                        TimeUnit::MICROS(_) => format!(
                            "Timestamp({}, micros)",
                            if is_adjusted_to_u_t_c { "utc" } else { "local" }
                        ),
                        TimeUnit::NANOS(_) => format!(
                            "Timestamp({}, nanos)",
                            if is_adjusted_to_u_t_c { "utc" } else { "local" }
                        ),
                    },
                    _ => format!("{logical:?}"),
                },
                None => String::new(),
            };

            let (codec_sum, enc_sum) = &summaries[*leaf_idx];
            let column_info = ColumnSchemaInfo {
                name: node.name().to_string(),
                repetition: repetition.clone(),
                physical: physical.clone(),
                logical: logical.clone(),
                codec: codec_sum.clone(),
                encoding: enc_sum.clone(),
                converted_type: node.get_basic_info().converted_type().to_string(),
            };
            map.insert(line.clone(), ColumnType::Primitive(column_info));
            lines.push(SchemaColumnType::Primitive {
                name: node.name().to_string(),
                display: line,
            });

            *leaf_idx += 1;
        } else {
            let repetition = format!("{:?}", node.get_basic_info().repetition());
            map.insert(line.clone(), ColumnType::Group(repetition));
            lines.push(SchemaColumnType::Group {
                name: node.name().to_string(),
                display: line,
            });
        }

        if node.is_group() {
            let fields = node.get_fields();
            let count = fields.len();
            for (idx, child) in fields.iter().enumerate() {
                let next_prefix = format!("{}{}", prefix, if is_last { "   " } else { "│  " });
                traverse(
                    child.as_ref(),
                    next_prefix,
                    idx == count - 1,
                    lines,
                    map,
                    leaf_idx,
                    summaries,
                );
            }
        }
    }

    let mut lines: Vec<SchemaColumnType> = Vec::new();
    lines.push(SchemaColumnType::Root {
        name: "root".to_string(),
        display: "└─ root".to_string(),
    });

    let mut column_to_type: HashMap<String, ColumnType> = HashMap::new();
    let children = root.get_fields();
    let count = children.len();
    let mut leaf_idx: usize = 0;

    for (idx, child) in children.iter().enumerate() {
        traverse(
            child.as_ref(),
            "   ".to_string(),
            idx == count - 1,
            &mut lines,
            &mut column_to_type,
            &mut leaf_idx,
            &leaf_summaries,
        );
    }

    Ok((lines, column_to_type))
}
