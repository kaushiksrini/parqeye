use std::collections::HashSet;

use parquet::basic::{LogicalType, TimeUnit, Type as PhysicalType};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::Type as ParquetType;
use ratatui::{
    style::{Color, Stylize},
    widgets::{Cell, Row},
};

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub min: Option<String>,
    pub max: Option<String>,
    pub nulls: u64,
    pub distinct: Option<u64>,
    pub total_compressed_size: u64,
    pub total_uncompressed_size: u64,
}

#[derive(Clone)]
pub struct ColumnSchemaInfo {
    pub name: String,
    pub repetition: String,
    pub physical: String,
    pub logical: String,
    pub codec: String,
    pub converted_type: String,
    pub encoding: String,
    pub dictionary_values: Option<Vec<String>>,
}

#[derive(Clone)]
pub enum SchemaInfo {
    Root {
        name: String,
        display: String,
    },
    Primitive {
        name: String,
        display: String,
        info: ColumnSchemaInfo,
        stats: ColumnStats,
    },
    Group {
        name: String,
        display: String,
        repetition: String,
    },
}

// TODO: Add Dictionary Values
pub struct FileSchema {
    pub columns: Vec<SchemaInfo>,
}

impl FileSchema {
    pub fn from_metadata(md: &ParquetMetaData) -> Result<FileSchema, Box<dyn std::error::Error>> {
        let schema_descr: &parquet::schema::types::SchemaDescriptor =
            md.file_metadata().schema_descr();
        let root = schema_descr.root_schema();

        // Pre-compute codec + encoding summary for every leaf column
        let mut summaries: Vec<(String, String)> = Vec::new();
        for (col_idx, _) in schema_descr.columns().iter().enumerate() {
            // use std::collections::BTreeSet;
            let mut codecs: HashSet<String> = HashSet::new();
            let mut encs: HashSet<String> = HashSet::new();

            md.row_groups().iter().for_each(|rg| {
                let col_chunk = rg.column(col_idx);
                codecs.insert(format!("{:?}", col_chunk.compression()));
                encs.extend(col_chunk.encodings().iter().map(|enc| format!("{enc:?}")));
            });

            let codec_summary = codecs.into_iter().collect::<Vec<_>>().join(", ");
            let enc_summary = encs.into_iter().collect::<Vec<_>>().join(", ");

            summaries.push((codec_summary, enc_summary));
        }

        let mut lines: Vec<SchemaInfo> = Vec::new();
        lines.push(SchemaInfo::Root {
            name: "root".to_string(),
            display: "└─ root".to_string(),
        });

        let children = root.get_fields();
        let count = children.len();
        let mut leaf_idx: usize = 0;

        for (idx, child) in children.iter().enumerate() {
            traverse(
                child.as_ref(),
                "   ".to_string(),
                idx == count - 1,
                &mut lines,
                &mut leaf_idx,
                &summaries,
                md,
            );
        }

        Ok(FileSchema { columns: lines })
    }

    pub fn column_group_name(&self, index: usize) -> String {
        match self.columns.get(index).unwrap() {
            SchemaInfo::Primitive { name, .. } => name.clone(),
            SchemaInfo::Group { name, .. } => name.clone(),
            _ => unreachable!(),
        }
    }

    pub fn column_size(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| matches!(c, SchemaInfo::Primitive { .. }))
            .count()
    }

    pub fn tree_width(&self) -> usize {
        self.columns
            .iter()
            .map(|c| match c {
                SchemaInfo::Root { display, .. } => display.len(),
                SchemaInfo::Primitive { display, .. } => display.len(),
                SchemaInfo::Group { display, .. } => display.len(),
            })
            .max()
            .unwrap_or(0)
            .max(24) // max for the bottom of the chart
    }

    pub fn primitive_column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|c| matches!(c, SchemaInfo::Primitive { .. }))
            .map(|c| match c {
                SchemaInfo::Primitive { name, .. } => name.clone(),
                _ => unreachable!(),
            })
            .collect()
    }

    pub fn generate_table_rows(&self, selected_index: Option<usize>) -> Vec<Row> {
        let mut primitive_index = 1; // Start counting primitives from 1 (like app does)

        self.columns
            .iter()
            .filter_map(|col| {
                if let SchemaInfo::Primitive { info, stats, .. } = col {
                    let compression_ratio = if stats.total_uncompressed_size > 0 {
                        format!(
                            "{:.2}x",
                            stats.total_uncompressed_size as f64
                                / stats.total_compressed_size as f64
                        )
                    } else {
                        "N/A".to_string()
                    };

                    let is_selected = selected_index == Some(primitive_index);

                    let mut row = Row::new([
                        Cell::from(info.repetition.clone()),
                        Cell::from(info.physical.clone()),
                        Cell::from(format_size(stats.total_compressed_size)),
                        Cell::from(format_size(stats.total_uncompressed_size)),
                        Cell::from(compression_ratio),
                        Cell::from(info.encoding.clone()),
                        Cell::from(info.codec.clone()),
                        Cell::from(stats.min.clone().unwrap_or_else(|| "NULL".to_string())),
                        Cell::from(stats.max.clone().unwrap_or_else(|| "NULL".to_string())),
                        Cell::from(stats.nulls.to_string()),
                    ]);

                    if is_selected {
                        row = row.style(
                            ratatui::style::Style::default()
                                .bg(Color::Yellow)
                                .fg(Color::Black),
                        );
                    }

                    primitive_index += 1;
                    Some(row)
                } else if let SchemaInfo::Group { repetition, .. } = col {
                    let row = Row::new(vec![
                        Cell::from(repetition.clone().green()),
                        Cell::from("group".green()),
                    ]);
                    Some(row)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn generate_table_rows_with_columns(
        &self,
        selected_index: usize,
        start_col: usize,
        num_cols: usize,
    ) -> (Vec<Row>, Vec<usize>) {
        let mut primitive_index = 1; // Start counting primitives from 1 (like app does)
        let mut column_widths = vec![0usize; num_cols];

        let rows = self
            .columns
            .iter()
            .filter_map(|col| {
                if let SchemaInfo::Primitive { info, stats, .. } = col {
                    let compression_ratio = if stats.total_uncompressed_size > 0 {
                        format!(
                            "{:.2}x",
                            stats.total_uncompressed_size as f64
                                / stats.total_compressed_size as f64
                        )
                    } else {
                        "N/A".to_string()
                    };

                    let is_selected = selected_index > 0 && selected_index == primitive_index;

                    // Create all cells first
                    let all_cells = vec![
                        info.repetition.clone(),
                        info.physical.clone(),
                        format_size(stats.total_compressed_size),
                        format_size(stats.total_uncompressed_size),
                        compression_ratio,
                        info.encoding.clone(),
                        info.codec.clone(),
                        stats.min.clone().unwrap_or_else(|| "NULL".to_string()),
                        stats.max.clone().unwrap_or_else(|| "NULL".to_string()),
                        stats.nulls.to_string(),
                    ];

                    // Select only the visible columns and track their content lengths
                    let visible_cell_contents: Vec<_> = all_cells
                        .into_iter()
                        .skip(start_col)
                        .take(num_cols)
                        .collect();

                    // Update column widths with the maximum seen so far
                    for (col_idx, content) in visible_cell_contents.iter().enumerate() {
                        column_widths[col_idx] = column_widths[col_idx].max(content.len());
                    }

                    // Create cells from the content
                    let visible_cells: Vec<_> =
                        visible_cell_contents.into_iter().map(Cell::from).collect();

                    let mut row = Row::new(visible_cells);

                    if is_selected {
                        row = row.style(
                            ratatui::style::Style::default()
                                .bg(Color::Yellow)
                                .fg(Color::Black),
                        );
                    }

                    primitive_index += 1;
                    Some(row)
                } else if let SchemaInfo::Group { repetition, .. } = col {
                    let all_cells = vec![
                        repetition.clone(),
                        "group".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                    ];

                    let visible_cell_contents: Vec<_> = all_cells
                        .into_iter()
                        .skip(start_col)
                        .take(num_cols)
                        .collect();

                    // Update column widths with the maximum seen so far
                    for (col_idx, content) in visible_cell_contents.iter().enumerate() {
                        column_widths[col_idx] = column_widths[col_idx].max(content.len());
                    }

                    let visible_cells: Vec<_> = visible_cell_contents
                        .into_iter()
                        .enumerate()
                        .map(|(idx, content)| {
                            if idx == 0 {
                                Cell::from(content.green())
                            } else if idx == 1 {
                                Cell::from(content.green())
                            } else {
                                Cell::from(content)
                            }
                        })
                        .collect();

                    let row = Row::new(visible_cells);
                    Some(row)
                } else {
                    None
                }
            })
            .collect();

        (rows, column_widths)
    }
}

fn traverse(
    node: &ParquetType,
    prefix: String,
    is_last: bool,
    lines: &mut Vec<SchemaInfo>,
    leaf_idx: &mut usize,
    summaries: &Vec<(String, String)>,
    md: &ParquetMetaData,
) {
    let connector: &'static str = if is_last { "└─" } else { "├─" };
    let line = format!("{}{} {}", prefix, connector, node.name());

    if node.is_primitive() {
        let repetition = format!("{:?}", node.get_basic_info().repetition());
        let physical = format!("{:?}", node.get_physical_type());
        let logical = match node.get_basic_info().logical_type() {
            Some(logical_type) => logical_type_to_string(&logical_type),
            None => String::new(),
        };

        let (codec_sum, enc_sum) = &summaries[*leaf_idx];
        let stats = aggregate_column_stats(md, *leaf_idx, node.get_physical_type());
        let info = ColumnSchemaInfo {
            name: node.name().to_string(),
            repetition: repetition.clone(),
            physical: physical.clone(),
            logical: logical.clone(),
            codec: codec_sum.clone(),
            encoding: enc_sum.clone(),
            converted_type: node.get_basic_info().converted_type().to_string(),
            dictionary_values: None,
        };
        lines.push(SchemaInfo::Primitive {
            name: node.name().to_string(),
            display: line,
            info,
            stats,
        });

        *leaf_idx += 1;
    } else {
        lines.push(SchemaInfo::Group {
            name: node.name().to_string(),
            display: line,
            repetition: format!("{:?}", node.get_basic_info().repetition()),
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
                leaf_idx,
                summaries,
                md,
            );
        }
    }
}

/// Efficiently aggregate column statistics across all row groups
fn aggregate_column_stats(
    md: &ParquetMetaData,
    col_idx: usize,
    physical: PhysicalType,
) -> ColumnStats {
    let (min_bytes, max_bytes, nulls, distinct, total_compressed_size, total_uncompressed_size) =
        md.row_groups().iter().fold(
            (
                None::<Vec<u8>>,
                None::<Vec<u8>>,
                0u64,
                None::<u64>,
                0u64,
                0u64,
            ),
            |(
                mut min_bytes,
                mut max_bytes,
                mut nulls,
                mut distinct,
                mut compressed,
                mut uncompressed,
            ),
             rg| {
                let col_meta = rg.column(col_idx);
                if let Some(stats) = col_meta.statistics() {
                    nulls += stats.null_count_opt().unwrap_or(0);
                    distinct =
                        Some(distinct.unwrap_or(0) + stats.distinct_count_opt().unwrap_or(0));

                    if let Some(min_b) = stats.min_bytes_opt() {
                        if min_bytes.as_ref().map_or(true, |mb| min_b < &mb[..]) {
                            min_bytes = Some(min_b.to_vec());
                        }
                    }
                    if let Some(max_b) = stats.max_bytes_opt() {
                        if max_bytes.as_ref().map_or(true, |mb| max_b > &mb[..]) {
                            max_bytes = Some(max_b.to_vec());
                        }
                    }
                }
                compressed += col_meta.compressed_size() as u64;
                uncompressed += col_meta.uncompressed_size() as u64;
                (
                    min_bytes,
                    max_bytes,
                    nulls,
                    distinct,
                    compressed,
                    uncompressed,
                )
            },
        );

    ColumnStats {
        min: min_bytes.as_deref().map(|b| decode_value(b, physical)),
        max: max_bytes.as_deref().map(|b| decode_value(b, physical)),
        nulls,
        distinct,
        total_compressed_size,
        total_uncompressed_size,
    }
}

/// Decode raw statistics bytes into a readable value based on the physical type
fn decode_value(bytes: &[u8], physical: PhysicalType) -> String {
    match physical {
        PhysicalType::INT32 if bytes.len() == 4 => {
            i32::from_le_bytes(bytes.try_into().unwrap()).to_string()
        }
        PhysicalType::INT64 if bytes.len() == 8 => {
            i64::from_le_bytes(bytes.try_into().unwrap()).to_string()
        }
        PhysicalType::FLOAT if bytes.len() == 4 => {
            format!("{:.4}", f32::from_le_bytes(bytes.try_into().unwrap()))
        }
        PhysicalType::DOUBLE if bytes.len() == 8 => {
            format!("{:.4}", f64::from_le_bytes(bytes.try_into().unwrap()))
        }
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => std::str::from_utf8(bytes)
            .map_or_else(
                |_| {
                    bytes
                        .iter()
                        .map(|b| format!("{b:02X}"))
                        .collect::<Vec<_>>()
                        .join("")
                },
                |s| s.to_string(),
            ),
        _ => bytes
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(""),
    }
}

fn logical_type_to_string(logical_type: &LogicalType) -> String {
    match logical_type {
        LogicalType::Decimal { scale, precision } => {
            format!("Decimal({scale},{precision})")
        }
        LogicalType::Integer {
            bit_width,
            is_signed,
        } => format!(
            "Integer({bit_width},{})",
            if *is_signed { "sign" } else { "unsign" }
        ),
        LogicalType::Time {
            is_adjusted_to_u_t_c,
            unit,
        } => match unit {
            TimeUnit::MILLIS(_) => format!(
                "Time({}, millis)",
                if *is_adjusted_to_u_t_c {
                    "utc"
                } else {
                    "local"
                }
            ),
            TimeUnit::MICROS(_) => format!(
                "Time({}, micros)",
                if *is_adjusted_to_u_t_c {
                    "utc"
                } else {
                    "local"
                }
            ),
            TimeUnit::NANOS(_) => format!(
                "Time({}, nanos)",
                if *is_adjusted_to_u_t_c {
                    "utc"
                } else {
                    "local"
                }
            ),
        },
        LogicalType::Timestamp {
            is_adjusted_to_u_t_c,
            unit,
        } => match unit {
            TimeUnit::MILLIS(_) => format!(
                "Timestamp({}, millis)",
                if *is_adjusted_to_u_t_c {
                    "utc"
                } else {
                    "local"
                }
            ),
            TimeUnit::MICROS(_) => format!(
                "Timestamp({}, micros)",
                if *is_adjusted_to_u_t_c {
                    "utc"
                } else {
                    "local"
                }
            ),
            TimeUnit::NANOS(_) => format!(
                "Timestamp({}, nanos)",
                if *is_adjusted_to_u_t_c {
                    "utc"
                } else {
                    "local"
                }
            ),
        },
        _ => format!("{:?}", logical_type),
    }
}

/// Format byte size into human-readable format
fn format_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}
