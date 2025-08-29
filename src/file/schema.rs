use std::collections::HashSet;

use parquet::basic::{LogicalType, TimeUnit};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::Type as ParquetType;

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub enum SchemaInfo {
    Root {
        name: String,
        display: String,
    },
    Primitive {
        name: String,
        display: String,
        info: ColumnSchemaInfo,
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
            );
        }

        Ok(FileSchema { columns: lines })
    }
}

fn traverse(
    node: &ParquetType,
    prefix: String,
    is_last: bool,
    lines: &mut Vec<SchemaInfo>,
    leaf_idx: &mut usize,
    summaries: &Vec<(String, String)>,
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
            );
        }
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
