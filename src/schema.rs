// src/schema.rs

use anyhow::Result;
use comfy_table::{
    Cell, Color, Table,
    presets::UTF8_FULL,
    modifiers::UTF8_ROUND_CORNERS,
    Attribute,
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::ParquetMetaData;

/// Read the file and return the Parquet reader
fn open_reader(path: &str) -> Result<SerializedFileReader<File>> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    Ok(reader)
}

/// Print a table of the schema: one row per leaf column descriptor.
pub fn print_schema_table(path: &str, show_stats: bool) -> Result<()> {
    // 1) open and get metadata
    let reader = open_reader(path)?;
    let md = reader.metadata();
    let schema = md.file_metadata().schema_descr();

    // 2) build comfy-table
    let mut table = Table::new();
    table.load_preset(UTF8_FULL).apply_modifier(UTF8_ROUND_CORNERS);

    // Build header cells dynamically based on flag
    let mut header_cells: Vec<Cell> = vec![
        Cell::new("#")
            .fg(Color::Green)
            .add_attribute(Attribute::Bold),
        Cell::new("Column Path")
            .fg(Color::Green)
            .add_attribute(Attribute::Bold),
        Cell::new("Physical Type")
            .fg(Color::Green)
            .add_attribute(Attribute::Bold),
        Cell::new("Logical Type")
            .fg(Color::Green)
            .add_attribute(Attribute::Bold),
        Cell::new("Max Rep/Def\nLevel")
            .fg(Color::Green)
            .add_attribute(Attribute::Bold),
        Cell::new("Codec")
            .fg(Color::Green)
            .add_attribute(Attribute::Bold),
        Cell::new("Encodings")
            .fg(Color::Green)
            .add_attribute(Attribute::Bold),
    ];

    if show_stats {
        header_cells.push(
            Cell::new("Stats")
                .fg(Color::Green)
                .add_attribute(Attribute::Bold),
        );
    }

    table.set_header(header_cells);

    // 3) iterate leaf columns
    for (i, col) in schema.columns().iter().enumerate() {
        let path = col.path();
        let physical_ty = col.physical_type();
        let physical = format!("{:?}", physical_ty);
        let logical = col
            .logical_type()
            .map(|lt| format!("{:?}", lt))
            .unwrap_or_else(|| "—".to_string());
        let repetition = format!("{:?}", col.max_rep_level());
        let definition = format!("{:?}", col.max_def_level());

        // Stats computation based on flag
        let stats = if show_stats {
            Some(stats_summary(&md, i, physical_ty))
        } else {
            None
        };

        // Gather codec and encoding summary for this column
        let (codec_summary, enc_summary) = codec_and_encoding_summary(&md, i);

        // Build row dynamically
        let mut row_cells: Vec<Cell> = vec![
            Cell::new(i).fg(Color::Cyan),
            Cell::new(path).fg(Color::Cyan),
            Cell::new(physical),
            Cell::new(logical),
            Cell::new(format!("{} / {}", repetition, definition)),
            Cell::new(codec_summary),
            Cell::new(enc_summary),
        ];

        if let Some(stats_val) = stats {
            row_cells.push(Cell::new(stats_val));
        }

        table.add_row(row_cells);
    }

    // 4) print it
    println!("\n{}", table);
    Ok(())
}

// ------------------------------------------------------------
// Helper functions for statistics
// ------------------------------------------------------------

/// Decode raw Parquet statistics bytes into a readable string based on the physical type.
fn decode_value(bytes: &[u8], physical: PhysicalType) -> String {
    match physical {
        PhysicalType::INT32 if bytes.len() == 4 => {
            let v = i32::from_le_bytes(bytes.try_into().unwrap());
            format!("{}", v)
        }
        PhysicalType::INT64 if bytes.len() == 8 => {
            let v: i64 = i64::from_le_bytes(bytes.try_into().unwrap());
            format!("{}", v)
        }
        PhysicalType::FLOAT if bytes.len() == 4 => {
            let v = f32::from_le_bytes(bytes.try_into().unwrap());
            format!("{:.4}", v)
        }
        PhysicalType::DOUBLE if bytes.len() == 8 => {
            let v = f64::from_le_bytes(bytes.try_into().unwrap());
            format!("{:.4}", v)
        }
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => match std::str::from_utf8(bytes) {
            Ok(s) => s.to_string(),
            Err(_) => bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(""),
        },
        _ => bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(""),
    }
}

/// Summarise statistics (min/max/null/distinct) for a column across all row groups.
fn stats_summary(md: &ParquetMetaData, col_idx: usize, physical: PhysicalType) -> String {

    let mut min_bytes: Option<Vec<u8>> = None;
    let mut max_bytes: Option<Vec<u8>> = None;
    let mut nulls: u64 = 0;
    let mut distinct: Option<u64> = None;

    for rg in md.row_groups() {
        let col_meta = rg.column(col_idx);
        if let Some(stats) = col_meta.statistics() {
            if let Some(n) = stats.null_count_opt() {
                nulls += n as u64;
            }
            if let Some(d) = stats.distinct_count_opt() {
                distinct = Some(distinct.unwrap_or(0) + d as u64);
            }
            if let Some(min) = stats.min_bytes_opt() {
                if min_bytes.is_none() || min < &min_bytes.as_ref().unwrap()[..] {
                    min_bytes = Some(min.to_vec());
                }
            }
            if let Some(max) = stats.max_bytes_opt() {
                if max_bytes.is_none() || max > &max_bytes.as_ref().unwrap()[..] {
                    max_bytes = Some(max.to_vec());
                }
            }
        }
    }

    let min_str = min_bytes
        .as_deref()
        .map(|b: &[u8]| decode_value(b, physical))
        .unwrap_or_else(|| "—".to_string());
    let max_str = max_bytes
        .as_deref()
        .map(|b| decode_value(b, physical))
        .unwrap_or_else(|| "—".to_string());

    if let Some(dist) = distinct {
        format!("min={}, max={}, nulls={}, distinct={}", min_str, max_str, nulls, dist)
    } else {
        format!("min={}, max={}, nulls={}", min_str, max_str, nulls)
    }
}

/// Summarise codec and encodings used for a column across all row groups.
fn codec_and_encoding_summary(md: &ParquetMetaData, col_idx: usize) -> (String, String) {
    use std::collections::HashSet;

    let mut codecs: HashSet<String> = HashSet::new();
    let mut encs: HashSet<String> = HashSet::new();

    for rg in md.row_groups() {
        let col = rg.column(col_idx);
        codecs.insert(format!("{:?}", col.compression()));
        for enc in col.encodings() {
            encs.insert(format!("{:?}", enc));
        }
    }

    let mut codec_vec: Vec<String> = codecs.into_iter().collect();
    codec_vec.sort();
    let codec_str = codec_vec.join(", ");

    let mut enc_vec: Vec<String> = encs.into_iter().collect();
    enc_vec.sort();
    let enc_str = enc_vec.join(", ");

    (codec_str, enc_str)
}