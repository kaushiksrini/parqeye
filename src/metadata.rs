use comfy_table::{Table, presets::UTF8_FULL, modifiers::UTF8_ROUND_CORNERS, Cell, Color};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use std::collections::{HashSet, HashMap};
use crate::utils::human_readable_bytes;


/// Convert a plain count into a human-readable string with K / M / B suffixes.
fn human_readable_count(n: u64) -> String {
    const UNITS: [&str; 4] = ["", "K", "M", "B"]; // up to billions
    let mut unit = 0;
    let mut value = n as f64;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}", n)
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

/// Truncate a string to `max` characters, adding an ellipsis if needed.
fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() > max {
        let truncated: String = s.chars().take(max - 1).collect();
        format!("{}…", truncated)
    } else {
        s.to_string()
    }
}

/// Format an integer with comma thousands separators (e.g. 52314009 -> "52,314,009").
fn commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len()/3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

/// Prints a summary table of all the fields you asked for.
pub fn print_metadata_table(path: &str) -> anyhow::Result<()> {
    let file = match File::open(&Path::new(path)) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open file: {}", e);
            Err(e)
        }?
    };

    // Create a Parquet file reader
    let reader: SerializedFileReader<File> = match SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to read Parquet file: {}", e);
            Err(e)
        }?
    };

    let md = reader.metadata();

    let binding = md.file_metadata().created_by();
    let version        = md.file_metadata().version();
    let created_by     = binding.as_deref().unwrap_or("—");
    let row_groups     = md.num_row_groups();
    let total_rows: i64 = md.row_groups().iter().map(|rg| rg.num_rows()).sum();
    let num_cols       = md.file_metadata().schema_descr().num_columns();

    // --------------------------------------------------
    // Column-chunk level aggregation
    // --------------------------------------------------
    let mut raw_size: u64 = 0;
    let mut compressed_size: u64 = 0;
    let mut encodings_seen: HashSet<String> = HashSet::new();
    let mut codec_counts: HashMap<String, usize> = HashMap::new();

    let mut rows_per_rg: Vec<i64> = Vec::with_capacity(row_groups as usize);

    for rg in md.row_groups() {
        rows_per_rg.push(rg.num_rows());
        for col in rg.columns() {
            raw_size += col.uncompressed_size() as u64;
            compressed_size += col.compressed_size() as u64;

            let codec_name = format!("{:?}", col.compression());
            *codec_counts.entry(codec_name).or_insert(0) += 1;

            for enc in col.encodings() {
                encodings_seen.insert(format!("{:?}", enc));
            }
        }
    }

    // Row-group stats
    let avg_rows = (total_rows as f64) / (row_groups as f64);
    let min_rows = rows_per_rg.iter().min().copied().unwrap_or(0);
    let max_rows = rows_per_rg.iter().max().copied().unwrap_or(0);

    // Size & compression ratio
    let raw_size_hr        = human_readable_bytes(raw_size);
    let compressed_size_hr = human_readable_bytes(compressed_size);
    let compression_ratio  = if compressed_size > 0 { raw_size as f64 / compressed_size as f64 } else { 0.0 };

    // Codec summary with counts
    let mut codec_vec: Vec<String> = codec_counts.iter()
        .map(|(c, n)| format!("{}({})", c, n))
        .collect();
    codec_vec.sort();
    let codec_summary = codec_vec.join("  ");

    // Encoding summary
    let mut encodings: Vec<String> = encodings_seen.into_iter().collect();
    encodings.sort();
    let encodings_summary = encodings.join(", ");

    // Average row size
    let avg_row_size = if total_rows > 0 { raw_size as f64 / total_rows as f64 } else { 0.0 };

    // User metadata (key-value pairs)
    let user_meta_full = md.file_metadata().key_value_metadata()
        .map(|kvs| kvs.iter().map(|kv| kv.key.clone()).collect::<Vec<_>>().join(", "))
        .unwrap_or_else(|| "—".to_string());

    let user_meta = truncate_str(&user_meta_full, 100);

    // Build a comfy-table
    // Build the table
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)              // UTF-8 borders
        .apply_modifier(UTF8_ROUND_CORNERS)  // rounded corners
        // header styling
        .set_header(vec![
            Cell::new("Attribute").fg(Color::Green).add_attribute(comfy_table::Attribute::Bold),
            Cell::new("Value").fg(Color::Yellow).add_attribute(comfy_table::Attribute::Bold),
        ]);

    // data rows
    for (attr, val) in &[
        ("File",                Path::new(path).file_name().unwrap_or_default().to_string_lossy().to_string()),
        ("Format version",      format!("{}  ({})", version, created_by)),
        ("Rows",                commas(total_rows as u64)),
        ("Columns",             num_cols.to_string()),
        ("Row groups",          format!("{}  (avg {}, min {}, max {})", row_groups, human_readable_count(avg_rows.round() as u64), human_readable_count(min_rows as u64), human_readable_count(max_rows as u64))),
        ("Size (raw)",          format!("{}", raw_size_hr)),
        ("Size (compressed)",   format!("{}  (ratio {:.1}×)", compressed_size_hr, compression_ratio)),
        ("Codecs (cols)",       codec_summary),
        ("Encodings",    encodings_summary),
        ("Avg row size",        format!("{:.0} B", avg_row_size)),
        ("User metadata (keys)",       user_meta),
    ] {
        table.add_row(vec![
            Cell::new(attr).fg(Color::Cyan),
            Cell::new(val),
        ]);
    }

    // print it out
    println!("\n{}", table);

    Ok(())
}