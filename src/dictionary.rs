use parquet::basic::{PageType, Type as PhysicalType};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;

// -----------------------------------------------------------------------------
// Helpers: discovery and parsing
// -----------------------------------------------------------------------------

fn physical_type_of(reader: &SerializedFileReader<File>, col_idx: usize) -> PhysicalType {
    reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .column(col_idx)
        .physical_type()
}

/// Collects all dictionary pages for the given column across row groups.
/// Copies page buffers so they can be parsed independently of the reader lifetimes.
fn collect_dictionary_pages(
    reader: &SerializedFileReader<File>,
    col_idx: usize,
) -> Result<Vec<(Vec<u8>, usize)>, Box<dyn std::error::Error>> {
    let mut pages: Vec<(Vec<u8>, usize)> = Vec::new();

    for rg_idx in 0..reader.num_row_groups() {
        let rg_reader = reader.get_row_group(rg_idx)?;
        let mut page_reader = rg_reader.get_column_page_reader(col_idx)?;

        while let Some(page) = page_reader.get_next_page()? {
            if page.page_type() == PageType::DICTIONARY_PAGE {
                let num_values = page.num_values() as usize;
                let buffer: Vec<u8> = page.buffer().to_vec();
                pages.push((buffer, num_values));
            }
        }
    }

    Ok(pages)
}

fn parse_dictionary_page(
    physical_type: PhysicalType,
    buffer: &[u8],
    num_values: usize,
    max_items: usize,
) -> Vec<String> {
    match physical_type {
        PhysicalType::BYTE_ARRAY => parse_byte_array_plain(buffer, num_values, max_items),
        PhysicalType::INT32 => parse_int32_plain(buffer, num_values, max_items),
        PhysicalType::INT64 => parse_int64_plain(buffer, num_values, max_items),
        PhysicalType::INT96 => parse_int96_plain(buffer, num_values, max_items),
        PhysicalType::FLOAT => parse_float_plain(buffer, num_values, max_items),
        PhysicalType::DOUBLE => parse_double_plain(buffer, num_values, max_items),
        _ => vec![unknown_as_hex_preview(physical_type, buffer)],
    }
}

fn parse_byte_array_plain(buffer: &[u8], num_values: usize, max_items: usize) -> Vec<String> {
    // BYTE_ARRAY PLAIN encoding: [length:4][data:length]...
    let mut values: Vec<String> = Vec::new();
    let mut offset: usize = 0;
    for _ in 0..num_values.min(max_items) {
        if offset + 4 > buffer.len() {
            break;
        }
        let length = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + length > buffer.len() {
            break;
        }

        let string_data = &buffer[offset..offset + length];
        match std::str::from_utf8(string_data) {
            Ok(s) => values.push(s.to_string()),
            Err(_) => {
                let hex = string_data
                    .iter()
                    .take(8)
                    .map(|b| format!("{b:02X}"))
                    .collect::<Vec<_>>()
                    .join("");
                values.push(format!("0x{hex}"));
            }
        }
        offset += length;
        if values.len() >= max_items {
            break;
        }
    }
    values
}

fn parse_int32_plain(buffer: &[u8], num_values: usize, max_items: usize) -> Vec<String> {
    let max_vals = (buffer.len() / 4).min(num_values).min(max_items);
    let mut values = Vec::with_capacity(max_vals);
    for i in 0..max_vals {
        let offset = i * 4;
        if offset + 4 <= buffer.len() {
            let value = i32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]);
            values.push(value.to_string());
        }
    }
    values
}

fn parse_int64_plain(buffer: &[u8], num_values: usize, max_items: usize) -> Vec<String> {
    let max_vals = (buffer.len() / 8).min(num_values).min(max_items);
    let mut values = Vec::with_capacity(max_vals);
    for i in 0..max_vals {
        let offset = i * 8;
        if offset + 8 <= buffer.len() {
            let value = i64::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
                buffer[offset + 4],
                buffer[offset + 5],
                buffer[offset + 6],
                buffer[offset + 7],
            ]);
            values.push(value.to_string());
        }
    }
    values
}

fn parse_int96_plain(buffer: &[u8], num_values: usize, max_items: usize) -> Vec<String> {
    // INT96 PLAIN encoding: [value:12][value:12]...
    let max_vals = (buffer.len() / 12).min(num_values).min(max_items);
    let mut values = Vec::with_capacity(max_vals);
    for i in 0..max_vals {
        let offset = i * 12;
        if offset + 12 <= buffer.len() {
            let nanos = u64::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
                buffer[offset + 4],
                buffer[offset + 5],
                buffer[offset + 6],
                buffer[offset + 7],
            ]);
            let julian_day = u32::from_le_bytes([
                buffer[offset + 8],
                buffer[offset + 9],
                buffer[offset + 10],
                buffer[offset + 11],
            ]);
            values.push(format_int96(nanos, julian_day));
        }
    }
    values
}

fn parse_float_plain(buffer: &[u8], num_values: usize, max_items: usize) -> Vec<String> {
    let max_vals = (buffer.len() / 4).min(num_values).min(max_items);
    let mut values = Vec::with_capacity(max_vals);
    for i in 0..max_vals {
        let offset = i * 4;
        if offset + 4 <= buffer.len() {
            let bytes = [
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ];
            let value = f32::from_le_bytes(bytes);
            values.push(format!("{value:.6}"));
        }
    }
    values
}

fn parse_double_plain(buffer: &[u8], num_values: usize, max_items: usize) -> Vec<String> {
    let max_vals = (buffer.len() / 8).min(num_values).min(max_items);
    let mut values = Vec::with_capacity(max_vals);
    for i in 0..max_vals {
        let offset = i * 8;
        if offset + 8 <= buffer.len() {
            let bytes = [
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
                buffer[offset + 4],
                buffer[offset + 5],
                buffer[offset + 6],
                buffer[offset + 7],
            ];
            let value = f64::from_le_bytes(bytes);
            values.push(format!("{value:.6}"));
        }
    }
    values
}

fn unknown_as_hex_preview(physical_type: PhysicalType, buffer: &[u8]) -> String {
    let hex_preview = buffer
        .iter()
        .take(32)
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(" ");
    format!("Binary({physical_type}): {hex_preview}")
}

fn format_int96(nanos: u64, julian_day: u32) -> String {
    // Convert Julian day to Unix epoch (Jan 1, 1970 = Julian day 2440588)
    const JULIAN_DAY_OF_EPOCH: i64 = 2440588;
    let days_since_epoch = julian_day as i64 - JULIAN_DAY_OF_EPOCH;
    let seconds_since_epoch = days_since_epoch * 24 * 60 * 60;
    let total_nanos = seconds_since_epoch * 1_000_000_000 + nanos as i64;

    let timestamp_secs = total_nanos / 1_000_000_000;
    let timestamp_nanos = total_nanos % 1_000_000_000;

    if let Some(datetime) = chrono::DateTime::from_timestamp(timestamp_secs, timestamp_nanos as u32)
    {
        datetime.format("%Y-%m-%d %H:%M:%S%.9f UTC").to_string()
    } else {
        format!("INT96(nanos={nanos}, julian_day={julian_day})")
    }
}

/// Extract dictionary values from dictionary pages for dictionary-encoded columns.
/// This implementation manually parses the dictionary page format for common types.
pub fn extract_dictionary_values(
    reader: &SerializedFileReader<File>,
    col_idx: usize,
    max_items: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let physical_type = physical_type_of(reader, col_idx);
    let pages = collect_dictionary_pages(reader, col_idx)?;

    let mut values: Vec<String> = Vec::new();
    for (buffer, num_values) in pages.into_iter() {
        if values.len() >= max_items {
            break;
        }
        let remaining = max_items - values.len();
        let mut page_vals = parse_dictionary_page(physical_type, &buffer, num_values, remaining);
        values.append(&mut page_vals);
    }

    Ok(values)
}
