use std::fs::File;
use parquet::basic::{PageType, Type as PhysicalType};
use parquet::file::reader::{FileReader, SerializedFileReader};

/// Extract dictionary values from dictionary pages for dictionary-encoded columns.
/// This implementation manually parses the dictionary page format for common types.
pub fn extract_dictionary_values(reader: &SerializedFileReader<File>, col_idx: usize, max_items: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut dictionary_values = Vec::new();
    let metadata = reader.metadata();
    let schema_descr = metadata.file_metadata().schema_descr();
    let column_descr = schema_descr.column(col_idx);
    let physical_type = column_descr.physical_type();
    
    // Iterate through all row groups to find dictionary pages
    for rg_idx in 0..reader.num_row_groups() {
        let rg_reader = reader.get_row_group(rg_idx)?;
        let mut page_reader = rg_reader.get_column_page_reader(col_idx)?;
        
        // Read pages to find dictionary page first
        while let Some(page) = page_reader.get_next_page()? {
            if page.page_type() == PageType::DICTIONARY_PAGE {
                let page_buffer = page.buffer();
                let num_values = page.num_values() as usize;
                
                // Manual parsing based on physical type and PLAIN encoding (most common for dictionary pages)
                match physical_type {
                    PhysicalType::BYTE_ARRAY => {
                        // BYTE_ARRAY PLAIN encoding: [length:4][data:length][length:4][data:length]...
                        let mut offset = 0;
                        let buffer_slice = &page_buffer[..];
                        
                        for _ in 0..num_values.min(max_items) {
                            if offset + 4 > buffer_slice.len() {
                                break;
                            }
                            
                            // Read length (little-endian u32)
                            let length = u32::from_le_bytes([
                                buffer_slice[offset],
                                buffer_slice[offset + 1],
                                buffer_slice[offset + 2],
                                buffer_slice[offset + 3],
                            ]) as usize;
                            offset += 4;
                            
                            if offset + length > buffer_slice.len() {
                                break;
                            }
                            
                            // Read string data
                            let string_data = &buffer_slice[offset..offset + length];
                            match std::str::from_utf8(string_data) {
                                Ok(s) => dictionary_values.push(s.to_string()),
                                Err(_) => {
                                    // If not valid UTF-8, show as hex
                                    let hex = string_data.iter()
                                        .take(8)
                                        .map(|b| format!("{:02X}", b))
                                        .collect::<Vec<_>>()
                                        .join("");
                                    dictionary_values.push(format!("0x{}", hex));
                                }
                            }
                            offset += length;
                            
                            if dictionary_values.len() >= max_items {
                                break;
                            }
                        }
                    }
                    PhysicalType::INT32 => {
                        // INT32 PLAIN encoding: [value:4][value:4]...
                        let buffer_slice = &page_buffer[..];
                        let max_vals = (buffer_slice.len() / 4).min(num_values).min(max_items);
                        
                        for i in 0..max_vals {
                            let offset = i * 4;
                            if offset + 4 <= buffer_slice.len() {
                                let value = i32::from_le_bytes([
                                    buffer_slice[offset],
                                    buffer_slice[offset + 1],
                                    buffer_slice[offset + 2],
                                    buffer_slice[offset + 3],
                                ]);
                                dictionary_values.push(value.to_string());
                            }
                        }
                    }
                    PhysicalType::INT64 => {
                        // INT64 PLAIN encoding: [value:8][value:8]...
                        let buffer_slice = &page_buffer[..];
                        let max_vals = (buffer_slice.len() / 8).min(num_values).min(max_items);
                        
                        for i in 0..max_vals {
                            let offset = i * 8;
                            if offset + 8 <= buffer_slice.len() {
                                let value = i64::from_le_bytes([
                                    buffer_slice[offset],
                                    buffer_slice[offset + 1],
                                    buffer_slice[offset + 2],
                                    buffer_slice[offset + 3],
                                    buffer_slice[offset + 4],
                                    buffer_slice[offset + 5],
                                    buffer_slice[offset + 6],
                                    buffer_slice[offset + 7],
                                ]);
                                dictionary_values.push(value.to_string());
                            }
                        }
                    }
                    PhysicalType::INT96 => {
                        // INT96 PLAIN encoding: [value:12][value:12]...
                        // INT96 is typically used for timestamps: 8 bytes nanoseconds + 4 bytes Julian day
                        let buffer_slice = &page_buffer[..];
                        let max_vals = (buffer_slice.len() / 12).min(num_values).min(max_items);
                        
                        for i in 0..max_vals {
                            let offset = i * 12;
                            if offset + 12 <= buffer_slice.len() {
                                // Read as little-endian: first 8 bytes are nanoseconds, last 4 bytes are Julian day
                                let nanos = u64::from_le_bytes([
                                    buffer_slice[offset],
                                    buffer_slice[offset + 1],
                                    buffer_slice[offset + 2],
                                    buffer_slice[offset + 3],
                                    buffer_slice[offset + 4],
                                    buffer_slice[offset + 5],
                                    buffer_slice[offset + 6],
                                    buffer_slice[offset + 7],
                                ]);
                                let julian_day = u32::from_le_bytes([
                                    buffer_slice[offset + 8],
                                    buffer_slice[offset + 9],
                                    buffer_slice[offset + 10],
                                    buffer_slice[offset + 11],
                                ]);
                                
                                // Convert Julian day to Unix epoch (Jan 1, 1970 = Julian day 2440588)
                                const JULIAN_DAY_OF_EPOCH: i64 = 2440588;
                                let days_since_epoch = julian_day as i64 - JULIAN_DAY_OF_EPOCH;
                                let seconds_since_epoch = days_since_epoch * 24 * 60 * 60;
                                let total_nanos = seconds_since_epoch * 1_000_000_000 + nanos as i64;
                                
                                // Convert to readable timestamp
                                let timestamp_secs = total_nanos / 1_000_000_000;
                                let timestamp_nanos = total_nanos % 1_000_000_000;
                                
                                // Format as ISO 8601 timestamp
                                if let Some(datetime) = chrono::DateTime::from_timestamp(timestamp_secs, timestamp_nanos as u32) {
                                    dictionary_values.push(datetime.format("%Y-%m-%d %H:%M:%S%.9f UTC").to_string());
                                } else {
                                    // Fallback: show raw values
                                    dictionary_values.push(format!("INT96(nanos={}, julian_day={})", nanos, julian_day));
                                }
                            }
                        }
                    }
                    PhysicalType::FLOAT => {
                        // FLOAT PLAIN encoding: [value:4][value:4]...
                        let buffer_slice = &page_buffer[..];
                        let max_vals = (buffer_slice.len() / 4).min(num_values).min(max_items);
                        
                        for i in 0..max_vals {
                            let offset = i * 4;
                            if offset + 4 <= buffer_slice.len() {
                                let bytes = [
                                    buffer_slice[offset],
                                    buffer_slice[offset + 1],
                                    buffer_slice[offset + 2],
                                    buffer_slice[offset + 3],
                                ];
                                let value = f32::from_le_bytes(bytes);
                                dictionary_values.push(format!("{:.6}", value));
                            }
                        }
                    }
                    PhysicalType::DOUBLE => {
                        // DOUBLE PLAIN encoding: [value:8][value:8]...
                        let buffer_slice = &page_buffer[..];
                        let max_vals = (buffer_slice.len() / 8).min(num_values).min(max_items);
                        
                        for i in 0..max_vals {
                            let offset = i * 8;
                            if offset + 8 <= buffer_slice.len() {
                                let bytes = [
                                    buffer_slice[offset],
                                    buffer_slice[offset + 1],
                                    buffer_slice[offset + 2],
                                    buffer_slice[offset + 3],
                                    buffer_slice[offset + 4],
                                    buffer_slice[offset + 5],
                                    buffer_slice[offset + 6],
                                    buffer_slice[offset + 7],
                                ];
                                let value = f64::from_le_bytes(bytes);
                                dictionary_values.push(format!("{:.6}", value));
                            }
                        }
                    }
                    _ => {
                        // For other types, show hex representation
                        let hex_preview = page_buffer
                            .iter()
                            .take(32)
                            .map(|b| format!("{:02X}", b))
                            .collect::<Vec<_>>()
                            .join(" ");
                        dictionary_values.push(format!("Binary({}): {}", physical_type, hex_preview));
                    }
                }
                
                if dictionary_values.len() >= max_items {
                    break;
                }
            }
        }
        
        if dictionary_values.len() >= max_items {
            break;
        }
    }
    
    Ok(dictionary_values)
}