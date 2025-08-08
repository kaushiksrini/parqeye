use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::Type;
use parquet::{errors::ParquetError, file::metadata::ParquetMetaData, record::Row};
use std::fs::File;
use std::io;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct VisualizeState {
    pub page_size: usize,
    pub offset_row: usize,
    pub visible_rows: Vec<Row>,
    pub column_names: Vec<String>,
    pub last_loaded_offset: usize,
    pub last_loaded_page_size: usize,
}

impl VisualizeState {
    pub fn new(page_size: usize) -> Self {
        Self {
            page_size,
            offset_row: 0,
            visible_rows: Vec::new(),
            column_names: Vec::new(),
            last_loaded_offset: usize::MAX,
            last_loaded_page_size: usize::MAX,
        }
    }
}

pub fn read_metadata(path: &str) -> io::Result<ParquetMetaData> {
    let file = File::open(Path::new(path))?;
    let reader = SerializedFileReader::new(file).map_err(|e| io::Error::other(e.to_string()))?;
    Ok(reader.metadata().clone())
}

// Lazily read a window of rows using parquet::record reader, avoiding loading entire file.
pub fn load_rows_window(path: &str, start: usize, limit: usize) -> io::Result<(Vec<Row>, Vec<String>)> {
    let file = File::open(Path::new(path))?;
    let reader = SerializedFileReader::new(file).map_err(|e| io::Error::other(e.to_string()))?;

    let iter = reader
        .get_row_iter(None)
        .map_err(|e| io::Error::other(e.to_string()))?;

    let rows: Vec<Row> = iter
        .skip(start)
        .take(limit)
        .collect::<Result<Vec<Row>, ParquetError>>()
        .map_err(|e| io::Error::other(e.to_string()))?;

    // Extract column names from schema root
    let md = read_metadata(path)?;
    let root: &Type = md.file_metadata().schema_descr().root_schema();
    let column_names: Vec<String> = match root {
        Type::GroupType { fields, .. } => fields.iter().map(|f| f.name().to_string()).collect(),
        _ => vec![],
    };

    Ok((rows, column_names))
}

pub fn format_row_value(row: &Row) -> String {
    format!("{}", row)
}


