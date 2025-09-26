use parquet::file::reader::SerializedFileReader;
use parquet::schema::types::Type;
use parquet::errors::ParquetError;
use parquet::record::Row;
use std::io;

struct FileContents {
    pub columns: Vec<String>,
    pub rows: Vec<Row>
}

impl FileContents {
    pub fn from_file(reader: &SerializedFileReader) -> io::Result<Self> {
        let md = reader.metadata()?;
        let root: &Type = md.file_metadata().schema_descr().root_schema();
        let columns: Vec<String> = match root {
            Type::GroupType { fields, .. } => fields.iter().map(|f| f.name().to_string()).collect(),
            _ => vec![],
        };

        let rows = reader
            .get_row_iter(None)
            .map_err(|e| io::Error::other(e.to_string()))?
            .collect::<Result<Vec<Row>, ParquetError>>()
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(Self { columns, rows })
    }
}