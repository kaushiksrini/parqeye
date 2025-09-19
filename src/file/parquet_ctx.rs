use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;

use crate::file::metadata::FileMetadata;
use crate::file::row_groups::RowGroups;
use crate::file::schema::FileSchema;

pub struct ParquetCtx {
    pub file_path: String,
    pub metadata: FileMetadata,
    pub row_groups: RowGroups,
    pub schema: FileSchema,
    // pub contents: Vec<u8>,
}

impl ParquetCtx {
    pub fn from_file(file_path: &str) -> Result<ParquetCtx, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let reader: SerializedFileReader<File> = SerializedFileReader::new(file)?;
        let md = reader.metadata();
        let row_groups = RowGroups::from_file_reader(&reader)?;

        // TODO: async calls?
        let metadata = FileMetadata::from_metadata(&md)?;
        let schema = FileSchema::from_metadata(&md)?;

        Ok(ParquetCtx {
            file_path: file_path.to_string(),
            metadata,
            row_groups,
            schema,
            // contents: Vec::new(),
        })
    }

    pub fn column_size(&self) -> usize {
        self.schema.column_size()
    }
}
