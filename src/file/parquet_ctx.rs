use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;

use crate::file::metadata::FileMetadata;
use crate::file::row_groups::RowGroups;
use crate::file::sample_data::ParquetSampleData;
use crate::file::schema::FileSchema;
pub struct ParquetCtx {
    pub file_path: String,
    pub metadata: FileMetadata,
    pub row_groups: RowGroups,
    pub schema: FileSchema,
    pub sample_data: Option<ParquetSampleData>,
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

        // Read sample data
        let sample_data = ParquetSampleData::read_sample_data(file_path).ok();

        Ok(ParquetCtx {
            file_path: file_path.to_string(),
            metadata,
            row_groups,
            schema,
            sample_data,
        })
    }

    pub fn column_size(&self) -> usize {
        self.schema.column_size()
    }
}
