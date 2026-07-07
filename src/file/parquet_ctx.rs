use parquet::file::reader::{FileReader, SerializedFileReader};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::rc::Rc;

use crate::file::metadata::FileMetadata;
use crate::file::row_groups::{RowGroupPageInfo, RowGroups, make_page_info};
use crate::file::sample_data::ParquetSampleData;
use crate::file::schema::FileSchema;
pub struct ParquetCtx {
    pub file_path: String,
    pub metadata: FileMetadata,
    pub row_groups: RowGroups,
    pub schema: FileSchema,
    pub sample_data: ParquetSampleData,
    /// Retained so page info can be read lazily (reuses the already-parsed footer).
    reader: SerializedFileReader<File>,
    /// Caches page info per (row group, column) so it is read/decompressed at most once.
    page_cache: RefCell<HashMap<(usize, usize), Rc<RowGroupPageInfo>>>,
}

impl ParquetCtx {
    pub fn from_file(file_path: &str) -> Result<ParquetCtx, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let reader: SerializedFileReader<File> = SerializedFileReader::new(file)?;
        let md = reader.metadata();
        let row_groups = RowGroups::from_file_reader(&reader)?;

        // TODO: async calls?
        let metadata = FileMetadata::from_metadata(md)?;
        let schema = FileSchema::from_metadata(md)?;

        // Read sample data
        let sample_data = ParquetSampleData::read_sample_data(file_path)?;

        Ok(ParquetCtx {
            file_path: file_path.to_string(),
            metadata,
            row_groups,
            schema,
            sample_data,
            reader,
            page_cache: RefCell::new(HashMap::new()),
        })
    }

    pub fn column_size(&self) -> usize {
        self.schema.column_size()
    }

    /// Lazily read (and cache) the page info for a single column chunk.
    ///
    /// Page enumeration decompresses each page's data buffer, so this is done on
    /// demand only for the row group / column currently being viewed. Results are
    /// cached, so re-navigating to the same column is free.
    pub fn page_info(&self, rg_idx: usize, col_idx: usize) -> Rc<RowGroupPageInfo> {
        if let Some(pages) = self.page_cache.borrow().get(&(rg_idx, col_idx)) {
            return Rc::clone(pages);
        }

        let pages = Rc::new(self.read_page_info(rg_idx, col_idx).unwrap_or_default());
        self.page_cache
            .borrow_mut()
            .insert((rg_idx, col_idx), Rc::clone(&pages));
        pages
    }

    fn read_page_info(
        &self,
        rg_idx: usize,
        col_idx: usize,
    ) -> Result<RowGroupPageInfo, Box<dyn std::error::Error>> {
        let mut page_reader = self
            .reader
            .get_row_group(rg_idx)?
            .get_column_page_reader(col_idx)?;
        Ok(make_page_info(&mut page_reader))
    }
}
