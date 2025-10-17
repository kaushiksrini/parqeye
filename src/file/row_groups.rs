use parquet::basic::{Encoding, PageType};
use parquet::column::page::{Page, PageReader};
use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
use parquet::file::reader::FileReader;
use parquet::file::reader::{ChunkReader, SerializedFileReader};
use parquet::file::statistics::Statistics;

pub struct RowGroupPageInfo {
    pub page_infos: Vec<PageInfo>,
}

pub struct HasStats {
    pub has_stats: bool,
    pub has_dictionary_page: bool,
    pub has_bloom_filter: bool,
    pub has_page_encoding_stats: bool,
}

pub struct PageInfo {
    pub page_type: String,
    pub size: usize,
    pub rows: usize,
    pub encoding: String,
}

pub struct RowGroupColumnStats {
    pub min: Option<String>,
    pub max: Option<String>,
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
}

pub struct RowGroupColumnMetadata {
    pub file_offset: i64,
    pub column_path: String,
    pub has_stats: HasStats,
    pub statistics: Option<RowGroupColumnStats>,
    pub total_compressed_size: i64,
    pub total_uncompressed_size: i64,
    pub compression_type: String,
    pub pages: RowGroupPageInfo,
}

pub struct RowGroups {
    pub row_groups: Vec<RowGroupStats>,
}

impl RowGroups {
    pub fn from_file_reader<R: ChunkReader + 'static>(
        reader: &SerializedFileReader<R>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let row_groups = (0..reader.metadata().num_row_groups())
            .map(|idx| RowGroupStats::from_file_reader(reader, idx))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { row_groups })
    }

    pub fn num_row_groups(&self) -> usize {
        self.row_groups.len()
    }
}

pub struct RowGroupStats {
    pub idx: usize,
    pub rows: i64,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
    pub compression_ratio: String,
    pub column_metadata: Vec<RowGroupColumnMetadata>,
}

impl RowGroupStats {
    pub fn from_file_reader<R: ChunkReader + 'static>(
        reader: &SerializedFileReader<R>,
        idx: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rg_md: &RowGroupMetaData = reader.metadata().row_group(idx);
        let compressed_size = rg_md.columns().iter().map(|c| c.compressed_size()).sum();
        let uncompressed_size = rg_md.columns().iter().map(|c| c.uncompressed_size()).sum();
        let compression_ratio = format!("{:.2}", uncompressed_size as f64 / compressed_size as f64);

        let column_metadata = (0..rg_md.num_columns())
            .map(|col_idx| RowGroupColumnMetadata::from_file_reader(reader, idx, col_idx))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(RowGroupStats {
            idx,
            rows: rg_md.num_rows(),
            compressed_size,
            uncompressed_size,
            compression_ratio,
            column_metadata,
        })
    }
}

impl RowGroupColumnMetadata {
    pub fn from_file_reader<R: ChunkReader + 'static>(
        reader: &SerializedFileReader<R>,
        rg_idx: usize,
        col_idx: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rg_md = reader.metadata().row_group(rg_idx);
        let column_chunk: &ColumnChunkMetaData = rg_md.column(col_idx);

        let mut page_reader = reader
            .get_row_group(rg_idx)?
            .get_column_page_reader(col_idx)?;
        let pages = Self::make_page_info(&mut page_reader)?;

        let statistics = RowGroupColumnStats::new(column_chunk.statistics());

        Ok(RowGroupColumnMetadata {
            file_offset: column_chunk.file_offset(),
            column_path: column_chunk.column_descr().path().to_string(),
            has_stats: HasStats {
                has_stats: column_chunk.statistics().is_some(),
                has_dictionary_page: column_chunk.dictionary_page_offset().is_some(),
                has_bloom_filter: column_chunk.bloom_filter_offset().is_some(),
                has_page_encoding_stats: column_chunk.page_encoding_stats().is_some()
                    && !column_chunk.page_encoding_stats().unwrap().is_empty(),
            },
            statistics,
            total_compressed_size: column_chunk.compressed_size(),
            total_uncompressed_size: column_chunk.uncompressed_size(),
            compression_type: column_chunk.compression().to_string(),
            pages,
        })
    }

    fn make_page_info(
        page_reader: &mut Box<dyn PageReader>,
    ) -> Result<RowGroupPageInfo, Box<dyn std::error::Error>> {
        let mut page_info = Vec::new();
        while let Ok(page) = page_reader.get_next_page() {
            if let Some(page) = page {
                page_info.push(PageInfo::from(&page));
            } else {
                break;
            }
        }
        Ok(RowGroupPageInfo {
            page_infos: page_info,
        })
    }
}

impl From<&Page> for PageInfo {
    fn from(page: &Page) -> Self {
        // Get the page reader for this column
        let page_type = match page.page_type() {
            PageType::DATA_PAGE => "Data Page".to_string(),
            PageType::INDEX_PAGE => "Index Page".to_string(),
            PageType::DICTIONARY_PAGE => "Dictionary Page".to_string(),
            PageType::DATA_PAGE_V2 => "Data Page V2".to_string(),
        };

        let encoding: String = match page.encoding() {
            Encoding::PLAIN => "Plain".to_string(),
            Encoding::PLAIN_DICTIONARY => "Plain Dictionary".to_string(),
            Encoding::RLE => "RLE".to_string(),
            Encoding::DELTA_BINARY_PACKED => "Delta Binary Packed".to_string(),
            Encoding::DELTA_LENGTH_BYTE_ARRAY => "Delta Length Byte Array".to_string(),
            Encoding::DELTA_BYTE_ARRAY => "Delta Byte Array".to_string(),
            Encoding::RLE_DICTIONARY => "RLE Dictionary".to_string(),
            Encoding::BYTE_STREAM_SPLIT => "Byte Stream Split".to_string(),
            _ => format!("{:?}", page.encoding()), // Handle any other encoding types
        };

        PageInfo {
            page_type,
            size: page.buffer().len(),
            rows: page.num_values() as usize,
            encoding,
        }
    }
}

macro_rules! extract_stat_value {
    ($stats:expr, $method:ident) => {
        match $stats {
            Statistics::Boolean(s) => s.$method().map(|v| v.to_string()),
            Statistics::Int32(s) => s.$method().map(|v| v.to_string()),
            Statistics::Int64(s) => s.$method().map(|v| v.to_string()),
            Statistics::Int96(s) => s.$method().map(|v| format!("{:?}", v)),
            Statistics::Float(s) => s.$method().map(|v| v.to_string()),
            Statistics::Double(s) => s.$method().map(|v| v.to_string()),
            Statistics::ByteArray(s) => s.$method().and_then(|bytes| {
                std::str::from_utf8(bytes.data())
                    .ok()
                    .map(|s| s.to_string())
            }),
            Statistics::FixedLenByteArray(s) => s.$method().and_then(|bytes| {
                std::str::from_utf8(bytes.data())
                    .ok()
                    .map(|s| s.to_string())
            }),
        }
    };
}

impl RowGroupColumnStats {
    fn new(stats: Option<&Statistics>) -> Option<Self> {
        if let Some(stats) = stats {
            Some(Self {
                min: extract_stat_value!(stats, min_opt),
                max: extract_stat_value!(stats, max_opt),
                null_count: stats.null_count_opt(),
                distinct_count: stats.distinct_count_opt(),
            })
        } else {
            None
        }
    }
}
