use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};

#[derive(Debug)]
pub struct ParquetFileMetadata {
    pub file_name: String,
    pub format_version: String,
    pub created_by: String,
    pub rows: u64,
    pub columns: u64,
    pub row_groups: u64,
    pub size_raw: u64,
    pub size_compressed: u64,
    pub compression_ratio: f64,
    pub codecs: Vec<String>,
    pub encodings: String,
    pub avg_row_size: u64,
}

pub fn extract_parquet_file_metadata(file_name: &str) -> Result<ParquetFileMetadata, Box<dyn std::error::Error>> {
    let file = File::open(&Path::new(file_name))?;
    let reader: SerializedFileReader<File> = SerializedFileReader::new(file)?;
    let md = reader.metadata();

    let binding = md.file_metadata().created_by();
    let version = md.file_metadata().version();
    let created_by = binding.as_deref().unwrap_or("—");
    let row_groups = md.num_row_groups();
    let total_rows: i64 = md.row_groups().iter().map(|rg| rg.num_rows()).sum();
    let num_cols = md.file_metadata().schema_descr().num_columns();

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

    let compression_ratio = if compressed_size > 0 { 
        raw_size as f64 / compressed_size as f64 
    } else { 
        0.0 
    };

    let mut codec_vec: Vec<String> = codec_counts.iter()
        .map(|(c, n)| format!("{}({})", c, n))
        .collect();
    codec_vec.sort();

    let mut encodings: Vec<String> = encodings_seen.into_iter().collect();
    encodings.sort();
    let encodings_summary = encodings.join(", ");

    let avg_row_size = if total_rows > 0 { 
        raw_size as f64 / total_rows as f64 
    } else { 
        0.0 
    };

    // Extract just the file name without the path
    let file_name = file_name.split("/").last().unwrap().to_string();
    
    Ok(ParquetFileMetadata {
        file_name,
        format_version: version.to_string(),
        created_by: created_by.to_string(),
        rows: total_rows as u64,
        columns: num_cols as u64,
        row_groups: row_groups as u64,
        size_raw: raw_size,
        size_compressed: compressed_size,
        compression_ratio,
        codecs: codec_vec,
        encodings: encodings_summary,
        avg_row_size: avg_row_size as u64,
    })
}