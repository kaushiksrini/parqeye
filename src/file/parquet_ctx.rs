use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::PathBuf;

use crate::file::error::FileIOError;
use crate::file::metadata::FileMetadata;
use crate::file::row_groups::RowGroups;
use crate::file::sample_data::ParquetSampleData;
use crate::file::schema::FileSchema;
pub struct ParquetCtx {
    pub file_path: String,
    pub metadata: FileMetadata,
    pub row_groups: RowGroups,
    pub schema: FileSchema,
    /// Preview data for the Visualize tab. This is `Err` (rather than fatal) when
    /// the reader can't decode the file's values — e.g. a type the polars-based
    /// reader doesn't support — so the rest of the file stays inspectable and the
    /// Visualize tab shows a warning instead of the app crashing.
    pub sample_data: Result<ParquetSampleData, String>,
}

impl ParquetCtx {
    pub fn from_file(file_path: &str) -> Result<ParquetCtx, FileIOError> {
        let path = PathBuf::from(file_path);

        let file = File::open(&path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => FileIOError::FileNotFound { path: path.clone() },
            std::io::ErrorKind::PermissionDenied => {
                FileIOError::PermissionDenied { path: path.clone() }
            }
            _ => FileIOError::Io { source: e },
        })?;

        let reader: SerializedFileReader<File> =
            SerializedFileReader::new(file).map_err(|e| FileIOError::InvalidParquet {
                path: path.clone(),
                details: e.to_string(),
            })?;

        let md = reader.metadata();

        let row_groups =
            RowGroups::from_file_reader(&reader).map_err(|e| FileIOError::MetadataError {
                details: format!("Failed to read row groups: {e}"),
            })?;

        let metadata = FileMetadata::from_metadata(md).map_err(|e| FileIOError::MetadataError {
            details: format!("Failed to read file metadata: {e}"),
        })?;

        let schema = FileSchema::from_metadata(md).map_err(|e| FileIOError::MetadataError {
            details: format!("Failed to parse schema: {e}"),
        })?;

        let sample_data = Self::load_sample_data(file_path);

        Ok(ParquetCtx {
            file_path: file_path.to_string(),
            metadata,
            row_groups,
            schema,
            sample_data,
        })
    }

    /// Read the preview data without ever taking the whole app down. A file may
    /// have perfectly valid metadata/schema yet contain values the reader can't
    /// decode; in that case the reader can either return an error or panic deep
    /// inside a dependency. Both are caught here and surfaced as a message so the
    /// Visualize tab can warn while the other tabs keep working.
    fn load_sample_data(file_path: &str) -> Result<ParquetSampleData, String> {
        // Silence the default panic hook for the duration of the read so a caught
        // panic doesn't dump a backtrace onto the terminal. This runs at startup,
        // before the TUI is initialized, so swapping the global hook is safe.
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ParquetSampleData::read_sample_data(file_path)
        }));
        std::panic::set_hook(previous_hook);

        match outcome {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(e)) => Err(e.to_string()),
            Err(panic) => Err(Self::panic_message(panic)),
        }
    }

    fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
        if let Some(msg) = panic.downcast_ref::<&str>() {
            msg.to_string()
        } else if let Some(msg) = panic.downcast_ref::<String>() {
            msg.clone()
        } else {
            "the parquet reader panicked while reading the data".to_string()
        }
    }

    pub fn column_size(&self) -> usize {
        self.schema.column_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_data_path(filename: &str) -> String {
        format!("{}/{}", crate::file::parquet_test_data(), filename)
    }

    /// Path to a file in the sibling `parquet-testing/bad_data` submodule.
    /// `parquet_test_data()` points at `parquet-testing/data`, so `../bad_data`
    /// hops over to the malformed fixtures.
    fn bad_data_path(filename: &str) -> String {
        format!("{}/../bad_data/{}", crate::file::parquet_test_data(), filename)
    }

    /// Malformed files (from `parquet-testing/bad_data`) whose footer metadata is
    /// intact but whose data pages are corrupt: the reader can open them and read
    /// schema/row-group/metadata, but decoding the values fails. These exercise
    /// the graceful path where the app still loads and the Visualize tab warns
    /// instead of crashing.
    const BAD_DATA_FILES: &[&str] = &[
        "ARROW-RS-GH-6229-DICTHEADER.parquet",
        "ARROW-RS-GH-6229-LEVELS.parquet",
        "ARROW-GH-41321.parquet",
        "ARROW-GH-41317.parquet",
        "ARROW-GH-43605.parquet",
        "ARROW-GH-45185.parquet",
    ];

    #[test]
    fn test_file_not_found() {
        let path = test_data_path("this_file_does_not_exist.parquet");
        let Err(err) = ParquetCtx::from_file(&path) else {
            panic!("Expected error for nonexistent file");
        };
        assert!(
            matches!(err, FileIOError::FileNotFound { .. }),
            "Expected FileNotFound, got: {err}"
        );
    }

    #[test]
    fn test_non_parquet_file() {
        let path = test_data_path("README.md");
        let Err(err) = ParquetCtx::from_file(&path) else {
            panic!("Expected error for non-parquet file");
        };
        assert!(
            matches!(err, FileIOError::InvalidParquet { .. }),
            "Expected InvalidParquet, got: {err}"
        );
    }

    #[test]
    fn test_valid_parquet_file_alltypes_plain() {
        let path = test_data_path("alltypes_plain.parquet");
        let result = ParquetCtx::from_file(&path);
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
    }

    #[test]
    fn test_valid_parquet_file_alltypes_dictionary() {
        let path = test_data_path("alltypes_dictionary.parquet");
        let result = ParquetCtx::from_file(&path);
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
    }

    #[test]
    fn test_valid_parquet_file_nulls() {
        let path = test_data_path("nulls.snappy.parquet");
        let result = ParquetCtx::from_file(&path);
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
    }

    #[test]
    fn test_file_not_found_error_contains_path() {
        let path = test_data_path("nonexistent.parquet");
        let Err(err) = ParquetCtx::from_file(&path) else {
            panic!("Expected error for nonexistent file");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent.parquet"),
            "Error message should contain the file path, got: {msg}"
        );
    }

    #[test]
    fn test_invalid_parquet_error_contains_path() {
        let path = test_data_path("README.md");
        let Err(err) = ParquetCtx::from_file(&path) else {
            panic!("Expected error for non-parquet file");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("README.md"),
            "Error message should contain the file path, got: {msg}"
        );
    }

    #[test]
    fn test_corrupt_parquet_from_bad_data() {
        let path = bad_data_path("PARQUET-1481.parquet");
        let result = ParquetCtx::from_file(&path);
        assert!(result.is_err(), "Expected error for corrupt parquet file");
    }

    /// Files with corrupt data (but readable metadata) must never load their
    /// sample data or take the app down. `from_file` should return without
    /// panicking, and either fail outright or open with `sample_data` reported as
    /// an error — which is what drives the warning on the Visualize tab.
    #[test]
    fn test_bad_data_files_do_not_load_sample_data() {
        for filename in BAD_DATA_FILES {
            let path = bad_data_path(filename);
            match ParquetCtx::from_file(&path) {
                Ok(ctx) => assert!(
                    ctx.sample_data.is_err(),
                    "expected sample data to fail to load for {filename}, but it loaded"
                ),
                // Failing earlier (e.g. a corrupt footer) is still a clean,
                // non-crashing failure.
                Err(_) => {}
            }
        }
    }
}
