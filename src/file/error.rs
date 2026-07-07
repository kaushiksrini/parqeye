use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FileIOError {
    #[error("File not found: '{path}'")]
    FileNotFound { path: PathBuf },

    #[error("Permission denied: '{path}'")]
    PermissionDenied { path: PathBuf },

    #[error("Not a valid Parquet file: '{path}'\nDetails: {details}")]
    InvalidParquet { path: PathBuf, details: String },

    #[error("Failed to read file metadata: {details}")]
    MetadataError { details: String },

    #[error("Failed to read sample data: {details}")]
    SampleDataError { details: String },

    #[error("File I/O error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_not_found_message() {
        let err = FileIOError::FileNotFound {
            path: PathBuf::from("/tmp/nonexistent.parquet"),
        };
        assert_eq!(
            err.to_string(),
            "File not found: '/tmp/nonexistent.parquet'"
        );
    }

    #[test]
    fn test_permission_denied_message() {
        let err = FileIOError::PermissionDenied {
            path: PathBuf::from("/etc/shadow"),
        };
        assert_eq!(err.to_string(), "Permission denied: '/etc/shadow'");
    }

    #[test]
    fn test_invalid_parquet_message() {
        let err = FileIOError::InvalidParquet {
            path: PathBuf::from("bad.parquet"),
            details: "not a valid parquet magic number".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("Not a valid Parquet file: 'bad.parquet'"));
        assert!(msg.contains("not a valid parquet magic number"));
    }

    #[test]
    fn test_metadata_error_message() {
        let err = FileIOError::MetadataError {
            details: "corrupt row group".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Failed to read file metadata: corrupt row group"
        );
    }

    #[test]
    fn test_sample_data_error_message() {
        let err = FileIOError::SampleDataError {
            details: "unsupported column type".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Failed to read sample data: unsupported column type"
        );
    }

    #[test]
    fn test_io_error_from_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "disk failure");
        let err: FileIOError = io_err.into();
        assert!(err.to_string().contains("disk failure"));
    }
}
