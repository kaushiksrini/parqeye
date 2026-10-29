use std::cell::{Ref, RefCell};
use std::sync::Mutex;

use polars::prelude::*;

type LoadedRows = (Vec<Vec<String>>, Vec<String>);

const WINDOW_ROWS: usize = 2000;
const ROW_PREFETCH: usize = 500;

/// Serializes temporary replacement of the process-wide panic hook.
static POLARS_PANIC_HOOK_LOCK: Mutex<()> = Mutex::new(());

/// The currently materialized slice of rows: `rows[i]` is the file row at
/// absolute index `start + i`.
#[derive(Debug, Default)]
pub struct RowWindow {
    pub start: usize,
    pub rows: Vec<Vec<String>>,
}

impl RowWindow {
    fn covers(&self, view_start: usize, view_len: usize) -> bool {
        view_start >= self.start && view_start + view_len <= self.start + self.rows.len()
    }
}

/// A lazily-loaded, memory-bounded view over a parquet file's rows.
#[derive(Debug, Default)]
pub struct ParquetSampleData {
    file_path: String,
    pub flattened_columns: Vec<String>,
    pub total_columns: usize,
    pub total_rows: usize,
    window: RefCell<RowWindow>,
}

impl ParquetSampleData {
    /// Open the file and load the first window. Polars panics for some unsupported
    /// parquet formats, so those panics are returned as ordinary errors.
    pub fn open(
        file_path: &str,
        total_rows: usize,
    ) -> Result<ParquetSampleData, Box<dyn std::error::Error>> {
        let path = file_path.to_string();
        catch_polars_panic(move || Self::open_with_polars(&path, total_rows))
    }

    fn open_with_polars(
        file_path: &str,
        total_rows: usize,
    ) -> Result<ParquetSampleData, Box<dyn std::error::Error>> {
        let first_len = WINDOW_ROWS.min(total_rows);
        let (rows, columns) = if first_len > 0 {
            Self::load_with_polars(file_path, 0, first_len)?
        } else {
            let mut lf = LazyFrame::scan_parquet(PlRefPath::new(file_path), Default::default())?;
            let schema = lf.collect_schema()?;
            let names = schema.iter_names().map(|s| s.to_string()).collect();
            (Vec::new(), names)
        };

        Ok(ParquetSampleData {
            file_path: file_path.to_string(),
            total_columns: columns.len(),
            flattened_columns: columns,
            total_rows,
            window: RefCell::new(RowWindow { start: 0, rows }),
        })
    }

    /// Read `len` rows at `offset`; the slice is pushed into the parquet scan.
    fn load(
        file_path: &str,
        offset: usize,
        len: usize,
    ) -> Result<LoadedRows, Box<dyn std::error::Error>> {
        let path = file_path.to_string();
        catch_polars_panic(move || Self::load_with_polars(&path, offset, len))
    }

    fn load_with_polars(
        file_path: &str,
        offset: usize,
        len: usize,
    ) -> Result<LoadedRows, Box<dyn std::error::Error>> {
        let df = LazyFrame::scan_parquet(PlRefPath::new(file_path), Default::default())?
            .slice(offset as i64, len as IdxSize)
            .collect()?;
        let df = Self::flatten_struct_columns(df);

        let columns: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let mut rows = Vec::with_capacity(df.height());
        for row_idx in 0..df.height() {
            let mut row = Vec::with_capacity(columns.len());
            for col in df.columns() {
                let series = col.as_materialized_series();
                row.push(Self::get_value_as_string(series, row_idx));
            }
            rows.push(row);
        }

        Ok((rows, columns))
    }

    /// Reload the bounded window when the requested viewport is outside it.
    pub fn ensure_loaded(&self, view_start: usize, view_len: usize) {
        let view_len = view_len.min(self.total_rows.saturating_sub(view_start));
        if self.window.borrow().covers(view_start, view_len) {
            return;
        }

        let new_start = view_start.saturating_sub(ROW_PREFETCH);
        let len = WINDOW_ROWS.min(self.total_rows.saturating_sub(new_start));
        if len == 0 {
            return;
        }

        if let Ok((rows, _columns)) = Self::load(&self.file_path, new_start, len) {
            let mut window = self.window.borrow_mut();
            window.start = new_start;
            window.rows = rows;
        }
    }

    pub fn loaded(&self) -> Ref<'_, RowWindow> {
        self.window.borrow()
    }

    fn flatten_struct_columns(df: DataFrame) -> DataFrame {
        // Struct columns are currently displayed with their string representation.
        df
    }

    fn get_value_as_string(col: &Series, row_idx: usize) -> String {
        match col.get(row_idx) {
            Ok(any_value) if any_value.is_null() => "NULL".to_string(),
            Ok(any_value) => format!("{any_value}"),
            Err(_) => "NULL".to_string(),
        }
    }
}

fn catch_polars_panic<T>(
    operation: impl FnOnce() -> Result<T, Box<dyn std::error::Error>> + std::panic::UnwindSafe,
) -> Result<T, Box<dyn std::error::Error>> {
    let _lock = POLARS_PANIC_HOOK_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(operation);
    std::panic::set_hook(previous_hook);

    match result {
        Ok(result) => result,
        Err(payload) => Err(panic_message(&payload).into()),
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "polars panicked while reading this file".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_data_path(filename: &str) -> String {
        format!("{}/{}", crate::file::parquet_test_data(), filename)
    }

    fn wide_file() -> String {
        test_data_path("alltypes_tiny_pages.parquet")
    }

    #[test]
    fn open_reports_full_metadata_not_just_the_window() {
        let data = ParquetSampleData::open(&wide_file(), 7300).unwrap();
        assert_eq!(data.total_rows, 7300);
        assert_eq!(data.total_columns, 13);
        assert_eq!(data.loaded().rows.len(), WINDOW_ROWS);
        assert_eq!(data.loaded().start, 0);
    }

    #[test]
    fn ensure_loaded_slides_the_window_for_a_far_scroll() {
        let data = ParquetSampleData::open(&wide_file(), 7300).unwrap();
        data.ensure_loaded(6000, 40);
        let win = data.loaded();
        assert_eq!(win.start, 6000 - ROW_PREFETCH);
        assert!(win.covers(6000, 40));
        assert!(win.rows.len() <= WINDOW_ROWS);
    }

    #[test]
    fn ensure_loaded_is_a_noop_inside_the_window() {
        let data = ParquetSampleData::open(&wide_file(), 7300).unwrap();
        let start_before = data.loaded().start;
        data.ensure_loaded(50, 40);
        assert_eq!(data.loaded().start, start_before);
    }

    #[test]
    fn windowed_rows_match_a_direct_read_at_the_same_offset() {
        let data = ParquetSampleData::open(&wide_file(), 7300).unwrap();
        data.ensure_loaded(6000, 10);
        let (direct, _) = ParquetSampleData::load(&wide_file(), 6000, 10).unwrap();
        let win = data.loaded();
        let rel = 6000 - win.start;
        assert_eq!(&win.rows[rel..rel + 10], &direct[..]);
    }

    fn write_geoarrow_wkb_file() -> std::path::PathBuf {
        use arrow::array::{ArrayRef, BinaryArray, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use std::collections::HashMap;
        use std::sync::Arc;

        let field = Field::new("geom", DataType::Binary, true).with_metadata(HashMap::from([
            (
                "ARROW:extension:name".to_string(),
                "geoarrow.wkb".to_string(),
            ),
            (
                "ARROW:extension:metadata".to_string(),
                r#"{"crs":"EPSG:4326"}"#.to_string(),
            ),
        ]));
        let schema = Arc::new(Schema::new(vec![field]));
        let values: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![
            Some(&[0x01, 0x01, 0x00, 0x00, 0x00][..]),
            None,
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        let path = std::env::temp_dir().join(format!(
            "parqeye_geoarrow_wkb_{}.parquet",
            std::process::id()
        ));
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    #[test]
    fn reads_arrow_extension_types() {
        let path = write_geoarrow_wkb_file();
        let data = ParquetSampleData::open(path.to_str().unwrap(), 2).unwrap();
        assert_eq!(data.flattened_columns, vec!["geom"]);
        assert_eq!(data.total_rows, 2);
        assert_eq!(data.loaded().rows[1][0], "NULL");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unsupported_file_errors_instead_of_panicking() {
        let err =
            ParquetSampleData::open(&test_data_path("geospatial/crs-srid.parquet"), 1).unwrap_err();
        assert!(
            err.to_string().contains("LogicalType"),
            "unexpected error: {err}"
        );
    }
}
