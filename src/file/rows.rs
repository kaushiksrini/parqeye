//! Bounded Arrow reads for row scans and scattered result windows. Values are
//! converted to Polars series so every path uses the table's cell formatter.
use arrow::{array::Array, record_batch::RecordBatch};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection},
};
use polars::prelude::*;
use std::fs::File;

pub(crate) const BATCH_ROWS: usize = 2_000;

pub(crate) fn project(
    builder: ParquetRecordBatchReaderBuilder<File>,
    names: &[String],
) -> Result<ParquetRecordBatchReaderBuilder<File>, String> {
    let indices = names
        .iter()
        .map(|name| builder.schema().index_of(name).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
    Ok(builder.with_projection(mask).with_batch_size(BATCH_ROWS))
}

pub(crate) fn selected_rows(
    path: &str,
    ids: &[usize],
    names: &[String],
) -> Result<Vec<Vec<String>>, String> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let file = File::open(path).map_err(|e| e.to_string())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        file,
        ArrowReaderOptions::new()
            .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Optional),
    )
    .map_err(|e| e.to_string())?;
    let total = builder.metadata().file_metadata().num_rows() as usize;
    let selection = RowSelection::from_consecutive_ranges(ids.iter().map(|&r| r..r + 1), total);
    let reader = project(builder, names)?
        .with_row_selection(selection)
        .build()
        .map_err(|e| e.to_string())?;
    let types = super::sample_data::ParquetSampleData::column_types(path, names)?;
    let mut rows = Vec::with_capacity(ids.len());
    for batch in reader {
        let batch = batch.map_err(|e| e.to_string())?;
        let series = batch_series(&batch, names, &types)?;
        for row in 0..batch.num_rows() {
            rows.push(format_row(&series, row)?);
        }
    }
    if rows.len() != ids.len() {
        return Err("Unable to read all requested file rows".into());
    }
    Ok(rows)
}

/// The C Data Interface transfers buffer ownership between the two Arrow
/// implementations without changing data types or copying decoded values.
pub(crate) fn batch_series(
    batch: &RecordBatch,
    names: &[String],
    types: &[DataType],
) -> Result<Vec<Series>, String> {
    super::sample_data::catch_polars_panic(std::panic::AssertUnwindSafe(|| {
        names
            .iter()
            .zip(types)
            .map(|(name, dtype)| {
                let column = batch
                    .column_by_name(name)
                    .ok_or("Missing projected column")?;
                let (array, _) = arrow::ffi::to_ffi(&column.to_data())?;
                let schema =
                    arrow::ffi::FFI_ArrowSchema::try_from(batch.schema().field_with_name(name)?)?;
                // SAFETY: both crates declare these repr(C) structures using the
                // Arrow C Data Interface ABI. Moving them transfers their release
                // callbacks exactly once. The live schema describes this array.
                let (array, schema): (
                    polars_arrow::ffi::ArrowArray,
                    polars_arrow::ffi::ArrowSchema,
                ) = unsafe {
                    (
                        std::mem::transmute::<
                            arrow::ffi::FFI_ArrowArray,
                            polars_arrow::ffi::ArrowArray,
                        >(array),
                        std::mem::transmute::<
                            arrow::ffi::FFI_ArrowSchema,
                            polars_arrow::ffi::ArrowSchema,
                        >(schema),
                    )
                };
                let field = unsafe { polars_arrow::ffi::import_field_from_c(&schema) }?;
                let array =
                    unsafe { polars_arrow::ffi::import_array_from_c(array, field.dtype.clone()) }?;
                let series = Series::try_from((&field, array))?;
                Ok(if series.dtype() == dtype {
                    series
                } else {
                    series.cast(dtype)?
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()
    }))
    .map_err(|e| e.to_string())
}

pub(crate) fn format_row(series: &[Series], row: usize) -> Result<Vec<String>, String> {
    series
        .iter()
        .map(|s| {
            s.get(row)
                .map(|v| super::sample_data::format_cell(&v))
                .map_err(|e| e.to_string())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{file::sample_data::ParquetSampleData, view::RowIndex};
    use std::sync::Arc;
    #[test]
    fn scattered_reads_match_the_existing_formatter_for_scalar_nested_and_temporal_values() {
        for file in [
            "alltypes_plain.parquet",
            "nested_lists.snappy.parquet",
            "nested_maps.snappy.parquet",
            "binary.parquet",
            "nested_structs.rust.parquet",
            "int64_decimal.parquet",
        ] {
            let path = format!("{}/{file}", crate::file::parquet_test_data());
            let data = ParquetSampleData::open(&path, 100).unwrap();
            let expected = data.loaded().rows.clone();
            let ids: Vec<_> = (0..expected.len()).step_by(2).collect();
            let actual = selected_rows(&path, &ids, &data.flattened_columns).unwrap();
            assert_eq!(
                actual,
                ids.iter().map(|&r| expected[r].clone()).collect::<Vec<_>>(),
                "{file}"
            );
        }
    }
    #[test]
    fn filtered_windows_are_bounded_invalidate_on_index_change_and_restore_raw_rows() {
        let fixture = crate::tabs::visualize::filter::tests::fixture();
        let path = fixture.0.to_str().unwrap();
        let data = ParquetSampleData::open(path, 6005).unwrap();
        let rows = RowIndex::Filtered(Arc::new([0, 2501, 5999, 6004].into_iter().collect()));
        data.ensure_view_loaded(&rows, 1, 1).unwrap();
        assert!(data.loaded().rows.len() <= 3);
        assert_eq!(data.loaded().rows[1][0], "2501");
        data.ensure_view_loaded(&rows, 3, 1).unwrap();
        let window = data.loaded();
        assert_eq!(window.rows[3 - window.start][0], "6004");
        drop(window);
        // A new index with the same window dimensions must never reuse values.
        let changed = RowIndex::Filtered(Arc::new([1, 2502, 5998, 6003].into_iter().collect()));
        data.ensure_view_loaded(&changed, 3, 1).unwrap();
        let window = data.loaded();
        assert_eq!(window.rows[3 - window.start][0], "6003");
        drop(window);
        data.ensure_view_loaded(&RowIndex::All, 0, 2).unwrap();
        assert_eq!(data.loaded().rows[1][0], "1");
        data.ensure_view_loaded(&RowIndex::Filtered(Arc::default()), 0, 2)
            .unwrap();
        assert!(data.loaded().rows.is_empty());
    }
}
