//! Incremental AND filters over the current row index. Only a bounded batch of
//! projected cells is decoded at a time; no matching cell values are retained.
use super::search::SearchPattern;
use crate::{
    file::rows::{BATCH_ROWS, batch_series, project},
    jobs::{Task, Worker},
    view::RowIndex,
};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
};
use roaring::RoaringTreemap;
use std::{
    collections::HashSet,
    fs::File,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

const INDEX_BUDGET: usize = 128 * 1024 * 1024;

pub(crate) struct FilterRequest {
    pub path: String,
    pub columns: Vec<String>,
    pub rows: RowIndex,
    pub pattern: SearchPattern,
}

/// An applied condition keeps its original column scope for highlighting, even
/// after columns are hidden, restored or reordered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowFilter {
    pattern: SearchPattern,
    columns: HashSet<String>,
}

impl RowFilter {
    pub(crate) fn new(pattern: SearchPattern, columns: impl IntoIterator<Item = String>) -> Self {
        Self {
            pattern,
            columns: columns.into_iter().collect(),
        }
    }

    pub fn expression(&self) -> &str {
        self.pattern.expression()
    }

    pub(crate) fn matches(&self, column: &str, value: &str) -> bool {
        self.columns.contains(column) && self.pattern.is_match(value)
    }
}

pub(crate) struct FilterResult {
    pub rows: RowIndex,
    pub filter: RowFilter,
}
pub(crate) struct FilterUpdate {
    pub rows: RoaringTreemap,
}
pub(crate) type FilterWorker = Worker<FilterRequest>;

/// Sparse prior results can skip pages as well as whole row groups. Cap the
/// selector list; dense/fragmented groups fall back to a bounded batch scan.
fn sparse_selection(
    rows: &RoaringTreemap,
    start: usize,
    end: usize,
    cancel: &AtomicBool,
) -> Option<RowSelection> {
    if rows.range_cardinality(start as u64..end as u64) > ((end - start) / 2) as u64 {
        return None;
    }
    let mut ids = rows.iter();
    ids.advance_to(start as u64);
    let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
    for id in ids.take_while(|&r| r < end as u64) {
        if cancel.load(Ordering::Relaxed) {
            return None;
        }
        let local = id as usize - start;
        if let Some(last) = ranges.last_mut()
            && last.end == local
        {
            last.end += 1;
        } else {
            if ranges.len() >= 16_384 {
                return None;
            }
            ranges.push(local..local + 1);
        }
    }
    Some(RowSelection::from_consecutive_ranges(
        ranges.into_iter(),
        end - start,
    ))
}

impl Task for FilterRequest {
    type Output = FilterResult;
    type Update = FilterUpdate;
    type Cache = ();
    const NAME: &'static str = "parqeye-query";
    fn run(
        &self,
        cancel: &AtomicBool,
        progress: &AtomicUsize,
        _: &mut (),
        publish: &mut dyn FnMut(FilterUpdate) -> bool,
    ) -> Result<FilterResult, String> {
        let mut matches = RoaringTreemap::new();
        let mut delta = RoaringTreemap::new();
        let mut last_publish = Instant::now();
        let mut published = false;
        let result = |matches| FilterResult {
            rows: RowIndex::Filtered(Arc::new(matches)),
            filter: RowFilter::new(self.pattern.clone(), self.columns.iter().cloned()),
        };
        if cancel.load(Ordering::Relaxed)
            || self.columns.is_empty()
            || matches!(&self.rows, RowIndex::Filtered(rows) if rows.is_empty())
        {
            return Ok(result(matches));
        }
        let file = File::open(&self.path).map_err(|e| e.to_string())?;
        let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())
            .map_err(|e| e.to_string())?;
        let types =
            crate::file::sample_data::ParquetSampleData::column_types(&self.path, &self.columns)?;
        let mut start = 0usize;
        for (group, rg) in metadata.metadata().row_groups().iter().enumerate() {
            if cancel.load(Ordering::Relaxed) {
                return Ok(result(matches));
            }
            let end = start + rg.num_rows() as usize;
            if let RowIndex::Filtered(rows) = &self.rows
                && rows.range_cardinality(start as u64..end as u64) == 0
            {
                start = end;
                progress.store(end, Ordering::Relaxed);
                continue;
            }
            let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                file.try_clone().map_err(|e| e.to_string())?,
                metadata.clone(),
            )
            .with_row_groups(vec![group]);
            let selection = match &self.rows {
                RowIndex::Filtered(rows) => sparse_selection(rows, start, end, cancel),
                RowIndex::All => None,
            };
            let mut selected_ids = match (&self.rows, &selection) {
                (RowIndex::Filtered(rows), Some(_)) => {
                    let mut iter = rows.iter();
                    iter.advance_to(start as u64);
                    Some(iter)
                }
                _ => None,
            };
            let builder = project(builder, &self.columns)?;
            let builder = if let Some(selection) = selection {
                builder.with_row_selection(selection)
            } else {
                builder
            };
            if cancel.load(Ordering::Relaxed) {
                return Ok(result(matches));
            }
            let reader = builder.build().map_err(|e| e.to_string())?;
            let mut file_row = start;
            for batch in reader {
                if cancel.load(Ordering::Relaxed) {
                    return Ok(result(matches));
                }
                let batch = batch.map_err(|e| e.to_string())?;
                let columns = batch_series(&batch, &self.columns, &types)?;
                for row in 0..batch.num_rows() {
                    if cancel.load(Ordering::Relaxed) {
                        return Ok(result(matches));
                    }
                    if let Some(ids) = &mut selected_ids {
                        file_row = ids.next().ok_or("Unable to map filtered row")? as usize;
                    }
                    let eligible = match &self.rows {
                        RowIndex::All => true,
                        RowIndex::Filtered(rows) => rows.contains(file_row as u64),
                    };
                    if eligible {
                        for column in &columns {
                            if cancel.load(Ordering::Relaxed) {
                                return Ok(result(matches));
                            }
                            let value = column.get(row).map_err(|e| e.to_string())?;
                            if self
                                .pattern
                                .is_match(&crate::file::sample_data::format_cell(&value))
                            {
                                matches.insert(file_row as u64);
                                delta.insert(file_row as u64);
                                break;
                            }
                        }
                    }
                    file_row += 1;
                }
                progress.store(file_row, Ordering::Relaxed);
                // First results arrive after the first matching batch. Later
                // deltas are throttled and capped, without copying the full index.
                if !delta.is_empty()
                    && (!published
                        || last_publish.elapsed() >= Duration::from_millis(100)
                        || delta.len() >= 65_536)
                {
                    if matches.serialized_size() > INDEX_BUDGET {
                        return Err(
                            "Too many row matches to index (128 MiB limit); narrow the regex"
                                .into(),
                        );
                    }
                    if !publish(FilterUpdate {
                        rows: std::mem::take(&mut delta),
                    }) {
                        return Ok(result(matches));
                    }
                    published = true;
                    last_publish = Instant::now();
                }
                if file_row % (BATCH_ROWS * 32) < BATCH_ROWS
                    && matches.serialized_size() > INDEX_BUDGET
                {
                    return Err(
                        "Too many row matches to index (128 MiB limit); narrow the regex".into(),
                    );
                }
            }
            start = end;
            progress.store(end, Ordering::Relaxed);
        }
        if matches.serialized_size() > INDEX_BUDGET {
            return Err("Too many row matches to index (128 MiB limit); narrow the regex".into());
        }
        if !delta.is_empty() && !publish(FilterUpdate { rows: delta }) {
            return Ok(result(matches));
        }
        Ok(result(matches))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use std::{path::PathBuf, sync::atomic::AtomicU64};

    pub(crate) struct Fixture(pub PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    pub(crate) fn fixture() -> Fixture {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "parqeye-filter-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(0..6005)),
            Arc::new(StringArray::from_iter_values(
                (0..6005).map(|row| if row % 3 == 0 { "Failed" } else { "ok" }),
            )),
            Arc::new(StringArray::from(
                (0..6005)
                    .map(|row| match row % 5 {
                        0 => Some("timeout"),
                        1 => Some("Failed"),
                        2 => Some("ÉCHEC"),
                        3 => Some(""),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
        ];
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2500))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Fixture(path)
    }
    fn request(path: &str, columns: &[&str], expression: &str, rows: RowIndex) -> FilterRequest {
        FilterRequest {
            path: path.into(),
            columns: columns.iter().map(|s| (*s).into()).collect(),
            rows,
            pattern: SearchPattern::new(expression).unwrap(),
        }
    }
    fn run(request: FilterRequest) -> RowIndex {
        request
            .run(
                &AtomicBool::new(false),
                &AtomicUsize::new(0),
                &mut (),
                &mut |_| true,
            )
            .unwrap()
            .rows
    }
    fn ids(index: &RowIndex) -> Vec<usize> {
        (0..index.len(6005))
            .map(|p| index.file_row(p).unwrap())
            .collect()
    }
    #[test]
    fn filter_publishes_bounded_deltas_before_finishing_and_their_union_equals_the_result() {
        let fixture = fixture();
        let request = request(fixture.0.to_str().unwrap(), &["id"], ".*", RowIndex::All);
        let progress = AtomicUsize::new(0);
        let mut combined = RoaringTreemap::new();
        let mut first_progress = None;
        let mut batches = 0;
        let result = request
            .run(&AtomicBool::new(false), &progress, &mut (), &mut |update| {
                first_progress.get_or_insert(progress.load(Ordering::Relaxed));
                assert!(update.rows.len() <= 65_536 + BATCH_ROWS as u64);
                assert!(
                    combined
                        .max()
                        .zip(update.rows.min())
                        .is_none_or(|(last, next)| last < next)
                );
                combined |= update.rows;
                batches += 1;
                true
            })
            .unwrap();
        assert!(first_progress.unwrap() < 6005);
        assert!(batches >= 2);
        assert_eq!(combined.len(), 6005);
        assert_eq!(
            ids(&result.rows),
            combined.iter().map(|r| r as usize).collect::<Vec<_>>()
        );
    }

    #[test]
    fn row_filter_matches_any_projected_cell_and_composes_with_and_across_groups() {
        let fixture = fixture();
        let path = fixture.0.to_str().unwrap();
        let status = run(request(path, &["status"], "^failed$", RowIndex::All));
        assert_eq!(
            ids(&status),
            (0..6005).filter(|r| r % 3 == 0).collect::<Vec<_>>()
        );
        let both = run(request(
            path,
            &["message", "status"],
            "failed",
            RowIndex::All,
        ));
        assert_eq!(
            ids(&both),
            (0..6005)
                .filter(|r| r % 3 == 0 || r % 5 == 1)
                .collect::<Vec<_>>()
        );
        let narrowed = run(request(path, &["message"], "^timeout$", status));
        assert_eq!(ids(&narrowed), (0..6005).step_by(15).collect::<Vec<_>>());
        // Two matches in different row groups, beyond the initial UI window.
        let sparse = run(request(path, &["id"], "^(42|5999)$", both));
        assert_eq!(ids(&sparse), vec![42]);
        let unicode = run(request(path, &["message"], "^échec$", RowIndex::All));
        assert_eq!(ids(&unicode), (2..6005).step_by(5).collect::<Vec<_>>());
        let nulls = run(request(path, &["message"], "^NULL$", RowIndex::All));
        assert_eq!(ids(&nulls), (4..6005).step_by(5).collect::<Vec<_>>());
        let empty = run(request(path, &["message"], "^$", RowIndex::All));
        assert_eq!(ids(&empty), (3..6005).step_by(5).collect::<Vec<_>>());
        let sensitive = run(request(path, &["status"], "(?-i)^failed$", RowIndex::All));
        assert_eq!(sensitive.len(6005), 0);
    }
    #[test]
    fn cancelled_and_empty_filters_do_not_open_the_file_and_errors_propagate() {
        let request = request("nonexistent", &["id"], "value", RowIndex::All);
        assert!(
            request
                .run(
                    &AtomicBool::new(true),
                    &AtomicUsize::new(0),
                    &mut (),
                    &mut |_| true
                )
                .is_ok()
        );
        assert!(
            request
                .run(
                    &AtomicBool::new(false),
                    &AtomicUsize::new(0),
                    &mut (),
                    &mut |_| true
                )
                .is_err()
        );
        let empty = FilterRequest {
            rows: RowIndex::Filtered(Arc::default()),
            ..request
        };
        assert!(
            empty
                .run(
                    &AtomicBool::new(false),
                    &AtomicUsize::new(0),
                    &mut (),
                    &mut |_| true
                )
                .is_ok()
        );
    }
}
