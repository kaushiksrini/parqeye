//! Cell search over bounded chunks. A single worker retains only its latest
//! chunk, so repeated n/N within it reuse decoded values without a file scan.
use crate::file::sample_data::ParquetSampleData;
use crate::{
    jobs::{Task, Worker},
    view::RowIndex,
};
use regex::{Regex, RegexBuilder};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const CHUNK_ROWS: usize = 2_000;

#[derive(Clone, Debug)]
pub(crate) struct SearchPattern(Regex);

impl SearchPattern {
    pub fn new(expression: &str) -> Result<Self, String> {
        RegexBuilder::new(expression)
            .case_insensitive(true)
            .build()
            .map(Self)
            .map_err(super::command::invalid_regex)
    }

    pub fn expression(&self) -> &str {
        self.0.as_str()
    }

    pub fn is_match(&self, value: &str) -> bool {
        self.0.is_match(value)
    }
}

impl PartialEq for SearchPattern {
    fn eq(&self, other: &Self) -> bool {
        self.expression() == other.expression()
    }
}
impl Eq for SearchPattern {}

pub(crate) struct SearchRequest {
    pub path: String,
    /// Names in visible order, including columns horizontally off screen.
    pub columns: Vec<String>,
    pub total_rows: usize,
    pub rows: RowIndex,
    pub pattern: SearchPattern,
    pub row: usize,
    pub column: usize,
    pub backwards: bool,
    pub include_current: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SearchHit {
    pub row: usize,
    pub column: usize,
    pub wrapped: bool,
}

pub(crate) type SearchWorker = Worker<SearchRequest>;

impl Task for SearchRequest {
    type Output = Option<SearchHit>;
    type Update = std::convert::Infallible;
    type Cache = ChunkCache;
    const NAME: &'static str = "parqeye-find";
    fn initial_progress(&self) -> usize {
        self.rows
            .file_row(self.row)
            .map_or(0, |row| row.saturating_add(1).min(self.total_rows))
    }
    fn run(
        &self,
        cancel: &AtomicBool,
        progress: &AtomicUsize,
        cache: &mut ChunkCache,
        _: &mut dyn FnMut(Self::Update) -> bool,
    ) -> Result<Self::Output, String> {
        find(self, cancel, progress, cache)
    }
}

#[derive(Default)]
pub(crate) struct ChunkCache {
    path: String,
    columns: Vec<String>,
    start: usize,
    rows: Vec<Vec<String>>,
}

impl ChunkCache {
    fn prepare(&mut self, request: &SearchRequest) {
        if self.path != request.path || self.columns != request.columns {
            self.path.clone_from(&request.path);
            self.columns.clone_from(&request.columns);
            self.rows.clear();
        }
    }

    fn row(&mut self, request: &SearchRequest, row: usize) -> Result<&[String], String> {
        if row < self.start || row >= self.start + self.rows.len() {
            let start = row / CHUNK_ROWS * CHUNK_ROWS;
            let rows = ParquetSampleData::search_rows(
                &request.path,
                start,
                CHUNK_ROWS.min(request.total_rows - start),
                &request.columns,
            )?;
            self.start = start;
            self.rows = rows;
        }
        self.rows
            .get(row - self.start)
            .map(Vec::as_slice)
            .ok_or_else(|| "Unable to read search row".into())
    }
}

fn find(
    request: &SearchRequest,
    cancel: &AtomicBool,
    current_row: &AtomicUsize,
    cache: &mut ChunkCache,
) -> Result<Option<SearchHit>, String> {
    let columns = request.columns.len();
    let visible_rows = request.rows.len(request.total_rows);
    let cells = visible_rows
        .checked_mul(columns)
        .ok_or("Too many cells to search")?;
    if cells == 0 {
        return Ok(None);
    }
    cache.prepare(request);
    let mut position =
        request.row.min(visible_rows - 1) * columns + request.column.min(columns - 1);
    let mut wrapped = false;
    let advance = |position: &mut usize, wrapped: &mut bool| {
        if request.backwards {
            if *position == 0 {
                *position = cells - 1;
                *wrapped = true;
            } else {
                *position -= 1;
            }
        } else if *position == cells - 1 {
            *position = 0;
            *wrapped = true;
        } else {
            *position += 1;
        }
    };
    if !request.include_current {
        advance(&mut position, &mut wrapped);
    }
    let mut filtered = match &request.rows {
        RowIndex::Filtered(rows) => Some((rows, rows.iter())),
        RowIndex::All => None,
    };
    let mut last_position: Option<usize> = None;
    let mut row = 0;
    for _ in 0..cells {
        if cancel.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let view_row = position / columns;
        let column = position % columns;
        if last_position != Some(view_row) {
            row = if let Some((rows, iter)) = &mut filtered {
                let sequential = last_position.is_some_and(|previous| {
                    if request.backwards {
                        previous.checked_sub(1) == Some(view_row)
                    } else {
                        previous + 1 == view_row
                    }
                });
                if !sequential {
                    *iter = rows.iter();
                    let file_row = rows.select(view_row as u64).ok_or("Invalid visible row")?;
                    if request.backwards {
                        iter.advance_back_to(file_row);
                    } else {
                        iter.advance_to(file_row);
                    }
                }
                if request.backwards {
                    iter.next_back()
                } else {
                    iter.next()
                }
                .ok_or("Invalid visible row")? as usize
            } else {
                view_row
            };
            current_row.store(row + 1, Ordering::Relaxed);
            last_position = Some(view_row);
        }
        let values = cache.row(request, row)?;
        if cancel.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if values
            .get(column)
            .is_some_and(|value| request.pattern.is_match(value))
        {
            return Ok(Some(SearchHit {
                row: position / columns,
                column,
                wrapped,
            }));
        }
        advance(&mut position, &mut wrapped);
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(expression: &str) -> SearchRequest {
        SearchRequest {
            path: "cached-only.parquet".into(),
            columns: vec!["a".into(), "b".into()],
            rows: RowIndex::All,
            total_rows: 3,
            pattern: SearchPattern::new(expression).unwrap(),
            row: 0,
            column: 0,
            backwards: false,
            include_current: true,
        }
    }

    fn cache(request: &SearchRequest) -> ChunkCache {
        ChunkCache {
            path: request.path.clone(),
            columns: request.columns.clone(),
            start: 0,
            rows: [["error one", "ÉRROR"], ["ok", "error two"], ["NULL", "ok"]]
                .into_iter()
                .map(|row| row.into_iter().map(String::from).collect())
                .collect(),
        }
    }

    fn run(request: &SearchRequest, cache: &mut ChunkCache) -> Option<SearchHit> {
        find(
            request,
            &AtomicBool::new(false),
            &AtomicUsize::new(0),
            cache,
        )
        .unwrap()
    }

    #[test]
    fn filtered_find_skips_excluded_rows_and_preserves_file_progress_in_both_directions() {
        let mut request = request("^error two$");
        let mut cache = cache(&request);
        request.rows = RowIndex::Filtered(std::sync::Arc::new([0, 2].into_iter().collect()));
        assert_eq!(run(&request, &mut cache), None);
        request.pattern = SearchPattern::new("^NULL$").unwrap();
        let progress = AtomicUsize::new(0);
        assert_eq!(
            find(&request, &AtomicBool::new(false), &progress, &mut cache).unwrap(),
            Some(SearchHit {
                row: 1,
                column: 0,
                wrapped: false
            })
        );
        assert_eq!(progress.load(Ordering::Relaxed), 3);
        request.row = 1;
        request.pattern = SearchPattern::new("error|érror").unwrap();
        request.backwards = true;
        request.include_current = false;
        assert_eq!(
            run(&request, &mut cache),
            Some(SearchHit {
                row: 0,
                column: 1,
                wrapped: false
            })
        );
        request.row = 0;
        request.column = 0;
        assert_eq!(
            run(&request, &mut cache),
            Some(SearchHit {
                row: 0,
                column: 1,
                wrapped: true
            })
        );
        request.backwards = false;
        request.row = 1;
        assert_eq!(
            run(&request, &mut cache),
            Some(SearchHit {
                row: 0,
                column: 0,
                wrapped: true
            })
        );
    }

    #[test]
    fn searches_cells_in_both_directions_and_wraps_without_a_global_match_list() {
        let mut request = request("error|érror");
        let mut cache = cache(&request);
        let next = |row, column, wrapped| {
            Some(SearchHit {
                row,
                column,
                wrapped,
            })
        };
        assert_eq!(run(&request, &mut cache), next(0, 0, false));
        request.include_current = false;
        assert_eq!(run(&request, &mut cache), next(0, 1, false));
        request.column = 1;
        assert_eq!(run(&request, &mut cache), next(1, 1, false));
        request.row = 1;
        assert_eq!(run(&request, &mut cache), next(0, 0, true));
        request.backwards = true;
        assert_eq!(run(&request, &mut cache), next(0, 1, false));
        request.row = 0;
        request.column = 0;
        assert_eq!(run(&request, &mut cache), next(1, 1, true));
        // A pattern with only one hit returns to that cell after a complete wrap.
        request.pattern = SearchPattern::new("^error one$").unwrap();
        assert_eq!(run(&request, &mut cache), next(0, 0, true));
    }

    #[test]
    fn progress_follows_file_rows_from_the_cursor_in_both_directions_and_after_wrap() {
        for (pattern, row, column, backwards, expected_row, wrapped) in [
            ("^error two$", 1, 0, false, 2, false),
            ("^NULL$", 1, 1, false, 3, false),
            ("^érror$", 1, 1, true, 1, false),
            ("^error one$", 2, 1, false, 1, true),
            ("^NULL$", 0, 0, true, 3, true),
        ] {
            let mut request = request(pattern);
            request.row = row;
            request.column = column;
            request.backwards = backwards;
            request.include_current = false;
            let progress = AtomicUsize::new(row + 1);
            let hit = find(
                &request,
                &AtomicBool::new(false),
                &progress,
                &mut cache(&request),
            )
            .unwrap()
            .unwrap();
            assert_eq!(hit.row + 1, expected_row);
            assert_eq!(hit.wrapped, wrapped);
            assert_eq!(progress.load(Ordering::Relaxed), expected_row);
        }
    }

    #[test]
    fn matching_supports_substrings_anchors_unicode_nulls_and_inline_case_flags() {
        let mut request = request("ERROR");
        let mut cache = cache(&request);
        assert_eq!(run(&request, &mut cache).unwrap().column, 0);
        request.pattern = SearchPattern::new("^error$").unwrap();
        assert_eq!(run(&request, &mut cache), None);
        request.pattern = SearchPattern::new("^érror$").unwrap();
        assert_eq!(run(&request, &mut cache).unwrap().column, 1);
        request.pattern = SearchPattern::new("(?-i)ERROR").unwrap();
        assert_eq!(run(&request, &mut cache), None);
        request.pattern = SearchPattern::new("^NULL$").unwrap();
        assert_eq!(run(&request, &mut cache).unwrap().row, 2);
        assert!(
            SearchPattern::new("[")
                .unwrap_err()
                .starts_with("Invalid regex:")
        );
    }

    #[test]
    fn empty_and_cancelled_searches_never_read_the_file() {
        let mut request = request("anything");
        let mut cache = ChunkCache::default();
        assert_eq!(
            find(
                &request,
                &AtomicBool::new(true),
                &AtomicUsize::new(0),
                &mut cache
            )
            .unwrap(),
            None
        );
        request.total_rows = 0;
        assert_eq!(run(&request, &mut cache), None);
        request.total_rows = 10;
        request.columns.clear();
        assert_eq!(run(&request, &mut cache), None);
    }

    #[test]
    fn search_reads_beyond_the_ui_window_and_keeps_only_one_chunk() {
        let path = format!(
            "{}/alltypes_tiny_pages.parquet",
            crate::file::parquet_test_data()
        );
        let data = ParquetSampleData::open(&path, 7300).unwrap();
        let mut request = request(".");
        request.path = path.clone();
        request.total_rows = 7300;
        request.row = CHUNK_ROWS - 1;
        request.column = 0;
        request.include_current = false;
        request.columns = vec!["id".into()];
        let mut cache = ChunkCache::default();
        assert_eq!(
            run(&request, &mut cache),
            Some(SearchHit {
                row: CHUNK_ROWS,
                column: 0,
                wrapped: false
            })
        );
        assert_eq!(cache.start, CHUNK_ROWS);
        assert!(cache.rows.len() <= CHUNK_ROWS);
        assert_eq!(data.loaded().start, 0);
        // Same chunk: even a nonexistent path would fail if it were re-read.
        cache.path = "cached-only.parquet".into();
        request.path = cache.path.clone();
        request.row = CHUNK_ROWS;
        assert_eq!(run(&request, &mut cache).unwrap().row, CHUNK_ROWS + 1);
        request.path = path;
        request.backwards = true;
        assert_eq!(run(&request, &mut cache).unwrap().row, CHUNK_ROWS - 1);
    }

    #[test]
    fn changed_column_order_invalidates_the_chunk() {
        let mut request = request("anything");
        let mut cache = cache(&request);
        request.columns.reverse();
        // The cached-only path is deliberately nonexistent: reusing stale rows
        // would return a result instead of reporting the required reload error.
        assert!(
            find(
                &request,
                &AtomicBool::new(false),
                &AtomicUsize::new(0),
                &mut cache
            )
            .is_err()
        );
    }
}
