//! Cell search over bounded chunks. A single worker retains only its latest
//! chunk, so repeated n/N within it reuse decoded values without a file scan.
use crate::file::sample_data::ParquetSampleData;
use regex::{Regex, RegexBuilder};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread,
};

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

struct Job {
    generation: u64,
    request: SearchRequest,
    cancel: Arc<AtomicBool>,
    scanned: Arc<AtomicUsize>,
}

struct Reply {
    generation: u64,
    result: Result<Option<SearchHit>, String>,
}

struct Pending {
    generation: u64,
    cancel: Arc<AtomicBool>,
    scanned: Arc<AtomicUsize>,
}

#[derive(Default)]
pub(crate) struct SearchWorker {
    sender: Option<Sender<Job>>,
    receiver: Option<Receiver<Reply>>,
    generation: u64,
    pending: Option<Pending>,
}

impl SearchWorker {
    pub fn start(&mut self, request: SearchRequest) -> io::Result<()> {
        self.cancel();
        if self.sender.is_none() {
            let (jobs_tx, jobs_rx) = mpsc::channel::<Job>();
            let (results_tx, results_rx) = mpsc::channel();
            thread::Builder::new()
                .name("parqeye-find".into())
                .spawn(move || {
                    let mut cache = ChunkCache::default();
                    while let Ok(job) = jobs_rx.recv() {
                        if job.cancel.load(Ordering::Relaxed) {
                            continue;
                        }
                        let result = find(&job.request, &job.cancel, &job.scanned, &mut cache);
                        if !job.cancel.load(Ordering::Relaxed)
                            && results_tx
                                .send(Reply {
                                    generation: job.generation,
                                    result,
                                })
                                .is_err()
                        {
                            break;
                        }
                    }
                })?;
            self.sender = Some(jobs_tx);
            self.receiver = Some(results_rx);
        }
        self.generation += 1;
        let cancel = Arc::new(AtomicBool::new(false));
        let scanned = Arc::new(AtomicUsize::new(0));
        self.sender
            .as_ref()
            .unwrap()
            .send(Job {
                generation: self.generation,
                request,
                cancel: Arc::clone(&cancel),
                scanned: Arc::clone(&scanned),
            })
            .map_err(|_| io::Error::other("search worker stopped"))?;
        self.pending = Some(Pending {
            generation: self.generation,
            cancel,
            scanned,
        });
        Ok(())
    }

    pub fn is_running(&self) -> bool {
        self.pending.is_some()
    }

    pub fn scanned_rows(&self) -> usize {
        self.pending
            .as_ref()
            .map_or(0, |job| job.scanned.load(Ordering::Relaxed))
    }

    pub fn cancel(&mut self) {
        if let Some(job) = self.pending.take() {
            job.cancel.store(true, Ordering::Relaxed);
        }
    }

    pub fn poll(&mut self) -> Option<Result<Option<SearchHit>, String>> {
        let receiver = self.receiver.as_ref()?;
        loop {
            match receiver.try_recv() {
                Ok(reply) => {
                    if self
                        .pending
                        .as_ref()
                        .is_some_and(|job| job.generation == reply.generation)
                    {
                        self.pending = None;
                        return Some(reply.result);
                    }
                }
                Err(mpsc::TryRecvError::Empty) => return None,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.sender = None;
                    return self
                        .pending
                        .take()
                        .map(|_| Err("Search worker stopped".into()));
                }
            }
        }
    }
}

impl Drop for SearchWorker {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Default)]
struct ChunkCache {
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
    scanned: &AtomicUsize,
    cache: &mut ChunkCache,
) -> Result<Option<SearchHit>, String> {
    let columns = request.columns.len();
    let cells = request
        .total_rows
        .checked_mul(columns)
        .ok_or("Too many cells to search")?;
    if cells == 0 {
        return Ok(None);
    }
    cache.prepare(request);
    let mut position =
        request.row.min(request.total_rows - 1) * columns + request.column.min(columns - 1);
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
    let mut last_row = None;
    for _ in 0..cells {
        if cancel.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let row = position / columns;
        let column = position % columns;
        if last_row != Some(row) {
            scanned.fetch_add(1, Ordering::Relaxed);
            last_row = Some(row);
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
                row,
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

    #[test]
    fn worker_discards_stale_results_and_cancelled_generations() {
        let (sender, receiver) = mpsc::channel();
        let mut worker = SearchWorker {
            sender: None,
            receiver: Some(receiver),
            generation: 2,
            pending: Some(Pending {
                generation: 2,
                cancel: Arc::new(AtomicBool::new(false)),
                scanned: Arc::new(AtomicUsize::new(0)),
            }),
        };
        sender
            .send(Reply {
                generation: 1,
                result: Ok(Some(SearchHit {
                    row: 99,
                    column: 99,
                    wrapped: false,
                })),
            })
            .unwrap();
        sender
            .send(Reply {
                generation: 2,
                result: Ok(None),
            })
            .unwrap();
        assert_eq!(worker.poll(), Some(Ok(None)));
        assert!(!worker.is_running());
        let cancel = Arc::new(AtomicBool::new(false));
        worker.pending = Some(Pending {
            generation: 3,
            cancel: Arc::clone(&cancel),
            scanned: Arc::new(AtomicUsize::new(0)),
        });
        worker.cancel();
        assert!(cancel.load(Ordering::Relaxed));
        sender
            .send(Reply {
                generation: 3,
                result: Ok(None),
            })
            .unwrap();
        assert_eq!(worker.poll(), None);
    }
}
