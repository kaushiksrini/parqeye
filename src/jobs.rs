//! Shared lazy background workers with bounded streaming updates. Every update
//! and completion belongs to a generation; cancelled generations are discarded.
use std::{
    convert::Infallible,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender, SyncSender, TrySendError},
    },
    thread,
    time::Duration,
};

const UPDATE_QUEUE_CAPACITY: usize = 2;

pub(crate) trait Task: Send + 'static {
    type Output: Send + 'static;
    type Update: Send + 'static;
    type Cache: Default + Send + 'static;
    const NAME: &'static str;
    fn initial_progress(&self) -> usize {
        0
    }
    /// Return false from `publish` when cancelled or the consumer has gone away.
    fn run(
        &self,
        cancel: &AtomicBool,
        progress: &AtomicUsize,
        cache: &mut Self::Cache,
        publish: &mut dyn FnMut(Self::Update) -> bool,
    ) -> Result<Self::Output, String>;
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WorkerEvent<U, R> {
    Update(U),
    Finished(Result<R, String>),
}
type TaskEvent<T> = WorkerEvent<<T as Task>::Update, <T as Task>::Output>;
struct Job<T: Task> {
    generation: u64,
    request: T,
    cancel: Arc<AtomicBool>,
    progress: Arc<AtomicUsize>,
}
struct Reply<U, R> {
    generation: u64,
    event: WorkerEvent<U, R>,
}
struct Pending {
    generation: u64,
    cancel: Arc<AtomicBool>,
    progress: Arc<AtomicUsize>,
}

/// A full queue slows the producer without retaining unbounded result batches.
/// Unlike blocking send, this also wakes on cancellation if the UI stops polling.
fn send_reply<U, R>(
    sender: &SyncSender<Reply<U, R>>,
    mut reply: Reply<U, R>,
    cancel: &AtomicBool,
) -> bool {
    loop {
        if cancel.load(Ordering::Relaxed) {
            return false;
        }
        match sender.try_send(reply) {
            Ok(()) => return true,
            Err(TrySendError::Disconnected(_)) => return false,
            Err(TrySendError::Full(value)) => {
                reply = value;
                thread::sleep(Duration::from_millis(5));
            }
        }
    }
}

pub(crate) struct Worker<T: Task> {
    sender: Option<Sender<Job<T>>>,
    receiver: Option<Receiver<Reply<T::Update, T::Output>>>,
    generation: u64,
    pending: Option<Pending>,
}
impl<T: Task> Default for Worker<T> {
    fn default() -> Self {
        Self {
            sender: None,
            receiver: None,
            generation: 0,
            pending: None,
        }
    }
}
impl<T: Task> Worker<T> {
    /// Deterministic event delivery for integration tests of streaming consumers.
    #[cfg(test)]
    pub(crate) fn test_stream() -> (Self, impl Fn(usize, TaskEvent<T>)) {
        let (sender, receiver) = mpsc::channel();
        let progress = Arc::new(AtomicUsize::new(0));
        let worker = Self {
            sender: None,
            receiver: Some(receiver),
            generation: 1,
            pending: Some(Pending {
                generation: 1,
                cancel: Arc::new(AtomicBool::new(false)),
                progress: Arc::clone(&progress),
            }),
        };
        (worker, move |row, event| {
            progress.store(row, Ordering::Relaxed);
            assert!(
                sender
                    .send(Reply {
                        generation: 1,
                        event
                    })
                    .is_ok()
            );
        })
    }

    pub fn start(&mut self, request: T) -> io::Result<()> {
        self.cancel();
        if self.sender.is_none() {
            let (jobs_tx, jobs_rx) = mpsc::channel::<Job<T>>();
            let (results_tx, results_rx) = mpsc::sync_channel(UPDATE_QUEUE_CAPACITY);
            thread::Builder::new().name(T::NAME.into()).spawn(move || {
                let mut cache = T::Cache::default();
                while let Ok(job) = jobs_rx.recv() {
                    if job.cancel.load(Ordering::Relaxed) {
                        continue;
                    }
                    let mut publish = |update| {
                        send_reply(
                            &results_tx,
                            Reply {
                                generation: job.generation,
                                event: WorkerEvent::Update(update),
                            },
                            &job.cancel,
                        )
                    };
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        job.request
                            .run(&job.cancel, &job.progress, &mut cache, &mut publish)
                    }))
                    .unwrap_or_else(|_| Err("Background job failed".into()));
                    send_reply(
                        &results_tx,
                        Reply {
                            generation: job.generation,
                            event: WorkerEvent::Finished(result),
                        },
                        &job.cancel,
                    );
                }
            })?;
            self.sender = Some(jobs_tx);
            self.receiver = Some(results_rx);
        }
        self.generation += 1;
        let cancel = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(AtomicUsize::new(request.initial_progress()));
        self.sender
            .as_ref()
            .unwrap()
            .send(Job {
                generation: self.generation,
                request,
                cancel: Arc::clone(&cancel),
                progress: Arc::clone(&progress),
            })
            .map_err(|_| io::Error::other("Background worker stopped"))?;
        self.pending = Some(Pending {
            generation: self.generation,
            cancel,
            progress,
        });
        Ok(())
    }
    pub fn is_running(&self) -> bool {
        self.pending.is_some()
    }
    pub fn current_row(&self) -> usize {
        self.pending
            .as_ref()
            .map_or(0, |job| job.progress.load(Ordering::Relaxed))
    }
    pub fn cancel(&mut self) {
        if let Some(job) = self.pending.take() {
            job.cancel.store(true, Ordering::Relaxed);
        }
    }
    pub fn poll_event(&mut self) -> Option<TaskEvent<T>> {
        let receiver = self.receiver.as_ref()?;
        loop {
            match receiver.try_recv() {
                Ok(reply) => {
                    if self
                        .pending
                        .as_ref()
                        .is_some_and(|job| job.generation == reply.generation)
                    {
                        if matches!(reply.event, WorkerEvent::Finished(_)) {
                            self.pending = None;
                        }
                        return Some(reply.event);
                    }
                }
                Err(mpsc::TryRecvError::Empty) => return None,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.sender = None;
                    return self
                        .pending
                        .take()
                        .map(|_| WorkerEvent::Finished(Err("Background worker stopped".into())));
                }
            }
        }
    }
}
// Tasks such as find can continue to expose a single-result polling interface.
impl<T: Task<Update = Infallible>> Worker<T> {
    pub fn poll(&mut self) -> Option<Result<T::Output, String>> {
        match self.poll_event()? {
            WorkerEvent::Finished(result) => Some(result),
            WorkerEvent::Update(never) => match never {},
        }
    }
}
impl<T: Task> Drop for Worker<T> {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    struct StreamingTask {
        resume: Option<Receiver<()>>,
        sent: Arc<AtomicUsize>,
        done: Arc<AtomicBool>,
        count: usize,
    }
    impl Task for StreamingTask {
        type Output = usize;
        type Update = usize;
        type Cache = ();
        const NAME: &'static str = "test-stream";
        fn run(
            &self,
            _: &AtomicBool,
            progress: &AtomicUsize,
            _: &mut (),
            publish: &mut dyn FnMut(usize) -> bool,
        ) -> Result<usize, String> {
            for value in 1..=self.count {
                progress.store(value, Ordering::Relaxed);
                if !publish(value) {
                    break;
                }
                self.sent.fetch_add(1, Ordering::Relaxed);
                if value == 1
                    && let Some(resume) = &self.resume
                {
                    resume.recv().map_err(|e| e.to_string())?;
                }
            }
            self.done.store(true, Ordering::Relaxed);
            Ok(self.count)
        }
    }
    fn next_event<T: Task>(worker: &mut Worker<T>) -> WorkerEvent<T::Update, T::Output> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(event) = worker.poll_event() {
                return event;
            }
            assert!(Instant::now() < deadline, "worker did not publish");
            thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn worker_delivers_updates_while_task_is_unfinished() {
        let (resume, gate) = mpsc::channel();
        let mut worker = Worker::<StreamingTask>::default();
        let done = Arc::new(AtomicBool::new(false));
        worker
            .start(StreamingTask {
                resume: Some(gate),
                sent: Arc::default(),
                done: Arc::clone(&done),
                count: 3,
            })
            .unwrap();
        assert_eq!(next_event(&mut worker), WorkerEvent::Update(1));
        assert!(worker.is_running());
        assert!(!done.load(Ordering::Relaxed));
        assert!(worker.poll_event().is_none());
        resume.send(()).unwrap();
        assert_eq!(next_event(&mut worker), WorkerEvent::Update(2));
        assert_eq!(next_event(&mut worker), WorkerEvent::Update(3));
        assert_eq!(next_event(&mut worker), WorkerEvent::Finished(Ok(3)));
        assert!(!worker.is_running());
    }

    #[test]
    fn full_update_queue_bounds_the_producer_and_cancellation_unblocks_it() {
        let sent = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let mut worker = Worker::<StreamingTask>::default();
        worker
            .start(StreamingTask {
                resume: None,
                sent: Arc::clone(&sent),
                done: Arc::clone(&done),
                count: 1000,
            })
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while sent.load(Ordering::Relaxed) < UPDATE_QUEUE_CAPACITY {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(sent.load(Ordering::Relaxed), UPDATE_QUEUE_CAPACITY);
        assert!(!done.load(Ordering::Relaxed));
        worker.cancel();
        while !done.load(Ordering::Relaxed) {
            assert!(
                Instant::now() < deadline,
                "producer stayed blocked after cancellation"
            );
            thread::sleep(Duration::from_millis(2));
        }
        assert!(worker.poll_event().is_none());
    }

    struct TestTask;
    impl Task for TestTask {
        type Output = usize;
        type Update = usize;
        type Cache = ();
        const NAME: &'static str = "test-worker";
        fn run(
            &self,
            _: &AtomicBool,
            _: &AtomicUsize,
            _: &mut (),
            _: &mut dyn FnMut(usize) -> bool,
        ) -> Result<usize, String> {
            Ok(7)
        }
    }
    #[test]
    fn worker_discards_stale_updates_completions_and_cancelled_generations() {
        let (sender, receiver) = mpsc::channel();
        let mut worker = Worker::<TestTask> {
            sender: None,
            receiver: Some(receiver),
            generation: 2,
            pending: Some(Pending {
                generation: 2,
                cancel: Arc::new(AtomicBool::new(false)),
                progress: Arc::new(AtomicUsize::new(0)),
            }),
        };
        sender
            .send(Reply {
                generation: 1,
                event: WorkerEvent::Update(99),
            })
            .unwrap();
        sender
            .send(Reply {
                generation: 1,
                event: WorkerEvent::Finished(Ok(99)),
            })
            .unwrap();
        sender
            .send(Reply {
                generation: 2,
                event: WorkerEvent::Update(3),
            })
            .unwrap();
        sender
            .send(Reply {
                generation: 2,
                event: WorkerEvent::Finished(Ok(7)),
            })
            .unwrap();
        assert_eq!(worker.poll_event(), Some(WorkerEvent::Update(3)));
        assert!(worker.is_running());
        assert_eq!(worker.poll_event(), Some(WorkerEvent::Finished(Ok(7))));
        assert!(!worker.is_running());
        let cancel = Arc::new(AtomicBool::new(false));
        worker.pending = Some(Pending {
            generation: 3,
            cancel: Arc::clone(&cancel),
            progress: Arc::new(AtomicUsize::new(0)),
        });
        worker.cancel();
        assert!(cancel.load(Ordering::Relaxed));
        sender
            .send(Reply {
                generation: 3,
                event: WorkerEvent::Update(88),
            })
            .unwrap();
        sender
            .send(Reply {
                generation: 3,
                event: WorkerEvent::Finished(Ok(88)),
            })
            .unwrap();
        assert_eq!(worker.poll_event(), None);
    }
}
