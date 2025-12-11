use moirai::{
    coroutine::move_to,
    jobs::{JobLocation, JobQueue},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

// In this example we show how to create very simple custom runtime with
// main thread coroutines worker and a dedicated thread worker for heavier jobs.
// We omit many features a fancy runtime would have, but this should give
// you an idea how to build your own runtime on top of moirai primitives.
fn main() {
    println!(
        "Creating runtime on thread: {:?}",
        std::thread::current().id()
    );
    let runtime = Runtime::default();
    let thread_queue = runtime.thread.queue().clone();

    let job = runtime.coroutines.queue().spawn((), async move {
        println!("Running job on thread: {:?}", std::thread::current().id());

        move_to(JobLocation::Queue(thread_queue)).await;

        println!("Resumed job on thread: {:?}", std::thread::current().id());
    });

    while runtime.is_busy() {
        runtime.run_local();
    }
    job.wait().unwrap();
}

// A simple custom runtime with a local coroutine worker and a thread worker.
struct Runtime {
    pub coroutines: RuntimeWorker,
    pub thread: RuntimeWorker,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            coroutines: RuntimeWorker::local(),
            thread: RuntimeWorker::thread(JobLocation::UnnamedWorker),
        }
    }
}

impl Runtime {
    pub fn is_busy(&self) -> bool {
        !self.coroutines.queue().is_empty() || !self.thread.queue().is_empty()
    }

    // Run local jobs (coroutines) queue on the main thread.
    pub fn run_local(&self) {
        // Single queue pass that polls queue jobs. It doesn't run until queue
        // is empty, mainly because we don't want to block any thread infinitely,
        // rather we let user control the runtime loop.
        self.coroutines.queue().run(
            JobLocation::Local,
            // Since we have queue per worker, we can ignore matching jobs and
            // worker locations.
            true,
            Duration::from_millis(1),
            Default::default(),
            [],
            Default::default(),
            // For simplicity we don't use worker notifiers.
            Default::default(),
        );
    }
}

// A runtime worker that manages its own job queue and execution thread.
struct RuntimeWorker {
    queue: JobQueue,
    handle: Option<JoinHandle<()>>,
    terminate: Arc<AtomicBool>,
}

impl Drop for RuntimeWorker {
    fn drop(&mut self) {
        self.terminate.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

impl RuntimeWorker {
    fn queue(&self) -> &JobQueue {
        &self.queue
    }

    // Local worker does not need dedicated thread, since it runs on the main thread.
    fn local() -> Self {
        Self {
            queue: Default::default(),
            handle: None,
            terminate: Default::default(),
        }
    }

    // Thread worker spawns a dedicated thread to run its job queue.
    fn thread(location: JobLocation) -> Self {
        let location_clone = location.clone();
        let queue = JobQueue::default();
        let queue_clone = queue.clone();
        let terminate = Arc::new(AtomicBool::new(false));
        let terminate_clone = terminate.clone();

        // Spawn a thread to run the job queue until termination is requested.
        let handle = std::thread::spawn(move || {
            println!(
                "Starting runtime worker thread: {:?}",
                std::thread::current().id()
            );
            while !terminate_clone.load(Ordering::Relaxed) {
                queue_clone.run(
                    location_clone.clone(),
                    true,
                    Duration::from_millis(1),
                    Default::default(),
                    [],
                    Default::default(),
                    Default::default(),
                );
            }
            println!(
                "Terminating runtime worker thread: {:?}",
                std::thread::current().id()
            );
        });

        Self {
            queue,
            handle: Some(handle),
            terminate,
        }
    }
}
