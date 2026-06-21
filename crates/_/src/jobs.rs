use crate::{
    coroutine::context,
    job::{
        AllJobsHandle, Job, JobContext, JobHandle, JobLocation, JobObject, JobOptions, JobPriority,
        JobTokens,
    },
    queue::JobQueue,
    third_party::time::Duration,
};
use intuicio_data::{
    managed::{DynamicManagedLazy, ManagedLazy, gc::DynamicManagedGc},
    type_hash::TypeHash,
};
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{HashMap, HashSet},
    future::poll_fn,
    pin::{Pin, pin},
    sync::{
        Arc, Condvar, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
    thread::{JoinHandle, available_parallelism, spawn},
};
use typid::ID;

#[derive(Default, Clone, PartialEq, Eq)]
pub struct JobsTags {
    tags: HashSet<TypeHash>,
}

impl JobsTags {
    pub fn new(iter: impl IntoIterator<Item = TypeHash>) -> Self {
        Self {
            tags: iter.into_iter().collect(),
        }
    }

    pub fn with<T: 'static + ?Sized>(mut self) -> Self {
        self.add::<T>();
        self
    }

    pub fn extend(&mut self, iter: impl IntoIterator<Item = TypeHash>) {
        self.tags.extend(iter);
    }

    pub fn add<T: 'static + ?Sized>(&mut self) {
        self.tags.insert(TypeHash::of::<T>());
    }

    pub fn remove<T: 'static + ?Sized>(&mut self) {
        self.tags.remove(&TypeHash::of::<T>());
    }

    pub fn contains<T: 'static + ?Sized>(&self) -> bool {
        self.tags.contains(&TypeHash::of::<T>())
    }

    pub fn is_superset_of(&self, other: &JobsTags) -> bool {
        self.tags.is_superset(&other.tags)
    }

    pub fn is_subset_of(&self, other: &JobsTags) -> bool {
        self.tags.is_subset(&other.tags)
    }

    pub fn clear(&mut self) {
        self.tags.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    pub fn len(&self) -> usize {
        self.tags.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = TypeHash> {
        self.tags.iter().copied()
    }
}

pub enum JobsMetaValue {
    Gc(DynamicManagedGc),
    Lazy(DynamicManagedLazy),
}

impl JobsMetaValue {
    pub fn lazy(&self) -> DynamicManagedLazy {
        match self {
            JobsMetaValue::Gc(gc) => gc.lazy(),
            JobsMetaValue::Lazy(lazy) => lazy.clone(),
        }
    }
}

impl Clone for JobsMetaValue {
    fn clone(&self) -> Self {
        match self {
            JobsMetaValue::Gc(gc) => JobsMetaValue::Gc(gc.reference()),
            JobsMetaValue::Lazy(lazy) => JobsMetaValue::Lazy(lazy.clone()),
        }
    }
}

impl From<DynamicManagedGc> for JobsMetaValue {
    fn from(value: DynamicManagedGc) -> Self {
        JobsMetaValue::Gc(value)
    }
}

impl From<DynamicManagedLazy> for JobsMetaValue {
    fn from(value: DynamicManagedLazy) -> Self {
        JobsMetaValue::Lazy(value)
    }
}

#[derive(Default, Clone)]
pub struct JobsMeta {
    meta: Arc<RwLock<HashMap<Cow<'static, str>, JobsMetaValue>>>,
}

impl JobsMeta {
    pub fn with(self, name: impl Into<Cow<'static, str>>, value: impl Into<JobsMetaValue>) -> Self {
        if let Ok(mut meta) = self.meta.write() {
            meta.insert(name.into(), value.into());
        }
        self
    }

    pub fn with_many(
        self,
        iter: impl IntoIterator<Item = (Cow<'static, str>, JobsMetaValue)>,
    ) -> Self {
        if let Ok(mut meta) = self.meta.write() {
            meta.extend(iter);
        }
        self
    }

    pub fn set(&self, name: impl Into<Cow<'static, str>>, value: impl Into<JobsMetaValue>) {
        if let Ok(mut meta) = self.meta.write() {
            meta.insert(name.into(), value.into());
        }
    }

    pub fn set_many(&self, iter: impl IntoIterator<Item = (Cow<'static, str>, JobsMetaValue)>) {
        if let Ok(mut meta) = self.meta.write() {
            meta.extend(iter);
        }
    }

    pub fn unset(&self, name: &str) {
        if let Ok(mut meta) = self.meta.write() {
            meta.remove(name);
        }
    }

    pub fn get(&self, name: &str) -> Option<DynamicManagedLazy> {
        self.meta
            .read()
            .ok()
            .and_then(|meta| meta.get(name).map(|meta| meta.lazy()))
    }
}

impl FromIterator<(Cow<'static, str>, JobsMetaValue)> for JobsMeta {
    fn from_iter<T: IntoIterator<Item = (Cow<'static, str>, JobsMetaValue)>>(iter: T) -> Self {
        let mut meta = HashMap::new();
        for (name, value) in iter {
            meta.insert(name, value);
        }
        JobsMeta {
            meta: Arc::new(RwLock::new(meta)),
        }
    }
}

impl IntoIterator for JobsMeta {
    type Item = (Cow<'static, str>, JobsMetaValue);
    type IntoIter = std::collections::hash_map::IntoIter<Cow<'static, str>, JobsMetaValue>;

    fn into_iter(self) -> Self::IntoIter {
        match Arc::try_unwrap(self.meta) {
            Ok(rwlock) => rwlock.into_inner().unwrap().into_iter(),
            Err(arc) => arc.read().unwrap().clone().into_iter(),
        }
    }
}

#[derive(Default, Clone)]
pub struct WorkerNotifier {
    /// (ready, cond var)
    notify: Arc<(Mutex<bool>, Condvar)>,
}

impl WorkerNotifier {
    pub fn notify(&self) {
        let (lock, cvar) = &*self.notify;
        if let Ok(mut running) = lock.lock() {
            *running = true;
        }
        cvar.notify_all();
    }

    pub fn wait(&self, timeout: Duration) {
        let (lock, cvar) = &*self.notify;
        let Ok(mut ready) = lock.lock() else {
            return;
        };
        loop {
            let Ok((new, _)) = cvar.wait_timeout(ready, timeout) else {
                return;
            };
            ready = new;
            if *ready {
                break;
            }
        }
    }
}

pub struct Worker {
    location: JobLocation,
    thread: Option<JoinHandle<()>>,
    terminate: Arc<AtomicBool>,
    notifier: WorkerNotifier,
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.terminate.store(true, Ordering::Relaxed);
        self.notifier.notify();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Worker {
    pub fn location(&self) -> &JobLocation {
        &self.location
    }

    pub fn terminate(&self) {
        self.terminate.store(true, Ordering::Relaxed);
        self.notifier.notify();
    }

    pub fn new(
        iteration_timeout: Duration,
        worker_location: JobLocation,
        queue: JobQueue,
        global_meta: JobsMeta,
        worker_meta: JobsMeta,
        hash_tokens: JobTokens,
        notifier: WorkerNotifier,
    ) -> Worker {
        let terminate = Arc::new(AtomicBool::default());
        let terminate2 = terminate.clone();
        let worker_location2 = worker_location.clone();
        let notifier2 = notifier.clone();
        let thread = spawn(move || {
            Self::run(
                iteration_timeout,
                terminate2,
                worker_location2,
                queue,
                global_meta,
                worker_meta,
                hash_tokens,
                notifier2,
            );
        });
        Worker {
            location: worker_location,
            thread: Some(thread),
            terminate,
            notifier,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        iteration_timeout: Duration,
        terminate: Arc<AtomicBool>,
        worker_location: JobLocation,
        queue: JobQueue,
        global_meta: JobsMeta,
        worker_meta: JobsMeta,
        hash_tokens: JobTokens,
        notifier: WorkerNotifier,
    ) {
        let mut pending = vec![];
        loop {
            if terminate.load(Ordering::Relaxed) {
                return;
            }
            while let Some(object) = queue.dequeue(&worker_location, false) {
                if let Some(object) = queue.poll_once(
                    object,
                    global_meta.clone(),
                    worker_meta.clone(),
                    hash_tokens.clone(),
                    notifier.clone(),
                    true,
                ) {
                    pending.push(object);
                }
                if terminate.load(Ordering::Relaxed) {
                    return;
                }
            }
            queue.extend(pending.drain(..));
            if !queue.is_empty() {
                continue;
            }
            notifier.wait(iteration_timeout);
        }
    }

    pub(crate) fn exclusive(
        job: JobObject,
        queue: JobQueue,
        global_meta: JobsMeta,
        worker_meta: JobsMeta,
        hash_tokens: JobTokens,
        notifier: WorkerNotifier,
    ) -> JoinHandle<()> {
        spawn(move || {
            let mut pending = Some(job);
            loop {
                let Some(object) = pending.take() else {
                    return;
                };
                pending = queue.poll_once(
                    object,
                    global_meta.clone(),
                    worker_meta.clone(),
                    hash_tokens.clone(),
                    notifier.clone(),
                    false,
                );
            }
        })
    }
}

pub enum JobsWakerCommand {
    MoveTo(JobLocation),
    ChangePriority(JobPriority),
}

thread_local! {
    pub static CURRENT_RUNTIME: RefCell<Vec<JobsRuntime>> = Default::default();
}

pub struct JobsRuntimeGuard;

impl Drop for JobsRuntimeGuard {
    fn drop(&mut self) {
        CURRENT_RUNTIME.with(|cell| {
            let mut stack = cell.borrow_mut();
            stack.pop();
        });
    }
}

#[derive(Default, Clone)]
pub struct JobsRuntime {
    pub queue: JobQueue,
    pub location: JobLocation,
    pub context: JobContext,
    pub priority: JobPriority,
    pub global_meta: JobsMeta,
    pub worker_meta: JobsMeta,
    pub local_meta: JobsMeta,
    pub tags: JobsTags,
    pub hash_tokens: JobTokens,
    pub notify: WorkerNotifier,
    pub cancel: Arc<AtomicBool>,
    pub suspend: Arc<AtomicBool>,
}

impl JobsRuntime {
    #[must_use = "Guard must be held to keep the runtime active for duration of a scope"]
    pub fn enter(self) -> JobsRuntimeGuard {
        CURRENT_RUNTIME.with(|cell| {
            let mut stack = cell.borrow_mut();
            stack.push(self);
        });
        JobsRuntimeGuard
    }

    pub fn try_current() -> Option<JobsRuntime> {
        CURRENT_RUNTIME.with(|cell| {
            let stack = cell.borrow();
            stack.last().cloned()
        })
    }

    pub fn current() -> JobsRuntime {
        Self::try_current()
            .unwrap_or_else(|| panic!("There is no active JobsRuntime in the current scope"))
    }

    pub fn queue(mut self, queue: JobQueue) -> Self {
        self.queue = queue;
        self
    }

    pub fn location(mut self, location: JobLocation) -> Self {
        self.location = location;
        self
    }

    pub fn context(mut self, context: JobContext) -> Self {
        self.context = context;
        self
    }

    pub fn priority(mut self, priority: JobPriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn global_meta(mut self, global_meta: JobsMeta) -> Self {
        self.global_meta = global_meta;
        self
    }

    pub fn worker_meta(mut self, worker_meta: JobsMeta) -> Self {
        self.worker_meta = worker_meta;
        self
    }

    pub fn local_meta(mut self, local_meta: JobsMeta) -> Self {
        self.local_meta = local_meta;
        self
    }

    pub fn tags(mut self, tags: JobsTags) -> Self {
        self.tags = tags;
        self
    }

    pub fn hash_tokens(mut self, hash_tokens: JobTokens) -> Self {
        self.hash_tokens = hash_tokens;
        self
    }

    pub fn notify(mut self, notify: WorkerNotifier) -> Self {
        self.notify = notify;
        self
    }

    pub fn cancel(mut self, cancel: Arc<AtomicBool>) -> Self {
        self.cancel = cancel;
        self
    }

    pub fn suspend(mut self, suspend: Arc<AtomicBool>) -> Self {
        self.suspend = suspend;
        self
    }

    pub fn into_waker(self) -> (Waker, Receiver<JobsWakerCommand>) {
        JobsWaker::make(self)
    }

    pub fn poll_future<F: Future>(self, future: Pin<&mut F>) -> Poll<F::Output> {
        let (waker, _) = self.into_waker();
        let mut cx = Context::from_waker(&waker);
        future.poll(&mut cx)
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let mut future = pin!(future);
        loop {
            match self.clone().poll_future(future.as_mut()) {
                Poll::Ready(value) => return value,
                Poll::Pending => {}
            }
        }
    }

    pub async fn enable<F: Future>(&self, future: F) -> F::Output {
        let mut future = pin!(future);
        poll_fn(move |cx| {
            let result = self.clone().poll_future(future.as_mut());
            cx.waker().wake_by_ref();
            result
        })
        .await
    }

    pub fn run_queue(&self, queue: &JobQueue) {
        queue.run(
            JobLocation::Unknown,
            true,
            Duration::MAX,
            self.global_meta.clone(),
            Default::default(),
            self.hash_tokens.clone(),
            self.notify.clone(),
        );
    }

    pub fn run_queue_with_meta(&self, queue: &JobQueue, worker_meta: JobsMeta) {
        queue.run(
            JobLocation::Unknown,
            true,
            Duration::MAX,
            self.global_meta.clone(),
            worker_meta,
            self.hash_tokens.clone(),
            self.notify.clone(),
        );
    }

    pub fn run_queue_timeout(&self, queue: &JobQueue, timeout: Duration) {
        queue.run(
            JobLocation::Unknown,
            true,
            timeout,
            self.global_meta.clone(),
            Default::default(),
            self.hash_tokens.clone(),
            self.notify.clone(),
        );
    }

    pub fn run_queue_timeout_with_meta(
        &self,
        queue: &JobQueue,
        timeout: Duration,
        worker_meta: JobsMeta,
    ) {
        queue.run(
            JobLocation::Unknown,
            true,
            timeout,
            self.global_meta.clone(),
            worker_meta,
            self.hash_tokens.clone(),
            self.notify.clone(),
        );
    }

    pub fn get_meta(&self, name: &str) -> Option<DynamicManagedLazy> {
        self.local_meta
            .meta
            .read()
            .ok()
            .and_then(|meta| meta.get(name).map(|meta| meta.lazy()))
            .or_else(|| {
                self.worker_meta
                    .meta
                    .read()
                    .ok()
                    .and_then(|meta| meta.get(name).map(|meta| meta.lazy()))
                    .or_else(|| {
                        self.global_meta
                            .meta
                            .read()
                            .ok()
                            .and_then(|meta| meta.get(name).map(|meta| meta.lazy()))
                    })
            })
    }
}

pub struct JobsWaker {
    sender: Sender<JobsWakerCommand>,
    pub(crate) runtime: JobsRuntime,
}

static JOBS_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    JobsWaker::vtable_clone,
    |_| {},
    |_| {},
    JobsWaker::vtable_drop,
);

impl JobsWaker {
    fn vtable_clone(data: *const ()) -> RawWaker {
        let arc = unsafe { Arc::<Self>::from_raw(data as *const Self) };
        let cloned = arc.clone();
        std::mem::forget(arc);
        RawWaker::new(Arc::into_raw(cloned) as *const (), &JOBS_WAKER_VTABLE)
    }

    fn vtable_drop(data: *const ()) {
        let _ = unsafe { Arc::from_raw(data as *const Self) };
    }

    pub fn make(runtime: JobsRuntime) -> (Waker, Receiver<JobsWakerCommand>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        let arc = Arc::new(Self { sender, runtime });
        let raw = RawWaker::new(Arc::into_raw(arc) as *const (), &JOBS_WAKER_VTABLE);
        (unsafe { Waker::from_raw(raw) }, receiver)
    }

    pub fn try_cast(waker: &Waker) -> Option<&Self> {
        if std::ptr::eq(waker.vtable(), &JOBS_WAKER_VTABLE) {
            unsafe { waker.data().cast::<Self>().as_ref() }
        } else {
            None
        }
    }

    /// # Safety
    pub unsafe fn runtime(&self) -> &JobsRuntime {
        &self.runtime
    }

    /// # Safety
    pub unsafe fn runtime_mut(&mut self) -> &mut JobsRuntime {
        &mut self.runtime
    }

    pub(crate) fn command(&self, command: JobsWakerCommand) {
        let _ = self.sender.send(command);
    }
}

#[allow(clippy::manual_noop_waker)]
impl Wake for JobsWaker {
    fn wake(self: Arc<Self>) {}
}

pub struct Jobs {
    workers: Vec<Worker>,
    queue: JobQueue,
    meta: JobsMeta,
    hash_tokens: JobTokens,
    notifier: WorkerNotifier,
}

impl Default for Jobs {
    fn default() -> Self {
        Self::new(
            available_parallelism()
                .ok()
                .map(|v| v.get())
                .unwrap_or_default(),
            Duration::from_millis(1),
        )
    }
}

impl Jobs {
    pub fn local_only() -> Jobs {
        Jobs::new(0, Duration::ZERO)
    }

    pub fn new(unnamed_workers_count: usize, iteration_timeout: Duration) -> Jobs {
        let queue = JobQueue::default();
        let notifier = WorkerNotifier::default();
        let global_meta = JobsMeta::default();
        let worker_meta = JobsMeta::default();
        let hash_tokens = JobTokens::default();
        Jobs {
            workers: (0..unnamed_workers_count)
                .map(|_| {
                    Worker::new(
                        iteration_timeout,
                        JobLocation::UnnamedWorker,
                        queue.clone(),
                        global_meta.clone(),
                        worker_meta.clone(),
                        hash_tokens.clone(),
                        notifier.clone(),
                    )
                })
                .collect(),
            queue,
            meta: global_meta,
            hash_tokens,
            notifier,
        }
    }

    pub fn catch_panics(self) -> Self {
        self.queue.set_catch_panics(true);
        self
    }

    pub fn with_unnamed_worker(mut self, iteration_timeout: Duration) -> Self {
        self.add_unnamed_worker(iteration_timeout);
        self
    }

    pub fn with_named_worker(mut self, iteration_timeout: Duration, name: impl ToString) -> Self {
        self.add_named_worker(iteration_timeout, name);
        self
    }

    pub fn add_unnamed_worker(&mut self, iteration_timeout: Duration) {
        self.add_unnamed_worker_with_meta(iteration_timeout, Default::default());
    }

    pub fn add_unnamed_worker_with_meta(&mut self, iteration_timeout: Duration, meta: JobsMeta) {
        self.workers.push(Worker::new(
            iteration_timeout,
            JobLocation::UnnamedWorker,
            self.queue.clone(),
            self.meta.clone(),
            meta,
            self.hash_tokens.clone(),
            self.notifier.clone(),
        ));
    }

    pub fn add_named_worker(&mut self, iteration_timeout: Duration, name: impl ToString) {
        self.add_named_worker_with_meta(iteration_timeout, name, Default::default());
    }

    pub fn add_named_worker_with_meta(
        &mut self,
        iteration_timeout: Duration,
        name: impl ToString,
        meta: JobsMeta,
    ) {
        self.workers.push(Worker::new(
            iteration_timeout,
            JobLocation::named_worker(name),
            self.queue.clone(),
            self.meta.clone(),
            meta,
            self.hash_tokens.clone(),
            self.notifier.clone(),
        ));
    }

    pub fn remove_named_worker(&mut self, name: &str) {
        if let Some(index) = self.workers.iter().position(|worker| {
            if let JobLocation::NamedWorker(worker_name) = &worker.location {
                worker_name == name
            } else {
                false
            }
        }) {
            self.workers.swap_remove(index);
        }
    }

    pub fn unnamed_workers(&self) -> usize {
        self.workers
            .iter()
            .filter(|worker| worker.location == JobLocation::UnnamedWorker)
            .count()
    }

    pub fn named_workers(&self) -> impl Iterator<Item = &str> {
        self.workers.iter().filter_map(|worker| {
            if let JobLocation::NamedWorker(name) = &worker.location {
                Some(name.as_str())
            } else {
                None
            }
        })
    }

    pub fn set_meta(&self, name: impl Into<Cow<'static, str>>, value: DynamicManagedLazy) {
        self.meta.set(name, value);
    }

    pub fn unset_meta(&self, name: &str) {
        self.meta.unset(name);
    }

    pub fn get_meta<T>(&self, name: &str) -> Option<ManagedLazy<T>> {
        let Ok(meta) = self.meta.meta.read() else {
            return None;
        };
        meta.get(name)
            .cloned()
            .and_then(|value| value.lazy().into_typed::<T>().ok())
    }

    pub fn get_meta_dynamic(&self, name: &str) -> Option<DynamicManagedLazy> {
        let Ok(meta) = self.meta.meta.read() else {
            return None;
        };
        meta.get(name).map(|meta| meta.lazy())
    }

    pub fn runtime(&self) -> JobsRuntime {
        JobsRuntime {
            queue: self.queue.clone(),
            location: JobLocation::Local,
            context: Default::default(),
            priority: JobPriority::Normal,
            global_meta: self.meta.clone(),
            worker_meta: JobsMeta::default(),
            local_meta: JobsMeta::default(),
            tags: JobsTags::default(),
            hash_tokens: self.hash_tokens.clone(),
            notify: self.notifier.clone(),
            cancel: Arc::new(AtomicBool::default()),
            suspend: Arc::new(AtomicBool::default()),
        }
    }

    pub fn run_local(&self) {
        self.queue.run(
            JobLocation::Local,
            self.workers.is_empty(),
            Duration::MAX,
            self.meta.clone(),
            Default::default(),
            self.hash_tokens.clone(),
            self.notifier.clone(),
        );
    }

    pub fn run_local_with_meta(&self, worker_meta: JobsMeta) {
        self.queue.run(
            JobLocation::Local,
            self.workers.is_empty(),
            Duration::MAX,
            self.meta.clone(),
            worker_meta,
            self.hash_tokens.clone(),
            self.notifier.clone(),
        );
    }

    pub fn run_local_timeout(&self, timeout: Duration) {
        self.queue.run(
            JobLocation::Local,
            self.workers.is_empty(),
            timeout,
            self.meta.clone(),
            Default::default(),
            self.hash_tokens.clone(),
            self.notifier.clone(),
        );
    }

    pub fn run_local_timeout_with_meta(&self, timeout: Duration, worker_meta: JobsMeta) {
        self.queue.run(
            JobLocation::Local,
            self.workers.is_empty(),
            timeout,
            self.meta.clone(),
            worker_meta,
            self.hash_tokens.clone(),
            self.notifier.clone(),
        );
    }

    pub fn submit_queue(&self, queue: &JobQueue) {
        self.queue.append(queue);
        self.notifier.notify();
    }

    pub fn run_queue(&self, queue: &JobQueue) {
        queue.run(
            JobLocation::Unknown,
            true,
            Duration::MAX,
            self.meta.clone(),
            Default::default(),
            self.hash_tokens.clone(),
            self.notifier.clone(),
        );
    }

    pub fn run_queue_with_meta(&self, queue: &JobQueue, worker_meta: JobsMeta) {
        queue.run(
            JobLocation::Unknown,
            true,
            Duration::MAX,
            self.meta.clone(),
            worker_meta,
            self.hash_tokens.clone(),
            self.notifier.clone(),
        );
    }

    pub fn run_queue_timeout(&self, queue: &JobQueue, timeout: Duration) {
        queue.run(
            JobLocation::Unknown,
            true,
            timeout,
            self.meta.clone(),
            Default::default(),
            self.hash_tokens.clone(),
            self.notifier.clone(),
        );
    }

    pub fn run_queue_timeout_with_meta(
        &self,
        queue: &JobQueue,
        timeout: Duration,
        worker_meta: JobsMeta,
    ) {
        queue.run(
            JobLocation::Unknown,
            true,
            timeout,
            self.meta.clone(),
            worker_meta,
            self.hash_tokens.clone(),
            self.notifier.clone(),
        );
    }

    pub fn block_on<T: Send + 'static>(
        &self,
        job: impl Future<Output = T> + Send + Sync + 'static,
    ) -> Option<T> {
        let queue = JobQueue::default();
        let handle = queue.spawn(JobLocation::Local, job);
        while !queue.is_empty() {
            self.run_queue(&queue);
        }
        handle.wait()
    }

    #[inline]
    pub fn no_workers(&self) -> bool {
        self.workers.is_empty()
    }

    #[inline]
    pub fn workers_count(&self) -> usize {
        self.workers.len()
    }

    #[inline]
    pub fn queue(&self) -> &JobQueue {
        &self.queue
    }

    pub fn spawn<T: Send + 'static>(
        &self,
        options: impl Into<JobOptions>,
        job: impl Future<Output = T> + Send + Sync + 'static,
    ) -> JobHandle<T> {
        let options = options.into();
        let handle = self.queue.spawn(options, job);
        self.notifier.notify();
        handle
    }

    pub fn spawn_closure<T: Send + 'static>(
        &self,
        options: impl Into<JobOptions>,
        job: impl FnOnce(JobContext) -> T + Send + Sync + 'static,
    ) -> JobHandle<T> {
        let options = options.into();
        let handle = self.queue.spawn_closure(options, job);
        self.notifier.notify();
        handle
    }

    pub fn broadcast<T: Send + 'static>(
        &self,
        job: impl Fn(JobContext) -> T + Send + Sync + 'static,
    ) -> AllJobsHandle<T> {
        let current_thread = std::thread::current().id();
        let n = self
            .workers
            .iter()
            .filter(|worker| {
                worker
                    .thread
                    .as_ref()
                    .is_none_or(|t| t.thread().id() != current_thread)
            })
            .count();
        self.broadcast_n(n, job)
    }

    pub fn broadcast_n<T: Send + 'static>(
        &self,
        work_groups: usize,
        job: impl Fn(JobContext) -> T + Send + Sync + 'static,
    ) -> AllJobsHandle<T> {
        if work_groups == 0 || self.workers.is_empty() {
            return AllJobsHandle::new(job(Default::default()));
        }
        let job = Arc::new(job);
        #[cfg(debug_assertions)]
        let creation_backtrace = std::backtrace::Backtrace::capture().to_string();
        let handle = AllJobsHandle {
            jobs: (0..work_groups)
                .map(|group| {
                    let job = Arc::clone(&job);
                    let handle = JobHandle::<T>::default();
                    let handle2 = handle.clone();
                    self.queue.enqueue(JobObject {
                        id: ID::new(),
                        job: Job(Box::pin(async move {
                            handle2.put(job(context().await));
                        })),
                        context: JobContext {
                            work_group_index: group,
                            work_groups_count: work_groups,
                        },
                        location: JobLocation::other_than_current_thread(),
                        priority: JobPriority::High,
                        cancel: handle.cancel.clone(),
                        suspend: handle.suspend.clone(),
                        meta: handle.meta.clone(),
                        tags: Default::default(),
                        #[cfg(debug_assertions)]
                        creation_backtrace: creation_backtrace.clone(),
                        #[cfg(feature = "tracing")]
                        tracing_span: None,
                    });
                    handle
                })
                .collect::<Vec<_>>(),
        };
        self.notifier.notify();
        handle
    }

    pub fn scope<'env, T: Send + 'static, R>(
        &'env self,
        f: impl FnOnce(&mut ScopedJobs<'env, T>) -> R + 'env,
    ) -> (Vec<T>, R) {
        ScopedJobs::execute(self, f)
    }
}

pub struct ScopedJobs<'env, T: Send + 'static> {
    jobs: &'env Jobs,
    handles: AllJobsHandle<T>,
}

impl<'env, T: Send + 'static> ScopedJobs<'env, T> {
    pub fn scope<'env2: 'env, T2: Send + 'static, R>(
        &'env2 self,
        f: impl FnOnce(&mut ScopedJobs<'env2, T2>) -> R + 'env2,
    ) -> (Vec<T2>, R) {
        ScopedJobs::<T2>::execute::<R>(self.jobs, f)
    }

    pub fn execute<R>(jobs: &'env Jobs, f: impl FnOnce(&mut Self) -> R + 'env) -> (Vec<T>, R) {
        let mut scope = Self {
            jobs,
            handles: AllJobsHandle::default(),
        };
        let result = f(&mut scope);
        let output = scope.handles.wait().unwrap_or_default();
        (output, result)
    }

    pub fn spawn(
        &mut self,
        options: impl Into<JobOptions>,
        job: impl Future<Output = T> + Send + Sync + 'env,
    ) {
        let job = unsafe {
            std::mem::transmute::<
                Pin<Box<dyn Future<Output = T> + Send + Sync + 'env>>,
                Pin<Box<dyn Future<Output = T> + Send + Sync + 'static>>,
            >(Box::pin(job))
        };
        let handle = self.jobs.spawn(options, job);
        self.handles.add(handle);
    }

    pub fn spawn_closure(
        &mut self,
        options: impl Into<JobOptions>,
        job: impl FnOnce(JobContext) -> T + Send + Sync + 'env,
    ) {
        let job = unsafe {
            std::mem::transmute::<
                Box<dyn FnOnce(JobContext) -> T + Send + Sync + 'env>,
                Box<dyn FnOnce(JobContext) -> T + Send + Sync + 'static>,
            >(Box::new(job))
        };
        self.handles.add(self.jobs.spawn_closure(options, job));
    }

    pub fn broadcast(&mut self, job: impl Fn(JobContext) -> T + Send + Sync + 'env) {
        let job = unsafe {
            std::mem::transmute::<
                Box<dyn Fn(JobContext) -> T + Send + Sync + 'env>,
                Box<dyn Fn(JobContext) -> T + Send + Sync + 'static>,
            >(Box::new(job))
        };
        self.handles.extend(self.jobs.broadcast(job).into_inner());
    }

    pub fn broadcast_n(
        &mut self,
        work_groups: usize,
        job: impl Fn(JobContext) -> T + Send + Sync + 'env,
    ) {
        let job = unsafe {
            std::mem::transmute::<
                Box<dyn Fn(JobContext) -> T + Send + Sync + 'env>,
                Box<dyn Fn(JobContext) -> T + Send + Sync + 'static>,
            >(Box::new(job))
        };
        self.handles
            .extend(self.jobs.broadcast_n(work_groups, job).into_inner());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        coroutine::{
            acquire_token, location, meta, move_to, on_exit, spawn, spawn_closure, suspend,
            wait_polls, wait_time, with_all, with_any, yield_now,
        },
        job::JobResult,
    };
    use std::sync::atomic::AtomicUsize;

    const ITERATION_TIMEOUT: Duration = Duration::from_millis(1);

    #[test]
    fn test_jobs() {
        fn is_async<T: Send + Sync>() {}

        is_async::<Jobs>();

        let jobs = Jobs::default();
        let data = (0..100).collect::<Vec<_>>();
        let data2 = data.clone();

        let job = jobs.spawn_closure((), move |_| data.into_iter().sum::<usize>());

        let result = job.wait().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.spawn_closure(JobLocation::Local, move |_| {
            data2.into_iter().sum::<usize>()
        });

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.broadcast(move |ctx| ctx.work_group_index);
        let result = job.wait().unwrap().into_iter().sum::<usize>();
        assert_eq!(result, (0..jobs.workers.len()).sum());

        let job = jobs.broadcast_n(10, move |ctx| ctx.work_group_index);
        let result = job.wait().unwrap().into_iter().sum::<usize>();
        assert_eq!(result, {
            let mut accum = 0;
            for index in 0..10 {
                accum += index;
            }
            accum
        });
    }

    #[test]
    fn test_local_thread_only_jobs() {
        let jobs = Jobs::new(0, ITERATION_TIMEOUT);
        let data = (0..100).collect::<Vec<_>>();
        let data2 = data.clone();
        let data3 = data.clone();
        let data4 = data.clone();
        let data5 = data.clone();
        let data6 = data.clone();

        let job = jobs.spawn_closure((), move |_| data.into_iter().sum::<usize>());

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.spawn_closure(JobLocation::Local, move |_| {
            data2.into_iter().sum::<usize>()
        });

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.spawn_closure(JobLocation::UnnamedWorker, move |_| {
            data3.into_iter().sum::<usize>()
        });

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.spawn_closure(JobLocation::named_worker("temp"), move |_| {
            data4.into_iter().sum::<usize>()
        });

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.spawn_closure(JobLocation::current_thread(), move |_| {
            data5.into_iter().sum::<usize>()
        });

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.spawn_closure(JobLocation::other_than_current_thread(), move |_| {
            data6.into_iter().sum::<usize>()
        });

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.broadcast(move |_| 1);

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.wait().unwrap().into_iter().sum::<usize>();
        assert_eq!(result, 1);

        let job = jobs.broadcast_n(10, move |_| 1);

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.wait().unwrap().into_iter().sum::<usize>();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_spawn_closure_jobs() {
        let jobs = Jobs::new(0, ITERATION_TIMEOUT);
        let queue = JobQueue::default();
        let data = (0..100).collect::<Vec<_>>();

        let job = queue.spawn_closure((), move |_| data.into_iter().sum::<usize>());

        while !job.is_done() {
            jobs.run_queue(&queue);
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 4950);
    }

    #[test]
    fn test_scoped_jobs() {
        let jobs = Jobs::default();
        let mut data = (0..100).collect::<Vec<_>>();

        let output = jobs
            .scope(|scope| {
                scope.spawn_closure(JobLocation::NonLocal, |_| {
                    for value in &mut data {
                        *value *= 2;
                    }
                    data.iter().copied().sum::<usize>()
                });
            })
            .0
            .into_iter()
            .sum::<usize>();

        assert_eq!(output, 9900);
    }

    #[test]
    fn test_futures_spawn() {
        let jobs = Jobs::default();
        let data = (0..100).collect::<Vec<_>>();
        let data2 = data.clone();
        let data3 = data.clone();

        let job = jobs.spawn((), async move {
            let mut result = 0;
            for value in data {
                result += value;
                yield_now().await;
            }
            result
        });
        let result = jobs.block_on(job).unwrap().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.spawn(JobLocation::Local, async move {
            let mut result = 0;
            for value in data2 {
                result += value;
                yield_now().await;
            }
            result
        });
        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.spawn((), async {
            let result = Arc::new(AtomicUsize::new(0));
            let result1 = result.clone();
            let result2 = result.clone();
            with_all(vec![
                Box::pin(async move {
                    wait_time(Duration::from_millis(10)).await;
                    result1.fetch_add(1, Ordering::SeqCst);
                }),
                Box::pin(async move {
                    wait_time(Duration::from_millis(5)).await;
                    result2.fetch_add(2, Ordering::SeqCst);
                }),
            ])
            .await;
            result.load(Ordering::SeqCst)
        });
        let result = jobs.block_on(job).unwrap().unwrap();
        assert_eq!(result, 3);

        let job = jobs.spawn((), async {
            let result = Arc::new(AtomicUsize::new(0));
            let result1 = result.clone();
            let result2 = result.clone();
            with_any(vec![
                Box::pin(async move {
                    wait_polls(10).await;
                    result1.store(1, Ordering::SeqCst);
                }),
                Box::pin(async move {
                    wait_polls(5).await;
                    result2.store(2, Ordering::SeqCst);
                }),
            ])
            .await;
            result.load(Ordering::SeqCst)
        });
        let result = jobs.block_on(job).unwrap().unwrap();
        assert!(result > 0);

        let workers_count = jobs.workers.len();
        let job = jobs.spawn(JobLocation::Exclusive, async {
            let mut result = 0;
            for value in data3 {
                result += value;
                yield_now().await;
            }
            result
        });
        let result = jobs.block_on(job).unwrap().unwrap();
        assert_eq!(result, 4950);
        assert_eq!(jobs.workers.len(), workers_count);
    }

    #[test]
    fn test_futures_move() {
        let jobs = Jobs::new(1, ITERATION_TIMEOUT).with_named_worker(ITERATION_TIMEOUT, "foo");
        let unnamed_thread_id = jobs.workers[0].thread.as_ref().unwrap().thread().id();
        let named_thread_id = jobs.workers[1].thread.as_ref().unwrap().thread().id();
        let host_thread_id = std::thread::current().id();

        let job = jobs.spawn(JobLocation::Local, async move {
            yield_now().await;
            let a_thread_id = std::thread::current().id();
            assert_eq!(a_thread_id, host_thread_id);
            assert_eq!(location().await, JobLocation::Local);
            move_to(JobLocation::Unknown).await;

            let b_thread_id = std::thread::current().id();
            assert!(
                b_thread_id == unnamed_thread_id
                    || b_thread_id == named_thread_id
                    || b_thread_id == host_thread_id
            );
            assert_eq!(location().await, JobLocation::Unknown);
            move_to(JobLocation::named_worker("foo")).await;

            let c_thread_id = std::thread::current().id();
            assert_eq!(c_thread_id, named_thread_id);
            assert_eq!(location().await, JobLocation::named_worker("foo"));
            move_to(JobLocation::Exclusive).await;

            let d_thread_id = std::thread::current().id();
            assert!(
                d_thread_id != unnamed_thread_id
                    && d_thread_id != named_thread_id
                    && d_thread_id != host_thread_id
            );
            assert_eq!(location().await, JobLocation::Exclusive);
            move_to(JobLocation::Local).await;

            let e_thread_id = std::thread::current().id();
            assert_eq!(e_thread_id, host_thread_id);
            assert_eq!(location().await, JobLocation::Local);
            42
        });

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_futures_schedule() {
        let jobs = Jobs::new(1, ITERATION_TIMEOUT).with_named_worker(ITERATION_TIMEOUT, "foo");

        let job = jobs.spawn(JobLocation::Local, async {
            spawn(JobLocation::Local, async {
                println!("A: {:?}", location().await);
            })
            .await;
            spawn((), async {
                println!("B: {:?}", location().await);
            })
            .await;
            spawn(JobLocation::named_worker("foo"), async {
                println!("C: {:?}", location().await);
            })
            .await;
            spawn(JobLocation::Exclusive, async {
                println!("D: {:?}", location().await);
            })
            .await;
            spawn_closure(JobLocation::Local, |_| {
                println!("E: Local closure");
            })
            .await;
            spawn_closure((), |_| {
                println!("F: Unnamed worker closure");
            })
            .await;
            spawn_closure(JobLocation::named_worker("foo"), |_| {
                println!("G: Named worker closure");
            })
            .await;
            spawn_closure(JobLocation::Exclusive, |_| {
                println!("H: Exclusive worker closure");
            })
            .await;
            42
        });

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_futures_meta() {
        let jobs = Jobs::default();
        let mut value = 42usize;
        let (value_lazy, _value_lifetime) = DynamicManagedLazy::make(&mut value);
        jobs.set_meta("value", value_lazy);

        let job = jobs.spawn((), async {
            let value = meta::<usize>("value").await.unwrap();
            *value.read().unwrap()
        });

        let result = jobs.block_on(job).unwrap().unwrap();
        assert_eq!(result, 42);

        let mut flag = true;
        let (flag_lazy, _flag_lifetime) = DynamicManagedLazy::make(&mut flag);
        let job = jobs.spawn(JobOptions::default().meta("flag", flag_lazy), async {
            let flag = meta::<bool>("flag").await.unwrap();
            *flag.read().unwrap()
        });

        let result = jobs.block_on(job).unwrap().unwrap();
        assert!(result);

        let mut flag = true;
        let (flag_lazy, _flag_lifetime) = DynamicManagedLazy::make(&mut flag);
        let job = jobs.spawn(JobLocation::Local, async {
            let flag = meta::<bool>("flag").await.unwrap();
            *flag.read().unwrap()
        });

        while !job.is_done() {
            jobs.run_local_with_meta(
                [("flag".into(), flag_lazy.clone().into())]
                    .into_iter()
                    .collect(),
            );
        }
        let result = job.take_result().unwrap();
        assert!(result);
    }

    #[test]
    fn test_futures_acquire_token() {
        let jobs = Jobs::new(3, ITERATION_TIMEOUT);

        let a = jobs.spawn((), async {
            let _token = acquire_token(&"foo").await;
            std::thread::sleep(Duration::from_millis(50));
            for i in 0..10 {
                println!("{i}");
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        let b = jobs.spawn((), async {
            let _token = acquire_token(&"foo").await;
            std::thread::sleep(Duration::from_millis(50));
            for i in 10..20 {
                println!("{i}");
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        jobs.block_on(AllJobsHandle::many([a, b])).unwrap();

        #[cfg(not(miri))]
        {
            use std::path::Path;

            async fn load_file(path: impl AsRef<Path>) -> Option<String> {
                let path = path.as_ref();
                let _token = acquire_token(&path).await;
                std::fs::read_to_string(path).ok()
            }

            async fn save_file(path: impl AsRef<Path>, content: &str) {
                let path = path.as_ref();
                let _token = acquire_token(&path).await;
                let _ = std::fs::write(path, content);
            }

            const PATH: &str = "../resources/test.txt";
            let content = "Hello, Jobs!".repeat(1000);

            let a = jobs.spawn((), async move {
                std::thread::sleep(Duration::from_millis(50));
                save_file(PATH, &content).await;
            });
            let b = jobs.spawn((), async {
                std::thread::sleep(Duration::from_millis(50));
                let _ = load_file(PATH).await;
            });
            jobs.block_on(AllJobsHandle::many([a, b])).unwrap();
        }
    }

    #[test]
    fn test_futures_on_exit() {
        let jobs = Jobs::default();

        let state = Arc::new(AtomicBool::new(false));
        let state1 = state.clone();
        let job = jobs.spawn((), async {
            let _exit = on_exit(async move {
                state1.store(true, Ordering::SeqCst);
            })
            .await;
            42
        });

        let result = jobs.block_on(job).unwrap().unwrap();
        assert_eq!(result, 42);
        std::thread::sleep(Duration::from_millis(100));
        assert!(state.load(Ordering::SeqCst));

        let state = Arc::new(AtomicBool::new(false));
        let state1 = state.clone();
        let job = jobs.spawn((), async {
            let exit = on_exit(async move {
                state1.store(true, Ordering::SeqCst);
            })
            .await;
            exit.invalidate();
            42
        });

        let result = jobs.block_on(job).unwrap().unwrap();
        assert_eq!(result, 42);
        std::thread::sleep(Duration::from_millis(100));
        assert!(!state.load(Ordering::SeqCst));

        let state = Arc::new(AtomicBool::new(false));
        let state1 = state.clone();
        let job = jobs.spawn(JobLocation::Local, async {
            let exit = on_exit(async move {
                state1.store(true, Ordering::SeqCst);
            })
            .await;
            // job gets cancelled at this point,
            // so exit future won't get invalidated.
            exit.invalidate();
            42
        });

        job.cancel();
        while !job.is_done() {
            jobs.run_local();
        }
        assert_eq!(job.take(), JobResult::Cancelled);
        assert!(!state.load(Ordering::SeqCst));
    }

    #[test]
    fn test_futures_suspend() {
        let jobs = Jobs::default();

        let job = jobs.spawn(JobLocation::Local, async {
            suspend().await;
            42
        });

        assert!(!job.is_done());
        for _ in 0..10 {
            jobs.run_local();
        }
        assert!(!job.is_done());

        job.resume();
        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.take_result().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_futures_cancel() {
        let jobs = Jobs::default();

        let result = Arc::new(AtomicUsize::new(0));
        let result1 = result.clone();

        let job = jobs.spawn(JobLocation::Local, async move {
            loop {
                result1.fetch_add(1, Ordering::SeqCst);
                yield_now().await;
            }
        });

        assert!(!job.is_done());
        for _ in 0..10 {
            jobs.run_local();
        }
        assert!(!job.is_done());

        job.cancel();
        let prev = result.load(Ordering::SeqCst);
        jobs.run_local();
        let next = result.load(Ordering::SeqCst);
        assert_eq!(prev, next);
    }

    #[test]
    fn test_futures_panic() {
        let jobs = Jobs::default().catch_panics();

        let job = jobs.spawn(JobLocation::Local, async {
            println!("About to panic...");
            yield_now().await;
            if true {
                panic!("Intentional panic for testing");
            }
            yield_now().await;
            42
        });

        while !jobs.queue().is_empty() {
            jobs.run_local();
        }
        assert_eq!(job.take(), JobResult::InProgress);
    }

    #[test]
    fn test_futures_block_on() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter1 = counter.clone();

        Jobs::default().block_on(async move {
            for _ in 0..10 {
                counter1.fetch_add(1, Ordering::SeqCst);
                yield_now().await;
            }
        });

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }
}
