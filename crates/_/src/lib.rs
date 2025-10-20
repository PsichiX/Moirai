pub mod coroutine;
pub mod generator;
pub mod promise;

use crate::coroutine::context;
#[cfg(target_arch = "wasm32")]
use instant::{Duration, Instant};
use intuicio_data::managed::{DynamicManagedLazy, ManagedLazy};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{Duration, Instant};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    hash::{DefaultHasher, Hash, Hasher},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
    thread::{JoinHandle, ThreadId, available_parallelism, spawn},
};
use typid::ID;

struct Job(Pin<Box<dyn Future<Output = ()> + Send + Sync>>);

impl Job {
    fn poll(mut self, cx: &mut Context<'_>) -> Option<Self> {
        match self.0.as_mut().poll(cx) {
            Poll::Ready(_) => None,
            Poll::Pending => Some(self),
        }
    }
}

#[inline]
fn traced_spin_loop() {
    #[cfg(feature = "deadlock-trace")]
    println!(
        "* DEADLOCK BACKTRACE: {}",
        std::backtrace::Backtrace::force_capture()
    );
    std::hint::spin_loop();
}

pub struct JobHandle<T: Send + 'static> {
    result: Arc<Mutex<Option<Option<T>>>>,
    cancel: Arc<AtomicBool>,
    suspend: Arc<AtomicBool>,
    meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
}

impl<T: Send + 'static> Default for JobHandle<T> {
    fn default() -> Self {
        Self {
            result: Default::default(),
            cancel: Default::default(),
            suspend: Default::default(),
            meta: Default::default(),
        }
    }
}

impl<T: Send + 'static> JobHandle<T> {
    pub fn new(value: T) -> Self {
        Self {
            result: Arc::new(Mutex::new(Some(Some(value)))),
            cancel: Default::default(),
            suspend: Default::default(),
            meta: Default::default(),
        }
    }

    pub(crate) fn with_meta(
        self,
        iter: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
    ) -> Self {
        if let Ok(mut meta) = self.meta.write() {
            meta.extend(iter);
        }
        self
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }

    pub fn is_suspended(&self) -> bool {
        self.suspend.load(Ordering::Relaxed)
    }

    pub fn is_done(&self) -> bool {
        self.result
            .try_lock()
            .ok()
            .map(|guard| guard.is_some())
            .unwrap_or_default()
    }

    pub fn try_take(&self) -> Option<Option<T>> {
        self.result
            .try_lock()
            .ok()
            .and_then(|mut result| result.take())
    }

    pub fn wait(self) -> Option<T> {
        loop {
            if let Some(result) = self.try_take() {
                return result;
            } else {
                traced_spin_loop();
            }
        }
    }

    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
        if let Ok(mut result) = self.result.lock() {
            *result = Some(None);
        }
        self.resume();
    }

    pub fn suspend(&self) {
        self.suspend.store(true, Ordering::Relaxed);
    }

    pub fn resume(&self) {
        self.suspend.store(false, Ordering::Relaxed);
    }

    fn put(&self, value: T) {
        if let Ok(mut result) = self.result.lock() {
            *result = Some(Some(value));
        }
    }
}

impl<T: Send + 'static> Clone for JobHandle<T> {
    fn clone(&self) -> Self {
        Self {
            result: self.result.clone(),
            cancel: self.cancel.clone(),
            suspend: self.suspend.clone(),
            meta: self.meta.clone(),
        }
    }
}

impl<T: Send + 'static> Future for JobHandle<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.try_take() {
            cx.waker().wake_by_ref();
            Poll::Ready(result)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

pub struct AllJobsHandle<T: Send + 'static> {
    jobs: Vec<JobHandle<T>>,
}

impl<T: Send + 'static> Default for AllJobsHandle<T> {
    fn default() -> Self {
        Self {
            jobs: Default::default(),
        }
    }
}

impl<T: Send + 'static> AllJobsHandle<T> {
    pub fn new(value: T) -> Self {
        Self {
            jobs: vec![JobHandle::new(value)],
        }
    }

    pub fn into_inner(self) -> Vec<JobHandle<T>> {
        self.jobs
    }

    pub fn many(handles: impl IntoIterator<Item = JobHandle<T>>) -> Self {
        Self {
            jobs: handles.into_iter().collect(),
        }
    }

    pub fn add(&mut self, handle: JobHandle<T>) {
        self.jobs.push(handle);
    }

    pub fn extend(&mut self, handles: impl IntoIterator<Item = JobHandle<T>>) {
        self.jobs.extend(handles);
    }

    pub fn is_done(&self) -> bool {
        self.jobs.iter().all(|job| job.is_done())
    }

    pub fn try_take(&self) -> Option<Option<Vec<T>>> {
        self.is_done()
            .then(|| self.jobs.iter().flat_map(|job| job.try_take()).collect())
    }

    pub fn wait(self) -> Option<Vec<T>> {
        self.jobs.into_iter().map(|job| job.wait()).collect()
    }
}

impl<T: Send + 'static> Clone for AllJobsHandle<T> {
    fn clone(&self) -> Self {
        Self {
            jobs: self.jobs.clone(),
        }
    }
}

impl<T: Send + 'static> Future for AllJobsHandle<T> {
    type Output = Option<Vec<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.try_take() {
            cx.waker().wake_by_ref();
            Poll::Ready(result)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

pub struct AnyJobHandle<T: Send + 'static> {
    jobs: Vec<JobHandle<T>>,
}

impl<T: Send + 'static> Default for AnyJobHandle<T> {
    fn default() -> Self {
        Self {
            jobs: Default::default(),
        }
    }
}

impl<T: Send + 'static> AnyJobHandle<T> {
    pub fn new(value: T) -> Self {
        Self {
            jobs: vec![JobHandle::new(value)],
        }
    }

    pub fn into_inner(self) -> Vec<JobHandle<T>> {
        self.jobs
    }

    pub fn many(handles: impl IntoIterator<Item = JobHandle<T>>) -> Self {
        Self {
            jobs: handles.into_iter().collect(),
        }
    }

    pub fn add(&mut self, handle: JobHandle<T>) {
        self.jobs.push(handle);
    }

    pub fn extend(&mut self, handles: impl IntoIterator<Item = JobHandle<T>>) {
        self.jobs.extend(handles);
    }

    pub fn is_done(&self) -> bool {
        self.jobs.iter().any(|job| job.is_done())
    }

    pub fn try_take(&self) -> Option<Option<T>> {
        self.is_done()
            .then(|| self.jobs.iter().find_map(|job| job.try_take()).flatten())
    }

    pub fn wait(self) -> Option<T> {
        loop {
            if let Some(result) = self.try_take() {
                return result;
            } else {
                traced_spin_loop();
            }
        }
    }
}

impl<T: Send + 'static> Clone for AnyJobHandle<T> {
    fn clone(&self) -> Self {
        Self {
            jobs: self.jobs.clone(),
        }
    }
}

impl<T: Send + 'static> Future for AnyJobHandle<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.try_take() {
            cx.waker().wake_by_ref();
            Poll::Ready(result)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobContext {
    pub work_group_index: usize,
    pub work_groups_count: usize,
}

impl std::fmt::Display for JobContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "JobContext {{ work_group_index: {}, work_groups_count: {} }}",
            self.work_group_index, self.work_groups_count
        )
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum JobPriority {
    #[default]
    Normal,
    High,
}

impl std::fmt::Display for JobPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobPriority::Normal => write!(f, "Normal"),
            JobPriority::High => write!(f, "High"),
        }
    }
}

struct JobObject {
    pub id: ID<Jobs>,
    pub job: Job,
    pub context: JobContext,
    pub location: JobLocation,
    pub priority: JobPriority,
    pub cancel: Arc<AtomicBool>,
    pub suspend: Arc<AtomicBool>,
    pub meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
}

#[derive(Default, Clone)]
pub struct JobQueue {
    queue: Arc<RwLock<VecDeque<JobObject>>>,
}

impl JobQueue {
    pub fn is_empty(&self) -> bool {
        self.queue.read().map_or(true, |queue| queue.is_empty())
    }

    pub fn len(&self) -> usize {
        self.queue.read().map_or(0, |queue| queue.len())
    }

    pub fn clear(&self) {
        if let Ok(mut queue) = self.queue.write() {
            queue.clear();
        }
    }

    pub fn append(&self, other: &Self) {
        if let Ok(mut other_queue) = other.queue.write() {
            self.extend(other_queue.drain(..));
        }
    }

    pub fn spawn_on<T: Send + 'static>(
        &self,
        location: JobLocation,
        priority: JobPriority,
        job: impl Future<Output = T> + Send + Sync + 'static,
    ) -> JobHandle<T> {
        let handle = JobHandle::<T>::default();
        let handle2 = handle.clone();
        let job = Job(Box::pin(async move {
            handle2.put(job.await);
        }));
        self.schedule(location, priority, handle, job)
    }

    pub fn spawn_on_with_meta<T: Send + 'static>(
        &self,
        location: JobLocation,
        priority: JobPriority,
        meta: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
        job: impl Future<Output = T> + Send + Sync + 'static,
    ) -> JobHandle<T> {
        let handle = JobHandle::<T>::default().with_meta(meta);
        let handle2 = handle.clone();
        let job = Job(Box::pin(async move {
            handle2.put(job.await);
        }));
        self.schedule(location, priority, handle, job)
    }

    pub fn queue_on<T: Send + 'static>(
        &self,
        location: JobLocation,
        priority: JobPriority,
        job: impl FnOnce(JobContext) -> T + Send + Sync + 'static,
    ) -> JobHandle<T> {
        let handle = JobHandle::<T>::default();
        let handle2 = handle.clone();
        let job = Job(Box::pin(async move {
            handle2.put(job(context().await));
        }));
        self.schedule(location, priority, handle, job)
    }

    fn schedule<T: Send + 'static>(
        &self,
        location: JobLocation,
        priority: JobPriority,
        handle: JobHandle<T>,
        job: Job,
    ) -> JobHandle<T> {
        self.enqueue(JobObject {
            id: ID::new(),
            job,
            context: JobContext {
                work_group_index: 0,
                work_groups_count: 1,
            },
            location,
            priority,
            cancel: handle.cancel.clone(),
            suspend: handle.suspend.clone(),
            meta: handle.meta.clone(),
        });
        handle
    }

    fn enqueue(&self, object: JobObject) {
        if let Ok(mut queue) = self.queue.write() {
            if object.priority == JobPriority::High {
                queue.push_back(object);
            } else {
                queue.push_front(object);
            }
        }
    }

    fn dequeue(&self, target_location: &JobLocation, ignore_location: bool) -> Option<JobObject> {
        let mut queue = self.queue.write().ok()?;
        let object = queue.pop_back()?;
        if ignore_location {
            return Some(object);
        }
        match (&object.location, target_location) {
            (JobLocation::Local, JobLocation::Local)
            | (JobLocation::UnnamedWorker, JobLocation::UnnamedWorker) => Some(object),
            (JobLocation::NamedWorker(a), JobLocation::NamedWorker(b)) if a == b => Some(object),
            (JobLocation::ExactThread(a), _) => {
                if *a == std::thread::current().id() {
                    Some(object)
                } else {
                    queue.push_front(object);
                    None
                }
            }
            (JobLocation::OtherThanThread(a), _) => {
                if *a != std::thread::current().id() {
                    Some(object)
                } else {
                    queue.push_front(object);
                    None
                }
            }
            (JobLocation::NonLocal, JobLocation::Local) => {
                queue.push_front(object);
                None
            }
            (JobLocation::Unknown, _) => Some(object),
            _ => {
                queue.push_front(object);
                None
            }
        }
    }

    fn extend(&self, queue: impl IntoIterator<Item = JobObject>) {
        if let Ok(mut current_queue) = self.queue.write() {
            for object in queue {
                if object.priority == JobPriority::High {
                    current_queue.push_back(object);
                } else {
                    current_queue.push_front(object);
                }
            }
        }
    }
}

struct Worker {
    location: JobLocation,
    thread: Option<JoinHandle<()>>,
    terminate: Arc<AtomicBool>,
}

impl Worker {
    fn new(
        worker_location: JobLocation,
        queue: JobQueue,
        global_meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
        worker_meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
        hash_tokens: Arc<Mutex<HashSet<u64>>>,
        notify: Arc<(Mutex<bool>, Condvar)>,
    ) -> Worker {
        let terminate = Arc::new(AtomicBool::default());
        let terminate2 = terminate.clone();
        let worker_location2 = worker_location.clone();
        let thread = spawn(move || {
            let mut pending = vec![];
            loop {
                if terminate2.load(Ordering::Relaxed) {
                    return;
                }
                while let Some(object) = queue.dequeue(&worker_location2, false) {
                    let JobObject {
                        id,
                        job,
                        context,
                        location,
                        mut priority,
                        cancel,
                        suspend,
                        meta,
                    } = object;
                    let mut notify_workers = false;
                    let (poll_result, receiver) = if suspend.load(Ordering::Relaxed) {
                        let (_, rx) = std::sync::mpsc::channel();
                        (Some(job), rx)
                    } else {
                        let (waker, receiver) = JobsWaker::new_waker(
                            queue.clone(),
                            location.clone(),
                            context,
                            priority,
                            global_meta.clone(),
                            worker_meta.clone(),
                            meta.clone(),
                            hash_tokens.clone(),
                            cancel.clone(),
                            suspend.clone(),
                        );
                        let mut cx = Context::from_waker(&waker);
                        #[cfg(feature = "tracing")]
                        let _span = tracing::span!(
                            tracing::Level::TRACE,
                            "Job poll",
                            id = id.to_string(),
                            location = location.to_string(),
                            context = context.to_string(),
                            priority = priority.to_string(),
                            thread_id = format!("{:?}", std::thread::current().id()),
                        )
                        .entered();
                        let poll_result = job.poll(&mut cx);
                        (poll_result, receiver)
                    };
                    if let Some(job) = poll_result {
                        let mut move_to = None;
                        for command in receiver.try_iter() {
                            notify_workers = true;
                            match command {
                                JobsWakerCommand::MoveTo(location) => {
                                    move_to = Some(location);
                                }
                                JobsWakerCommand::ChangePriority(new_priority) => {
                                    priority = new_priority;
                                }
                            }
                        }
                        if let Some(location) = move_to {
                            pending.push(JobObject {
                                id,
                                job,
                                context,
                                location,
                                priority,
                                cancel,
                                suspend,
                                meta,
                            });
                        } else {
                            pending.push(JobObject {
                                id,
                                job,
                                context,
                                location,
                                priority,
                                cancel,
                                suspend,
                                meta,
                            });
                        }
                    }
                    if terminate2.load(Ordering::Relaxed) {
                        return;
                    }
                    if notify_workers {
                        let (lock, cvar) = &*notify;
                        if let Ok(mut running) = lock.lock() {
                            *running = true;
                        }
                        cvar.notify_all();
                    }
                }
                queue.extend(pending.drain(..));
                if !queue.is_empty() {
                    continue;
                }
                let (lock, cvar) = &*notify;
                let Ok(mut ready) = lock.lock() else {
                    return;
                };
                loop {
                    let Ok((new, _)) = cvar.wait_timeout(ready, Duration::from_millis(10)) else {
                        return;
                    };
                    ready = new;
                    if *ready {
                        break;
                    }
                }
            }
        });
        Worker {
            location: worker_location,
            thread: Some(thread),
            terminate,
        }
    }
}

pub(crate) enum JobsWakerCommand {
    MoveTo(JobLocation),
    ChangePriority(JobPriority),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum JobLocation {
    #[default]
    Unknown,
    Local,
    NonLocal,
    UnnamedWorker,
    NamedWorker(String),
    ExactThread(ThreadId),
    OtherThanThread(ThreadId),
}

impl JobLocation {
    pub fn named_worker(name: impl ToString) -> Self {
        JobLocation::NamedWorker(name.to_string())
    }

    pub fn thread(thread: ThreadId) -> Self {
        JobLocation::ExactThread(thread)
    }

    pub fn current_thread() -> Self {
        JobLocation::ExactThread(std::thread::current().id())
    }

    pub fn other_than_current_thread() -> Self {
        JobLocation::OtherThanThread(std::thread::current().id())
    }
}

impl std::fmt::Display for JobLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobLocation::Unknown => write!(f, "Unknown"),
            JobLocation::Local => write!(f, "Local"),
            JobLocation::NonLocal => write!(f, "Non-local"),
            JobLocation::UnnamedWorker => write!(f, "Unnamed worker"),
            JobLocation::NamedWorker(name) => write!(f, "Named worker: {name}"),
            JobLocation::ExactThread(id) => write!(f, "Exact thread: {id:?}"),
            JobLocation::OtherThanThread(id) => write!(f, "Other than thread: {id:?}"),
        }
    }
}

#[derive(Default)]
pub struct JobToken {
    hash_tokens: Arc<Mutex<HashSet<u64>>>,
    hash: u64,
}

impl Drop for JobToken {
    fn drop(&mut self) {
        let mut hash_tokens = self.hash_tokens.lock().unwrap();
        hash_tokens.remove(&self.hash);
    }
}

pub(crate) struct JobsWaker {
    sender: Sender<JobsWakerCommand>,
    queue: JobQueue,
    location: JobLocation,
    context: JobContext,
    priority: JobPriority,
    global_meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
    worker_meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
    local_meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
    hash_tokens: Arc<Mutex<HashSet<u64>>>,
    cancel: Arc<AtomicBool>,
    suspend: Arc<AtomicBool>,
}

impl JobsWaker {
    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(Self::vtable_clone, |_| {}, |_| {}, Self::vtable_drop);

    fn vtable_clone(data: *const ()) -> RawWaker {
        let arc = unsafe { Arc::<Self>::from_raw(data as *const Self) };
        let cloned = arc.clone();
        std::mem::forget(arc);
        RawWaker::new(Arc::into_raw(cloned) as *const (), &Self::VTABLE)
    }

    fn vtable_drop(data: *const ()) {
        let _ = unsafe { Arc::from_raw(data as *const Self) };
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_waker(
        queue: JobQueue,
        location: JobLocation,
        context: JobContext,
        priority: JobPriority,
        global_meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
        worker_meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
        local_meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
        hash_tokens: Arc<Mutex<HashSet<u64>>>,
        cancel: Arc<AtomicBool>,
        suspend: Arc<AtomicBool>,
    ) -> (Waker, Receiver<JobsWakerCommand>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        let arc = Arc::new(Self {
            sender,
            queue,
            location,
            context,
            priority,
            global_meta,
            worker_meta,
            local_meta,
            hash_tokens,
            cancel,
            suspend,
        });
        let raw = RawWaker::new(Arc::into_raw(arc) as *const (), &Self::VTABLE);
        (unsafe { Waker::from_raw(raw) }, receiver)
    }

    pub fn try_cast(waker: &Waker) -> Option<&Self> {
        if waker.vtable() == &Self::VTABLE {
            unsafe { waker.data().cast::<Self>().as_ref() }
        } else {
            None
        }
    }

    pub fn command(&self, command: JobsWakerCommand) {
        let _ = self.sender.send(command);
    }

    pub fn enqueue(&self, object: JobObject) {
        self.queue.enqueue(object);
    }

    pub fn queue(&self) -> JobQueue {
        self.queue.clone()
    }

    pub fn location(&self) -> JobLocation {
        self.location.clone()
    }

    pub fn context(&self) -> JobContext {
        self.context
    }

    pub fn priority(&self) -> JobPriority {
        self.priority
    }

    pub fn get_meta(&self, name: &str) -> Option<DynamicManagedLazy> {
        self.local_meta
            .read()
            .ok()
            .and_then(|meta| meta.get(name).cloned())
            .or_else(|| {
                self.worker_meta
                    .read()
                    .ok()
                    .and_then(|meta| meta.get(name).cloned())
                    .or_else(|| {
                        self.global_meta
                            .read()
                            .ok()
                            .and_then(|meta| meta.get(name).cloned())
                    })
            })
    }

    pub fn local_meta(&self) -> Arc<RwLock<HashMap<String, DynamicManagedLazy>>> {
        self.local_meta.clone()
    }

    pub fn cancel(&self) -> Arc<AtomicBool> {
        self.cancel.clone()
    }

    pub fn suspend(&self) -> Arc<AtomicBool> {
        self.suspend.clone()
    }

    pub fn acquire_token<T: Hash>(&self, subject: &T) -> Option<JobToken> {
        let mut hasher = DefaultHasher::new();
        subject.hash(&mut hasher);
        let hash = hasher.finish();
        let mut hash_tokens = self.hash_tokens.lock().unwrap();
        if hash_tokens.contains(&hash) {
            None
        } else {
            hash_tokens.insert(hash);
            Some(JobToken {
                hash_tokens: self.hash_tokens.clone(),
                hash,
            })
        }
    }

    pub fn acquire_token_timeout<T: Hash>(
        &self,
        subject: &T,
        timeout: Duration,
    ) -> Option<JobToken> {
        let mut hasher = DefaultHasher::new();
        subject.hash(&mut hasher);
        let hash = hasher.finish();
        let timer = Instant::now();
        while timer.elapsed() < timeout {
            let mut hash_tokens = self.hash_tokens.try_lock().unwrap();
            if hash_tokens.contains(&hash) {
                traced_spin_loop();
                continue;
            } else {
                hash_tokens.insert(hash);
                return Some(JobToken {
                    hash_tokens: self.hash_tokens.clone(),
                    hash,
                });
            }
        }
        None
    }
}

impl Wake for JobsWaker {
    fn wake(self: Arc<Self>) {}
}

pub struct Jobs {
    workers: Vec<Worker>,
    queue: JobQueue,
    meta: Arc<RwLock<HashMap<String, DynamicManagedLazy>>>,
    hash_tokens: Arc<Mutex<HashSet<u64>>>,
    /// (ready, cond var)
    notify: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for Jobs {
    fn drop(&mut self) {
        for worker in &self.workers {
            worker.terminate.store(true, Ordering::Relaxed);
        }
        let (lock, cvar) = &*self.notify;
        if let Ok(mut ready) = lock.lock() {
            *ready = true;
        };
        cvar.notify_all();
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

impl Default for Jobs {
    fn default() -> Self {
        Self::new(
            available_parallelism()
                .ok()
                .map(|v| v.get())
                .unwrap_or_default(),
        )
    }
}

impl Jobs {
    pub fn new(count: usize) -> Jobs {
        let queue = JobQueue::default();
        let notify = Arc::new((Mutex::default(), Condvar::new()));
        let global_meta = Arc::new(RwLock::new(HashMap::default()));
        let worker_meta = Arc::new(RwLock::new(HashMap::default()));
        let hash_tokens = Arc::new(Mutex::new(HashSet::default()));
        Jobs {
            workers: (0..count)
                .map(|_| {
                    Worker::new(
                        JobLocation::UnnamedWorker,
                        queue.clone(),
                        global_meta.clone(),
                        worker_meta.clone(),
                        hash_tokens.clone(),
                        notify.clone(),
                    )
                })
                .collect(),
            queue,
            meta: global_meta,
            hash_tokens,
            notify,
        }
    }

    pub fn with_unnamed_worker(mut self) -> Self {
        self.add_unnamed_worker();
        self
    }

    pub fn with_named_worker(mut self, name: impl ToString) -> Self {
        self.add_named_worker(name);
        self
    }

    pub fn add_unnamed_worker(&mut self) {
        self.add_unnamed_worker_with_meta([]);
    }

    pub fn add_unnamed_worker_with_meta(
        &mut self,
        meta: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
    ) {
        self.workers.push(Worker::new(
            JobLocation::UnnamedWorker,
            self.queue.clone(),
            self.meta.clone(),
            Arc::new(RwLock::new(meta.into_iter().collect::<HashMap<_, _>>())),
            self.hash_tokens.clone(),
            self.notify.clone(),
        ));
    }

    pub fn add_named_worker(&mut self, name: impl ToString) {
        self.add_named_worker_with_meta(name, []);
    }

    pub fn add_named_worker_with_meta(
        &mut self,
        name: impl ToString,
        meta: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
    ) {
        self.workers.push(Worker::new(
            JobLocation::named_worker(name),
            self.queue.clone(),
            self.meta.clone(),
            Arc::new(RwLock::new(meta.into_iter().collect::<HashMap<_, _>>())),
            self.hash_tokens.clone(),
            self.notify.clone(),
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
            let mut worker = self.workers.swap_remove(index);
            worker.terminate.store(true, Ordering::Relaxed);
            let (lock, cvar) = &*self.notify;
            if let Ok(mut ready) = lock.lock() {
                *ready = true;
            };
            cvar.notify_all();
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join();
            }
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

    pub fn set_meta(&self, name: impl ToString, value: DynamicManagedLazy) {
        let mut meta = self.meta.write().unwrap();
        meta.insert(name.to_string(), value);
    }

    pub fn unset_meta(&self, name: &str) {
        let mut meta = self.meta.write().unwrap();
        meta.remove(name);
    }

    pub fn get_meta<T>(&self, name: &str) -> Option<ManagedLazy<T>> {
        let meta = self.meta.read().unwrap();
        meta.get(name)
            .cloned()
            .and_then(|value| value.into_typed::<T>().ok())
    }

    pub fn get_meta_dynamic(&self, name: &str) -> Option<DynamicManagedLazy> {
        let meta = self.meta.read().unwrap();
        meta.get(name).cloned()
    }

    pub fn run_local(&self) {
        self.run_local_inner(Duration::MAX, []);
    }

    pub fn run_local_with_meta(
        &self,
        meta: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
    ) {
        self.run_local_inner(Duration::MAX, meta);
    }

    pub fn run_local_timeout(&self, timeout: Duration) {
        self.run_local_inner(timeout, []);
    }

    pub fn run_local_timeout_with_meta(
        &self,
        timeout: Duration,
        meta: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
    ) {
        self.run_local_inner(timeout, meta);
    }

    fn run_local_inner(
        &self,
        timeout: Duration,
        meta: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
    ) {
        let timer = Instant::now();
        let mut pending = vec![];
        let worker_meta = Arc::new(RwLock::new(meta.into_iter().collect::<HashMap<_, _>>()));
        while let Some(object) = self
            .queue
            .dequeue(&JobLocation::Local, self.workers.is_empty())
        {
            let JobObject {
                id,
                job,
                context,
                location,
                mut priority,
                cancel,
                suspend,
                meta,
            } = object;
            let mut notify_workers = false;
            let (poll_result, receiver) = if suspend.load(Ordering::Relaxed) {
                let (_, rx) = std::sync::mpsc::channel();
                (Some(job), rx)
            } else {
                let (waker, receiver) = JobsWaker::new_waker(
                    self.queue.clone(),
                    location.clone(),
                    context,
                    priority,
                    self.meta.clone(),
                    worker_meta.clone(),
                    meta.clone(),
                    self.hash_tokens.clone(),
                    cancel.clone(),
                    suspend.clone(),
                );
                let mut cx = Context::from_waker(&waker);
                #[cfg(feature = "tracing")]
                let _span = tracing::span!(
                    tracing::Level::TRACE,
                    "Job poll",
                    id = id.to_string(),
                    location = location.to_string(),
                    context = context.to_string(),
                    priority = priority.to_string(),
                    thread_id = format!("{:?}", std::thread::current().id()),
                )
                .entered();
                let poll_result = job.poll(&mut cx);
                (poll_result, receiver)
            };
            if let Some(job) = poll_result {
                let mut move_to = None;
                for command in receiver.try_iter() {
                    notify_workers = true;
                    match command {
                        JobsWakerCommand::MoveTo(location) => move_to = Some(location),
                        JobsWakerCommand::ChangePriority(new_priority) => {
                            priority = new_priority;
                        }
                    }
                }
                if let Some(location) = move_to {
                    pending.push(JobObject {
                        id,
                        job,
                        context,
                        location,
                        priority,
                        cancel,
                        suspend,
                        meta,
                    });
                } else {
                    pending.push(JobObject {
                        id,
                        job,
                        context,
                        location,
                        priority,
                        cancel,
                        suspend,
                        meta,
                    });
                }
            }
            if notify_workers {
                let (lock, cvar) = &*self.notify;
                if let Ok(mut running) = lock.lock() {
                    *running = true;
                }
                cvar.notify_all();
            }
            if timer.elapsed() >= timeout {
                break;
            }
        }
        self.queue.extend(pending);
    }

    pub fn submit_queue(&self, queue: &JobQueue) {
        self.queue.append(queue);
        let (lock, cvar) = &*self.notify;
        if let Ok(mut running) = lock.lock() {
            *running = true;
        }
        cvar.notify_all();
    }

    pub fn run_queue(&self, queue: &JobQueue) {
        self.run_queue_inner(queue, Duration::MAX, []);
    }

    pub fn run_queue_with_meta(
        &self,
        queue: &JobQueue,
        meta: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
    ) {
        self.run_queue_inner(queue, Duration::MAX, meta);
    }

    pub fn run_queue_timeout(&self, queue: &JobQueue, timeout: Duration) {
        self.run_queue_inner(queue, timeout, []);
    }

    pub fn run_queue_timeout_with_meta(
        &self,
        queue: &JobQueue,
        timeout: Duration,
        meta: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
    ) {
        self.run_queue_inner(queue, timeout, meta);
    }

    fn run_queue_inner(
        &self,
        queue: &JobQueue,
        timeout: Duration,
        meta: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
    ) {
        let timer = Instant::now();
        let mut pending = vec![];
        let worker_meta = Arc::new(RwLock::new(meta.into_iter().collect::<HashMap<_, _>>()));
        while let Some(object) = queue.dequeue(&JobLocation::Unknown, true) {
            let JobObject {
                id,
                job,
                context,
                location,
                mut priority,
                cancel,
                suspend,
                meta,
            } = object;
            let mut notify_workers = false;
            let (poll_result, receiver) = if suspend.load(Ordering::Relaxed) {
                let (_, rx) = std::sync::mpsc::channel();
                (Some(job), rx)
            } else {
                let (waker, receiver) = JobsWaker::new_waker(
                    queue.clone(),
                    location.clone(),
                    context,
                    priority,
                    self.meta.clone(),
                    worker_meta.clone(),
                    meta.clone(),
                    self.hash_tokens.clone(),
                    cancel.clone(),
                    suspend.clone(),
                );
                let mut cx = Context::from_waker(&waker);
                #[cfg(feature = "tracing")]
                let _span = tracing::span!(
                    tracing::Level::TRACE,
                    "Job poll",
                    id = id.to_string(),
                    location = location.to_string(),
                    context = context.to_string(),
                    priority = priority.to_string(),
                    thread_id = format!("{:?}", std::thread::current().id()),
                )
                .entered();
                let poll_result = job.poll(&mut cx);
                (poll_result, receiver)
            };
            if let Some(job) = poll_result {
                let mut move_to = None;
                for command in receiver.try_iter() {
                    notify_workers = true;
                    match command {
                        JobsWakerCommand::MoveTo(location) => move_to = Some(location),
                        JobsWakerCommand::ChangePriority(new_priority) => {
                            priority = new_priority;
                        }
                    }
                }
                if let Some(location) = move_to {
                    pending.push(JobObject {
                        id,
                        job,
                        context,
                        location,
                        priority,
                        cancel,
                        suspend,
                        meta,
                    });
                } else {
                    pending.push(JobObject {
                        id,
                        job,
                        context,
                        location,
                        priority,
                        cancel,
                        suspend,
                        meta,
                    });
                }
            }
            if notify_workers {
                let (lock, cvar) = &*self.notify;
                if let Ok(mut running) = lock.lock() {
                    *running = true;
                }
                cvar.notify_all();
            }
            if timer.elapsed() >= timeout {
                break;
            }
        }
        self.queue.extend(pending);
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    pub fn spawn_on<T: Send + 'static>(
        &self,
        location: JobLocation,
        priority: JobPriority,
        job: impl Future<Output = T> + Send + Sync + 'static,
    ) -> Result<JobHandle<T>, Box<dyn Error>> {
        let handle = self.queue.spawn_on(location, priority, job);
        let (lock, cvar) = &*self.notify;
        let mut running = lock.lock().map_err(|error| format!("{error}"))?;
        *running = true;
        cvar.notify_all();
        Ok(handle)
    }

    pub fn spawn_on_with_meta<T: Send + 'static>(
        &self,
        location: JobLocation,
        priority: JobPriority,
        meta: impl IntoIterator<Item = (String, DynamicManagedLazy)>,
        job: impl Future<Output = T> + Send + Sync + 'static,
    ) -> Result<JobHandle<T>, Box<dyn Error>> {
        let handle = self.queue.spawn_on_with_meta(location, priority, meta, job);
        let (lock, cvar) = &*self.notify;
        let mut running = lock.lock().map_err(|error| format!("{error}"))?;
        *running = true;
        cvar.notify_all();
        Ok(handle)
    }

    pub fn queue_on<T: Send + 'static>(
        &self,
        location: JobLocation,
        priority: JobPriority,
        job: impl FnOnce(JobContext) -> T + Send + Sync + 'static,
    ) -> Result<JobHandle<T>, Box<dyn Error>> {
        let handle = self.queue.queue_on(location, priority, job);
        let (lock, cvar) = &*self.notify;
        let mut running = lock.lock().map_err(|error| format!("{error}"))?;
        *running = true;
        cvar.notify_all();
        Ok(handle)
    }

    pub fn broadcast<T: Send + 'static>(
        &self,
        job: impl Fn(JobContext) -> T + Send + Sync + 'static,
    ) -> Result<AllJobsHandle<T>, Box<dyn Error>> {
        self.broadcast_n(self.workers.len(), job)
    }

    pub fn broadcast_n<T: Send + 'static>(
        &self,
        work_groups: usize,
        job: impl Fn(JobContext) -> T + Send + Sync + 'static,
    ) -> Result<AllJobsHandle<T>, Box<dyn Error>> {
        if self.workers.is_empty() {
            return Ok(AllJobsHandle::new(job(JobContext {
                work_group_index: 0,
                work_groups_count: 1,
            })));
        }
        let job = Arc::new(job);
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
                    });
                    handle
                })
                .collect::<Vec<_>>(),
        };
        let (lock, cvar) = &*self.notify;
        let mut running = lock.lock().map_err(|error| format!("{error}"))?;
        *running = true;
        cvar.notify_all();
        Ok(handle)
    }
}

pub struct ScopedJobs<'env, T: Send + 'static> {
    jobs: &'env Jobs,
    handles: AllJobsHandle<T>,
}

impl<T: Send + 'static> Drop for ScopedJobs<'_, T> {
    fn drop(&mut self) {
        self.execute_inner();
    }
}

impl<'env, T: Send + 'static> ScopedJobs<'env, T> {
    pub fn new(jobs: &'env Jobs) -> Self {
        Self {
            jobs,
            handles: Default::default(),
        }
    }

    pub fn spawn_on(
        &mut self,
        location: JobLocation,
        priority: JobPriority,
        job: impl Future<Output = T> + Send + Sync + 'env,
    ) -> Result<(), Box<dyn Error>> {
        let job = unsafe {
            std::mem::transmute::<
                Pin<Box<dyn Future<Output = T> + Send + Sync + 'env>>,
                Pin<Box<dyn Future<Output = T> + Send + Sync + 'static>>,
            >(Box::pin(job))
        };
        let handle = self.jobs.spawn_on(location, priority, job)?;
        self.handles.add(handle);
        Ok(())
    }

    pub fn queue_on(
        &mut self,
        location: JobLocation,
        priority: JobPriority,
        job: impl FnOnce(JobContext) -> T + Send + Sync + 'env,
    ) -> Result<(), Box<dyn Error>> {
        let job = unsafe {
            std::mem::transmute::<
                Box<dyn FnOnce(JobContext) -> T + Send + Sync + 'env>,
                Box<dyn FnOnce(JobContext) -> T + Send + Sync + 'static>,
            >(Box::new(job))
        };
        self.handles
            .add(self.jobs.queue_on(location, priority, job)?);
        Ok(())
    }

    pub fn broadcast(
        &mut self,
        job: impl Fn(JobContext) -> T + Send + Sync + 'env,
    ) -> Result<(), Box<dyn Error>> {
        let job = unsafe {
            std::mem::transmute::<
                Box<dyn Fn(JobContext) -> T + Send + Sync + 'env>,
                Box<dyn Fn(JobContext) -> T + Send + Sync + 'static>,
            >(Box::new(job))
        };
        self.handles.extend(self.jobs.broadcast(job)?.into_inner());
        Ok(())
    }

    pub fn broadcast_n(
        &mut self,
        work_groups: usize,
        job: impl Fn(JobContext) -> T + Send + Sync + 'env,
    ) -> Result<(), Box<dyn Error>> {
        let job = unsafe {
            std::mem::transmute::<
                Box<dyn Fn(JobContext) -> T + Send + Sync + 'env>,
                Box<dyn Fn(JobContext) -> T + Send + Sync + 'static>,
            >(Box::new(job))
        };
        self.handles
            .extend(self.jobs.broadcast_n(work_groups, job)?.into_inner());
        Ok(())
    }

    pub fn execute(mut self) -> Vec<T> {
        self.execute_inner()
    }

    fn execute_inner(&mut self) -> Vec<T> {
        std::mem::take(&mut self.handles).wait().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coroutine::{
        acquire_token, block_on, location, meta, move_to, on_exit, queue_on, spawn_on, suspend,
        wait_polls, wait_time, with_all, with_any, yield_now,
    };
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_jobs() {
        fn is_async<T: Send + Sync>() {}

        is_async::<Jobs>();

        let jobs = Jobs::default();
        let data = (0..100).collect::<Vec<_>>();
        let data2 = data.clone();

        let job = jobs
            .queue_on(JobLocation::Unknown, JobPriority::Normal, move |_| {
                data.into_iter().sum::<usize>()
            })
            .unwrap();

        let result = job.wait().unwrap();
        assert_eq!(result, 4950);

        let job = jobs
            .queue_on(JobLocation::Local, JobPriority::Normal, move |_| {
                data2.into_iter().sum::<usize>()
            })
            .unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.broadcast(move |ctx| ctx.work_group_index).unwrap();
        let result = job.wait().unwrap().into_iter().sum::<usize>();
        assert_eq!(result, (0..jobs.workers.len()).sum());

        let job = jobs
            .broadcast_n(10, move |ctx| ctx.work_group_index)
            .unwrap();
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
        let jobs = Jobs::new(0);
        let data = (0..100).collect::<Vec<_>>();
        let data2 = data.clone();
        let data3 = data.clone();
        let data4 = data.clone();
        let data5 = data.clone();
        let data6 = data.clone();

        let job = jobs
            .queue_on(JobLocation::Unknown, JobPriority::Normal, move |_| {
                data.into_iter().sum::<usize>()
            })
            .unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 4950);

        let job = jobs
            .queue_on(JobLocation::Local, JobPriority::Normal, move |_| {
                data2.into_iter().sum::<usize>()
            })
            .unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 4950);

        let job = jobs
            .queue_on(JobLocation::UnnamedWorker, JobPriority::Normal, move |_| {
                data3.into_iter().sum::<usize>()
            })
            .unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 4950);

        let job = jobs
            .queue_on(
                JobLocation::named_worker("temp"),
                JobPriority::Normal,
                move |_| data4.into_iter().sum::<usize>(),
            )
            .unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 4950);

        let job = jobs
            .queue_on(
                JobLocation::current_thread(),
                JobPriority::Normal,
                move |_| data5.into_iter().sum::<usize>(),
            )
            .unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 4950);

        let job = jobs
            .queue_on(
                JobLocation::other_than_current_thread(),
                JobPriority::Normal,
                move |_| data6.into_iter().sum::<usize>(),
            )
            .unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 4950);

        let job = jobs.broadcast(move |_| 1).unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.wait().unwrap().into_iter().sum::<usize>();
        assert_eq!(result, 1);

        let job = jobs.broadcast_n(10, move |_| 1).unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.wait().unwrap().into_iter().sum::<usize>();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_queue_jobs() {
        let jobs = Jobs::new(0);
        let queue = JobQueue::default();
        let data = (0..100).collect::<Vec<_>>();

        let job = queue.queue_on(JobLocation::Unknown, JobPriority::Normal, move |_| {
            data.into_iter().sum::<usize>()
        });

        while !job.is_done() {
            jobs.run_queue(&queue);
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 4950);
    }

    #[test]
    fn test_scoped_jobs() {
        let jobs = Jobs::default();
        let mut data = (0..100).collect::<Vec<_>>();

        let mut scope = ScopedJobs::new(&jobs);
        scope
            .queue_on(JobLocation::Unknown, JobPriority::Normal, |_| {
                for value in &mut data {
                    *value *= 2;
                }
                data.iter().copied().sum::<usize>()
            })
            .unwrap();

        let result = scope.execute().into_iter().sum::<usize>();
        assert_eq!(result, 9900);
    }

    #[test]
    fn test_futures_spawn() {
        let jobs = Jobs::default();
        let data = (0..100).collect::<Vec<_>>();
        let data2 = data.clone();

        let job = jobs
            .spawn_on(JobLocation::Unknown, JobPriority::Normal, async move {
                let mut result = 0;
                for value in data {
                    result += value;
                    yield_now().await;
                }
                result
            })
            .unwrap();

        let result = block_on(job).unwrap();
        assert_eq!(result, 4950);

        let job = jobs
            .spawn_on(JobLocation::Local, JobPriority::Normal, async move {
                let mut result = 0;
                for value in data2 {
                    result += value;
                    yield_now().await;
                }
                result
            })
            .unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 4950);

        let job = jobs
            .spawn_on(JobLocation::Unknown, JobPriority::Normal, async {
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
            })
            .unwrap();
        let result = block_on(job).unwrap();
        assert_eq!(result, 3);

        let job = jobs
            .spawn_on(JobLocation::Unknown, JobPriority::Normal, async {
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
            })
            .unwrap();
        let result = block_on(job).unwrap();
        assert!(result > 0);
    }

    #[test]
    fn test_futures_move() {
        let jobs = Jobs::new(1).with_named_worker("foo");

        let job = jobs
            .spawn_on(JobLocation::Local, JobPriority::Normal, async {
                yield_now().await;
                // A: Local
                println!("A: {:?}", location().await);
                move_to(JobLocation::Unknown).await;
                // B: UnnamedWorker
                println!("B: {:?}", location().await);
                move_to(JobLocation::named_worker("foo")).await;
                // C: NamedWorker("foo")
                println!("C: {:?}", location().await);
                move_to(JobLocation::Local).await;
                // D: Local
                println!("D: {:?}", location().await);
                42
            })
            .unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_futures_schedule() {
        let jobs = Jobs::new(1).with_named_worker("foo");

        let job = jobs
            .spawn_on(JobLocation::Local, JobPriority::Normal, async {
                spawn_on(JobLocation::Local, JobPriority::Normal, async {
                    println!("A: {:?}", location().await);
                })
                .await;
                spawn_on(JobLocation::Unknown, JobPriority::Normal, async {
                    println!("B: {:?}", location().await);
                })
                .await;
                spawn_on(
                    JobLocation::named_worker("foo"),
                    JobPriority::Normal,
                    async {
                        println!("C: {:?}", location().await);
                    },
                )
                .await;
                queue_on(JobLocation::Local, JobPriority::Normal, |_| {
                    println!("D: Local closure");
                })
                .await;
                queue_on(JobLocation::Unknown, JobPriority::Normal, |_| {
                    println!("E: Unnamed worker closure");
                })
                .await;
                queue_on(
                    JobLocation::named_worker("foo"),
                    JobPriority::Normal,
                    |_| {
                        println!("F: Named worker closure");
                    },
                )
                .await;
                42
            })
            .unwrap();

        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_futures_meta() {
        let jobs = Jobs::default();
        let mut value = 42usize;
        let (value_lazy, _value_lifetime) = DynamicManagedLazy::make(&mut value);
        jobs.set_meta("value", value_lazy);

        let job = jobs
            .spawn_on(JobLocation::Unknown, JobPriority::Normal, async {
                let value = meta::<usize>("value").await.unwrap();
                *value.read().unwrap()
            })
            .unwrap();

        let result = block_on(job).unwrap();
        assert_eq!(result, 42);

        let mut flag = true;
        let (flag_lazy, _flag_lifetime) = DynamicManagedLazy::make(&mut flag);
        let job = jobs
            .spawn_on_with_meta(
                JobLocation::Unknown,
                JobPriority::Normal,
                [("flag".to_owned(), flag_lazy)],
                async {
                    let flag = meta::<bool>("flag").await.unwrap();
                    *flag.read().unwrap()
                },
            )
            .unwrap();

        let result = block_on(job).unwrap();
        assert!(result);

        let mut flag = true;
        let (flag_lazy, _flag_lifetime) = DynamicManagedLazy::make(&mut flag);
        let job = jobs
            .spawn_on(JobLocation::Local, JobPriority::Normal, async {
                let flag = meta::<bool>("flag").await.unwrap();
                *flag.read().unwrap()
            })
            .unwrap();

        while !job.is_done() {
            jobs.run_local_with_meta([("flag".to_owned(), flag_lazy.clone())]);
        }
        let result = job.try_take().unwrap().unwrap();
        assert!(result);
    }

    #[test]
    fn test_futures_acquire_token() {
        let jobs = Jobs::new(3);

        let a = jobs
            .spawn_on(JobLocation::Unknown, JobPriority::Normal, async {
                let _token = acquire_token(&"foo").await;
                std::thread::sleep(Duration::from_millis(50));
                for i in 0..10 {
                    println!("{i}");
                    std::thread::sleep(Duration::from_millis(10));
                }
            })
            .unwrap();
        let b = jobs
            .spawn_on(JobLocation::Unknown, JobPriority::Normal, async {
                let _token = acquire_token(&"foo").await;
                std::thread::sleep(Duration::from_millis(50));
                for i in 10..20 {
                    println!("{i}");
                    std::thread::sleep(Duration::from_millis(10));
                }
            })
            .unwrap();
        block_on(AllJobsHandle::many([a, b])).unwrap();

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

            let a = jobs
                .spawn_on(JobLocation::Unknown, JobPriority::Normal, async move {
                    std::thread::sleep(Duration::from_millis(50));
                    save_file(PATH, &content).await;
                })
                .unwrap();
            let b = jobs
                .spawn_on(JobLocation::Unknown, JobPriority::Normal, async {
                    std::thread::sleep(Duration::from_millis(50));
                    let _ = load_file(PATH).await;
                })
                .unwrap();
            block_on(AllJobsHandle::many([a, b])).unwrap();
        }
    }

    #[test]
    fn test_futures_on_exit() {
        let jobs = Jobs::default();

        let state = Arc::new(AtomicBool::new(false));
        let state1 = state.clone();
        let job = jobs
            .spawn_on(JobLocation::Unknown, JobPriority::Normal, async {
                let _exit = on_exit(async move {
                    state1.store(true, Ordering::SeqCst);
                })
                .await;
                42
            })
            .unwrap();

        let result = block_on(job).unwrap();
        assert_eq!(result, 42);
        std::thread::sleep(Duration::from_millis(100));
        assert!(state.load(Ordering::SeqCst));

        let state = Arc::new(AtomicBool::new(false));
        let state1 = state.clone();
        let job = jobs
            .spawn_on(JobLocation::Unknown, JobPriority::Normal, async {
                let exit = on_exit(async move {
                    state1.store(true, Ordering::SeqCst);
                })
                .await;
                exit.invalidate();
                42
            })
            .unwrap();

        let result = block_on(job).unwrap();
        assert_eq!(result, 42);
        std::thread::sleep(Duration::from_millis(100));
        assert!(!state.load(Ordering::SeqCst));

        let state = Arc::new(AtomicBool::new(false));
        let state1 = state.clone();
        let job = jobs
            .spawn_on(JobLocation::Local, JobPriority::Normal, async {
                let exit = on_exit(async move {
                    state1.store(true, Ordering::SeqCst);
                })
                .await;
                // job gets cancelled at this point,
                // so exit future won't get invalidated.
                exit.invalidate();
                42
            })
            .unwrap();

        job.cancel();
        while !job.is_done() {
            jobs.run_local();
        }
        assert_eq!(job.try_take(), Some(None));
        assert!(!state.load(Ordering::SeqCst));
    }

    #[test]
    fn test_futures_suspend() {
        let jobs = Jobs::default();

        let job = jobs
            .spawn_on(JobLocation::Local, JobPriority::Normal, async {
                suspend().await;
                42
            })
            .unwrap();

        assert!(!job.is_done());
        for _ in 0..10 {
            jobs.run_local();
        }
        assert!(!job.is_done());

        job.resume();
        while !job.is_done() {
            jobs.run_local();
        }
        let result = job.try_take().unwrap().unwrap();
        assert_eq!(result, 42);
    }
}
