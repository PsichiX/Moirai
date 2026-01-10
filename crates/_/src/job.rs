use crate::{
    jobs::{Jobs, JobsMeta, JobsMetaValue, JobsTags},
    queue::JobQueue,
    third_party::time::{Duration, Instant},
    traced_spin_loop,
};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    thread::ThreadId,
};
use typid::ID;

#[derive(Default, Clone)]
pub struct JobOptions {
    pub location: JobLocation,
    pub priority: JobPriority,
    pub meta: HashMap<Cow<'static, str>, JobsMetaValue>,
    pub tags: JobsTags,
}

impl JobOptions {
    pub fn location(mut self, location: JobLocation) -> Self {
        self.location = location;
        self
    }

    pub fn priority(mut self, priority: JobPriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn meta(
        mut self,
        name: impl Into<Cow<'static, str>>,
        value: impl Into<JobsMetaValue>,
    ) -> Self {
        self.meta.insert(name.into(), value.into());
        self
    }

    pub fn meta_many(
        mut self,
        meta: impl IntoIterator<Item = (Cow<'static, str>, JobsMetaValue)>,
    ) -> Self {
        self.meta.extend(meta);
        self
    }

    pub fn tag<T: 'static + ?Sized>(mut self) -> Self {
        self.tags.add::<T>();
        self
    }
}

impl From<()> for JobOptions {
    fn from(_: ()) -> Self {
        Default::default()
    }
}

impl From<JobLocation> for JobOptions {
    fn from(location: JobLocation) -> Self {
        Self {
            location,
            ..Default::default()
        }
    }
}

impl From<JobPriority> for JobOptions {
    fn from(priority: JobPriority) -> Self {
        Self {
            priority,
            ..Default::default()
        }
    }
}

impl From<(JobLocation, JobPriority)> for JobOptions {
    fn from((location, priority): (JobLocation, JobPriority)) -> Self {
        Self {
            location,
            priority,
            ..Default::default()
        }
    }
}

pub(crate) struct Job(pub(crate) Pin<Box<dyn Future<Output = ()> + Send + Sync>>);

impl Job {
    pub(crate) fn poll(
        mut self,
        cx: &mut Context<'_>,
        catch_panics: bool,
        #[cfg(debug_assertions)] creation_backtrace: &str,
    ) -> Option<Self> {
        if catch_panics {
            let poll_result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    match self.0.as_mut().poll(cx) {
                        Poll::Ready(_) => None,
                        Poll::Pending => Some(self),
                    }
                }));
            match poll_result {
                Ok(result) => result,
                Err(_err) => {
                    #[cfg(debug_assertions)]
                    {
                        tracing::error!(
                            "Job panicked! Creation backtrace:\n{}",
                            creation_backtrace
                        );
                        if let Some(s) = _err.downcast_ref::<&str>() {
                            tracing::error!("Panic info: {}", s);
                        } else if let Some(s) = _err.downcast_ref::<String>() {
                            tracing::error!("Panic info: {}", s);
                        } else if let Some(e) = _err.downcast_ref::<Box<dyn std::error::Error>>() {
                            tracing::error!("Panic error: {}", e);
                        }
                    }
                    None
                }
            }
        } else {
            match self.0.as_mut().poll(cx) {
                Poll::Ready(_) => None,
                Poll::Pending => Some(self),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobResult<T: Send + 'static> {
    InProgress,
    Completed(T),
    Cancelled,
    Consumed,
}

#[allow(clippy::derivable_impls)]
impl<T: Send + 'static> Default for JobResult<T> {
    fn default() -> Self {
        JobResult::InProgress
    }
}

impl<T: Send + 'static> JobResult<T> {
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    pub fn is_consumed(&self) -> bool {
        matches!(self, Self::Consumed)
    }

    pub fn in_progress(&self) -> bool {
        matches!(self, Self::InProgress)
    }

    pub fn is_done(&self) -> bool {
        !self.in_progress()
    }

    pub fn take(&mut self) -> Self {
        match self {
            Self::Completed(_) | Self::Cancelled | Self::Consumed => {
                std::mem::replace(self, Self::Consumed)
            }
            Self::InProgress => Self::InProgress,
        }
    }

    pub fn into_result(self) -> Option<T> {
        match self {
            Self::Completed(value) => Some(value),
            Self::Cancelled | Self::Consumed | Self::InProgress => None,
        }
    }
}

pub struct JobHandle<T: Send + 'static> {
    result: Arc<Mutex<JobResult<T>>>,
    pub(crate) cancel: Arc<AtomicBool>,
    pub(crate) suspend: Arc<AtomicBool>,
    pub(crate) meta: JobsMeta,
    cancel_on_drop: bool,
}

impl<T: Send + 'static> Drop for JobHandle<T> {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            self.cancel();
        }
    }
}

impl<T: Send + 'static> Default for JobHandle<T> {
    fn default() -> Self {
        Self {
            result: Default::default(),
            cancel: Default::default(),
            suspend: Default::default(),
            meta: Default::default(),
            cancel_on_drop: false,
        }
    }
}

impl<T: Send + 'static> JobHandle<T> {
    pub fn new(value: T) -> Self {
        Self {
            result: Arc::new(Mutex::new(JobResult::Completed(value))),
            cancel: Default::default(),
            suspend: Default::default(),
            meta: Default::default(),
            cancel_on_drop: false,
        }
    }

    pub fn cancel_on_drop(mut self) -> Self {
        self.cancel_on_drop = true;
        self
    }

    pub(crate) fn with_meta(
        self,
        iter: impl IntoIterator<Item = (Cow<'static, str>, JobsMetaValue)>,
    ) -> Self {
        self.meta.set_many(iter);
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
            .map(|guard| guard.is_done())
            .unwrap_or_default()
    }

    pub fn take(&self) -> JobResult<T> {
        self.result
            .try_lock()
            .ok()
            .map(|mut result| result.take())
            .unwrap_or_default()
    }

    pub fn take_result(&self) -> Option<T> {
        self.take().into_result()
    }

    pub fn wait(self) -> Option<T> {
        loop {
            match self.take() {
                JobResult::InProgress => {
                    traced_spin_loop();
                }
                result => return result.into_result(),
            }
        }
    }

    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
        if let Ok(mut result) = self.result.lock() {
            *result = JobResult::Cancelled;
        }
        self.resume();
    }

    pub fn suspend(&self) {
        self.suspend.store(true, Ordering::Relaxed);
    }

    pub fn resume(&self) {
        self.suspend.store(false, Ordering::Relaxed);
    }

    pub(crate) fn put(&self, value: T) {
        if let Ok(mut result) = self.result.lock() {
            *result = JobResult::Completed(value);
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
            cancel_on_drop: self.cancel_on_drop,
        }
    }
}

impl<T: Send + 'static> Future for JobHandle<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.take() {
            JobResult::InProgress => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            result => {
                cx.waker().wake_by_ref();
                Poll::Ready(result.into_result())
            }
        }
    }
}

pub struct AllJobsHandle<T: Send + 'static> {
    pub(crate) jobs: Vec<JobHandle<T>>,
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

    pub fn iter(&self) -> impl Iterator<Item = &JobHandle<T>> {
        self.jobs.iter()
    }

    pub fn extend(&mut self, handles: impl IntoIterator<Item = JobHandle<T>>) {
        self.jobs.extend(handles);
    }

    pub fn is_done(&self) -> bool {
        self.jobs.is_empty() || self.jobs.iter().all(|job| job.is_done())
    }

    pub fn take(&self) -> Vec<JobResult<T>> {
        if self.is_done() {
            self.jobs.iter().map(|job| job.take()).collect()
        } else {
            Default::default()
        }
    }

    pub fn take_result(&self) -> Option<Vec<T>> {
        if self.is_done() {
            self.jobs
                .iter()
                .map(|job| job.take().into_result())
                .collect()
        } else {
            Default::default()
        }
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
        if self.is_done() {
            cx.waker().wake_by_ref();
            Poll::Ready(self.take_result())
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

    pub fn take(&self) -> JobResult<T> {
        if self.jobs.is_empty() {
            return JobResult::Cancelled;
        }
        for job in &self.jobs {
            if job.is_done() {
                return job.take();
            }
        }
        JobResult::InProgress
    }

    pub fn take_result(&self) -> Option<T> {
        self.take().into_result()
    }

    pub fn wait(self) -> Option<T> {
        self.jobs.into_iter().find_map(|job| job.wait())
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
        if self.is_done() {
            cx.waker().wake_by_ref();
            Poll::Ready(self.take_result())
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

impl Default for JobContext {
    fn default() -> Self {
        Self {
            work_group_index: 0,
            work_groups_count: 1,
        }
    }
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

#[derive(Debug, Default, Clone, PartialEq)]
pub enum JobLocation {
    #[default]
    Unknown,
    Local,
    NonLocal,
    UnnamedWorker,
    NamedWorker(String),
    ExactThread(ThreadId),
    OtherThanThread(ThreadId),
    Exclusive,
    Queue(JobQueue),
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
            JobLocation::Exclusive => write!(f, "Exclusive"),
            JobLocation::Queue(_) => write!(f, "Job queue"),
        }
    }
}

#[derive(Default, Clone)]
pub struct JobTokens {
    pub(crate) hash_tokens: Arc<Mutex<HashSet<u64>>>,
}

impl JobTokens {
    pub fn acquire_token<T: Hash>(&self, subject: &T) -> Option<JobToken> {
        let mut hasher = DefaultHasher::new();
        subject.hash(&mut hasher);
        let hash = hasher.finish();
        let Ok(mut hash_tokens) = self.hash_tokens.lock() else {
            return None;
        };
        if hash_tokens.contains(&hash) {
            None
        } else {
            hash_tokens.insert(hash);
            Some(JobToken {
                hash_tokens: self.clone(),
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
            let Ok(mut hash_tokens) = self.hash_tokens.try_lock() else {
                traced_spin_loop();
                continue;
            };
            if hash_tokens.contains(&hash) {
                traced_spin_loop();
                continue;
            } else {
                hash_tokens.insert(hash);
                return Some(JobToken {
                    hash_tokens: self.clone(),
                    hash,
                });
            }
        }
        None
    }
}

#[derive(Default)]
pub struct JobToken {
    pub(crate) hash_tokens: JobTokens,
    pub(crate) hash: u64,
}

impl Drop for JobToken {
    fn drop(&mut self) {
        if let Ok(mut hash_tokens) = self.hash_tokens.hash_tokens.lock() {
            hash_tokens.remove(&self.hash);
        }
    }
}

pub struct JobObject {
    pub(crate) id: ID<Jobs>,
    pub(crate) job: Job,
    pub(crate) context: JobContext,
    pub(crate) location: JobLocation,
    pub(crate) priority: JobPriority,
    pub(crate) cancel: Arc<AtomicBool>,
    pub(crate) suspend: Arc<AtomicBool>,
    pub(crate) meta: JobsMeta,
    pub(crate) tags: JobsTags,
    #[cfg(debug_assertions)]
    pub(crate) creation_backtrace: String,
    #[cfg(feature = "tracing")]
    pub(crate) tracing_span: Option<tracing::Span>,
}

impl JobObject {
    pub fn id(&self) -> ID<Jobs> {
        self.id
    }

    pub fn context(&self) -> &JobContext {
        &self.context
    }

    pub fn location(&self) -> &JobLocation {
        &self.location
    }

    pub fn priority(&self) -> JobPriority {
        self.priority
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }

    pub fn is_suspended(&self) -> bool {
        self.suspend.load(Ordering::Relaxed)
    }

    pub fn meta(&self) -> &JobsMeta {
        &self.meta
    }

    pub fn tags(&self) -> &JobsTags {
        &self.tags
    }

    #[cfg(debug_assertions)]
    pub fn creation_backtrace(&self) -> &str {
        &self.creation_backtrace
    }

    #[cfg(feature = "tracing")]
    pub fn tracing_span(&self) -> Option<&tracing::Span> {
        self.tracing_span.as_ref()
    }
}
