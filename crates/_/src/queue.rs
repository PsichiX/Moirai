use crate::{
    coroutine::context,
    job::{Job, JobContext, JobHandle, JobLocation, JobObject, JobOptions, JobPriority, JobTokens},
    jobs::{JobsMeta, JobsRuntime, JobsTags, JobsWakerCommand, Worker, WorkerNotifier},
    third_party::time::{Duration, Instant},
};
use std::{
    collections::LinkedList,
    pin::Pin,
    sync::{
        Arc, LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use typid::ID;

#[derive(Default, Clone)]
pub struct JobQueue {
    queue: Arc<RwLock<LinkedList<JobObject>>>,
    catch_panics: Arc<AtomicBool>,
}

impl JobQueue {
    pub fn catch_panics(self) -> Self {
        self.set_catch_panics(true);
        self
    }

    pub fn does_catch_panics(&self) -> bool {
        self.catch_panics.load(Ordering::Relaxed)
    }

    pub fn set_catch_panics(&self, catch: bool) {
        self.catch_panics.store(catch, Ordering::Relaxed);
    }

    pub fn take_job_objects(&self) -> LinkedList<JobObject> {
        if let Ok(mut queue) = self.inner_mut() {
            std::mem::take(&mut *queue)
        } else {
            LinkedList::new()
        }
    }

    pub fn extract_if(&self, filter: impl Fn(&JobObject) -> bool) -> Vec<JobObject> {
        if let Ok(mut queue) = self.inner_mut() {
            queue.extract_if(|object| filter(object)).collect()
        } else {
            Default::default()
        }
    }

    pub fn take(&self) -> LinkedList<JobObject> {
        if let Ok(mut queue) = self.inner_mut() {
            std::mem::take(&mut *queue)
        } else {
            Default::default()
        }
    }

    pub fn extend(&self, queue: impl IntoIterator<Item = JobObject>) {
        if let Ok(mut current_queue) = self.inner_mut() {
            for object in queue {
                if object.priority == JobPriority::High {
                    current_queue.push_front(object);
                } else {
                    current_queue.push_back(object);
                }
            }
        }
    }

    pub fn inner<'a>(&'a self) -> LockResult<RwLockReadGuard<'a, LinkedList<JobObject>>> {
        self.queue.read()
    }

    pub fn inner_mut<'a>(&'a self) -> LockResult<RwLockWriteGuard<'a, LinkedList<JobObject>>> {
        self.queue.write()
    }

    pub fn is_empty(&self) -> bool {
        self.inner().map_or(true, |queue| queue.is_empty())
    }

    pub fn len(&self) -> usize {
        self.inner().map_or(0, |queue| queue.len())
    }

    pub fn filter_count(&self, f: impl Fn(&JobObject) -> bool) -> usize {
        self.inner()
            .map(|queue| queue.iter().filter(|object| f(object)).count())
            .unwrap_or_default()
    }

    pub fn clear(&self) {
        if let Ok(mut queue) = self.inner_mut() {
            queue.clear();
        }
    }

    pub fn append(&self, other: &Self) {
        if let Ok(mut other_queue) = other.inner_mut() {
            self.extend(std::mem::take(&mut *other_queue));
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        &self,
        worker_location: JobLocation,
        ignore_location: bool,
        timeout: Duration,
        global_meta: JobsMeta,
        worker_meta: JobsMeta,
        hash_tokens: JobTokens,
        notifier: WorkerNotifier,
    ) {
        let timer = Instant::now();
        let mut pending = vec![];
        while let Some(object) = self.dequeue(&worker_location, ignore_location) {
            if let Some(object) = self.poll_once(
                object,
                global_meta.clone(),
                worker_meta.clone(),
                hash_tokens.clone(),
                notifier.clone(),
                true,
            ) {
                pending.push(object);
            }
            if timer.elapsed() >= timeout {
                break;
            }
        }
        self.extend(pending);
    }

    pub fn poll_once(
        &self,
        object: JobObject,
        global_meta: JobsMeta,
        worker_meta: JobsMeta,
        hash_tokens: JobTokens,
        notifier: WorkerNotifier,
        use_exclusive_worker: bool,
    ) -> Option<JobObject> {
        if use_exclusive_worker && object.location == JobLocation::Exclusive {
            Worker::exclusive(
                object,
                self.clone(),
                global_meta.clone(),
                Default::default(),
                hash_tokens.clone(),
                notifier.clone(),
            );
            return None;
        }
        let JobObject {
            id,
            job,
            context,
            location,
            mut priority,
            cancel,
            suspend,
            meta,
            tags,
            #[cfg(debug_assertions)]
            creation_backtrace,
            #[cfg(feature = "tracing")]
            mut tracing_span,
        } = object;
        let (poll_result, receiver) = if cancel.load(Ordering::Relaxed) {
            let (_, rx) = std::sync::mpsc::channel();
            (None, rx)
        } else if suspend.load(Ordering::Relaxed) {
            let (_, rx) = std::sync::mpsc::channel();
            (Some(job), rx)
        } else {
            let (waker, receiver) = JobsRuntime {
                queue: self.clone(),
                location: location.clone(),
                context,
                priority,
                global_meta: global_meta.clone(),
                worker_meta: worker_meta.clone(),
                local_meta: meta.clone(),
                tags: tags.clone(),
                hash_tokens: hash_tokens.clone(),
                notify: notifier.clone(),
                cancel: cancel.clone(),
                suspend: suspend.clone(),
            }
            .into_waker();
            let mut cx = Context::from_waker(&waker);
            #[cfg(feature = "tracing")]
            let _span = {
                let span = tracing::span!(
                    tracing::Level::TRACE,
                    "Job poll",
                    id = id.to_string(),
                    location = location.to_string(),
                    context = context.to_string(),
                    priority = priority.to_string(),
                    thread_id = format!("{:?}", std::thread::current().id()),
                );
                if let Some(last) = tracing_span.take() {
                    span.follows_from(last);
                }
                span.entered()
            };
            let poll_result = job.poll(
                &mut cx,
                self.does_catch_panics(),
                #[cfg(debug_assertions)]
                &creation_backtrace,
            );
            (poll_result, receiver)
        };
        let mut notify_workers = false;
        let result = if let Some(job) = poll_result {
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
            match move_to {
                Some(JobLocation::Queue(q)) => {
                    q.enqueue(JobObject {
                        id,
                        job,
                        context,
                        location,
                        priority,
                        cancel,
                        suspend,
                        meta,
                        tags,
                        #[cfg(debug_assertions)]
                        creation_backtrace,
                        #[cfg(feature = "tracing")]
                        tracing_span,
                    });
                    None
                }
                Some(location) => {
                    self.enqueue(JobObject {
                        id,
                        job,
                        context,
                        location,
                        priority,
                        cancel,
                        suspend,
                        meta,
                        tags,
                        #[cfg(debug_assertions)]
                        creation_backtrace,
                        #[cfg(feature = "tracing")]
                        tracing_span,
                    });
                    None
                }
                None => Some(JobObject {
                    id,
                    job,
                    context,
                    location,
                    priority,
                    cancel,
                    suspend,
                    meta,
                    tags,
                    #[cfg(debug_assertions)]
                    creation_backtrace,
                    #[cfg(feature = "tracing")]
                    tracing_span,
                }),
            }
        } else {
            None
        };
        if notify_workers {
            notifier.notify();
        }
        result
    }

    pub fn spawn<T: Send + 'static>(
        &self,
        options: impl Into<JobOptions>,
        job: impl Future<Output = T> + Send + Sync + 'static,
    ) -> JobHandle<T> {
        let options = options.into();
        let handle = JobHandle::<T>::default().with_meta(options.meta);
        let handle2 = handle.clone();
        let job = Job(Box::pin(async move {
            handle2.put(job.await);
        }));
        self.schedule(
            options.location,
            options.priority,
            options.tags,
            handle,
            job,
        )
    }

    pub fn spawn_closure<T: Send + 'static>(
        &self,
        options: impl Into<JobOptions>,
        job: impl FnOnce(JobContext) -> T + Send + Sync + 'static,
    ) -> JobHandle<T> {
        let options = options.into();
        let handle = JobHandle::<T>::default().with_meta(options.meta);
        let handle2 = handle.clone();
        let job = Job(Box::pin(async move {
            handle2.put(job(context().await));
        }));
        self.schedule(
            options.location,
            options.priority,
            options.tags,
            handle,
            job,
        )
    }

    fn schedule<T: Send + 'static>(
        &self,
        location: JobLocation,
        priority: JobPriority,
        tags: JobsTags,
        handle: JobHandle<T>,
        job: Job,
    ) -> JobHandle<T> {
        #[cfg(debug_assertions)]
        let creation_backtrace = std::backtrace::Backtrace::capture().to_string();
        self.enqueue(JobObject {
            id: ID::new(),
            job,
            context: Default::default(),
            location,
            priority,
            cancel: handle.cancel.clone(),
            suspend: handle.suspend.clone(),
            meta: handle.meta.clone(),
            tags,
            #[cfg(debug_assertions)]
            creation_backtrace,
            #[cfg(feature = "tracing")]
            tracing_span: None,
        });
        handle
    }

    pub fn enqueue(&self, object: JobObject) {
        if let Ok(mut queue) = self.inner_mut() {
            if object.priority == JobPriority::High {
                queue.push_front(object);
            } else {
                queue.push_back(object);
            }
        }
    }

    pub fn dequeue(
        &self,
        target_location: &JobLocation,
        ignore_location: bool,
    ) -> Option<JobObject> {
        let mut queue = self.inner_mut().ok()?;
        let mut extract = queue.extract_if(|object| {
            if ignore_location || object.location == JobLocation::Exclusive {
                true
            } else {
                match (&object.location, target_location) {
                    (JobLocation::Local, JobLocation::Local)
                    | (JobLocation::UnnamedWorker, JobLocation::UnnamedWorker) => true,
                    (JobLocation::NamedWorker(a), JobLocation::NamedWorker(b)) if a == b => true,
                    (JobLocation::ExactThread(a), _) => *a == std::thread::current().id(),
                    (JobLocation::OtherThanThread(a), _) => *a != std::thread::current().id(),
                    (JobLocation::NonLocal, b) => b != &JobLocation::Local,
                    (JobLocation::Queue(q), _) => q == self,
                    (JobLocation::Unknown, _) => true,
                    _ => false,
                }
            }
        });
        extract.next()
    }
}

impl Future for JobQueue {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.is_empty() {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            cx.waker().wake_by_ref();
            Poll::Ready(())
        }
    }
}

impl PartialEq for JobQueue {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.queue, &other.queue)
    }
}

impl std::fmt::Debug for JobQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobQueue")
            .field("len", &self.len())
            .finish()
    }
}
