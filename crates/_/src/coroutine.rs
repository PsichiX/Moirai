use crate::{
    job::{Job, JobContext, JobHandle, JobLocation, JobObject, JobOptions, JobPriority, JobToken},
    jobs::{JobsMeta, JobsRuntime, JobsTags, JobsWaker, JobsWakerCommand},
    queue::JobQueue,
    third_party::time::{Duration, Instant},
};
use intuicio_data::{
    lifetime::LifetimeWeakState,
    managed::{DynamicManagedLazy, ManagedLazy},
};
use std::{
    future::poll_fn,
    hash::Hash,
    pin::{Pin, pin},
    sync::{Arc, Mutex, RwLock, atomic::Ordering, mpsc::Receiver},
    task::Poll,
};
use typid::ID;

#[derive(Default)]
pub struct OnExit {
    job: Option<JobObject>,
    queue: JobQueue,
    closure: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl Drop for OnExit {
    fn drop(&mut self) {
        if let Some(object) = self.job.take() {
            self.queue.enqueue(object);
        }
        if let Some(closure) = self.closure.take() {
            closure();
        }
    }
}

impl OnExit {
    pub fn invalidate(mut self) {
        self.job = None;
        self.closure = None;
    }
}

/// IMPORTANT: You must assign the result of this function to a named variable,
/// otherwise the future will be executed immediately!
#[must_use]
pub async fn on_exit(future: impl Future<Output = ()> + Send + Sync + 'static) -> OnExit {
    let mut job = Some(Job(Box::pin(future)));
    #[cfg(debug_assertions)]
    let creation_backtrace = std::backtrace::Backtrace::capture().to_string();
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            if let Some(job) = job.take() {
                OnExit {
                    job: Some(JobObject {
                        id: ID::new(),
                        job,
                        context: Default::default(),
                        location: JobLocation::current_thread(),
                        priority: JobPriority::High,
                        cancel: waker.runtime.cancel.clone(),
                        suspend: waker.runtime.suspend.clone(),
                        meta: waker.runtime.local_meta.clone(),
                        tags: JobsTags::default(),
                        #[cfg(debug_assertions)]
                        creation_backtrace: creation_backtrace.clone(),
                        #[cfg(feature = "tracing")]
                        tracing_span: None,
                    }),
                    queue: waker.runtime.queue.clone(),
                    closure: None,
                }
            } else {
                Default::default()
            }
        } else {
            Default::default()
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

/// IMPORTANT: You must assign the result of this function to a named variable,
/// otherwise the future will be executed immediately!
#[must_use]
pub async fn on_exit_closure(f: impl FnOnce() + Send + Sync + 'static) -> OnExit {
    let mut f = Some(f);
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            if let Some(closure) = f.take() {
                OnExit {
                    job: None,
                    queue: waker.runtime.queue.clone(),
                    closure: Some(Box::new(closure)),
                }
            } else {
                Default::default()
            }
        } else {
            Default::default()
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

pub async fn yield_now() {
    wait_polls(1).await
}

pub async fn with_all<T>(
    mut futures: Vec<Pin<Box<dyn Future<Output = T> + Send + Sync>>>,
) -> Vec<T> {
    let mut results = Vec::with_capacity(futures.len());
    let count = futures.len();
    poll_fn(move |cx| {
        futures.retain_mut(|future| match future.as_mut().poll(cx) {
            Poll::Ready(output) => {
                results.push(output);
                false
            }
            Poll::Pending => true,
        });
        if results.len() == count {
            cx.waker().wake_by_ref();
            Poll::Ready(std::mem::take(&mut results))
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn with_any<T>(
    mut futures: Vec<Pin<Box<dyn Future<Output = T> + Send + Sync>>>,
) -> Option<T> {
    poll_fn(move |cx| {
        for future in &mut futures {
            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                cx.waker().wake_by_ref();
                return Poll::Ready(Some(output));
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

pub enum CompletionImportance<T> {
    Required(Pin<Box<dyn Future<Output = T> + Send + Sync>>),
    Ignored(Pin<Box<dyn Future<Output = T> + Send + Sync>>),
}

impl<T> CompletionImportance<T> {
    pub fn required(future: impl Future<Output = T> + Send + Sync + 'static) -> Self {
        CompletionImportance::Required(Box::pin(future))
    }

    pub fn ignored(future: impl Future<Output = T> + Send + Sync + 'static) -> Self {
        CompletionImportance::Ignored(Box::pin(future))
    }
}

#[allow(clippy::type_complexity)]
pub async fn with_importance<T>(mut futures: Vec<CompletionImportance<T>>) -> Vec<T> {
    let mut results = Vec::with_capacity(futures.len());
    let count = futures
        .iter()
        .filter(|f| matches!(f, CompletionImportance::Required(_)))
        .count();
    poll_fn(move |cx| {
        futures.retain_mut(|future| match future {
            CompletionImportance::Required(future) => match future.as_mut().poll(cx) {
                Poll::Ready(output) => {
                    results.push(output);
                    false
                }
                Poll::Pending => true,
            },
            CompletionImportance::Ignored(future) => future.as_mut().poll(cx).is_pending(),
        });
        if results.len() == count {
            cx.waker().wake_by_ref();
            Poll::Ready(std::mem::take(&mut results))
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn location() -> JobLocation {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.runtime.location.clone()
        } else {
            JobLocation::Unknown
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

pub async fn context() -> JobContext {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.runtime.context
        } else {
            Default::default()
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

pub async fn runtime() -> JobsRuntime {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.runtime.clone()
        } else {
            Default::default()
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

pub async fn priority() -> JobPriority {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.runtime.priority
        } else {
            Default::default()
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

pub async fn suspend() {
    let mut executed = false;
    poll_fn(move |cx| {
        let waker = cx.waker();
        if executed {
            waker.wake_by_ref();
            Poll::Ready(())
        } else {
            if let Some(waker) = JobsWaker::try_cast(waker) {
                waker.runtime.suspend.store(true, Ordering::Relaxed);
            }
            executed = true;
            waker.wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn acquire_token<T: Hash>(subject: &T) -> JobToken {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.runtime.hash_tokens.acquire_token(subject)
        } else {
            Some(JobToken::default())
        };
        waker.wake_by_ref();
        match result {
            Some(token) => Poll::Ready(token),
            None => Poll::Pending,
        }
    })
    .await
}

pub async fn acquire_token_timeout<T: Hash>(subject: &T, timeout: Duration) -> JobToken {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker
                .runtime
                .hash_tokens
                .acquire_token_timeout(subject, timeout)
        } else {
            Some(JobToken::default())
        };
        waker.wake_by_ref();
        match result {
            Some(token) => Poll::Ready(token),
            None => Poll::Pending,
        }
    })
    .await
}

pub async fn meta_store_local<T>(name: impl ToString, lazy: ManagedLazy<T>) {
    let mut name = Some(name.to_string());
    let mut lazy = Some(lazy.into_dynamic());
    poll_fn(move |cx| {
        let waker = cx.waker();
        if let Some(waker) = JobsWaker::try_cast(waker) {
            waker
                .runtime
                .local_meta
                .set(name.take().unwrap(), lazy.take().unwrap());
        }
        waker.wake_by_ref();
        Poll::Ready(())
    })
    .await
}

pub async fn meta<T>(name: &str) -> Option<ManagedLazy<T>> {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker
                .runtime
                .get_meta(name)
                .and_then(|lazy| lazy.into_typed::<T>().ok())
        } else {
            None
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

pub async fn meta_dynamic(name: &str) -> Option<DynamicManagedLazy> {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.runtime.get_meta(name)
        } else {
            None
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

pub async fn tags() -> JobsTags {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.runtime.tags.clone()
        } else {
            Default::default()
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

pub async fn move_to(location: JobLocation) {
    let mut executed = false;
    poll_fn(move |cx| {
        let waker = cx.waker();
        if executed {
            waker.wake_by_ref();
            Poll::Ready(())
        } else {
            if let Some(waker) = JobsWaker::try_cast(waker) {
                waker.command(JobsWakerCommand::MoveTo(location.clone()));
            }
            executed = true;
            waker.wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn change_priority(priority: JobPriority) {
    let mut executed = false;
    poll_fn(move |cx| {
        let waker = cx.waker();
        if executed {
            waker.wake_by_ref();
            Poll::Ready(())
        } else {
            if let Some(waker) = JobsWaker::try_cast(waker) {
                waker.command(JobsWakerCommand::ChangePriority(priority));
            }
            executed = true;
            waker.wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn spawn<T: Send + 'static>(
    options: impl Into<JobOptions>,
    job: impl Future<Output = T> + Send + Sync + 'static,
) -> JobHandle<T> {
    let options = options.into();
    let handle = JobHandle::default().with_meta(options.meta);
    let handle2 = handle.clone();
    let result = handle.clone();
    let mut job = Some(Job(Box::pin(async move {
        handle.put(job.await);
    })));
    #[cfg(debug_assertions)]
    let creation_backtrace = std::backtrace::Backtrace::capture().to_string();
    poll_fn(move |cx| {
        let waker = cx.waker();
        if let Some(job) = job.take() {
            if let Some(waker) = JobsWaker::try_cast(waker) {
                waker.runtime.queue.enqueue(JobObject {
                    id: ID::new(),
                    job,
                    context: Default::default(),
                    location: options.location.clone(),
                    priority: options.priority,
                    cancel: handle2.cancel.clone(),
                    suspend: handle2.suspend.clone(),
                    meta: waker.runtime.local_meta.clone(),
                    tags: options.tags.clone(),
                    #[cfg(debug_assertions)]
                    creation_backtrace: creation_backtrace.clone(),
                    #[cfg(feature = "tracing")]
                    tracing_span: None,
                });
            }
            waker.wake_by_ref();
            Poll::Pending
        } else {
            waker.wake_by_ref();
            Poll::Ready(())
        }
    })
    .await;
    result
}

pub async fn spawn_closure<T: Send + 'static>(
    options: impl Into<JobOptions>,
    job: impl FnOnce(JobContext) -> T + Send + Sync + 'static,
) -> JobHandle<T> {
    let options = options.into();
    let handle = JobHandle::default().with_meta(options.meta);
    let handle2 = handle.clone();
    let result = handle.clone();
    let mut job = Some(Job(Box::pin(async move {
        handle.put(job(context().await));
    })));
    #[cfg(debug_assertions)]
    let creation_backtrace = std::backtrace::Backtrace::capture().to_string();
    poll_fn(move |cx| {
        let waker = cx.waker();
        if let Some(job) = job.take() {
            if let Some(waker) = JobsWaker::try_cast(waker) {
                waker.runtime.queue.enqueue(JobObject {
                    id: ID::new(),
                    job,
                    context: Default::default(),
                    location: options.location.clone(),
                    priority: options.priority,
                    cancel: handle2.cancel.clone(),
                    suspend: handle2.suspend.clone(),
                    meta: waker.runtime.local_meta.clone(),
                    tags: options.tags.clone(),
                    #[cfg(debug_assertions)]
                    creation_backtrace: creation_backtrace.clone(),
                    #[cfg(feature = "tracing")]
                    tracing_span: None,
                });
            }
            waker.wake_by_ref();
            Poll::Pending
        } else {
            waker.wake_by_ref();
            Poll::Ready(())
        }
    })
    .await;
    result
}

pub async fn queue() -> JobQueue {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.runtime.queue.clone()
        } else {
            JobQueue::default()
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

pub async fn run_queue(queue: &JobQueue) {
    poll_fn(|cx| {
        let waker = cx.waker();
        if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.runtime.run_queue(queue);
        }
        waker.wake_by_ref();
        Poll::Ready(())
    })
    .await
}

pub async fn run_queue_with_meta(queue: &JobQueue, worker_meta: JobsMeta) {
    let mut worker_meta = Some(worker_meta);
    poll_fn(move |cx| {
        let waker = cx.waker();
        if let Some(waker) = JobsWaker::try_cast(waker) {
            waker
                .runtime
                .run_queue_with_meta(queue, worker_meta.take().unwrap());
        }
        waker.wake_by_ref();
        Poll::Ready(())
    })
    .await
}

pub async fn run_queue_timeout(queue: &JobQueue, timeout: Duration) {
    poll_fn(|cx| {
        let waker = cx.waker();
        if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.runtime.run_queue_timeout(queue, timeout);
        }
        waker.wake_by_ref();
        Poll::Ready(())
    })
    .await
}

pub async fn run_queue_timeout_with_meta(
    queue: &JobQueue,
    timeout: Duration,
    worker_meta: JobsMeta,
) {
    let mut worker_meta = Some(worker_meta);
    poll_fn(move |cx| {
        let waker = cx.waker();
        if let Some(waker) = JobsWaker::try_cast(waker) {
            waker
                .runtime
                .run_queue_timeout_with_meta(queue, timeout, worker_meta.take().unwrap());
        }
        waker.wake_by_ref();
        Poll::Ready(())
    })
    .await
}

pub async fn cancellable<F: Future>(
    mut condition: impl FnMut() -> bool,
    future: F,
) -> Option<F::Output> {
    let mut future = pin!(future);
    poll_fn(move |cx| {
        if condition() {
            cx.waker().wake_by_ref();
            Poll::Ready(None)
        } else {
            match future.as_mut().poll(cx) {
                Poll::Ready(output) => {
                    cx.waker().wake_by_ref();
                    Poll::Ready(Some(output))
                }
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
    })
    .await
}

pub async fn lifetime_bound<F: Future>(
    lifetimes: impl IntoIterator<Item = LifetimeWeakState>,
    future: F,
) -> Option<F::Output> {
    let states = lifetimes.into_iter().collect::<Vec<_>>();
    cancellable(
        || states.iter().any(|state| state.upgrade().is_none()),
        future,
    )
    .await
}

pub async fn duration_bound<F: Future>(duration: Duration, future: F) -> Option<F::Output> {
    let start = Instant::now();
    cancellable(move || start.elapsed() >= duration, future).await
}

pub async fn wait_polls(mut count: usize) {
    poll_fn(move |cx| {
        if count == 0 {
            cx.waker().wake_by_ref();
            Poll::Ready(())
        } else {
            count -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn wait_time(duration: Duration) -> Duration {
    let timer = Instant::now();
    poll_fn(move |cx| {
        let elapsed = timer.elapsed();
        if elapsed >= duration {
            cx.waker().wake_by_ref();
            Poll::Ready(elapsed.saturating_sub(duration))
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn wait_for_mutex<T>(notify: Arc<Mutex<T>>, mut f: impl FnMut(&T) -> bool) {
    poll_fn(move |cx| {
        if let Ok(value) = notify.try_lock() {
            if f(&value) {
                cx.waker().wake_by_ref();
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        } else {
            cx.waker().wake_by_ref();
            Poll::Ready(())
        }
    })
    .await
}

pub async fn wait_for_rwlock<T>(notify: Arc<RwLock<T>>, mut f: impl FnMut(&T) -> bool) {
    poll_fn(move |cx| {
        if let Ok(notify) = notify.try_read() {
            if f(&notify) {
                cx.waker().wake_by_ref();
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        } else {
            cx.waker().wake_by_ref();
            Poll::Ready(())
        }
    })
    .await
}

pub async fn wait_for_receiver<T>(notify: Receiver<T>) -> T {
    poll_fn(move |cx| {
        if let Ok(value) = notify.try_recv() {
            cx.waker().wake_by_ref();
            Poll::Ready(value)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn wait_for_meta<T>(name: &str) -> ManagedLazy<T> {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = JobsWaker::try_cast(waker).and_then(|waker| {
            waker
                .runtime
                .get_meta(name)
                .and_then(|lazy| lazy.into_typed::<T>().ok())
        });
        if let Some(result) = result {
            cx.waker().wake_by_ref();
            Poll::Ready(result)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn wait_for_meta_dynamic(name: &str) -> DynamicManagedLazy {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = JobsWaker::try_cast(waker).and_then(|waker| waker.runtime.get_meta(name));
        if let Some(result) = result {
            cx.waker().wake_by_ref();
            Poll::Ready(result)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn wait_for(condition: impl Fn() -> bool) {
    poll_fn(move |cx| {
        if condition() {
            cx.waker().wake_by_ref();
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

#[derive(Debug, Default, Clone)]
pub struct StrategyDecision {
    pub change_location: Option<JobLocation>,
    pub change_priority: Option<JobPriority>,
    pub cancel: bool,
}

impl StrategyDecision {
    pub fn location(mut self, location: JobLocation) -> Self {
        self.change_location = Some(location);
        self
    }

    pub fn priority(mut self, priority: JobPriority) -> Self {
        self.change_priority = Some(priority);
        self
    }

    pub fn cancel(mut self) -> Self {
        self.cancel = true;
        self
    }
}

pub async fn strategy<F: Future>(
    mut strategy: impl FnMut() -> StrategyDecision,
    future: F,
) -> Option<F::Output> {
    let mut future = pin!(future);
    poll_fn(move |cx| match future.as_mut().poll(cx) {
        Poll::Ready(output) => {
            cx.waker().wake_by_ref();
            Poll::Ready(Some(output))
        }
        Poll::Pending => {
            let waker = cx.waker();
            let StrategyDecision {
                change_location,
                change_priority,
                cancel,
            } = strategy();
            if cancel {
                cx.waker().wake_by_ref();
                return Poll::Ready(None);
            }
            if (change_location.is_some() || change_priority.is_some())
                && let Some(waker) = JobsWaker::try_cast(waker)
            {
                if let Some(location) = change_location {
                    waker.command(JobsWakerCommand::MoveTo(location.clone()));
                }
                if let Some(priority) = change_priority {
                    waker.command(JobsWakerCommand::ChangePriority(priority));
                }
            }
            waker.wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

pub async fn strategy_timeline<F: Future>(
    timeline: impl IntoIterator<Item = (Duration, StrategyDecision)>,
    future: F,
) -> Option<F::Output> {
    let mut timer = Instant::now();
    let mut timeline = timeline.into_iter();
    let mut next = timeline.next();
    strategy(
        move || {
            if let Some((timeout, decision)) = next.clone() {
                if timer.elapsed() >= timeout {
                    next = timeline.next();
                    timer = Instant::now();
                    decision
                } else {
                    Default::default()
                }
            } else {
                Default::default()
            }
        },
        future,
    )
    .await
}
