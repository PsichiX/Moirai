use crate::jobs::{
    Job, JobContext, JobHandle, JobLocation, JobObject, JobOptions, JobPriority, JobQueue,
    JobToken, JobsWaker, JobsWakerCommand,
};
#[cfg(target_arch = "wasm32")]
use instant::{Duration, Instant};
use intuicio_data::{
    lifetime::LifetimeWeakState,
    managed::{DynamicManagedLazy, ManagedLazy},
};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{Duration, Instant};
use std::{
    future::poll_fn,
    hash::Hash,
    pin::Pin,
    sync::{Arc, Mutex, RwLock, atomic::Ordering, mpsc::Receiver},
    task::Poll,
};
use typid::ID;

#[derive(Default)]
pub struct OnExit {
    job: Option<JobObject>,
    queue: JobQueue,
}

impl Drop for OnExit {
    fn drop(&mut self) {
        if let Some(object) = self.job.take() {
            self.queue.enqueue(object);
        }
    }
}

impl OnExit {
    pub fn invalidate(mut self) {
        self.job = None;
    }
}

pub async fn yield_now() {
    wait_polls(1).await
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
            Poll::Ready(elapsed - duration)
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

pub async fn location() -> JobLocation {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker.location()
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
            waker.context()
        } else {
            JobContext {
                work_group_index: 0,
                work_groups_count: 1,
            }
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
            waker.priority()
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
                waker.suspend.store(true, Ordering::Relaxed);
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
            waker.acquire_token(subject)
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
            waker.acquire_token_timeout(subject, timeout)
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

pub async fn meta<T>(name: &str) -> Option<ManagedLazy<T>> {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = if let Some(waker) = JobsWaker::try_cast(waker) {
            waker
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
            waker.get_meta(name)
        } else {
            None
        };
        waker.wake_by_ref();
        Poll::Ready(result)
    })
    .await
}

pub async fn wait_for_meta<T>(name: &str) -> ManagedLazy<T> {
    poll_fn(move |cx| {
        let waker = cx.waker();
        let result = JobsWaker::try_cast(waker).and_then(|waker| {
            waker
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
        let result = JobsWaker::try_cast(waker).and_then(|waker| waker.get_meta(name));
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
                waker.enqueue(JobObject {
                    id: ID::new(),
                    job,
                    context: JobContext {
                        work_group_index: 0,
                        work_groups_count: 1,
                    },
                    location: options.location.clone(),
                    priority: options.priority,
                    cancel: handle2.cancel.clone(),
                    suspend: handle2.suspend.clone(),
                    meta: waker.local_meta(),
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
                waker.enqueue(JobObject {
                    id: ID::new(),
                    job,
                    context: JobContext {
                        work_group_index: 0,
                        work_groups_count: 1,
                    },
                    location: options.location.clone(),
                    priority: options.priority,
                    cancel: handle2.cancel.clone(),
                    suspend: handle2.suspend.clone(),
                    meta: waker.local_meta(),
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
                        context: JobContext {
                            work_group_index: 0,
                            work_groups_count: 1,
                        },
                        location: JobLocation::current_thread(),
                        priority: JobPriority::High,
                        cancel: waker.cancel(),
                        suspend: waker.suspend(),
                        meta: waker.local_meta(),
                        #[cfg(debug_assertions)]
                        creation_backtrace: creation_backtrace.clone(),
                        #[cfg(feature = "tracing")]
                        tracing_span: None,
                    }),
                    queue: waker.queue(),
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

pub async fn cancellable<F: Future>(condition: impl Fn() -> bool, future: F) -> Option<F::Output> {
    let mut future = Box::pin(future);
    poll_fn(move |cx| {
        if condition() {
            cx.waker().wake_by_ref();
            Poll::Ready(None)
        } else {
            cx.waker().wake_by_ref();
            future.as_mut().poll(cx).map(Some)
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
