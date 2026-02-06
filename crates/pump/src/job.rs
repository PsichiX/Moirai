use moirai::{
    job::{JobHandle, JobLocation},
    queue::JobQueue,
};
use std::{future::poll_fn, task::Poll, time::Duration};

pub struct JobPump<T: Send + 'static> {
    queue: JobQueue,
    job: JobHandle<T>,
}

impl<T: Send + 'static> JobPump<T> {
    pub fn new(job: impl Future<Output = T> + Send + Sync + 'static) -> Self {
        let queue = JobQueue::default();
        let job = queue.spawn(JobLocation::Local, job);
        Self { queue, job }
    }

    pub fn pump(self) -> Result<T, Self> {
        self.queue.run(
            JobLocation::Local,
            true,
            Duration::MAX,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        );
        match self.job.take_result() {
            Some(val) => Ok(val),
            None => Err(self),
        }
    }

    pub fn pump_all(mut self) -> T {
        loop {
            match self.pump() {
                Ok(val) => return val,
                Err(pump) => self = pump,
            }
        }
    }

    pub async fn into_future(self) -> T {
        poll_fn(|ctx| {
            self.queue.run(
                JobLocation::Local,
                true,
                Duration::MAX,
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            );
            match self.job.take_result() {
                Some(val) => {
                    ctx.waker().wake_by_ref();
                    Poll::Ready(val)
                }
                None => {
                    ctx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moirai::coroutine::yield_now;

    #[test]
    fn test_job_pump() {
        let mut pump = JobPump::new(async {
            yield_now().await;
            42
        });
        pump = pump.pump().err().unwrap();
        assert_eq!(pump.pump().ok(), Some(42));
    }

    #[pollster::test]
    async fn test_job_pump_async() {
        let pump = JobPump::new(async {
            yield_now().await;
            42
        });
        assert_eq!(pump.into_future().await, 42);
    }
}
