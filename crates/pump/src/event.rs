use flume::{Receiver, bounded};
use moirai::{job::JobLocation, queue::JobQueue};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + Sync>>;
type Factory<Input, Output> = Box<dyn Fn(Input) -> BoxFuture<Output> + Send + Sync>;

pub struct EventPump<Input, Output>
where
    Input: Send + Sync + 'static,
    Output: Send + Sync + 'static,
{
    queue: JobQueue,
    factory: Factory<Input, Output>,
}

impl<Input, Output> EventPump<Input, Output>
where
    Input: Send + Sync + 'static,
    Output: Send + Sync + 'static,
{
    pub fn new<Fun, Fut>(factory: Fun) -> Self
    where
        Fun: Fn(Input) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Output> + Send + Sync + 'static,
    {
        let queue = JobQueue::default();
        Self {
            queue,
            factory: Box::new(move |input| Box::pin(factory(input))),
        }
    }

    pub fn send(&self, input: Input) -> Receiver<Output> {
        let future = (self.factory)(input);
        let (sender, receiver) = bounded(1);
        self.queue.spawn(JobLocation::Local, async move {
            let output = future.await;
            sender.send(output).unwrap();
        });
        receiver
    }

    pub fn pump(&self) {
        self.queue.run(
            JobLocation::Local,
            true,
            Duration::MAX,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        );
    }

    pub fn pump_all(&self) {
        while !self.is_complete() {
            self.pump();
        }
    }

    pub fn is_complete(&self) -> bool {
        self.queue.is_empty()
    }
}

impl<Input, Output> Future for EventPump<Input, Output>
where
    Input: Send + Sync + 'static,
    Output: Send + Sync + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.pump();
        if self.is_complete() {
            cx.waker().wake_by_ref();
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flume::TryRecvError;
    use moirai::coroutine::yield_now;

    #[test]
    fn test_event_pump() {
        let pump = EventPump::new(|input: i32| async move {
            let value = input * 2;
            yield_now().await;
            value.to_string()
        });
        let receiver = pump.send(21);
        pump.pump();
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        pump.pump();
        assert_eq!(receiver.try_recv().unwrap().as_str(), "42");
    }

    #[pollster::test]
    async fn test_event_pump_async() {
        let pump = EventPump::new(|input: i32| async move {
            let value = input * 2;
            yield_now().await;
            value.to_string()
        });
        let receiver = pump.send(21);
        pump.await;
        assert_eq!(receiver.recv_async().await.unwrap().as_str(), "42");
    }
}
