use flume::{Receiver, Sender, bounded, unbounded};
use moirai::{job::JobLocation, queue::JobQueue};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + Sync>>;
type Factory<Input, Output> = Box<dyn Fn(Input) -> BoxFuture<Output> + Send + Sync>;

pub struct StreamPump<Input, Output>
where
    Input: Send + Sync + 'static,
    Output: Send + Sync + 'static,
{
    sender_outside: Sender<Input>,
    receiver_inside: Receiver<Input>,
    sender_inside: Sender<Output>,
    receiver_outside: Receiver<Output>,
    queue: JobQueue,
    factory: Factory<Input, Output>,
}

impl<Input, Output> StreamPump<Input, Output>
where
    Input: Send + Sync + 'static,
    Output: Send + Sync + 'static,
{
    pub fn unbounded<Fun, Fut>(factory: Fun) -> Self
    where
        Fun: Fn(Input) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Output> + Send + Sync + 'static,
    {
        let (sender_outside, receiver_inside) = unbounded();
        let (sender_inside, receiver_outside) = unbounded();
        let queue = JobQueue::default();
        Self {
            sender_outside,
            receiver_inside,
            sender_inside,
            receiver_outside,
            queue,
            factory: Box::new(move |input| Box::pin(factory(input))),
        }
    }

    pub fn bounded<Fun, Fut>(capacity: usize, factory: Fun) -> Self
    where
        Fun: Fn(Input) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Output> + Send + Sync + 'static,
    {
        let (sender_outside, receiver_inside) = bounded(capacity);
        let (sender_inside, receiver_outside) = bounded(capacity);
        let queue = JobQueue::default();
        Self {
            sender_outside,
            receiver_inside,
            sender_inside,
            receiver_outside,
            queue,
            factory: Box::new(move |input| Box::pin(factory(input))),
        }
    }

    pub fn sender(&self) -> Sender<Input> {
        self.sender_outside.clone()
    }

    pub fn receiver(&self) -> Receiver<Output> {
        self.receiver_outside.clone()
    }

    pub fn duplex(&self) -> (Sender<Input>, Receiver<Output>) {
        (self.sender_outside.clone(), self.receiver_outside.clone())
    }

    pub fn pump(&self) {
        while let Ok(input) = self.receiver_inside.try_recv() {
            let future = (self.factory)(input);
            let sender_inside = self.sender_inside.clone();
            self.queue.spawn(JobLocation::Local, async move {
                let output = future.await;
                sender_inside.send(output).unwrap();
            });
        }
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

impl<Input, Output> Future for StreamPump<Input, Output>
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
    fn test_stream_pump() {
        let pump = StreamPump::unbounded(|input: i32| async move {
            let value = input * 2;
            yield_now().await;
            value.to_string()
        });
        let (sender, receiver) = pump.duplex();
        sender.send(21).unwrap();
        pump.pump();
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        pump.pump();
        assert_eq!(receiver.try_recv().unwrap().as_str(), "42");
    }

    #[pollster::test]
    async fn test_stream_pump_async() {
        let pump = StreamPump::unbounded(|input: i32| async move {
            let value = input * 2;
            yield_now().await;
            value.to_string()
        });
        let (sender, receiver) = pump.duplex();
        sender.send(21).unwrap();
        pump.await;
        assert_eq!(receiver.recv_async().await.unwrap().as_str(), "42");
    }
}
