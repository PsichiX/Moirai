use flume::{Receiver, bounded};
use moirai::{job::JobLocation, queue::JobQueue};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[derive(Default)]
pub struct ManualPump {
    queue: JobQueue,
}

impl ManualPump {
    pub fn spawn<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + Sync + 'static,
    ) -> Receiver<T> {
        let (sender, receiver) = bounded(1);
        self.queue.spawn(JobLocation::Local, async move {
            let result = future.await;
            sender.send(result).unwrap();
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
        while !self.queue.is_empty() {
            self.pump();
        }
    }

    pub fn is_complete(&self) -> bool {
        self.queue.is_empty()
    }
}

impl Future for ManualPump {
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
    fn test_manual_pump() {
        let pump = ManualPump::default();
        let receiver = pump.spawn(async {
            yield_now().await;
            42
        });

        pump.pump();
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        pump.pump();
        assert_eq!(receiver.try_recv().unwrap(), 42);
    }

    #[pollster::test]
    async fn test_manual_pump_async() {
        let pump = ManualPump::default();
        let receiver = pump.spawn(async {
            yield_now().await;
            42
        });

        pump.await;
        assert_eq!(receiver.recv_async().await.unwrap(), 42);
    }
}
