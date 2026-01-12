use std::sync::mpsc::{Receiver, Sender};

use moirai::third_party::intuicio_data::shared::AsyncShared;

pub struct Events<T: Send + 'static> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T: Send + 'static> Default for Events<T> {
    fn default() -> Self {
        let (sender, receiver) = std::sync::mpsc::channel();
        Self { sender, receiver }
    }
}

impl<T: Send + 'static> Events<T> {
    pub fn sender(&self) -> Sender<T> {
        self.sender.clone()
    }

    pub fn receive(&self) -> impl Iterator<Item = T> + '_ {
        self.receiver.try_iter()
    }
}

pub mod mpmc {
    use moirai::coroutine::yield_now;

    use super::*;
    use std::collections::VecDeque;

    pub fn channel<T: Send + 'static>() -> (Sender<T>, Receiver<T>) {
        let shared = AsyncShared::new(Default::default());
        (
            Sender {
                shared: shared.clone(),
            },
            Receiver { shared },
        )
    }

    pub struct Sender<T: Send + 'static> {
        shared: AsyncShared<VecDeque<T>>,
    }

    impl<T: Send + 'static> Sender<T> {
        pub async fn send(&self, value: T) {
            loop {
                if let Some(mut shared) = self.shared.write() {
                    shared.push_back(value);
                    return;
                }
                yield_now().await;
            }
        }
    }

    pub struct Receiver<T: Send + 'static> {
        shared: AsyncShared<VecDeque<T>>,
    }

    impl<T: Send + 'static> Receiver<T> {
        pub async fn receive(&self) -> T {
            loop {
                if let Some(mut shared) = self.shared.write()
                    && let Some(value) = shared.pop_front()
                {
                    return value;
                }
                yield_now().await;
            }
        }
    }
}

pub mod oneshot {
    use super::*;

    pub fn channel<T: Send + 'static>() -> (Sender<T>, Receiver<T>) {
        let shared = AsyncShared::new(None);
        (
            Sender {
                shared: shared.clone(),
            },
            Receiver { shared },
        )
    }

    pub struct Sender<T: Send + 'static> {
        shared: AsyncShared<Option<T>>,
    }

    impl<T: Send + 'static> Sender<T> {
        pub fn send(&self, value: T) {
            *self.shared.write().unwrap() = Some(value);
        }
    }

    pub struct Receiver<T: Send + 'static> {
        shared: AsyncShared<Option<T>>,
    }

    impl<T: Send + 'static> Receiver<T> {
        pub fn try_recv(&self) -> Option<T> {
            self.shared.write().unwrap().take()
        }
    }
}
