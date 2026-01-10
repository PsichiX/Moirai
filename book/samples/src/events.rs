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

pub mod oneshot {
    use super::*;

    pub fn channel<T: Send + 'static>() -> (Sender<T>, Receiver<T>) {
        let shared = AsyncShared::new(None);
        (
            Sender {
                sender: shared.clone(),
            },
            Receiver { receiver: shared },
        )
    }

    pub struct Sender<T: Send + 'static> {
        sender: AsyncShared<Option<T>>,
    }

    impl<T: Send + 'static> Sender<T> {
        pub fn send(&self, value: T) {
            *self.sender.write().unwrap() = Some(value);
        }
    }

    pub struct Receiver<T: Send + 'static> {
        receiver: AsyncShared<Option<T>>,
    }

    impl<T: Send + 'static> Receiver<T> {
        pub fn try_recv(&self) -> Option<T> {
            self.receiver.write().unwrap().take()
        }
    }
}
