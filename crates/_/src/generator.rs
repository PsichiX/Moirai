use std::{
    cell::Cell,
    future::poll_fn,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
};

pub trait IntoGenerator<T> {
    fn into_generator(self) -> GeneratorIter<T>;
}

impl<T, F: Future<Output = ()> + 'static> IntoGenerator<T> for F {
    fn into_generator(self) -> GeneratorIter<T> {
        GeneratorIter::new(self)
    }
}

pub struct GeneratorIter<T> {
    future: Pin<Box<dyn Future<Output = ()>>>,
    yielded_value: Rc<Cell<Option<T>>>,
}

impl<T> GeneratorIter<T> {
    pub fn new<F: Future<Output = ()> + 'static>(future: F) -> Self {
        GeneratorIter {
            future: Box::pin(future),
            yielded_value: Default::default(),
        }
    }
}

impl<T> Iterator for GeneratorIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let waker = GeneratorWaker::<T>::new_waker(self.yielded_value.clone());
        let mut context = Context::from_waker(&waker);
        match self.future.as_mut().poll(&mut context) {
            Poll::Ready(_) => None,
            Poll::Pending => self.yielded_value.take(),
        }
    }
}

pub async fn gen_yield<T>(value: T) {
    let mut value = Some(value);
    poll_fn(move |cx| {
        let waker = cx.waker();
        if let Some(value) = value.take() {
            if let Some(waker) = GeneratorWaker::<T>::try_cast(waker) {
                waker.yielded_value.set(Some(value));
            }
            waker.wake_by_ref();
            Poll::Pending
        } else {
            waker.wake_by_ref();
            Poll::Ready(())
        }
    })
    .await
}

struct GeneratorWaker<T> {
    yielded_value: Rc<Cell<Option<T>>>,
}

impl<T> GeneratorWaker<T> {
    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(Self::vtable_clone, |_| {}, |_| {}, Self::vtable_drop);

    fn vtable_clone(data: *const ()) -> RawWaker {
        let arc = unsafe { Arc::<Self>::from_raw(data as *const Self) };
        let cloned = arc.clone();
        std::mem::forget(arc);
        RawWaker::new(Arc::into_raw(cloned) as *const (), &Self::VTABLE)
    }

    fn vtable_drop(data: *const ()) {
        let _ = unsafe { Arc::from_raw(data as *const Self) };
    }

    fn new_waker(yielded_value: Rc<Cell<Option<T>>>) -> Waker {
        let arc = Arc::new(Self { yielded_value });
        let raw = RawWaker::new(Arc::into_raw(arc) as *const (), &Self::VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    fn try_cast(waker: &Waker) -> Option<&Self> {
        if waker.vtable() == &Self::VTABLE {
            unsafe { waker.data().cast::<Self>().as_ref() }
        } else {
            None
        }
    }
}

impl<T> Wake for GeneratorWaker<T> {
    fn wake(self: Arc<Self>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::marker::PhantomData;

    #[test]
    fn test_generator() {
        let provided = async {
            for i in 0..5 {
                gen_yield(i).await;
            }
            for i in -10..-5 {
                gen_yield(i).await;
            }
        }
        .into_generator()
        .collect::<Vec<i32>>();

        assert_eq!(provided, vec![0, 1, 2, 3, 4, -10, -9, -8, -7, -6]);
    }

    #[test]
    fn test_generator_no_send_sync() {
        struct Foo {
            value: i32,
            _phantom: PhantomData<*const ()>,
        }

        impl Foo {
            fn new(value: i32) -> Self {
                Foo {
                    value,
                    _phantom: PhantomData,
                }
            }

            fn value(&self) -> i32 {
                self.value
            }
        }

        let provided = async {
            for i in 0..5 {
                gen_yield(Foo::new(i)).await;
            }
            for i in -10..-5 {
                gen_yield(Foo::new(i)).await;
            }
        }
        .into_generator()
        .map(|v: Foo| v.value())
        .collect::<Vec<i32>>();

        assert_eq!(provided, vec![0, 1, 2, 3, 4, -10, -9, -8, -7, -6]);
    }
}
