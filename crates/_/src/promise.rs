use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub struct Promise<T, E> {
    future: BoxFuture<'static, Result<T, E>>,
}

impl<T: 'static + Send, E: 'static + Send> Promise<T, E> {
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
    {
        Self {
            future: Box::pin(future),
        }
    }

    pub fn resolve(value: T) -> Self {
        Self::new(async move { Ok(value) })
    }

    pub fn reject(error: E) -> Self {
        Self::new(async move { Err(error) })
    }

    pub fn all<I>(promises: I) -> Promise<Vec<T>, E>
    where
        I: IntoIterator<Item = Promise<T, E>>,
    {
        let promises = promises.into_iter().collect::<Vec<_>>();
        let future = async move {
            let mut result = Vec::with_capacity(promises.len());
            for promise in promises {
                result.push(promise.future.await?);
            }
            Ok(result)
        };
        Promise::new(future)
    }

    pub fn any<I>(promises: I) -> Promise<T, E>
    where
        I: IntoIterator<Item = Promise<T, E>>,
    {
        let mut promises = promises
            .into_iter()
            .map(|p| Box::pin(p.future))
            .collect::<Vec<_>>();
        let future = async move {
            loop {
                for future in promises.iter_mut() {
                    let polled = std::future::poll_fn(|cx| match future.as_mut().poll(cx) {
                        Poll::Ready(result) => Poll::Ready(Some(result)),
                        Poll::Pending => Poll::Pending,
                    })
                    .await;
                    if let Some(result) = polled {
                        return result;
                    }
                }
            }
        };
        Promise::new(future)
    }

    pub fn then<T2, Fut>(self, f: impl FnOnce(T) -> Fut + Send + 'static) -> Promise<T2, E>
    where
        Fut: Future<Output = Result<T2, E>> + Send + 'static,
        T2: 'static + Send,
    {
        let future = async move {
            match self.future.await {
                Ok(val) => f(val).await,
                Err(err) => Err(err),
            }
        };
        Promise::new(future)
    }

    pub fn catch<Fut>(self, f: impl FnOnce(E) -> Fut + Send + 'static) -> Promise<T, E>
    where
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let future = async move {
            match self.future.await {
                Ok(val) => Ok(val),
                Err(err) => f(err).await,
            }
        };
        Promise::new(future)
    }

    pub fn transform<T2, E2, Fut>(
        self,
        f: impl FnOnce(Result<T, E>) -> Fut + Send + 'static,
    ) -> Promise<T2, E2>
    where
        Fut: Future<Output = Result<T2, E2>> + Send + 'static,
        T2: 'static + Send,
        E2: 'static + Send,
    {
        let future = async move {
            let result = self.await;
            f(result).await
        };
        Promise::new(future)
    }

    pub fn finally<Fut>(self, f: impl FnOnce() -> Fut + Send + 'static) -> Promise<T, E>
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let future = async move {
            let result = self.await;
            f().await;
            result
        };
        Promise::new(future)
    }
}

impl<T, E> Future for Promise<T, E> {
    type Output = Result<T, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::poll_fn,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    async fn delay(duration: Duration) {
        let timer = Instant::now();
        poll_fn(|cx| {
            if timer.elapsed() >= duration {
                cx.waker().wake_by_ref();
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }

    #[pollster::test]
    async fn test_promise_resolve() {
        let promise = Promise::<i32, &str>::resolve(42);
        assert_eq!(promise.await.unwrap(), 42);
    }

    #[pollster::test]
    async fn test_promise_reject() {
        let promise = Promise::<i32, &str>::reject("error");
        assert_eq!(promise.await.unwrap_err(), "error");
    }

    #[pollster::test]
    async fn test_promise_then() {
        let promise = Promise::<i32, &str>::resolve(2).then(|x| async move { Ok(x * 3) });
        assert_eq!(promise.await.unwrap(), 6);
    }

    #[pollster::test]
    async fn test_promise_catch() {
        let promise = Promise::<i32, &str>::reject("error").catch(|_| async { Ok(99) });
        assert_eq!(promise.await.unwrap(), 99);
    }

    #[pollster::test]
    async fn test_promise_finally() {
        let flag = Arc::new(Mutex::new(false));
        let flag_clone = Arc::clone(&flag);
        let promise = Promise::<i32, &str>::resolve(5).finally(move || async move {
            let mut flag = flag_clone.lock().unwrap();
            *flag = true;
        });
        assert_eq!(promise.await.unwrap(), 5);
        assert!(*flag.lock().unwrap());
    }

    #[pollster::test]
    async fn test_promise_transform() {
        let promise = Promise::<i32, &str>::resolve(10)
            .transform(|result| async move { result.map(|v| v * 2) });
        assert_eq!(promise.await.unwrap(), 20);
    }

    #[pollster::test]
    async fn test_promise_all() {
        let promise = Promise::all([
            Promise::<i32, &str>::resolve(1),
            Promise::<i32, &str>::resolve(2),
            Promise::<i32, &str>::resolve(3),
        ]);
        assert_eq!(promise.await.unwrap(), vec![1, 2, 3]);
    }

    #[pollster::test]
    async fn test_promise_any() {
        let promise = Promise::any([
            Promise::<i32, &str>::new(async {
                delay(Duration::from_millis(10)).await;
                Ok(1)
            }),
            Promise::<i32, &str>::new(async {
                delay(Duration::from_millis(0)).await;
                Ok(2)
            }),
            Promise::<i32, &str>::new(async {
                delay(Duration::from_millis(30)).await;
                Ok(3)
            }),
        ]);
        assert_eq!(promise.await.unwrap(), 1);
    }

    #[pollster::test]
    async fn test_promise_chain() {
        let promise = Promise::<i32, String>::new(async {
            delay(Duration::from_millis(10)).await;
            Ok(1)
        })
        .then(|x| async move { Ok(x + 3) })
        .transform(|result| async move { result.map(|v| v * 2) })
        .catch(|err| async move {
            println!("Caught an error: {}", err);
            Ok(0)
        })
        .finally(|| async { println!("Done") });
        assert_eq!(promise.await.unwrap(), 8);
    }
}
