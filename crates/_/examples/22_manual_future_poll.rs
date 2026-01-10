use intuicio_data::managed::ManagedLazy;
use moirai::{
    coroutine::{meta, yield_now},
    jobs::{JobsMeta, JobsRuntime},
    queue::JobQueue,
};
use std::{pin::pin, task::Poll};

// This example demonstrates how to manually poll futures using the JobsRuntime.
// Jobs runtime is used internally by Moirai to run async coroutines that must
// be aware of the jobs system.
// It is an useful primitive for building custom async executors or integrating
// Moirai-enabled futures with other async runtimes.
fn main() {
    // We can do a single poll on a future.
    {
        let future = async {
            let mut counter = 0;
            for _ in 0..10 {
                counter += 1;
                yield_now().await;
            }
            counter
        };
        let mut future = pin!(future);

        loop {
            match JobsRuntime::default().poll_future(future.as_mut()) {
                Poll::Ready(counter) => {
                    println!("Poll future | Counter: {}", counter);
                    break;
                }
                Poll::Pending => {}
            }
        }
    }

    // As well as block on a future until its completion.
    let counter = JobsRuntime::default().block_on(async {
        let mut counter = 0;
        for _ in 0..10 {
            counter += 1;
            yield_now().await;
        }
        counter
    });
    println!("Block on future | Counter: {}", counter);

    // Finally, we can spawn a job into a queue and run it using provided runtime.
    {
        let queue = JobQueue::default();
        let job = queue.spawn((), async {
            let mut counter = 0;
            for _ in 0..10 {
                counter += 1;
                yield_now().await;
            }
            counter
        });

        while !job.is_done() {
            JobsRuntime::default().run_queue(&queue);
        }
        println!("Run job queue | Counter: {}", job.take_result().unwrap());
    }

    // JobsRuntime can also be activated for a scope duration.
    {
        async fn increment_counter() {
            let counter = meta::<i32>("counter").await.unwrap();
            *counter.write().unwrap() += 1;
            yield_now().await;
        }

        pollster::block_on(async {
            // Without runtime scope, we can't access Moirai features.
            assert!(meta::<i32>("counter").await.is_none());
        });

        // Prepare meta value for use in Moirai-enabled future.
        // Meta values rely on managed lazy references to use Rust's borrow
        // checking rules at runtime.
        let mut counter = 0;
        let (counter_lazy, _counter_lifetime) = ManagedLazy::make(&mut counter);
        let local_meta = JobsMeta::default().with("counter", counter_lazy.into_dynamic());

        pollster::block_on(async {
            // You do some work with your main async runtime-enabled features [...]

            // Let's say you spawn some future in another runtime and you want to
            // use Moirai-enabled functionality without spawning it in Jobs, so you
            // enter temporary runtime scope.
            let _guard = JobsRuntime::default().local_meta(local_meta).enter();

            // Then to run some Moirai-enabled code, you use active scope runtime.
            JobsRuntime::current()
                .enable(async {
                    for _ in 0..10 {
                        increment_counter().await;
                    }
                })
                .await;
        });
        println!("Runtime scope | Counter: {}", counter);
    }
}
