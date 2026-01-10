use intuicio_data::managed::ManagedLazy;
use moirai::{
    coroutine::meta,
    jobs::{Jobs, JobsMeta, JobsRuntime},
};
use std::time::Duration;
use tokio::runtime::Builder;

const COUNTER_META: &str = "counter";

// Example showing mixing Moirai with other async runtimes in both directions.
fn main() {
    main_moirai_in_tokio();
    main_tokio_in_moirai();
}

#[tokio::main]
async fn main_moirai_in_tokio() {
    // Create a counter variable outside of the runtime.
    let mut counter = 0;

    // Create a lazy managed reference to the counter variable for use as meta.
    // Meta values are Moirai-only feature so it will easily show when future
    // doesn't have access to Moirai runtime at the moment.
    let (counter_lazy, _counter_lifetime) = ManagedLazy::make(&mut counter);
    let local_meta = JobsMeta::default().with(COUNTER_META, counter_lazy.into_dynamic());

    // Create Moirai runtime and enter it in current scope.
    // Btw. In case of doing only single future, we could omit entering runtime
    // and just use it directly in place for `enable`.
    let _guard = JobsRuntime::default().local_meta(local_meta).enter();

    // Enable a future with use of current Moirai runtime features.
    JobsRuntime::current()
        .enable(async {
            for _ in 0..10 {
                increment_counter().await;
            }
        })
        .await;

    println!("Tokio counter: {}", counter);
}

fn main_tokio_in_moirai() {
    // Create Tokio runtime and enter it in current scope.
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let _guard = runtime.enter();

    // Spawn a Moirai blocking job that uses Tokio features.
    let value = Jobs::default()
        .block_on(async {
            let mut counter = 0;
            for _ in 0..10 {
                counter += 1;
                // Use of Tokio-enabled future.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            counter
        })
        .unwrap();

    println!("Moirai counter: {}", value);
}

// Increment meta value we lazily referenced in current runtime.
async fn increment_counter() {
    // Use of Moirai-enabled future.
    *meta::<i32>(COUNTER_META).await.unwrap().write().unwrap() += 1;
}
