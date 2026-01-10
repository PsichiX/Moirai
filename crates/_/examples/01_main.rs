use moirai::{coroutine::yield_now, jobs::Jobs};

fn main() {
    // Create a Jobs runtime and run an async block within it, making this
    // runtime "global" for entire application lifetime.
    // Normally you would rather use Jobs as part of your application resources
    // to have more control over managing jobs lifecycle.
    //
    // It's worth mentioning, that Moirai doesn't have a proc macro for this
    // pattern like Tokio does, simply because Moirai primitives are supposed
    // to be part of the application, not the entire application itself.
    Jobs::default().block_on(async {
        let mut counter = 0;
        for _ in 0..10 {
            counter += 1;
            yield_now().await;
        }
        println!("Counter: {}", counter);
    });
}
