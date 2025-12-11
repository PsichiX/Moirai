use moirai::{coroutine::yield_now, jobs::Jobs};

fn main() {
    // Create a Jobs runtime. By default it constructs number of unnamed workers
    // equal to the number of CPU cores.
    let jobs = Jobs::default();

    // Spawn a new job onto the Jobs runtime. By default it spawns to unknown
    // worker location, meaning any worker can steal this job as soon as it's
    // free from work.
    let job = jobs.spawn((), async {
        let mut counter = 0;
        for _ in 0..10 {
            counter += 1;
            // Yield control to allow other coroutines to run.
            yield_now().await;
        }
        counter
    });

    // Wait blocking for the spawned job to complete.
    let mut counter = job.wait().unwrap();
    println!("Counter: {}", counter);

    // We can also spawn closures as jobs. It's good for when your job isn't async.
    let job = jobs.spawn_closure((), move |_| {
        while counter < 20 {
            counter += 1;
        }
        counter
    });

    let counter = job.wait().unwrap();
    println!("Counter: {}", counter);
}
