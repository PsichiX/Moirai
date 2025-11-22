use moirai::{
    coroutine::{suspend, wait_time},
    jobs::Jobs,
};
use std::time::Duration;

fn main() {
    let jobs = Jobs::default();

    // Spawn a job that suspends itself and when resumed, it waits to yield a result.
    let job = jobs
        .spawn((), async {
            println!("Suspending job...");
            suspend().await;
            println!("Job resumed!");
            wait_time(Duration::from_millis(500)).await;
            42
        })
        .unwrap();

    println!("Waiting 500 milliseconds before resuming...");
    std::thread::sleep(Duration::from_millis(500));
    println!("Resuming job...");
    job.resume();

    std::thread::sleep(Duration::from_millis(100));
    println!("Cancelling job...");
    job.cancel();

    // If job wouldn't be cancelled, result would be 42 instead of None.
    println!("Job result: {:?}", job.wait());
}
