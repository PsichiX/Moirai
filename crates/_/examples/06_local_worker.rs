use moirai::{
    coroutine::yield_now,
    jobs::{JobLocation, Jobs},
};

fn main() {
    let jobs = Jobs::default();

    // Spawn a job on the local worker.
    let job = jobs.spawn(JobLocation::Local, async {
        let mut counter = 0;
        for _ in 0..10 {
            counter += 1;
            yield_now().await;
        }
        counter
    });

    while !jobs.queue_is_empty() {
        // Run jobs on the local worker. This step is needed in order to process
        // jobs, as they are only ran on demand, since they doesn't have a
        // dedicated thread. Additionally this runs only single pass of future
        // polls for each stored local job, not blocking untill all are done.
        // This allows to interleave local job processing with other application
        // logic, which is crucial for games responsiveness.
        // Usually what local jobs are used for, is also known as coroutines in
        // game development - an useful way to schedule heavier logic that
        // doesn't or shouldn't be ran in threads.
        jobs.run_local();
    }

    let counter = job.wait().unwrap();
    println!("Counter: {}", counter);
}
