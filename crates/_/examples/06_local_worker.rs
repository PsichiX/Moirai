use moirai::{coroutine::yield_now, job::JobLocation, jobs::Jobs};

fn main() {
    let jobs = Jobs::default();

    let job = jobs.spawn(JobLocation::Local, async {
        let mut counter = 0;
        for _ in 0..10 {
            counter += 1;
            // Yield execution to allow other jobs to run.
            yield_now().await;
        }
        counter
    });

    while !jobs.queue().is_empty() {
        // Run jobs on the local worker. This step is needed in order to process
        // local jobs, as they are only ran on demand, since they doesn't have a
        // dedicated thread. Additionally this runs only single pass of future
        // polls for each stored local job, not blocking untill all are done.
        // This allows to interleave local job processing with other application
        // logic, which is crucial for games responsiveness.
        // Usually what local jobs are used for, is also known as coroutines in
        // game development - an useful way to schedule heavier logic that
        // doesn't or shouldn't be ran in threads.
        jobs.run_local();
    }

    // Retrieve the job result when it's completed.
    let counter = job.take_result().unwrap();
    println!("Counter: {}", counter);
}
