use moirai::{
    coroutine::{change_priority, yield_now},
    jobs::{JobLocation, JobPriority, Jobs},
};

fn main() {
    let jobs = Jobs::default();

    jobs.spawn(JobLocation::Local, async {
        println!("Job A running with normal priority.");
        yield_now().await;
        println!("Job A continuing on normal priority.");
    });

    jobs.spawn(JobLocation::Local, async {
        println!("Job B running with normal priority.");
        yield_now().await;
        println!("Job B continuing on normal priority.");
    });

    jobs.spawn(JobLocation::Local, async {
        println!("Job C changing its priority to high. Executes last.");
        change_priority(JobPriority::High).await;
        println!("Job C continuing on high priority. Executes first.");
    });

    // We spawned local jobs to better show deterministic execution order.
    // In case of threaded jobs, the order may vary due to jobs stealing,
    // while priority is kept within a single thread.
    while !jobs.queue_is_empty() {
        println!("Running local jobs");
        jobs.run_local();
    }
}
