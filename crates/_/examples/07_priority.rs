use moirai::jobs::{JobLocation, JobPriority, Jobs};

fn main() {
    let jobs = Jobs::default();

    jobs.spawn(JobLocation::Local, async {
        println!("This is a normal priority job. Will run second.");
    });

    jobs.spawn((JobLocation::Local, JobPriority::High), async {
        println!("This is a high priority job. Will run first.");
    });

    // We spawned local jobs to better show deterministic execution order.
    // In case of threaded jobs, the order may vary due to jobs stealing,
    // while priority is kept within a single thread.
    while !jobs.queue_is_empty() {
        jobs.run_local();
    }
}
