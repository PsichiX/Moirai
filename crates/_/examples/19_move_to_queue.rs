use moirai::{
    coroutine::move_to,
    jobs::{JobLocation, JobQueue, Jobs},
};

fn main() {
    let jobs = Jobs::default();
    let queue = JobQueue::default();
    let queue2 = queue.clone();

    // Start job on the local worker.
    let job = jobs.spawn(JobLocation::Local, async move {
        println!("Job running on local worker");

        // Move to dedicated queue.
        move_to(JobLocation::Queue(queue2)).await;

        println!("Job running on dedicated queue");
    });

    // First run local jobs.
    println!("Running local jobs");
    while !jobs.queue_is_empty() {
        jobs.run_local();
    }

    // Then run jobs in the dedicated queue.
    println!("Running jobs in dedicated queue");
    while !queue.is_empty() {
        jobs.run_queue(&queue);
    }

    job.wait().unwrap();
}
