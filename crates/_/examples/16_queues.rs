use moirai::{jobs::Jobs, queue::JobQueue};

fn main() {
    let jobs = Jobs::default();

    // Create standalone job queue.
    let queue = JobQueue::default();

    // Spawn a job in the standalone queue.
    let job = queue.spawn((), async {
        let mut counter = 0;
        for _ in 0..5 {
            counter += 1;
        }
        counter
    });

    // Run the standalone job queue blocking current thread.
    // Job queues are useful for special, custom scheduling scenarios, where we
    // might want to control when and how jobs are executed outside of job
    // system automatic scheduling.
    jobs.run_queue(&queue);

    let result = job.wait().unwrap();
    println!("Job result: {}", result);
}
