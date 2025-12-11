use intuicio_data::lifetime::Lifetime;
use moirai::{
    coroutine::{lifetime_bound, run_queue},
    jobs::{JobLocation, JobQueue, Jobs},
};

// This example demonstrates how to create and use custom job queues with
// dedicated scheduler threads for different subsystems (e.g., AI and Physics).
// Each subsystem has its own job queue and an exclusive worker that continuously
// processes jobs from that queue. This allows for fine-grained control over job
// scheduling and execution in a multi-threaded environment.
// In real world scenario, subsystem schedulers will get spawn along with engine,
// and queues will be exposed by the engine for user to schedule jobs into them.
// That way we ensure critical subsystems have dedicated resources for their
// tasks to complete as soon as possible, without interference of other jobs
// when subsystem tasks would be spawned into jobs system itself.
fn main() {
    let jobs = Jobs::default();

    // Create custom job queues for different subsystems.
    let ai_queue = JobQueue::default();
    let ai_queue2 = ai_queue.clone();
    let physics_queue = JobQueue::default();
    let physics_queue2 = physics_queue.clone();

    // Exclusive jobs will be tied to this lifetime and stop as soon as the
    // lifetime drops. This prevents spawned exclusive worker from doing it's
    // job indefinitely.
    let lifetime = Lifetime::default();

    // Spawn exclusive workers for each custom job queue, bound to the lifetime.
    jobs.spawn(
        JobLocation::Exclusive,
        lifetime_bound([lifetime.state().downgrade()], async move {
            println!("Starting AI scheduler thread.");
            // Run the AI job queue in a loop.
            // Single queue run does only one pass over scheduled jobs.
            // Looping allows us to keep processing jobs as they are scheduled.
            loop {
                run_queue(&ai_queue2).await;
            }
        }),
    );

    jobs.spawn(
        JobLocation::Exclusive,
        lifetime_bound([lifetime.state().downgrade()], async move {
            println!("Starting Physics scheduler thread.");
            loop {
                run_queue(&physics_queue2).await;
            }
        }),
    );

    // Spawn jobs in the custom job queues.
    let ai_job = ai_queue.spawn((), async {
        println!(
            "Running AI job on thread: {:?}",
            std::thread::current().id()
        );
    });

    let physics_job = physics_queue.spawn((), async {
        println!(
            "Running Physics job on thread: {:?}",
            std::thread::current().id()
        );
    });

    // Wait for jobs to complete to ensure all scheduled tasks have finished.
    ai_job.wait().unwrap();
    physics_job.wait().unwrap();
}
