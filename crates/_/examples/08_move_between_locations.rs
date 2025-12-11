use moirai::{
    coroutine::move_to,
    jobs::{JobLocation, Jobs},
};

fn main() {
    let jobs = Jobs::default();

    let data = (0..100).collect::<Vec<_>>();

    // Start job on the local worker.
    let job = jobs.spawn(JobLocation::Local, async move {
        // lightweight data preparation.
        let filtered = data.into_iter().filter(|x| x % 2 == 0);

        // Move to a different worker for heavy computation.
        // Moving job between locations from within job is quite ergonomic
        // and powerful mechanism that lets job itself control where it runs.
        move_to(JobLocation::other_than_current_thread()).await;

        // Continue computation on the new worker.
        filtered.fold(0, |acc, x| acc + x * x)
    });

    while !jobs.queue_is_empty() {
        jobs.run_local();
    }

    let result = job.wait().unwrap();
    println!("Result: {}", result);
}
