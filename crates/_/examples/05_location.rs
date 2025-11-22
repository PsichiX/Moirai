use moirai::{
    coroutine::yield_now,
    jobs::{JobLocation, Jobs},
};

fn main() {
    // Create a Jobs runtime with a named worker "foo".
    let jobs = Jobs::default().with_named_worker("foo");

    // Spawn a job on an unnamed worker.
    jobs.spawn(JobLocation::UnnamedWorker, async {
        let mut counter = 0;
        for _ in 0..10 {
            counter += 1;
            yield_now().await;
        }
        counter
    })
    .unwrap()
    .wait()
    .unwrap();

    // Spawn a job on the named worker "foo".
    jobs.spawn(JobLocation::named_worker("foo"), async {
        let mut counter = 0;
        for _ in 0..10 {
            counter += 1;
            yield_now().await;
        }
        counter
    })
    .unwrap()
    .wait()
    .unwrap();

    // Spawn a job on any worker other than the current one.
    jobs.spawn(JobLocation::other_than_current_thread(), async {
        let mut counter = 0;
        for _ in 0..10 {
            counter += 1;
            yield_now().await;
        }
        counter
    })
    .unwrap()
    .wait()
    .unwrap();

    // Spawn a job on an exclusive worker. Exclusive workers are spawn for single
    // jobs that should not share a thread with other jobs.
    // Note: Using exclusive workers can be expensive in terms of system resources.
    // This is useful only for long running critical background jobs.
    jobs.spawn(JobLocation::Exclusive, async {
        let mut counter = 0;
        for _ in 0..10 {
            counter += 1;
            yield_now().await;
        }
        counter
    })
    .unwrap()
    .wait()
    .unwrap();
}
