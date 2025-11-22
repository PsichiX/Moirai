use moirai::jobs::Jobs;

fn main() {
    let jobs = Jobs::default();

    // Spawn a job that panics.
    // The panic will be caught and won't crash the whole program,
    // instead panic is logged using `tracing`.
    jobs.spawn((), async {
        panic!("This job panics!");
    })
    .unwrap();

    // Just waiting for the job to be processed.
    while !jobs.queue_is_empty() {}
}
