use moirai::{coroutine::on_exit, jobs::Jobs};

fn main() {
    let jobs = Jobs::default();

    jobs.spawn((), async {
        println!("Job started");

        // on_exit allows to run some cleanup code when the job is exiting,
        // either by finishing normally, being cancelled, or panicking.
        // It's useful to do it in case where we have some resources that need
        // to be released no matter how the job ends, without duplicating
        // cleanup code on every exit scenario.
        let _exit = on_exit(async {
            println!("Job is exiting, performing cleanup");
        })
        .await;

        println!("Job is doing work...");

        // We can also invalidate exit work if we don't want it to run,
        // for example, if the job finished successfully and no cleanup is needed.
        // _exit.invalidate();
    })
    .wait();

    while !jobs.queue().is_empty() {}
}
