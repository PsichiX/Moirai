use moirai::{
    coroutine::{spawn, spawn_closure},
    jobs::{JobLocation, Jobs},
};

fn main() {
    let jobs = Jobs::default();

    jobs.spawn((), async {
        // We can spawn jobs that themselves spawn other jobs.
        // Spawned jobs await only to yield their handles, so if we need to wait
        // for them to finish we have to do it ourselves.
        spawn(JobLocation::other_than_current_thread(), async {
            println!("Hello from nested spawn!");
        })
        .await
        .wait();

        spawn_closure(JobLocation::other_than_current_thread(), |_| {
            println!("Hello from nested spawn_closure!");
        })
        .await
        .wait();
    })
    .unwrap()
    .wait();
}
