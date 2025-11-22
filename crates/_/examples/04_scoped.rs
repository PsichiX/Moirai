use moirai::jobs::{Jobs, ScopedJobs};

fn main() {
    let jobs = Jobs::default();

    let data = [1, 2, 3, 4, 5];

    let mut scoped = ScopedJobs::new(&jobs);

    // Scoped jobs allow for spawning jobs that can borrow data from the
    // surrounding scope.
    scoped.spawn((), async { data[0] }).unwrap();
    scoped.spawn_closure((), |_| data[1]).unwrap();
    scoped
        .broadcast_n(3, |ctx| data[2 + ctx.work_group_index])
        .unwrap();

    // Executing the scoped jobs will block current thread until all jobs are
    // done and return their results.
    // If instead of executing, you drop the scoped jobs, all jobs will be
    // cancelled, so make sure you better always execute them to not lose work.
    let result = scoped.execute().into_iter().sum::<usize>();
    println!("Result: {}", result);
}
