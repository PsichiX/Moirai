use moirai::jobs::Jobs;

fn main() {
    let jobs = Jobs::default();

    let data = [1, 2, 3, 4, 5];

    // Scoped jobs allow for spawning jobs that can borrow data from the
    // surrounding scope. Scope can spawn multiple jobs and wait for all of them
    // to finish, returning their output alongside the result of the closure as
    // a tuple: `(Vec<Output>, Result)`.
    let output = jobs
        .scope(|scope| {
            scope.spawn((), async { data[0] }).unwrap();

            scope.spawn_closure((), |_| data[1]).unwrap();

            scope
                .broadcast_n(3, |ctx| data[2 + ctx.work_group_index])
                .unwrap();
        })
        .0
        .into_iter()
        .sum::<usize>();

    println!("Result: {}", output);
}
