use moirai::jobs::Jobs;

fn main() {
    let jobs = Jobs::default();

    // Example data set.
    let data = (0..100).collect::<Vec<_>>();
    let data2 = data.clone();

    // Broadcast a job to all available work groups to process chunks of the data.
    let result = jobs
        .broadcast(move |ctx| {
            let chunk_size = data.len().div_ceil(ctx.work_groups_count);
            let start = ctx.work_group_index * chunk_size;
            let end = ((ctx.work_group_index + 1) * chunk_size).min(data.len());
            let chunk = &data[start..end];
            let sum = chunk.iter().sum::<usize>();
            println!(
                "Work group {}: processing indices {}..{}, sum = {}",
                ctx.work_group_index, start, end, sum
            );
            sum
        })
        .wait()
        .into_iter()
        .flatten()
        .sum::<usize>();
    println!("Total sum: {}", result);

    // Broadcast a job to exactly 2 workers to process chunks of the data.
    let result = jobs
        .broadcast_n(2, move |ctx| {
            let chunk_size = data2.len().div_ceil(ctx.work_groups_count);
            let start = ctx.work_group_index * chunk_size;
            let end = ((ctx.work_group_index + 1) * chunk_size).min(data2.len());
            let chunk = &data2[start..end];
            let sum = chunk.iter().sum::<usize>();
            println!(
                "Work group {}: processing indices {}..{}, sum = {}",
                ctx.work_group_index, start, end, sum
            );
            sum
        })
        .wait()
        .into_iter()
        .flatten()
        .sum::<usize>();
    println!("Total sum: {}", result);
}
