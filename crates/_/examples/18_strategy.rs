use moirai::{
    coroutine::{StrategyDecision, location, priority, strategy_timeline, yield_now},
    jobs::{JobLocation, JobPriority, Jobs},
};
use std::time::Duration;

fn main() {
    let jobs = Jobs::default();

    // Strategy timeline allows to change job location, prority or even cancel
    // the job at specific time intervals.
    // This is useful in cases where we want to run a job with different
    // parameters at different stages of its execution, like starting low
    // priority job and in case it takes longer, we move it do threads,
    // and if it hangs we can abort it after some timeout.

    let timeline = [
        (
            Duration::from_millis(100),
            StrategyDecision::default().priority(JobPriority::High),
        ),
        (
            Duration::from_millis(100),
            StrategyDecision::default()
                .priority(JobPriority::Normal)
                .location(JobLocation::other_than_current_thread()),
        ),
        (
            Duration::from_millis(100),
            StrategyDecision::default().cancel(),
        ),
    ];

    let task = async {
        loop {
            let location = location().await;
            let priority = priority().await;
            println!(
                "Runing on location: {:?}, priority: {:?}",
                location, priority
            );
            std::thread::sleep(Duration::from_millis(50));
            yield_now().await;
        }
    };

    let job = jobs.spawn(JobLocation::Local, strategy_timeline(timeline, task));

    while !jobs.queue_is_empty() {
        jobs.run_local();
    }

    job.wait().unwrap();
}
