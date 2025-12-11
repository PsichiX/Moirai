use intuicio_data::managed::Managed;
use moirai::{
    coroutine::{meta, move_to},
    jobs::{JobLocation, JobQueue, Jobs},
};
use std::time::{Duration, Instant};

fn main() {
    let jobs = Jobs::default();

    // Somewhere in your application you store next frame queue - location where
    // you aggreagate all jobs that should run in the next frame.
    let mut next_frame_queue = Managed::new(JobQueue::default());

    // Meta values are great for passing data "global" to the current jobs run
    // context, that's accessible from any job in any worker.
    // Although in this simple scenario if you trigger waiting for next frame
    // from other than local worker (main thread), your job will continue
    // on the local worker, which might be not what you want.
    // In that case you might rather want to implement next frame queue per
    // worker, which ties to structure in `17_custom_scheduler.rs` example,
    // where you spawn exclusive workers and run jobs with their personal next
    // frame queues, additionally awaiting for signal from main thread telling
    // when to continue next frame.
    // Although to be completely honest, next frame queues are mostly useful on
    // main thread, and waiting for next frame in dedicated workers usually
    // makes no sense, as dedicated workers are meant for specialized tasks.
    jobs.set_meta(NEXT_FRAME_QUEUE, next_frame_queue.lazy().into_dynamic());

    // Start job on the local worker.
    let job = jobs.spawn(JobLocation::Local, async {
        println!("Before");

        // Wait till the next frame.
        wait_for_next_frame().await;

        println!("After");
    });

    // Emulate frame rate timer.
    let mut timer = Instant::now();

    // Emulate main loop with frame boundaries.
    // Here we just run until queues are empty,
    // but in your app you tell when to stop.
    while !jobs.queue_is_empty() || !next_frame_queue.read().unwrap().is_empty() {
        println!(
            "Submiting next frame jobs: {}",
            next_frame_queue.read().unwrap().len()
        );

        // At the beggining of the frame, we submit next aggregated jobs
        // to the main jobs queue. That way we implement typical coroutines.
        jobs.submit_queue(&next_frame_queue.read().unwrap());

        println!("Running local jobs");

        // After submitting, we run local jobs until main queue is empty.
        // If you run local queue once per frame, jobs on hitting pending would
        // never get chance to run again in the same frame, which we care about
        // when we need to progress jobs until next frame hits.
        while jobs.queue_filter_count(|_, location, _, _, _| *location == JobLocation::Local) > 0 {
            // We need to update timeout based on elapsed time to keep frame
            // rate stable. If we run local queue without timeout, jobs might
            // consume more time that frame budget allows. Or worse, if no next
            // frame wait is called in a job, job might block entire application.
            // And if it happen that some job will take more time than frame
            // budget allows, it automatically will get continued in next frame.
            let timeout = Duration::from_millis(16).saturating_sub(timer.elapsed());

            jobs.run_local_timeout(timeout);
        }

        // Refresh next frame timer.
        timer = Instant::now();
    }

    job.wait().unwrap();
}

const NEXT_FRAME_QUEUE: &str = "next_frame_queue";

// Waiting for next frame means we need to move running job from current queue
// to the next frame queue.
async fn wait_for_next_frame() {
    let Some(next_frame_queue) = meta::<JobQueue>(NEXT_FRAME_QUEUE).await else {
        return;
    };
    let Some(next_frame_queue) = next_frame_queue.read().map(|queue| queue.clone()) else {
        return;
    };
    move_to(JobLocation::Queue(next_frame_queue)).await;
}
