use moirai::{coroutine::yield_now, job::JobLocation, jobs::Jobs};
use moirai_book_samples::{
    game::{Game, GameState, GameStateChange},
    utils::quit_if_jobs_completed,
};
use std::time::Duration;

fn main() {
    /* ANCHOR: setup */
    let jobs = Jobs::default();
    /* ANCHOR_END: setup */
    Game::new(Example { jobs }).run_blocking();
}

struct Example {
    jobs: Jobs,
}

impl GameState for Example {
    fn enter(&mut self) {
        /* ANCHOR: spawn-job */
        self.jobs.spawn(JobLocation::Local, async {
            let mut counter = 0;
            for _ in 0..10 {
                counter += 1;
                // Yield execution to allow other jobs to run.
                yield_now().await;
            }
            println!("Counter: {:?}", counter);
        });
        /* ANCHOR_END: spawn-job */
    }

    fn frame(&mut self, _delta_time: Duration) -> GameStateChange {
        /* ANCHOR: game-frame */
        self.jobs.run_local();
        /* ANCHOR_END: game-frame */

        quit_if_jobs_completed(&self.jobs)
    }
}
