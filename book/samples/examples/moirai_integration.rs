use moirai::jobs::Jobs;

/* ANCHOR: main */
fn main() {
    // Setup Moirai engine with default jobs system.
    let engine = Engine {
        jobs: Jobs::default(),
    };

    // Game frame loop.
    loop {
        // Typical game update logic would go here...

        // Do single Moirai local jobs run to make progress on coroutines at the
        // end of the frame. Single run suspends at any `.await` point.
        engine.jobs.run_local();
    }
}

struct Engine {
    jobs: Jobs,
}
/* ANCHOR_END: main */
