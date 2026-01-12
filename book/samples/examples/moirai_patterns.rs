use moirai::{
    coroutine::{CompletionImportance, spawn, wait_time, with_all, with_any, with_importance},
    job::JobLocation,
    jobs::Jobs,
};
use moirai_book_samples::events::mpmc::channel;
use std::time::Duration;

fn main() {
    example(|jobs| {
        println!("--- Sequential ---");
        jobs.spawn(JobLocation::Local, sequential());
    });
    example(|jobs| {
        println!("--- Fire and Forget ---");
        jobs.spawn(JobLocation::Local, fire_and_forget());
    });
    example(|jobs| {
        println!("--- Parent-Child ---");
        jobs.spawn(JobLocation::Local, parent_child());
    });
    example(|jobs| {
        println!("--- Wait for All ---");
        jobs.spawn(JobLocation::Local, wait_for_all());
    });
    example(|jobs| {
        println!("--- Wait for Any ---");
        jobs.spawn(JobLocation::Local, wait_for_any());
    });
    example(|jobs| {
        println!("--- Wait for Some ---");
        jobs.spawn(JobLocation::Local, wait_for_some());
    });
    example(|jobs| {
        println!("--- Supervisor ---");
        jobs.spawn(JobLocation::Local, supervisor());
    });
    example(|jobs| {
        println!("--- Actor ---");
        jobs.spawn(JobLocation::Local, actor());
    });
}

fn example(f: impl FnOnce(&Jobs) + 'static) {
    let jobs = Jobs::default();
    f(&jobs);
    while !jobs.queue().is_empty() {
        jobs.run_local();
    }
}

async fn sequential() {
    /* ANCHOR: sequential */
    // Run work sequentially.
    log("Step 1").await;
    log("Step 2").await;
    log("Step 3").await;
    /* ANCHOR_END: sequential */
}

async fn fire_and_forget() {
    /* ANCHOR: fire-and-forget */
    log("Parent Start").await;

    // Spawn a child job.
    spawn(JobLocation::Local, async {
        log("Child Start").await;
        log("Child End").await;
    })
    .await;

    log("Parent End").await;
    /* ANCHOR_END: fire-and-forget */
}

async fn parent_child() {
    /* ANCHOR: parent-child */
    log("Parent Start").await;

    // Spawn a child job.
    let child_job = spawn(JobLocation::Local, async {
        log("Child Start").await;
        log("Child End").await;
    })
    .await;

    log("Parent Await").await;

    // Await the child job to complete before exiting this coroutine.
    child_job.await;

    log("Parent End").await;
    /* ANCHOR_END: parent-child */
}

async fn wait_for_all() {
    /* ANCHOR: wait-for-all */
    // Run multiple work in parallel and wait for all to complete.
    with_all(vec![
        Box::pin(log_delay("Work 1", 100)),
        Box::pin(log_delay("Work 2", 200)),
        Box::pin(log_delay("Work 3", 300)),
    ])
    .await;
    /* ANCHOR_END: wait-for-all */
}

async fn wait_for_any() {
    /* ANCHOR: wait-for-any */
    // Run multiple work in parallel and wait until quickest of them completes.
    with_any(vec![
        Box::pin(log_delay("Work 1", 100)),
        Box::pin(log_delay("Work 2", 200)),
        Box::pin(log_delay("Work 3", 300)),
    ])
    .await;
    /* ANCHOR_END: wait-for-any */
}

async fn wait_for_some() {
    /* ANCHOR: wait-for-some */
    // Run multiple work in parallel and wait until some of them complete, while
    // ignoring other that didn't had time and were not required to complete.
    with_importance(vec![
        CompletionImportance::ignored(Box::pin(log_delay("Work 1", 100))),
        CompletionImportance::required(Box::pin(log_delay("Work 2", 200))),
        CompletionImportance::ignored(Box::pin(log_delay("Work 3", 300))),
    ])
    .await;
    /* ANCHOR_END: wait-for-some */
}

async fn supervisor() {
    /* ANCHOR: supervisor */
    // Spawn a supervisor job that controls retries.
    let job = spawn(JobLocation::Local, async {
        // Supervise certain job to automatically restart when it ends unexpectedly.
        loop {
            // Spawn a supervised child job.
            spawn(JobLocation::Local, async {
                println!("Supervised job started.");

                // Emulate job that can end unexpectedly.
                wait_time(Duration::from_millis(rand::random_range(100..300))).await;
            })
            .await
            .await;
        }
    })
    .await;

    // Kill supervisor after some time.
    wait_time(Duration::from_secs(1)).await;
    job.cancel();
    job.await;
    /* ANCHOR_END: supervisor */
}

async fn actor() {
    /* ANCHOR: actor */
    // Define message type used for communication with spawned actor.
    enum Message {
        Tick,
        Terminate,
    }

    // Create communication channel.
    let (tx, rx) = channel::<Message>();

    // Spawn actor job to run in the background.
    let child_job = spawn(JobLocation::Local, async move {
        let mut ticks = 0;
        loop {
            // Do some work based on messages received from outside world.
            let message = rx.receive().await;
            match message {
                Message::Tick => {
                    ticks += 1;
                    println!("Ticks: {}", ticks);
                }
                Message::Terminate => {
                    break;
                }
            }
        }
        ticks
    })
    .await;

    // Control actor via sending messages.
    tx.send(Message::Tick).await;
    wait(300).await;

    tx.send(Message::Tick).await;
    wait(200).await;

    tx.send(Message::Tick).await;
    wait(100).await;

    tx.send(Message::Terminate).await;
    println!("Counted ticks: {}", child_job.await.unwrap());
    /* ANCHOR_END: actor */
}

async fn log(message: &str) {
    log_delay(message, 100).await;
}

async fn log_delay(message: &str, milliseconds: u64) {
    wait(milliseconds).await;
    println!("{}", message);
}

async fn wait(milliseconds: u64) {
    wait_time(Duration::from_millis(milliseconds)).await;
}
