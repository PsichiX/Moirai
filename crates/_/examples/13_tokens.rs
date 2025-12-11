use std::time::Duration;

use moirai::{coroutine::acquire_token, jobs::Jobs};

fn main() {
    let jobs = Jobs::default();

    let a = jobs.spawn((), async {
        // Tokens allow to guard accessing shared resources without locking.
        // For example to ensure single file won't be read and written at
        // the same time. Tokens accept anything that implements `Hash`, so
        // strings, integers, or custom types can be used.
        println!("Acquiring token foo in job A");
        let _token = acquire_token(&"foo").await;

        println!("Job A does work");
        std::thread::sleep(Duration::from_millis(100));
    });

    let b = jobs.spawn((), async {
        println!("Acquiring token foo in job B");
        let _token = acquire_token(&"foo").await;

        println!("Job B does work");
        std::thread::sleep(Duration::from_millis(100));
    });

    a.wait().unwrap();
    b.wait().unwrap();
}
