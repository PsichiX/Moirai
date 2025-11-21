use moirai::{
    coroutine::{spawn_on, yield_now},
    jobs::Jobs,
};

fn main() {
    Jobs::default().block_on(async move {
        let counter = spawn_on((), async move {
            let mut counter = 0;
            for _ in 0..10 {
                counter += 1;
                yield_now().await;
            }
            counter
        })
        .await
        .await
        .unwrap();

        println!("Counter: {}", counter);
    });
}
