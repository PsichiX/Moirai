use intuicio_data::managed::DynamicManagedLazy;
use moirai::{coroutine::meta, jobs::Jobs};

fn main() {
    let jobs = Jobs::default();

    // Setting meta data that can be accessed by jobs.
    let mut value = 42usize;
    let (value_lazy, _value_lifetime) = DynamicManagedLazy::make(&mut value);
    jobs.set_meta("value", value_lazy);

    let result = jobs
        .spawn((), async {
            // Accessing meta data inside a job.
            let value = meta::<usize>("value").await.unwrap();
            *value.read().unwrap()
        })
        .unwrap()
        .wait()
        .unwrap();

    println!("Meta data value: {}", result);
}
