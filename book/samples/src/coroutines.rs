use moirai::{
    coroutine::{meta, move_to},
    job::JobLocation,
    jobs::{Jobs, JobsMeta},
    queue::JobQueue,
    third_party::intuicio_data::managed::DynamicManagedLazy,
};

const NEXT_FRAME_QUEUE_META: &str = "~next-frame-queue~";

#[derive(Default)]
pub struct Coroutines {
    jobs: Jobs,
    next_frame_queue: JobQueue,
}

impl Coroutines {
    pub fn new(jobs: Jobs) -> Self {
        let next_frame_queue = JobQueue::default();
        next_frame_queue.set_catch_panics(jobs.queue().does_catch_panics());
        Self {
            jobs,
            next_frame_queue,
        }
    }

    pub fn catch_panics(self) -> Self {
        self.jobs.queue().set_catch_panics(true);
        self.next_frame_queue.set_catch_panics(true);
        self
    }

    pub fn run_frame(&mut self, meta: JobsMeta) {
        self.jobs.submit_queue(&self.next_frame_queue);

        let (queue_lazy, _lifetime) = DynamicManagedLazy::make(&mut self.next_frame_queue);
        meta.set(NEXT_FRAME_QUEUE_META, queue_lazy);

        while !self.jobs.queue().is_empty() {
            self.jobs.run_local_with_meta(meta.clone());
        }
    }

    pub fn queue(&self) -> &JobQueue {
        &self.next_frame_queue
    }
}

pub async fn next_frame() {
    let queue = meta::<JobQueue>(NEXT_FRAME_QUEUE_META)
        .await
        .unwrap()
        .read()
        .unwrap()
        .clone();
    move_to(JobLocation::Queue(queue)).await;
}
