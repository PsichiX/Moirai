use bevy::{
    app::{App, Plugin, Update},
    ecs::{resource::Resource, world::World},
};
use moirai::{
    jobs::{Jobs, JobsMeta},
    queue::JobQueue,
    third_party::intuicio_data::managed::ManagedLazy,
};

const WORLD_META: &str = "~world~";
const NEXT_FRAME_META: &str = "~next-frame~";

pub struct MoiraiPlugin;

impl Plugin for MoiraiPlugin {
    fn build(&self, app: &mut App) {
        app.insert_resource(Coroutines::default());
        app.add_systems(Update, coroutine_system);
    }
}

#[derive(Resource)]
pub struct Coroutines {
    pub jobs: Jobs,
    next_frame_queue: JobQueue,
}

impl Default for Coroutines {
    fn default() -> Self {
        Self::new(Jobs::local_only())
    }
}

impl Coroutines {
    pub fn new(jobs: Jobs) -> Self {
        Self {
            jobs,
            next_frame_queue: Default::default(),
        }
    }
}

fn coroutine_system(world: &mut World) {
    let mut next_frame = world.resource::<Coroutines>().next_frame_queue.clone();

    let (world_lazy, _world_lifetime) = ManagedLazy::make(world);
    let (next_frame_lazy, _next_frame_lifetime) = ManagedLazy::make(&mut next_frame);
    let meta = JobsMeta::default()
        .with(WORLD_META, world_lazy.into_dynamic())
        .with(NEXT_FRAME_META, next_frame_lazy.into_dynamic());

    {
        let coroutines = world.resource::<Coroutines>();
        coroutines.jobs.submit_queue(&coroutines.next_frame_queue);
        while !coroutines.jobs.queue().is_empty() {
            coroutines.jobs.run_local_with_meta(meta.clone());
        }
    }
}

pub mod coroutine {
    use super::*;
    use bevy::time::Time;
    use moirai::{
        coroutine::{meta, move_to},
        job::JobLocation,
    };

    pub async fn world() -> ManagedLazy<World> {
        meta::<World>(WORLD_META).await.unwrap()
    }

    pub async fn next_frame() {
        let next_frame_queue = meta::<JobQueue>(NEXT_FRAME_META)
            .await
            .unwrap()
            .read()
            .unwrap()
            .clone();

        move_to(JobLocation::Queue(next_frame_queue)).await;
    }

    pub async fn wait_secs(seconds: f32) {
        let start = world()
            .await
            .read()
            .unwrap()
            .resource::<Time>()
            .elapsed_secs();

        loop {
            let now = world()
                .await
                .read()
                .unwrap()
                .resource::<Time>()
                .elapsed_secs();

            if now - start >= seconds {
                break;
            }

            next_frame().await;
        }
    }
}
