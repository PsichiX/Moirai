use bevy::{camera::ScalingMode, prelude::*};
use futures::join;
use moirai::{coroutine::spawn, job::JobLocation};
use moirai_bevy::{
    Coroutines, MoiraiPlugin,
    coroutine::{next_frame, wait_secs, world},
};

/* ANCHOR: main */
fn main() {
    let mut app = App::new();
    app.add_plugins((DefaultPlugins, MoiraiPlugin))
        .add_systems(Startup, setup);
    app.run();
}
/* ANCHOR_END: main */

/* ANCHOR: setup */
fn setup(
    mut commands: Commands,
    asset_server: Res<AssetServer>,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
    coroutines: Res<Coroutines>,
) {
    let font = asset_server.load("noto.ttf");

    commands.spawn((
        Camera2d,
        Projection::Orthographic(OrthographicProjection {
            scaling_mode: ScalingMode::FixedVertical {
                viewport_height: 500.0,
            },
            ..OrthographicProjection::default_2d()
        }),
    ));

    let player_dialogue = commands
        .spawn((
            Text2d::default(),
            TextFont {
                font: font.clone(),
                font_size: 20.0,
                ..Default::default()
            },
            TextLayout::new_with_justify(Justify::Center).with_no_wrap(),
            TextColor::WHITE,
            Transform::from_xyz(0.0, 50.0, 0.0),
        ))
        .id();

    let player = commands
        .spawn((
            Mesh2d(meshes.add(Circle::new(15.0))),
            MeshMaterial2d(materials.add(Color::linear_rgb(0.0, 0.47, 0.95))),
            Transform::from_xyz(-150.0, 0.0, 0.0),
        ))
        .add_child(player_dialogue)
        .id();

    let enemy_dialogue = commands
        .spawn((
            Text2d::default(),
            TextFont {
                font: font.clone(),
                font_size: 20.0,
                ..Default::default()
            },
            TextLayout::new_with_justify(Justify::Center).with_no_wrap(),
            TextColor::WHITE,
            Transform::from_xyz(0.0, 50.0, 0.0),
        ))
        .id();

    let enemy = commands
        .spawn((
            Mesh2d(meshes.add(Rectangle::new(30.0, 30.0))),
            MeshMaterial2d(materials.add(Color::linear_rgb(0.95, 0.16, 0.22))),
            Transform::from_xyz(100.0, 0.0, 0.0),
        ))
        .add_child(enemy_dialogue)
        .id();

    coroutines.jobs.spawn(
        JobLocation::Local,
        cutscene(player, player_dialogue, enemy, enemy_dialogue),
    );
}
/* ANCHOR_END: setup */

/* ANCHOR: cutscene */
async fn cutscene(player: Entity, player_dialogue: Entity, enemy: Entity, enemy_dialogue: Entity) {
    move_character(player, vec2(-100.0, 0.0), 50.0).await;

    timed_dialogue(enemy_dialogue, "You dare challenge me, mortal?", 2.0).await;

    move_character(player, vec2(-15.0, 0.0), 100.0).await;

    timed_dialogue(player_dialogue, "I will defeat you!", 2.0).await;

    move_character(enemy, vec2(15.0, 0.0), 80.0).await;

    timed_dialogue(enemy_dialogue, "Prepare to meet your maker!", 2.0).await;

    join!(
        spawn(
            JobLocation::Local,
            move_character(player, vec2(-50.0, 0.0), 150.0),
        ),
        spawn(
            JobLocation::Local,
            move_character(enemy, vec2(50.0, 0.0), 150.0),
        )
    );

    join!(
        move_character(player, vec2(0.0, 0.0), 200.0),
        move_character(enemy, vec2(0.0, 0.0), 200.0)
    );

    join!(
        move_character(player, vec2(-150.0, 0.0), 200.0),
        move_character(enemy, vec2(-15.0, 0.0), 50.0),
    );

    timed_dialogue(player_dialogue, "Argh! Such pain!", 2.0).await;

    timed_dialogue(enemy_dialogue, "Victory is mine!", 2.0).await;

    timed_dialogue(player_dialogue, "You've won!", 1.0).await;
    timed_dialogue(player_dialogue, "I surrender...!", 1.0).await;
}
/* ANCHOR_END: cutscene */

/* ANCHOR: dialogue */
async fn show_dialogue(dialogue: Entity, content: impl ToString) {
    let world = world().await;
    let world = &mut *world.write().unwrap();
    let mut text = world.get_mut::<Text2d>(dialogue).unwrap();
    text.0 = content.to_string();
}
/* ANCHOR_END: dialogue */

async fn hide_dialogue(dialogue: Entity) {
    let world = world().await;
    let world = &mut *world.write().unwrap();
    let mut text = world.get_mut::<Text2d>(dialogue).unwrap();
    text.0.clear();
}

async fn timed_dialogue(dialogue: Entity, text: impl ToString, duration: f32) {
    show_dialogue(dialogue, text).await;
    wait_secs(duration).await;
    hide_dialogue(dialogue).await;
}

async fn move_character(character: Entity, target: Vec2, speed: f32) {
    loop {
        {
            let world = world().await;
            let world = &mut *world.write().unwrap();
            let dt = world.resource::<Time>().delta_secs();

            let mut transform = world.get_mut::<Transform>(character).unwrap();
            let direction = (target - transform.translation.truncate()).normalize_or_zero();
            let distance = transform.translation.truncate().distance(target);

            let speed = speed * dt;
            if distance < speed {
                transform.translation = target.extend(transform.translation.z);
                break;
            } else {
                transform.translation += direction.extend(0.0) * speed;
            }
        }

        next_frame().await;
    }
}
