use futures::join;
use macroquad::prelude::{
    coroutines::wait_seconds,
    scene::{Handle, Node, RefMut, add_node, get_node, set_camera},
    *,
};
use moirai::{coroutine::spawn, job::JobLocation, jobs::Jobs};

const ZOOMOUT: f32 = 200.0;

/* ANCHOR: main */
#[macroquad::main("Game")]
async fn main() {
    // Setup local-only jobs system, as we shouldn't use multithreading for
    // macroquad.
    let jobs = Jobs::local_only();

    // Create player and enemy characters as nodes in scene graph.
    let player = add_node(GameObject {
        position: vec2(-150.0, 0.0),
        shape: Shape::Circle { radius: 15.0 },
        color: BLUE,
        dialogue: None,
    });
    let enemy = add_node(GameObject {
        position: vec2(100.0, 0.0),
        shape: Shape::Square { size: 30.0 },
        color: RED,
        dialogue: None,
    });

    // Spawn cutscene coroutine to be played with Moirai.
    jobs.spawn(JobLocation::Local, cutscene(player, enemy));

    // Main game loop.
    loop {
        // Run local jobs to make progress on coroutines.
        jobs.run_local();

        clear_background(LIGHTGRAY);
        let camera = Camera2D {
            zoom: vec2(
                1.0 / ZOOMOUT * screen_height() / screen_width(),
                1.0 / ZOOMOUT,
            ),
            target: vec2(0.0, 0.0),
            ..Default::default()
        };
        set_camera(0, Some(camera));
        next_frame().await
    }
}
/* ANCHOR_END: main */

/* ANCHOR: cutscene */
async fn cutscene(player: Handle<GameObject>, enemy: Handle<GameObject>) {
    move_character(player, vec2(-100.0, 0.0), 50.0).await;

    timed_dialogue(enemy, "You dare challenge me, mortal?", 2.0).await;

    move_character(player, vec2(-15.0, 0.0), 100.0).await;

    timed_dialogue(player, "I will defeat you!", 2.0).await;

    move_character(enemy, vec2(15.0, 0.0), 80.0).await;

    timed_dialogue(enemy, "Prepare to meet your maker!", 2.0).await;

    wait_for_battle_start().await;

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

    timed_dialogue(player, "Argh! Such pain!", 2.0).await;

    timed_dialogue(enemy, "Victory is mine!", 2.0).await;

    timed_dialogue(player, "You've won!", 1.0).await;
    timed_dialogue(player, "I surrender...!", 1.0).await;
}
/* ANCHOR_END: cutscene */

/* ANCHOR: show-dialogue */
async fn show_dialogue(character: Handle<GameObject>, text: impl ToString) {
    get_node(character).dialogue = Some(text.to_string());
}
/* ANCHOR_END: show-dialogue */

async fn hide_dialogue(character: Handle<GameObject>) {
    get_node(character).dialogue = None;
}

async fn timed_dialogue(character: Handle<GameObject>, text: impl ToString, duration: f32) {
    show_dialogue(character, text).await;
    wait_seconds(duration).await;
    hide_dialogue(character).await;
}

async fn move_character(character: Handle<GameObject>, target: Vec2, speed: f32) {
    loop {
        {
            let mut node = get_node(character);
            let direction = (target - node.position).normalize_or_zero();
            let distance = node.position.distance(target);

            let dt = get_frame_time();
            let speed = speed * dt;
            if distance < speed {
                node.position = target;
                break;
            } else {
                node.position += direction * speed;
            }
        }

        next_frame().await;
    }
}

async fn wait_for_battle_start() {
    let dialogue = add_node(Dialogue {
        text: "Press SPACE to start the battle!".to_string(),
    });

    loop {
        if is_key_pressed(KeyCode::Space) {
            break;
        }
        next_frame().await;
    }

    get_node(dialogue).delete();
}

struct GameObject {
    position: Vec2,
    shape: Shape,
    color: Color,
    dialogue: Option<String>,
}

impl Node for GameObject {
    fn draw(node: RefMut<Self>)
    where
        Self: Sized,
    {
        match node.shape {
            Shape::Circle { radius } => {
                draw_circle(node.position.x, node.position.y, radius, node.color);
            }
            Shape::Square { size } => {
                draw_rectangle(
                    node.position.x - size / 2.0,
                    node.position.y - size / 2.0,
                    size,
                    size,
                    node.color,
                );
            }
        }

        if let Some(dialogue) = node.dialogue.as_ref() {
            let text_dimensions = measure_text(dialogue, None, 20, 1.0);
            draw_text(
                dialogue,
                node.position.x - text_dimensions.width / 2.0,
                node.position.y - 40.0,
                20.0,
                BLACK,
            );
        }
    }
}

enum Shape {
    Circle { radius: f32 },
    Square { size: f32 },
}

struct Dialogue {
    text: String,
}

impl Node for Dialogue {
    fn draw(node: RefMut<Self>)
    where
        Self: Sized,
    {
        let text_dimensions = measure_text(&node.text, None, 20, 1.0);
        draw_text(
            &node.text,
            -text_dimensions.width / 2.0,
            ZOOMOUT / 2.0 - text_dimensions.height - 20.0,
            20.0,
            BLACK,
        );
    }
}
