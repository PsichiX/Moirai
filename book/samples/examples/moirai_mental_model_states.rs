use crossterm::{
    event::{Event, KeyCode},
    style::Stylize,
};
use moirai::{
    coroutine::{CompletionImportance, meta, with_importance},
    job::{JobHandle, JobLocation},
    third_party::intuicio_data::{managed::DynamicManagedLazy, shared::AsyncShared},
};
use moirai_book_samples::{
    coroutines::{Coroutines, next_frame},
    game::{Game, GameState, GameStateChange},
    terminal::Terminal,
    utils::is_key_pressed,
};
use std::time::Duration;

fn main() {
    Game::new(Example::default()).run_blocking();
}

const TIME_SCALE: f32 = 5.0;
const DAMAGE_STAMINA_MIN_REQUIREMENT: f32 = 5.0;
const LOG_CAPACITY: usize = 5;
const EVENTS_META: &str = "~events~";
const DELTA_TIME_META: &str = "~delta-time~";
const LOG_META: &str = "~log~";

struct Example {
    terminal: Terminal,
    coroutines: Coroutines,
    player: AsyncShared<Character>,
    enemy: AsyncShared<Character>,
    log: Vec<String>,
}

impl Default for Example {
    fn default() -> Self {
        let coroutines = Coroutines::default();

        let player = AsyncShared::new(Character::new("Player"));
        let enemy = AsyncShared::new(Character::new("Enemy"));

        let player_shared = player.clone();
        let enemy_shared = enemy.clone();
        player.write().unwrap().timeline(&coroutines, async move {
            /* ANCHOR: player-timeline */
            loop {
                let events = events().await;

                if is_key_pressed(&events, KeyCode::Enter) {
                    attack(&player_shared, &enemy_shared).await;
                } else if is_key_pressed(&events, KeyCode::Char(' ')) {
                    block(player_shared.clone()).await;
                } else {
                    update(&player_shared).await;
                }

                next_frame().await;
            }
            /* ANCHOR_END: player-timeline */
        });

        let player_shared = player.clone();
        let enemy_shared = enemy.clone();
        enemy.write().unwrap().timeline(&coroutines, async move {
            /* ANCHOR: enemy-timeline */
            loop {
                charge(&enemy_shared, 10.0).await;
                attack(&enemy_shared, &player_shared).await;
                charge(&enemy_shared, 10.0).await;
                block(enemy_shared.clone()).await;
                charge(&enemy_shared, 20.0).await;
                attack(&enemy_shared, &player_shared).await;
            }
            /* ANCHOR_END: enemy-timeline */
        });

        Self {
            terminal: Terminal::default(),
            coroutines,
            player,
            enemy,
            log: Vec::new(),
        }
    }
}

impl GameState for Example {
    fn frame(&mut self, mut delta_time: Duration) -> GameStateChange {
        let mut events = Terminal::events().collect::<Vec<_>>();

        draw_system(&mut self.terminal, &self.player, &self.enemy, &self.log);

        {
            let (events_lazy, _events_lifemtime) = DynamicManagedLazy::make(&mut events);
            let (dt_lazy, _dt_lifemtime) = DynamicManagedLazy::make(&mut delta_time);
            let (log_lazy, _log_lifetime) = DynamicManagedLazy::make(&mut self.log);
            self.coroutines.run_frame(
                [
                    (EVENTS_META.into(), events_lazy.into()),
                    (DELTA_TIME_META.into(), dt_lazy.into()),
                    (LOG_META.into(), log_lazy.into()),
                ]
                .into_iter()
                .collect(),
            );
        }

        {
            let player = self.player.write().unwrap();
            if !player.is_alive() {
                player.timeline.cancel();
            }
            let enemy = self.enemy.write().unwrap();
            if !enemy.is_alive() {
                enemy.timeline.cancel();
            }
        }

        if is_key_pressed(&events, KeyCode::Esc) {
            GameStateChange::Quit
        } else {
            GameStateChange::None
        }
    }
}

struct Character {
    name: String,
    health: f32,
    stamina: f32,
    timeline: JobHandle<()>,
}

impl Character {
    fn new(name: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            health: 100.0,
            stamina: 0.0,
            timeline: Default::default(),
        }
    }

    fn timeline(
        &mut self,
        coroutines: &Coroutines,
        timeline: impl Future<Output = ()> + Send + Sync + 'static,
    ) {
        self.timeline = coroutines
            .queue()
            .spawn(JobLocation::Local, timeline)
            .cancel_on_drop();
    }

    fn is_alive(&self) -> bool {
        self.health > 0.0
    }
}

async fn update(this: &AsyncShared<Character>) {
    let dt = delta_time().await.as_secs_f32() * TIME_SCALE;
    let mut this = this.write().unwrap();
    this.stamina = (this.stamina + dt).min(100.0);
}

async fn attack(this: &AsyncShared<Character>, target: &AsyncShared<Character>) {
    log(format!(
        "{} attacks {} with {:.02} hitpoints",
        this.read().unwrap().name,
        target.read().unwrap().name,
        this.read().unwrap().stamina
    ))
    .await;

    let mut this = this.write().unwrap();
    let mut target = target.write().unwrap();

    if this.stamina >= DAMAGE_STAMINA_MIN_REQUIREMENT {
        target.health = (target.health - this.stamina).max(0.0);
        this.stamina = 0.0;
    }
}

/* ANCHOR: charge */
async fn charge(this: &AsyncShared<Character>, target_stamina: f32) {
    loop {
        let dt = delta_time().await.as_secs_f32() * TIME_SCALE;
        {
            let mut this = this.write().unwrap();
            this.stamina = (this.stamina + dt).min(100.0);
            if this.stamina >= target_stamina {
                break;
            }
        }

        next_frame().await;
    }
}
/* ANCHOR_END: charge */

async fn block(this: AsyncShared<Character>) {
    log(format!(
        "{} blocks with {:.02} cooldown",
        this.read().unwrap().name,
        this.read().unwrap().stamina
    ))
    .await;

    let health = this.read().unwrap().health;

    loop {
        let dt = delta_time().await.as_secs_f32() * TIME_SCALE;
        {
            let mut this = this.write().unwrap();
            this.health = health;
            this.stamina = (this.stamina - dt).min(100.0);
            if this.stamina <= 0.0 {
                break;
            }
        }

        next_frame().await;
    }

    log(format!("{} stops blocking", this.read().unwrap().name)).await;
}

async fn parry(this: AsyncShared<Character>, target: AsyncShared<Character>, mut cooldown: f32) {
    let origin_health = this.read().unwrap().health;
    let damage = loop {
        let dt = delta_time().await.as_secs_f32() * TIME_SCALE;
        cooldown -= dt;
        if cooldown <= 0.0 {
            return;
        }

        let current_health = this.read().unwrap().health;
        if current_health < origin_health {
            break origin_health - current_health;
        }

        next_frame().await;
    };

    log(format!(
        "{} parries {} with {:.02} hitpoints",
        this.read().unwrap().name,
        target.read().unwrap().name,
        damage
    ))
    .await;

    let mut this = this.write().unwrap();
    let mut target = target.write().unwrap();

    target.health = (target.health - damage).max(0.0);
    this.stamina = 0.0;
    this.health = (this.health + damage).min(100.0);
}

fn draw_system(
    terminal: &mut Terminal,
    player: &AsyncShared<Character>,
    enemy: &AsyncShared<Character>,
    log: &[String],
) {
    terminal.begin_draw(true);

    let player = player.read().unwrap();
    let enemy = enemy.read().unwrap();

    match (player.is_alive(), enemy.is_alive()) {
        (true, true) => {
            terminal.display([1, 1], "Player:".green().bold());
            terminal.display([2, 2], format!("Health: {:.1}", player.health));
            terminal.display([2, 3], format!("Stamina: {:.1}", player.stamina));

            terminal.display([1, 6], "Enemy:".red().bold());
            terminal.display([2, 7], format!("Health: {:.1}", enemy.health));
            terminal.display([2, 8], format!("Stamina: {:.1}", enemy.stamina));

            terminal.display([20, 1], "Log:".underlined());
            for (i, log) in log.iter().rev().take(LOG_CAPACITY).enumerate() {
                terminal.display([21, 2 + i as u16], log);
            }
        }
        (false, false) => {
            terminal.display([1, 1], "It's a draw!".yellow().bold());
        }
        (true, false) => {
            terminal.display([1, 1], "You are victorious!".green().bold());
            terminal.display([2, 2], format!("Health: {:.1}", player.health));
        }
        (false, true) => {
            terminal.display([1, 1], "You have been defeated!".red().bold());
            terminal.display([2, 2], format!("Health: {:.1}", enemy.health));
        }
    }

    terminal.end_draw();
}

async fn delta_time() -> Duration {
    *meta::<Duration>(DELTA_TIME_META)
        .await
        .unwrap()
        .read()
        .unwrap()
}

async fn events() -> Vec<Event> {
    meta::<Vec<Event>>(EVENTS_META)
        .await
        .unwrap()
        .read()
        .unwrap()
        .clone()
}

async fn log(content: impl ToString) {
    meta::<Vec<String>>(LOG_META)
        .await
        .unwrap()
        .write()
        .unwrap()
        .push(content.to_string());
}

// ===

#[allow(dead_code)]
async fn example_with_parry(
    enemy_shared: AsyncShared<Character>,
    player_shared: AsyncShared<Character>,
) {
    /* ANCHOR: enemy-parry */
    loop {
        charge(&enemy_shared, 10.0).await;
        attack(&enemy_shared, &player_shared).await;
        charge(&enemy_shared, 10.0).await;

        let parry = parry(enemy_shared.clone(), player_shared.clone(), 10.0);
        let block = block(enemy_shared.clone());

        with_importance(vec![
            CompletionImportance::ignored(parry),
            CompletionImportance::required(block),
        ])
        .await;

        charge(&enemy_shared, 20.0).await;
        attack(&enemy_shared, &player_shared).await;
    }
    /* ANCHOR_END: enemy-parry */
}

#[allow(dead_code)]
async fn example_with_waiting_for_input(
    player: &AsyncShared<Character>,
    enemy: &AsyncShared<Character>,
) {
    /* ANCHOR: waiting-for-input */
    loop {
        let events = events().await;

        if is_key_pressed(&events, KeyCode::Enter) {
            attack(player, enemy).await;
        } else if is_key_pressed(&events, KeyCode::Char(' ')) {
            block(player.clone()).await;
        } else {
            update(player).await;
        }
    }
    /* ANCHOR_END: waiting-for-input */
}
