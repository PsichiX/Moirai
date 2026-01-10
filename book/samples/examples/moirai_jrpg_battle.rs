use crossterm::{
    event::{Event, KeyCode},
    style::Stylize,
    terminal::size,
};
use moirai::{
    coroutine::{meta, spawn, wait_time, yield_now},
    job::JobLocation,
    jobs::Jobs,
    third_party::intuicio_data::{managed::DynamicManagedLazy, shared::AsyncShared},
};
use moirai_book_samples::{
    game::{Game, GameState, GameStateChange},
    terminal::Terminal,
    utils::{is_key_pressed, text_size, text_wrap},
};
use std::time::Duration;

fn main() {
    Game::new(Jrpg::default()).run_blocking();
}

const EVENTS_META: &str = "~events~";
const TURN_INDEX_META: &str = "~turn-index~";
const SELECTED_ACTION_META: &str = "~selected-action~";
const UI_SCREEN: &str = "~ui-screen~";
const LOG_META: &str = "~log~";
const STEP_DELAY: Duration = Duration::from_secs(1);
const HEALTH_CAPACITY: usize = 100;
const MANA_CAPACITY: usize = 100;

struct Jrpg {
    terminal: Terminal,
    jobs: Jobs,
    turn_index: usize,
    ui_screen: Box<dyn UiScreen>,
    selected_action: Option<Action>,
    log: Vec<String>,
}

impl Default for Jrpg {
    fn default() -> Self {
        /* ANCHOR: setup */
        let jobs = Jobs::default();

        let player = AsyncShared::new(Character::new("Player"));
        let enemy = AsyncShared::new(Character::new("Enemy"));

        jobs.queue()
            .spawn(JobLocation::Local, battle(player, enemy));
        /* ANCHOR_END: setup */

        Self {
            terminal: Terminal::default(),
            jobs,
            turn_index: 0,
            ui_screen: Box::new(LogsScreen),
            selected_action: None,
            log: Default::default(),
        }
    }
}

impl GameState for Jrpg {
    fn frame(&mut self, _delta_time: Duration) -> GameStateChange {
        /* ANCHOR: frame */
        let mut events = Terminal::events().collect::<Vec<_>>();

        self.ui_screen.update(&events, &mut self.selected_action);

        self.render_system();

        self.coroutines_system(&mut events);

        if is_key_pressed(&events, KeyCode::Esc) {
            GameStateChange::Quit
        } else {
            GameStateChange::None
        }
        /* ANCHOR_END: frame */
    }
}

impl Jrpg {
    fn render_system(&mut self) {
        self.terminal.begin_draw(true);

        self.ui_screen.render(&mut self.terminal, &self.log);

        self.terminal.end_draw();
    }

    fn coroutines_system(&mut self, events: &mut Vec<Event>) {
        /* ANCHOR: coroutines-system */
        let (events_lazy, _events_lifetime) = DynamicManagedLazy::make(events);
        let (turn_lazy, _turn_lifetime) = DynamicManagedLazy::make(&mut self.turn_index);
        let (action_lazy, _action_lifetime) = DynamicManagedLazy::make(&mut self.selected_action);
        let (ui_screen_lazy, _ui_screen_lifetime) = DynamicManagedLazy::make(&mut self.ui_screen);
        let (log_lazy, _log_lifetime) = DynamicManagedLazy::make(&mut self.log);

        self.jobs.run_local_with_meta(
            [
                (EVENTS_META.into(), events_lazy.into()),
                (TURN_INDEX_META.into(), turn_lazy.into()),
                (SELECTED_ACTION_META.into(), action_lazy.into()),
                (UI_SCREEN.into(), ui_screen_lazy.into()),
                (LOG_META.into(), log_lazy.into()),
            ]
            .into_iter()
            .collect(),
        );
        /* ANCHOR_END: coroutines-system */
    }
}

/* ANCHOR: battle */
async fn battle(player: AsyncShared<Character>, enemy: AsyncShared<Character>) {
    log("Battle starts!").await;

    // Battle turns loop.
    loop {
        if win_conditions(player.clone(), enemy.clone()).await {
            break;
        }

        present_information(player.clone(), enemy.clone()).await;

        player_turn(player.clone(), enemy.clone()).await;

        enemy_turn(enemy.clone(), player.clone()).await;

        go_to_next_turn().await;
    }

    log("Hit ESC to exit the game!").await;
}
/* ANCHOR_END: battle */

async fn win_conditions(player: AsyncShared<Character>, enemy: AsyncShared<Character>) -> bool {
    if enemy.read().unwrap().health == 0 {
        log("Enemy has been defeated! You win!").await;
        true
    } else if player.read().unwrap().health == 0 {
        log("Player has been defeated! Game over.").await;
        true
    } else {
        false
    }
}

async fn present_information(player: AsyncShared<Character>, enemy: AsyncShared<Character>) {
    log(format!(
        "* New turn #{}\n\
        - Player health: {}\n\
        - Player mana: {}\n\
        - Enemy health: {}",
        turn_index().await + 1,
        player.read().unwrap().health,
        player.read().unwrap().mana,
        enemy.read().unwrap().health,
    ))
    .await;

    wait_time(STEP_DELAY).await;
}

/* ANCHOR: player-turn */
async fn player_turn(player: AsyncShared<Character>, enemy: AsyncShared<Character>) {
    log("* Player turn.\nHit ENTER to take an action!").await;

    wait_for_key(KeyCode::Enter).await;

    loop {
        match selected_action().await {
            Action::SkipTurn => {
                wait_time(STEP_DELAY).await;
                return;
            }
            Action::Forfeit => {
                player.write().unwrap().health = 0;
                yield_now().await;
                return;
            }
            Action::SwordSwing => {
                sword_swing(player.clone(), enemy.clone()).await;
                return;
            }
            Action::PoisonSpell => {
                if poison_spell(player.clone(), enemy.clone()).await {
                    return;
                }
            }
            Action::FireSpell => {
                if fire_spell(player.clone(), enemy.clone()).await {
                    return;
                }
            }
            Action::CureSpell => {
                if cure_spell(player.clone()).await {
                    return;
                }
            }
        }

        yield_now().await;
    }
}
/* ANCHOR_END: player-turn */

/* ANCHOR: enemy-turn */
async fn enemy_turn(enemy: AsyncShared<Character>, player: AsyncShared<Character>) {
    log("* Enemy turn").await;

    wait_time(STEP_DELAY).await;

    let action_probabilities = [
        (4, Action::SwordSwing),
        (2, Action::PoisonSpell),
        (1, Action::FireSpell),
        (
            if enemy.read().unwrap().health < 50 {
                4
            } else {
                1
            },
            Action::CureSpell,
        ),
    ];

    loop {
        match select_random_action(&action_probabilities) {
            Action::SwordSwing => {
                sword_swing(enemy.clone(), player.clone()).await;
                return;
            }
            Action::PoisonSpell => {
                if poison_spell(enemy.clone(), player.clone()).await {
                    return;
                }
            }
            Action::FireSpell => {
                if fire_spell(enemy.clone(), player.clone()).await {
                    return;
                }
            }
            Action::CureSpell => {
                if cure_spell(enemy.clone()).await {
                    return;
                }
            }
            _ => {
                wait_time(STEP_DELAY).await;
                return;
            }
        }

        yield_now().await;
    }
}
/* ANCHOR_END: enemy-turn */

fn select_random_action(action_probabilities: &[(usize, Action)]) -> Action {
    let total_weight = action_probabilities
        .iter()
        .map(|(weight, _)| *weight)
        .sum::<usize>();
    let mut choice = rand::random_range(0..total_weight) % total_weight;

    for (weight, action) in action_probabilities.iter().copied() {
        if choice < weight {
            return action;
        } else {
            choice -= weight;
        }
    }

    Action::SkipTurn
}

/* ANCHOR: sword-swing */
async fn sword_swing(this: AsyncShared<Character>, target: AsyncShared<Character>) {
    {
        let mut target = target.write().unwrap();
        target.health = target.health.saturating_sub(10).max(0);
    }

    log(format!(
        "{} swings sword at {}",
        this.read().unwrap().name,
        target.read().unwrap().name
    ))
    .await;

    wait_time(STEP_DELAY).await;
}
/* ANCHOR_END: sword-swing */

/* ANCHOR: poison-spell */
async fn poison_spell(this: AsyncShared<Character>, target: AsyncShared<Character>) -> bool {
    let cast = {
        let mut this = this.write().unwrap();
        if let Some(mana) = this.mana.checked_sub(30) {
            this.mana = mana;
            true
        } else {
            false
        }
    };

    if cast {
        spawn(JobLocation::Local, poison_spell_effect(target.clone())).await;

        log(format!(
            "{} casts poison spell at {}",
            this.read().unwrap().name,
            target.read().unwrap().name
        ))
        .await;

        wait_time(STEP_DELAY).await;
    }
    cast
}
/* ANCHOR_END: poison-spell */

/* ANCHOR: poison-spell-effect */
async fn poison_spell_effect(target: AsyncShared<Character>) {
    for index in 0..6 {
        {
            let mut target = target.write().unwrap();
            target.health = target.health.saturating_sub(5).max(0);
        }

        log(format!(
            "Poison status damage dealt to {} ({}/6)",
            target.read().unwrap().name,
            index + 1,
        ))
        .await;

        next_turn().await;
    }
}
/* ANCHOR_END: poison-spell-effect */

/* ANCHOR: fire-spell */
async fn fire_spell(this: AsyncShared<Character>, target: AsyncShared<Character>) -> bool {
    // Attempt to cast the spell by checking and deducting mana.
    let cast = {
        let mut this = this.write().unwrap();
        if let Some(mana) = this.mana.checked_sub(40) {
            this.mana = mana;
            true
        } else {
            false
        }
    };

    if cast {
        // Spawn the fire spell effect as a concurrent job to be ran in the
        // background, alongside of battle loop job, and await until it completes.
        spawn(JobLocation::Local, fire_spell_effect(target.clone())).await;

        log(format!(
            "{} casts fire spell at {}",
            this.read().unwrap().name,
            target.read().unwrap().name
        ))
        .await;

        wait_time(STEP_DELAY).await;
    }
    cast
}
/* ANCHOR_END: fire-spell */

/* ANCHOR: fire-spell-effect */
async fn fire_spell_effect(target: AsyncShared<Character>) {
    // Run this effect for 2 battle turns.
    for index in 0..2 {
        {
            // Deal high damage to target.
            let mut target = target.write().unwrap();
            target.health = target.health.saturating_sub(20).max(0);
        }

        log(format!(
            "Fire status damage dealt to {} ({}/2)",
            target.read().unwrap().name,
            index + 1,
        ))
        .await;

        // Wait until next turn to apply next tick of damage.
        next_turn().await;
    }
}
/* ANCHOR_END: fire-spell-effect */

/* ANCHOR: cure-spell */
async fn cure_spell(target: AsyncShared<Character>) -> bool {
    let cast = {
        let mut target = target.write().unwrap();
        if let Some(mana) = target.mana.checked_sub(25) {
            target.mana = mana;
            target.health = (target.health + 25).min(HEALTH_CAPACITY);
            true
        } else {
            false
        }
    };

    if cast {
        log(format!(
            "{} casts cure spell on itself",
            target.read().unwrap().name
        ))
        .await;

        wait_time(STEP_DELAY).await;
    }
    cast
}
/* ANCHOR_END: cure-spell */

const ACTIONS: [Action; 6] = [
    Action::SkipTurn,
    Action::Forfeit,
    Action::SwordSwing,
    Action::PoisonSpell,
    Action::FireSpell,
    Action::CureSpell,
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Action {
    SkipTurn,
    Forfeit,
    SwordSwing,
    PoisonSpell,
    FireSpell,
    CureSpell,
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Action::SkipTurn => "Skip Turn",
            Action::Forfeit => "Forfeit",
            Action::SwordSwing => "Sword Swing",
            Action::PoisonSpell => "Poison Spell",
            Action::FireSpell => "Fire Spell",
            Action::CureSpell => "Cure Spell",
        };
        write!(f, "{}", text)
    }
}

trait UiScreen {
    #[allow(unused_variables)]
    fn update(&mut self, events: &[Event], selected_action: &mut Option<Action>) {}

    #[allow(unused_variables)]
    fn render(&self, terminal: &mut Terminal, log: &[String]) {}
}

struct LogsScreen;

impl UiScreen for LogsScreen {
    fn render(&self, terminal: &mut Terminal, log: &[String]) {
        let (w, h) = size().unwrap();
        let mut y = 0;

        for (index, text) in log.iter().rev().enumerate() {
            let text = text_wrap(text, w as usize);
            let size = text_size(&text);
            if y + size.y > h as usize {
                break;
            }

            let text = if index == 0 {
                text.white().bold()
            } else {
                text.white().italic()
            };
            terminal.display([0, y as _], text);
            y += size.y + 1;
        }
    }
}

#[derive(Default)]
struct TakeActionScreen {
    selected_index: usize,
}

impl UiScreen for TakeActionScreen {
    /* ANCHOR: take-action-screen-update */
    fn update(&mut self, events: &[Event], selected_action: &mut Option<Action>) {
        if is_key_pressed(events, KeyCode::Up) {
            self.selected_index = (self.selected_index + ACTIONS.len() - 1) % ACTIONS.len();
        } else if is_key_pressed(events, KeyCode::Down) {
            self.selected_index = (self.selected_index + 1) % ACTIONS.len();
        } else if is_key_pressed(events, KeyCode::Enter) {
            *selected_action = Some(ACTIONS[self.selected_index]);
        }
    }
    /* ANCHOR_END: take-action-screen-update */

    fn render(&self, terminal: &mut Terminal, _log: &[String]) {
        let (w, _) = size().unwrap();
        let mut y = 0;

        let text = text_wrap("Your turn, take an action:", w as usize);
        let size = text_size(&text);

        terminal.display([0, y as _], text.green());

        y += size.y + 1;

        for (index, action) in ACTIONS.iter().enumerate() {
            let text = if index == self.selected_index {
                format!("> {}", action)
            } else {
                format!("  {}", action)
            };

            let text = text_wrap(&text, w as usize);
            let size = text_size(&text);

            let text = if index == self.selected_index {
                text.black().on_white().bold()
            } else {
                text.white().on_dark_grey().italic()
            };

            terminal.display([0, y as _], text);
            y += size.y + 1;
        }
    }
}

struct Character {
    name: String,
    health: usize,
    mana: usize,
}

impl Character {
    fn new(name: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            health: HEALTH_CAPACITY,
            mana: MANA_CAPACITY,
        }
    }
}

/* ANCHOR: go-to-next-turn */
async fn go_to_next_turn() {
    *meta::<usize>(TURN_INDEX_META)
        .await
        .unwrap()
        .write()
        .unwrap() += 1;

    yield_now().await;
}
/* ANCHOR_END: go-to-next-turn */

async fn turn_index() -> usize {
    *meta::<usize>(TURN_INDEX_META)
        .await
        .unwrap()
        .read()
        .unwrap()
}

/* ANCHOR: next-turn */
async fn next_turn() {
    let index = turn_index().await;
    while turn_index().await <= index {
        yield_now().await;
    }
}
/* ANCHOR_END: next-turn */

async fn events() -> Vec<Event> {
    meta::<Vec<Event>>(EVENTS_META)
        .await
        .unwrap()
        .read()
        .unwrap()
        .clone()
}

/* ANCHOR: wait-for-key */
async fn wait_for_key(key: KeyCode) {
    loop {
        let events = events().await;
        if is_key_pressed(&events, key) {
            return;
        }

        // Yield to allow other coroutines to run (we suspend this coroutine).
        // Without suspending, this coroutine will block entirely and prevent
        // other coroutines from running in local thread (and all our coroutines
        // are running in local thread, so deadlock guaranteed).
        yield_now().await;
    }
}
/* ANCHOR_END: wait-for-key */

/* ANCHOR: selected-action */
async fn selected_action() -> Action {
    // Change UI to take action screen.
    // We hold current UI screen reference available in meta storage to be able
    // to access it at any time during coroutine work.
    *meta::<Box<dyn UiScreen>>(UI_SCREEN)
        .await
        .unwrap()
        .write()
        .unwrap() = Box::new(TakeActionScreen::default());

    loop {
        // Check if an action has been selected from the list.
        // Currently selected action by the user is stored in meta storage.
        let action = meta::<Option<Action>>(SELECTED_ACTION_META)
            .await
            .unwrap()
            .write()
            .unwrap()
            .take();

        if let Some(action) = action {
            // Change UI back to logs screen.
            *meta::<Box<dyn UiScreen>>(UI_SCREEN)
                .await
                .unwrap()
                .write()
                .unwrap() = Box::new(LogsScreen);

            // Return the selected action.
            return action;
        }

        yield_now().await;
    }
}
/* ANCHOR_END: selected-action */

async fn log(content: impl ToString) {
    meta::<Vec<String>>(LOG_META)
        .await
        .unwrap()
        .write()
        .unwrap()
        .push(content.to_string());

    yield_now().await;
}
