use crossterm::{
    event::{Event, KeyCode},
    style::Stylize,
};
use moirai_book_samples::{
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

#[derive(Default)]
struct Example {
    terminal: Terminal,
    player: Character,
    enemy: Npc,
    log: Vec<String>,
}

impl GameState for Example {
    fn frame(&mut self, delta_time: Duration) -> GameStateChange {
        /* ANCHOR: frame */
        // Collect application input events that happened since last frame.
        let events = Terminal::events().collect::<Vec<_>>();

        // Run game systems
        if self.player.is_alive() && self.enemy.character.is_alive() {
            update_player_system(
                &events,
                &mut self.player,
                &mut self.enemy.character,
                &mut self.log,
                delta_time.as_secs_f32(),
            );
            update_enemy_system(
                &mut self.enemy,
                &mut self.player,
                &mut self.log,
                delta_time.as_secs_f32(),
            );
        }
        draw_system(
            &mut self.terminal,
            &self.player,
            &self.enemy.character,
            &self.log,
        );
        /* ANCHOR_END: frame */

        if is_key_pressed(&events, KeyCode::Esc) {
            GameStateChange::Quit
        } else {
            GameStateChange::None
        }
    }
}

struct Character {
    health: f32,
    stamina: f32,
    blocking_cooldown: f32,
}

impl Default for Character {
    fn default() -> Self {
        Self {
            health: 100.0,
            stamina: 0.0,
            blocking_cooldown: 0.0,
        }
    }
}

impl Character {
    fn is_alive(&self) -> bool {
        self.health > 0.0
    }

    fn is_blocking(&self) -> bool {
        self.blocking_cooldown > f32::EPSILON
    }

    fn update(&mut self, delta_time: f32) {
        if self.is_blocking() {
            self.blocking_cooldown = (self.blocking_cooldown - delta_time * TIME_SCALE).max(0.0);
        } else {
            self.stamina = (self.stamina + delta_time * TIME_SCALE).min(25.0);
        }
    }

    fn deal_damage(&mut self, target: &mut Character) -> bool {
        if !target.is_blocking() && self.stamina >= DAMAGE_STAMINA_MIN_REQUIREMENT {
            target.health = (target.health - self.stamina).max(0.0);
            self.stamina = 0.0;
            true
        } else {
            false
        }
    }

    fn block(&mut self) {
        self.blocking_cooldown = self.stamina;
        self.stamina = 0.0;
    }
}

#[derive(Default)]
struct Npc {
    character: Character,
    fight_phase_index: usize,
}

/* ANCHOR: fight-state */
enum NpcFightState {
    Charge { target_stamina: f32 },
    Attack,
    Block,
}
/* ANCHOR_END: fight-state */

/* ANCHOR: fight-pattern */
const NPC_FIGHT_PATTERN: &[NpcFightState] = &[
    NpcFightState::Charge {
        target_stamina: 10.0,
    },
    NpcFightState::Attack,
    NpcFightState::Charge {
        target_stamina: 10.0,
    },
    NpcFightState::Block,
    NpcFightState::Charge {
        target_stamina: 20.0,
    },
    NpcFightState::Attack,
];
/* ANCHOR_END: fight-pattern */

fn update_player_system(
    events: &[Event],
    player: &mut Character,
    enemy: &mut Character,
    log: &mut Vec<String>,
    delta_time: f32,
) {
    /* ANCHOR: update-player-system */
    // Update player character state, such as stamina regeneration and blocking cooldown.
    player.update(delta_time);

    // Perform actions based on inputs.
    if is_key_pressed(events, KeyCode::Enter) {
        if player.deal_damage(enemy) {
            log.push("Player attacking!".to_string());
        }
    } else if is_key_pressed(events, KeyCode::Char(' ')) {
        player.block();
        log.push("Player blocking!".to_string());
    }
    /* ANCHOR_END: update-player-system */
}

fn update_enemy_system(
    enemy: &mut Npc,
    player: &mut Character,
    log: &mut Vec<String>,
    delta_time: f32,
) {
    /* ANCHOR: update-enemy-system */
    // Update NPC character state, such as stamina regeneration and blocking cooldown.
    enemy.character.update(delta_time);

    // Get current NPC state or reset if out of bounds, to loop through pattern.
    let Some(state) = NPC_FIGHT_PATTERN.get(enemy.fight_phase_index) else {
        enemy.fight_phase_index = 0;
        return;
    };

    // Perform an action of currently active state.
    match state {
        // Stay idle while recharching stamina.
        NpcFightState::Charge { target_stamina } => {
            if enemy.character.stamina >= *target_stamina {
                enemy.fight_phase_index += 1;
            }
        }
        // Attack the player with accumulated stamina.
        NpcFightState::Attack => {
            if enemy.character.deal_damage(player) {
                log.push("Enemy attacking!".to_string());
            }
            enemy.fight_phase_index += 1;
        }
        // Block the player's incoming attack for time of accumulated stamina.
        NpcFightState::Block => {
            enemy.character.block();
            log.push("Enemy blocking!".to_string());
            enemy.fight_phase_index += 1;
        }
    }
    /* ANCHOR_END: update-enemy-system */
}

fn draw_system(terminal: &mut Terminal, player: &Character, enemy: &Character, log: &[String]) {
    terminal.begin_draw(true);

    match (player.is_alive(), enemy.is_alive()) {
        (true, true) => {
            terminal.display([1, 1], "Player:".green().bold());
            terminal.display([2, 2], format!("Health: {:.1}", player.health));
            terminal.display([2, 3], format!("Stamina: {:.1}", player.stamina));
            terminal.display([2, 4], format!("Cooldown: {:.1}", player.blocking_cooldown));

            terminal.display([1, 6], "Enemy:".red().bold());
            terminal.display([2, 7], format!("Health: {:.1}", enemy.health));
            terminal.display([2, 8], format!("Stamina: {:.1}", enemy.stamina));
            terminal.display([2, 9], format!("Cooldown: {:.1}", enemy.blocking_cooldown));

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
