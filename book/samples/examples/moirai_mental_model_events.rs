use crossterm::event::KeyCode;
use moirai::job::{JobHandle, JobLocation};
use moirai_book_samples::{
    coroutines::{Coroutines, next_frame},
    events::oneshot::{Receiver, Sender, channel},
    game::{Game, GameState, GameStateChange},
    terminal::Terminal,
    utils::is_key_pressed,
};
use std::time::Duration;

fn main() {
    Game::new(Example::default()).run_blocking();
}

struct Example {
    _terminal: Terminal,
    coroutines: Coroutines,
    _quest: JobHandle<()>,
    go_to_events: Sender<String>,
    pick_up_object: Sender<String>,
    quest_done: Receiver<()>,
}

impl Default for Example {
    fn default() -> Self {
        let coroutines = Coroutines::default();
        let (go_to_events_sender, go_to_events_receiver) = channel();
        let (pick_up_object_sender, pick_up_object_receiver) = channel();
        let (quest_done_sender, quest_done_receiver) = channel();

        let quest = coroutines
            .queue()
            .spawn(JobLocation::Local, async move {
                /* ANCHOR: timeline */
                println!("Starting quest...");

                wait_for_event("tavern", &go_to_events_receiver).await;
                println!("Arrived at the tavern.");

                wait_for_event("food", &pick_up_object_receiver).await;
                println!("Picked up food.");

                wait_for_event("castle", &go_to_events_receiver).await;
                println!("Arrived at the castle.");

                wait_for_event("drink", &pick_up_object_receiver).await;
                println!("Picked up drink.");

                quest_done_sender.send(());
                println!("Quest completed!");
                /* ANCHOR_END: timeline */
            })
            .cancel_on_drop();

        Self {
            _terminal: Terminal::default(),
            coroutines,
            _quest: quest,
            go_to_events: go_to_events_sender,
            pick_up_object: pick_up_object_sender,
            quest_done: quest_done_receiver,
        }
    }
}

impl GameState for Example {
    fn frame(&mut self, _delta_time: Duration) -> GameStateChange {
        let events = Terminal::events().collect::<Vec<_>>();

        if is_key_pressed(&events, KeyCode::Char('q')) {
            self.go_to_events.send("tavern".to_string());
            println!("> go to tavern.");
        }

        if is_key_pressed(&events, KeyCode::Char('a')) {
            self.go_to_events.send("castle".to_string());
            println!("> go to castle.");
        }

        if is_key_pressed(&events, KeyCode::Char('w')) {
            self.pick_up_object.send("food".to_string());
            println!("> pick up food.");
        }

        if is_key_pressed(&events, KeyCode::Char('s')) {
            self.pick_up_object.send("drink".to_string());
            println!("> pick up drink.");
        }

        self.coroutines.run_frame(Default::default());

        if self.quest_done.try_recv().is_some() {
            println!("All done! Press ESC to quit.");
        }

        if is_key_pressed(&events, KeyCode::Esc) {
            GameStateChange::Quit
        } else {
            GameStateChange::None
        }
    }
}

async fn wait_for_event(id: &str, events: &Receiver<String>) {
    loop {
        if let Some(event) = events.try_recv()
            && event == id
        {
            return;
        }
        next_frame().await;
    }
}
