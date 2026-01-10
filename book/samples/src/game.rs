use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Default)]
pub enum GameStateChange {
    #[default]
    None,
    Switch(Box<dyn GameState>),
    Quit,
}

pub trait GameState {
    fn enter(&mut self) {}

    fn exit(&mut self) {}

    #[allow(unused_variables)]
    fn frame(&mut self, delta_time: Duration) -> GameStateChange {
        GameStateChange::None
    }
}

pub struct Game {
    state: Box<dyn GameState>,
    timer: Instant,
}

impl Drop for Game {
    fn drop(&mut self) {
        self.state.exit();
    }
}

impl Game {
    pub fn new(state: impl GameState + 'static) -> Self {
        let mut state = Box::new(state);
        state.enter();
        Self {
            state,
            timer: Instant::now(),
        }
    }

    pub fn frame(&mut self) -> bool {
        let elapsed = self.timer.elapsed();
        self.timer = Instant::now();
        match self.state.frame(elapsed) {
            GameStateChange::None => true,
            GameStateChange::Switch(game_state) => {
                self.state.exit();
                self.state = game_state;
                self.state.enter();
                true
            }
            GameStateChange::Quit => false,
        }
    }

    pub fn run_blocking(mut self) {
        let global_buffer = Arc::new(Mutex::new(String::new()));
        let old_hook = std::panic::take_hook();
        std::panic::set_hook({
            let global_buffer = global_buffer.clone();
            Box::new(move |info| {
                let mut global_buffer = global_buffer.lock().unwrap();
                if let Some(s) = info.payload_as_str() {
                    global_buffer.push_str(s);
                }
                if let Some(location) = info.location() {
                    global_buffer.push_str(&format!(
                        "\nPanic occurred in file '{}' at line {}",
                        location.file(),
                        location.line()
                    ));
                }
            })
        });
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| while self.frame() {}));
        std::panic::set_hook(old_hook);
        if result.is_err() {
            drop(self);
            eprintln!("Panic occured!");
            eprintln!("{}", global_buffer.lock().unwrap());
        }
    }
}
