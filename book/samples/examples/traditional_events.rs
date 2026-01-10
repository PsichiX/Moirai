use crossterm::{
    event::{Event, KeyCode},
    style::Stylize,
    terminal::size,
};
use moirai_book_samples::{
    events::Events,
    game::{Game, GameState, GameStateChange},
    terminal::Terminal,
    utils::{is_key_pressed, text_size, text_wrap},
};
use std::{collections::HashMap, sync::mpsc::Sender, time::Duration};

fn main() {
    Game::new(Example::default()).run_blocking();
}

struct Example {
    terminal: Terminal,
    events: Events<DialogueEvent>,
    dialogue: DialogueWidget,
    conversation: Conversation,
}

impl Default for Example {
    fn default() -> Self {
        /* ANCHOR: conversation */
        let conversation = Conversation::default()
            .point(
                "start",
                ConversationPoint::new("Hello, Adventurer!\nWhere would you like to go?")
                    .option(ConversationOption::new("Tavern", "tavern"))
                    .option(ConversationOption::new("Forest", "forest"))
                    .option(ConversationOption::new("Bed", "bed")),
            )
            .point(
                "tavern",
                ConversationPoint::new("You entered the tavern and found a cozy spot to rest.")
                    .option(ConversationOption::new("Order a beer", "beer"))
                    .option(ConversationOption::new("Exit", "start")),
            )
            .point(
                "beer",
                ConversationPoint::new("You ordered a refreshing beer and enjoyed your time!")
                    .option(ConversationOption::new("Pay and leave", "pay"))
                    .option(ConversationOption::new("Don't pay and leave", "no-pay")),
            )
            .point(
                "pay",
                ConversationPoint::new("You paid the bartender and left the tavern.")
                    .option(ConversationOption::new("Exit", "start")),
            )
            .point(
                "no-pay",
                ConversationPoint::new(
                    "The bartender caught you! You had to run away to the forest!",
                )
                .option(ConversationOption::new("Run", "forest")),
            )
            .point(
                "forest",
                ConversationPoint::new("You ventured into the forest. Rogue wolf appeared!")
                    .option(ConversationOption::new("Fight wolf with sword", "fight"))
                    .option(ConversationOption::new("Run back to town", "start")),
            )
            .point(
                "fight",
                ConversationPoint::new("You bravely fought the wolf and won!")
                    .option(ConversationOption::new("Return to town", "start")),
            )
            .point(
                "bed",
                ConversationPoint::new("You went to sleep. Next day, you feel refreshed!")
                    .option(ConversationOption::new("Wake up", "start")),
            );
        /* ANCHOR_END: conversation */

        let events = Events::default();
        let on_confirm = events.sender();
        Self {
            terminal: Terminal::default(),
            events,
            dialogue: DialogueWidget::new("start", on_confirm),
            conversation,
        }
    }
}

impl GameState for Example {
    fn frame(&mut self, _delta_time: Duration) -> GameStateChange {
        let events = Terminal::events().collect::<Vec<_>>();

        self.dialogue.handle_input(&events, &self.conversation);

        /* ANCHOR: receive-events */
        // Receive all pending events sent from UI.
        for event in self.events.receive() {
            match event {
                DialogueEvent::ShowDialogue { id } => {
                    // We got an event from dialogue widget, so we update said
                    // widget to show new conversation point.
                    self.dialogue = DialogueWidget::new(id, self.events.sender());
                }
            }
        }
        /* ANCHOR_END: receive-events */

        self.terminal.begin_draw(true);
        self.dialogue.draw(&mut self.terminal, &self.conversation);
        self.terminal.end_draw();

        if is_key_pressed(&events, KeyCode::Esc) {
            GameStateChange::Quit
        } else {
            GameStateChange::None
        }
    }
}

enum DialogueEvent {
    ShowDialogue { id: String },
}

/* ANCHOR: widget */
struct DialogueWidget {
    // Current conversation point ID.
    id: String,
    // Currently selected option index.
    index: usize,
    // Sender to emit events when an option is confirmed.
    on_confirm: Sender<DialogueEvent>,
}
/* ANCHOR_END: widget */

impl DialogueWidget {
    fn new(id: impl ToString, on_confirm: Sender<DialogueEvent>) -> Self {
        Self {
            id: id.to_string(),
            index: 0,
            on_confirm,
        }
    }

    fn handle_input(&mut self, events: &[Event], conversation: &Conversation) {
        /* ANCHOR: confirm-selection */
        let Some(conversation_point) = conversation.get(&self.id) else {
            return;
        };

        // Move to previous option.
        if is_key_pressed(events, KeyCode::Up) {
            self.index = (self.index + conversation_point.options.len() - 1)
                % conversation_point.options.len();
        } else
        // Move to next option.
        if is_key_pressed(events, KeyCode::Down) {
            self.index = (self.index + 1) % conversation_point.options.len();
        } else
        // Confirm selection.
        if is_key_pressed(events, KeyCode::Enter) {
            let id = conversation_point.options[self.index].jump_to.clone();
            self.on_confirm
                .send(DialogueEvent::ShowDialogue { id })
                .unwrap();
            self.index = 0;
        }
        /* ANCHOR_END: confirm-selection */
    }

    fn draw(&self, terminal: &mut Terminal, conversation: &Conversation) {
        let Some(conversation_point) = conversation.get(&self.id) else {
            return;
        };
        let (w, _) = size().unwrap();

        let message = text_wrap(&conversation_point.message, w as usize);
        terminal.display([0, 0], &message);
        let mut y = text_size(&message).y;
        terminal.display([0, y as u16], "-".repeat(w as usize));
        y += 1;

        for (i, option) in conversation_point.options.iter().enumerate() {
            let prefix = if i == self.index { "> " } else { "  " };
            let content = text_wrap(&format!("{}{}", prefix, option.text), w as usize - 1);
            let height = text_size(&content).y;
            let content = if i == self.index {
                content.on_white().black().bold().italic()
            } else {
                content.on_black().white()
            };
            terminal.display([0, y as u16], content);
            y += height;
        }
    }
}

#[derive(Clone)]
struct ConversationPoint {
    message: String,
    options: Vec<ConversationOption>,
}

impl ConversationPoint {
    fn new(message: impl ToString) -> Self {
        Self {
            message: message.to_string(),
            options: Default::default(),
        }
    }

    fn option(mut self, option: ConversationOption) -> Self {
        self.options.push(option);
        self
    }
}

#[derive(Clone)]
struct ConversationOption {
    text: String,
    jump_to: String,
}

impl ConversationOption {
    fn new(text: impl ToString, jump_to: impl ToString) -> Self {
        Self {
            text: text.to_string(),
            jump_to: jump_to.to_string(),
        }
    }
}

#[derive(Default)]
struct Conversation {
    points: HashMap<String, ConversationPoint>,
}

impl Conversation {
    fn point(mut self, id: impl ToString, point: ConversationPoint) -> Self {
        self.points.insert(id.to_string(), point);
        self
    }

    fn get(&self, id: &str) -> Option<&ConversationPoint> {
        self.points.get(id)
    }
}
