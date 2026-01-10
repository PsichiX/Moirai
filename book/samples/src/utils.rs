use crate::game::GameStateChange;
use crossterm::event::{Event, KeyCode};
use moirai::jobs::Jobs;
use textwrap::Options;
use vek::Vec2;

pub fn quit_if_jobs_completed(jobs: &Jobs) -> GameStateChange {
    if jobs.queue().is_empty() {
        GameStateChange::Quit
    } else {
        GameStateChange::None
    }
}

pub fn is_key_pressed(events: &[Event], key_code: KeyCode) -> bool {
    events
        .iter()
        .any(|event| matches!(event, Event::Key(key_event) if key_event.code == key_code))
}

pub fn text_wrap(text: &str, width: usize) -> String {
    textwrap::wrap(text, Options::new(width)).join("\n")
}

pub fn text_size(text: &str) -> Vec2<usize> {
    let lines: Vec<&str> = text.lines().collect();
    let height = lines.len();
    let width = lines
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0);
    Vec2::new(width, height)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_word_wrap() {
        let text = "This is a sample text that needs to be wrapped properly.";
        let wrapped = text_wrap(text, 10);
        let expected = "This is\na sample\ntext that\nneeds to\nbe wrapped\nproperly.";
        assert_eq!(wrapped, expected);

        let text = "Hi Abracadabra";
        let wrapped = text_wrap(text, 5);
        let expected = "Hi\nAbrac\nadabr\na";
        assert_eq!(wrapped, expected);
    }
}
