use crossterm::{
    cursor::{Hide, MoveTo, Show},
    event::{DisableMouseCapture, EnableMouseCapture, Event, poll, read},
    execute, queue,
    style::Print,
    terminal::{
        Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
        enable_raw_mode,
    },
};
use std::{fmt::Display, io::Stdout, time::Duration};
use vek::Vec2;

pub struct Terminal {
    output: Stdout,
}

impl Drop for Terminal {
    fn drop(&mut self) {
        execute!(self.output, Show).unwrap();
        execute!(self.output, DisableMouseCapture).unwrap();
        execute!(self.output, LeaveAlternateScreen).unwrap();
        disable_raw_mode().unwrap();
    }
}

impl Default for Terminal {
    fn default() -> Self {
        Self::new(std::io::stdout())
    }
}

impl Terminal {
    pub fn new(mut output: Stdout) -> Self {
        enable_raw_mode().unwrap();
        execute!(output, EnterAlternateScreen).unwrap();
        execute!(output, EnableMouseCapture).unwrap();
        execute!(output, Hide).unwrap();
        Self { output }
    }

    pub fn events() -> impl Iterator<Item = Event> {
        std::iter::from_fn(|| {
            if poll(Duration::ZERO).unwrap() {
                read().ok()
            } else {
                None
            }
        })
    }

    pub fn begin_draw(&mut self, clear_screen: bool) {
        if clear_screen {
            self.clear_screen();
        }
    }

    pub fn end_draw(&mut self) {
        execute!(self.output).unwrap();
    }

    pub fn clear_screen(&mut self) {
        queue!(self.output, Clear(ClearType::All), MoveTo(0, 0)).unwrap();
    }

    pub fn output(&mut self) -> &mut Stdout {
        &mut self.output
    }

    pub fn display<D: Display>(&mut self, location: impl Into<Vec2<u16>>, content: D) {
        let location = location.into();
        queue!(self.output, MoveTo(location.x, location.y), Print(content)).unwrap();
    }
}
