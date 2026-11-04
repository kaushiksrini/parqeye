use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptKind {
    GoToRow,
}

impl PromptKind {
    pub fn prefix(self) -> char {
        match self {
            Self::GoToRow => ':',
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandPrompt {
    pub kind: PromptKind,
    pub buffer: String,
    /// Byte offset into `buffer`. All editing helpers preserve a char boundary.
    pub cursor: usize,
    pub error: Option<String>,
}

impl CommandPrompt {
    pub fn new(kind: PromptKind) -> Self {
        Self {
            kind,
            buffer: String::new(),
            cursor: 0,
            error: None,
        }
    }

    pub fn handle_key_event(&mut self, key_event: KeyEvent) -> PromptEvent {
        match key_event.code {
            KeyCode::Esc => PromptEvent::Cancel,
            KeyCode::Enter => PromptEvent::Submit {
                kind: self.kind,
                input: self.buffer.clone(),
            },
            KeyCode::Backspace if self.buffer.is_empty() => PromptEvent::Cancel,
            KeyCode::Backspace => {
                self.backspace();
                PromptEvent::Continue
            }
            KeyCode::Delete => {
                self.delete();
                PromptEvent::Continue
            }
            KeyCode::Left => {
                self.move_left();
                PromptEvent::Continue
            }
            KeyCode::Right => {
                self.move_right();
                PromptEvent::Continue
            }
            KeyCode::Home => {
                self.cursor = 0;
                PromptEvent::Continue
            }
            KeyCode::End => {
                self.cursor = self.buffer.len();
                PromptEvent::Continue
            }
            KeyCode::Char('c') if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
                PromptEvent::Cancel
            }
            KeyCode::Char(character)
                if !key_event
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.buffer.insert(self.cursor, character);
                self.cursor += character.len_utf8();
                self.error = None;
                PromptEvent::Continue
            }
            _ => PromptEvent::Continue,
        }
    }

    fn backspace(&mut self) {
        if let Some(previous) = self.buffer[..self.cursor].chars().next_back() {
            let previous_len = previous.len_utf8();
            let previous_cursor = self.cursor - previous_len;
            self.buffer.drain(previous_cursor..self.cursor);
            self.cursor = previous_cursor;
            self.error = None;
        }
    }

    fn delete(&mut self) {
        if let Some(next) = self.buffer[self.cursor..].chars().next() {
            self.buffer
                .drain(self.cursor..self.cursor + next.len_utf8());
            self.error = None;
        }
    }

    fn move_left(&mut self) {
        if let Some(previous) = self.buffer[..self.cursor].chars().next_back() {
            self.cursor -= previous.len_utf8();
        }
    }

    fn move_right(&mut self) {
        if let Some(next) = self.buffer[self.cursor..].chars().next() {
            self.cursor += next.len_utf8();
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromptEvent {
    Continue,
    Cancel,
    Submit { kind: PromptKind, input: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VisualizeCommand {
    GoToRow(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandParseError {
    MissingRow,
    InvalidRow,
}

impl fmt::Display for CommandParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRow => write!(formatter, "enter a row number"),
            Self::InvalidRow => write!(formatter, "row must be a positive integer"),
        }
    }
}

pub fn parse_command(kind: PromptKind, input: &str) -> Result<VisualizeCommand, CommandParseError> {
    match kind {
        PromptKind::GoToRow => {
            let input = input.trim();
            if input.is_empty() {
                return Err(CommandParseError::MissingRow);
            }

            input
                .parse::<usize>()
                .ok()
                .filter(|row| *row > 0)
                .map(VisualizeCommand::GoToRow)
                .ok_or(CommandParseError::InvalidRow)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_one_based_row_number() {
        assert_eq!(
            parse_command(PromptKind::GoToRow, "42"),
            Ok(VisualizeCommand::GoToRow(42))
        );
    }

    #[test]
    fn rejects_empty_zero_and_non_numeric_rows() {
        assert_eq!(
            parse_command(PromptKind::GoToRow, ""),
            Err(CommandParseError::MissingRow)
        );
        assert_eq!(
            parse_command(PromptKind::GoToRow, "0"),
            Err(CommandParseError::InvalidRow)
        );
        assert_eq!(
            parse_command(PromptKind::GoToRow, "four"),
            Err(CommandParseError::InvalidRow)
        );
    }

    #[test]
    fn editor_inserts_at_and_moves_around_char_boundaries() {
        let mut prompt = CommandPrompt::new(PromptKind::GoToRow);
        prompt.handle_key_event(KeyEvent::new(KeyCode::Char('4'), KeyModifiers::NONE));
        prompt.handle_key_event(KeyEvent::new(KeyCode::Char('2'), KeyModifiers::NONE));
        prompt.handle_key_event(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE));
        prompt.handle_key_event(KeyEvent::new(KeyCode::Char('1'), KeyModifiers::NONE));
        assert_eq!(prompt.buffer, "412");
        assert_eq!(prompt.cursor, 2);

        prompt.handle_key_event(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        assert_eq!(prompt.buffer, "42");
        assert_eq!(prompt.cursor, 1);
    }
}
