use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::text::Span;
use regex::{RegexBuilder, RegexSetBuilder};
use regex_syntax::{
    ast::{Ast, parse::Parser},
    hir::translate::TranslatorBuilder,
};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptKind {
    GoToRow,
    Columns,
}

impl PromptKind {
    pub fn prefix(self) -> char {
        match self {
            Self::GoToRow => ':',
            Self::Columns => '*',
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
    /// Candidate source indices, resolved only when column-filter input changes.
    pub column_matches: Option<Vec<usize>>,
}

impl CommandPrompt {
    pub fn new(kind: PromptKind) -> Self {
        Self {
            kind,
            buffer: String::new(),
            cursor: 0,
            error: None,
            column_matches: None,
        }
    }

    /// A horizontally scrolled input slice and its cursor's terminal-cell offset.
    pub fn visible_input(&self, width: u16) -> (&str, u16) {
        let mut start = self.cursor;
        for (offset, _) in self.buffer[..self.cursor].char_indices().rev() {
            if Span::raw(&self.buffer[offset..self.cursor]).width() >= usize::from(width) {
                break;
            }
            start = offset;
        }
        let cursor = Span::raw(&self.buffer[start..self.cursor]).width() as u16;
        (&self.buffer[start..], cursor)
    }

    pub fn resolve_columns(&mut self, names: &[String]) {
        match match_columns(&self.buffer, names) {
            Ok(indices) => {
                self.column_matches = Some(indices);
                self.error = None;
            }
            Err(error) => {
                self.column_matches = None;
                self.error = Some(error);
            }
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
    FilterColumns(String),
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
        PromptKind::Columns => Ok(VisualizeCommand::FilterColumns(input.to_string())),
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

pub fn match_columns(input: &str, names: &[String]) -> Result<Vec<usize>, String> {
    if input.is_empty() {
        return Ok((0..names.len()).collect());
    }
    let regex = RegexBuilder::new(input)
        .case_insensitive(true)
        .build()
        .map_err(invalid_regex)?;
    // Parse syntax before optimization can merge, factor, or reorder branches.
    // Grouped alternatives, escaped pipes and character classes remain intact.
    let ast = Parser::new().parse(input).map_err(invalid_regex)?;
    let indices: Vec<_> = if let Ast::Alternation(alternation) = &ast {
        let mut translator = TranslatorBuilder::new().case_insensitive(true).build();
        let patterns = alternation
            .asts
            .iter()
            .map(|branch| {
                // Reuse the translator: unscoped flags in an earlier branch also
                // affect later branches. HIR prints those flags as explicit matches.
                translator
                    .translate(input, branch)
                    .map(|hir| hir.to_string())
                    .map_err(invalid_regex)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let regexes = RegexSetBuilder::new(&patterns)
            .build()
            .map_err(invalid_regex)?;
        let mut buckets = vec![Vec::new(); patterns.len()];
        for (index, name) in names.iter().enumerate() {
            // First matching alternative owns the column, even if a later
            // alternative matches text earlier in the name. No duplicates.
            if let Some(branch) = regexes.matches(name).iter().next() {
                buckets[branch].push(index);
            }
        }
        buckets.into_iter().flatten().collect()
    } else {
        names
            .iter()
            .enumerate()
            .filter_map(|(index, name)| regex.is_match(name).then_some(index))
            .collect()
    };
    if indices.is_empty() {
        Err("No columns match; edit the regex or clear it to show all columns".to_string())
    } else {
        Ok(indices)
    }
}

fn invalid_regex(error: impl fmt::Display) -> String {
    format!(
        "Invalid regex: {}",
        error
            .to_string()
            .lines()
            .last()
            .unwrap_or("invalid expression")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn top_level_alternatives_order_columns_and_keep_each_source_column_once() {
        let columns = names(&["a", "b", "c"]);
        assert_eq!(match_columns("a|c|b", &columns).unwrap(), vec![0, 2, 1]);
        assert_eq!(match_columns("a|c|b|c", &columns).unwrap(), vec![0, 2, 1]);
        assert_eq!(
            match_columns("^c$|^a$|^b$", &columns).unwrap(),
            vec![2, 0, 1]
        );
        assert_eq!(
            match_columns("missing|^c$|^a$", &columns).unwrap(),
            vec![2, 0]
        );
        assert_eq!(
            match_columns("^c$|.*|^a$", &columns).unwrap(),
            vec![2, 0, 1]
        );
        assert_eq!(match_columns("^c$|", &columns).unwrap(), vec![2, 0, 1]);
        assert_eq!(match_columns("|^c$", &columns).unwrap(), vec![0, 1, 2]);
        assert_eq!(match_columns("^(c|a|b)$", &columns).unwrap(), vec![0, 1, 2]);
        assert_eq!(match_columns("", &columns).unwrap(), vec![0, 1, 2]);
        let columns = names(&["b", "cab", "a", "ac", "c", "b2"]);
        assert_eq!(
            match_columns("^c|a|b", &columns).unwrap(),
            vec![1, 4, 2, 3, 0, 5]
        );
        let columns = names(&["early late", "early", "late"]);
        assert_eq!(
            match_columns("late|^early", &columns).unwrap(),
            vec![0, 2, 1]
        );
        let columns = names(&["b", "a", "A", "é", "É"]);
        assert_eq!(
            match_columns("^é$|^a$|^b$|^A$", &columns).unwrap(),
            vec![3, 4, 1, 2, 0]
        );
    }

    #[test]
    fn ordered_matching_preserves_regex_groups_escapes_classes_and_flag_scope() {
        let columns = names(&[
            "a", "A", "b", "B", "c", "C", "a|b", "|", "x|y", "#", "b c", "c\nx", "(ab)",
        ]);
        let cases: &[(&str, &[usize])] = &[
            (r"^a\|b$|^c$", &[6, 4, 5]),
            (r"^x[|]y$|^[|]$", &[8, 7]),
            (r"^\(ab\)$|^c$", &[12, 4, 5]),
            ("^(c|a)$|^b$", &[0, 1, 4, 5, 2, 3]),
            ("(?-i)^A$|^b$|(?i)^c$", &[1, 2, 4, 5]),
            ("(?-i:^A$)|^b$", &[1, 2, 3]),
            ("(?s)^c.x$|^a$", &[11, 0, 1]),
            ("(?m)^x$|^b$", &[11, 2, 3]),
            ("(?x)^c$ # ignored | pipe\n | ^a$", &[4, 5, 0, 1]),
            (r"(?x)^\#$|^b\ c$|^a$", &[9, 10, 0, 1]),
            (r"(?-u:^c$)|^a$", &[4, 5, 0, 1]),
        ];
        for &(pattern, expected) in cases {
            let actual = match_columns(pattern, &columns).unwrap();
            assert_eq!(actual, expected, "{pattern:?}");
            // Ordering must not change which columns match the original regex.
            let regex = RegexBuilder::new(pattern)
                .case_insensitive(true)
                .build()
                .unwrap();
            let membership: Vec<_> = columns
                .iter()
                .enumerate()
                .filter_map(|(i, name)| regex.is_match(name).then_some(i))
                .collect();
            let mut sorted = actual;
            sorted.sort_unstable();
            assert_eq!(sorted, membership, "{pattern:?}");
        }
    }

    #[test]
    fn column_regex_uses_standard_alternatives_anchors_and_unicode_case_folding() {
        let names: Vec<String> = [
            "abc_id",
            "ABC",
            "xdefx",
            "h.ij",
            "hXij",
            "qwe",
            "qwe_more",
            "ÉTAT",
            " first name ",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        assert_eq!(
            match_columns("^abc|def|h.ij$|qwe", &names).unwrap(),
            (0..7).collect::<Vec<_>>()
        );
        assert_eq!(match_columns("^(abc|qwe)$", &names).unwrap(), vec![1, 5]);
        assert_eq!(match_columns(r"^h\.ij$", &names).unwrap(), vec![3]);
        assert_eq!(match_columns("^état$", &names).unwrap(), vec![7]);
        assert_eq!(match_columns("^ first name $", &names).unwrap(), vec![8]);
        assert!(match_columns("^first name$", &names).is_err());
        assert_eq!(match_columns("", &names).unwrap().len(), names.len());
        assert_eq!(match_columns(" ", &names).unwrap(), vec![8]);
        assert!(
            match_columns("[", &names)
                .unwrap_err()
                .starts_with("Invalid regex:")
        );
        assert!(
            match_columns("missing", &names)
                .unwrap_err()
                .starts_with("No columns match")
        );
        assert_eq!(match_columns("", &[]).unwrap(), Vec::<usize>::new());
    }

    #[test]
    fn matches_large_schemas_and_long_names_in_source_order() {
        let names: Vec<_> = (0..10_000)
            .map(|index| format!("{}_{index}", "long_name_".repeat(40)))
            .collect();
        assert_eq!(match_columns("_(9999|2)$", &names).unwrap(), vec![2, 9999]);
    }

    #[test]
    fn input_scroll_keeps_unicode_cursor_visible_and_supports_home_and_end() {
        let mut prompt = CommandPrompt::new(PromptKind::Columns);
        prompt.buffer = "資料📊customer_identifier".into();
        prompt.cursor = prompt.buffer.len();
        for width in 1..35 {
            let (visible, cursor) = prompt.visible_input(width);
            assert!(cursor < width);
            assert!(prompt.buffer.ends_with(visible));
            assert!(Span::raw(visible).width() < usize::from(width));
        }
        prompt.handle_key_event(KeyCode::Home.into());
        assert_eq!(prompt.visible_input(8), (prompt.buffer.as_str(), 0));
        for _ in 0..3 {
            prompt.handle_key_event(KeyCode::Right.into());
        }
        assert_eq!(prompt.visible_input(8).1, 6);
        prompt.handle_key_event(KeyCode::End.into());
        assert_eq!(prompt.visible_input(8), ("ntifier", 7));
    }

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
