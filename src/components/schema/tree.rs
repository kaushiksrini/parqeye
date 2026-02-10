use crate::file::schema::SchemaInfo;
use crate::search::filter_schema_with_positions;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Style, Stylize},
    symbols::border,
    text::{Line, Span},
    widgets::{Block, List, ListItem, Widget},
};
use std::collections::HashMap;

pub struct SchemaTreeComponent<'a> {
    pub schema_columns: &'a Vec<SchemaInfo>,
    pub selected_index: usize,
    pub scroll_offset: usize,
    pub title: String,
    pub title_color: Color,
    pub root_color: Color,
    pub primitive_color: Color,
    pub group_color: Color,
    pub selected_color: Color,
    pub border_style: border::Set,
    pub show_legend: bool,
    pub search_query: Option<&'a str>,
    pub search_active: bool,
    pub cursor_pos: usize,
}

impl<'a> SchemaTreeComponent<'a> {
    pub fn new(schema_columns: &'a Vec<SchemaInfo>) -> Self {
        Self {
            schema_columns,
            selected_index: 0,
            scroll_offset: 0,
            title: "Schema Tree".to_string(),
            title_color: Color::Yellow,
            root_color: Color::LightYellow,
            primitive_color: Color::White,
            group_color: Color::Green,
            selected_color: Color::Yellow,
            border_style: border::ROUNDED,
            show_legend: true,
            search_query: None,
            search_active: false,
            cursor_pos: 0,
        }
    }

    pub fn with_selected_index(mut self, index: usize) -> Self {
        self.selected_index = index;
        self
    }

    pub fn with_scroll_offset(mut self, offset: usize) -> Self {
        self.scroll_offset = offset;
        self
    }

    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }

    pub fn with_colors(
        mut self,
        root: Color,
        primitive: Color,
        group: Color,
        selected: Color,
    ) -> Self {
        self.root_color = root;
        self.primitive_color = primitive;
        self.group_color = group;
        self.selected_color = selected;
        self
    }

    pub fn with_border_style(mut self, border_style: border::Set) -> Self {
        self.border_style = border_style;
        self
    }

    pub fn with_legend(mut self, show: bool) -> Self {
        self.show_legend = show;
        self
    }

    pub fn with_search(mut self, query: &'a str, active: bool, cursor_pos: usize) -> Self {
        self.search_query = Some(query);
        self.search_active = active;
        self.cursor_pos = cursor_pos;
        self
    }
}

/// Build a Line with highlighted match positions for a primitive display string.
/// The display string format is like "   ├─ column_name", so we need to find
/// the name portion and apply highlighting to the matched character positions.
fn build_highlighted_line(
    display: &str,
    name: &str,
    match_positions: &[usize],
    base_color: Color,
    highlight_color: Color,
    is_selected: bool,
    selected_color: Color,
) -> Line<'static> {
    // Find where the name starts in the display string
    let name_start = match display.rfind(name) {
        Some(pos) => pos,
        None => return Line::from(display.to_string()).style(Style::default().fg(base_color)),
    };

    let prefix = &display[..name_start];
    let name_part = &display[name_start..];

    // Build spans for the name with highlighting
    let mut spans = vec![Span::styled(
        prefix.to_string(),
        Style::default().fg(base_color),
    )];

    // Convert match positions to a set for O(1) lookup
    let match_set: std::collections::HashSet<usize> = match_positions.iter().copied().collect();

    // Build spans character by character for the name portion
    let mut current_span = String::new();
    let mut is_highlighted = false;

    for (char_idx, c) in name_part.chars().enumerate() {
        let should_highlight = match_set.contains(&char_idx);

        if should_highlight != is_highlighted && !current_span.is_empty() {
            // Push the accumulated span
            let style = if is_highlighted {
                Style::default().fg(Color::Black).bg(highlight_color)
            } else {
                Style::default().fg(base_color)
            };
            spans.push(Span::styled(std::mem::take(&mut current_span), style));
        }

        current_span.push(c);
        is_highlighted = should_highlight;
    }

    // Push the final span
    if !current_span.is_empty() {
        let style = if is_highlighted {
            Style::default().fg(Color::Black).bg(highlight_color)
        } else {
            Style::default().fg(base_color)
        };
        spans.push(Span::styled(current_span, style));
    }

    let mut line = Line::from(spans);
    if is_selected {
        line = line.style(Style::default().bg(selected_color).fg(Color::Black));
    }
    line
}

impl<'a> Widget for SchemaTreeComponent<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Filter schema items if search query is present
        let (filtered_indices, match_positions): (Vec<usize>, HashMap<usize, Vec<usize>>) =
            match self.search_query {
                Some(query) if !query.is_empty() => {
                    let result = filter_schema_with_positions(self.schema_columns, query);
                    (result.indices, result.match_positions)
                }
                _ => ((0..self.schema_columns.len()).collect(), HashMap::new()),
            };

        // Create a mapping from primitive column index to schema tree index
        let primitive_to_schema_map: Vec<usize> = self
            .schema_columns
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| matches!(line, SchemaInfo::Primitive { .. }).then_some(idx))
            .collect();

        // Calculate visible range based on scroll offset and available height
        let visible_height = area.height.saturating_sub(1) as usize; // Account for borders + legend
        let start_idx = self.scroll_offset;
        let end_idx = (start_idx + visible_height).min(filtered_indices.len());

        let items: Vec<ListItem> = filtered_indices
            .iter()
            .skip(start_idx)
            .take(end_idx - start_idx)
            .map(|&idx| {
                let line = &self.schema_columns[idx];
                let is_selected = if self.selected_index > 0 {
                    // Convert primitive index (1-based) to schema tree index
                    primitive_to_schema_map
                        .get(self.selected_index - 1)
                        .is_some_and(|&schema_idx| idx == schema_idx)
                } else {
                    false
                };

                match line {
                    SchemaInfo::Root { display: d, .. } => {
                        // If search is active, render search input instead of root display
                        if self.search_active {
                            let query = self.search_query.unwrap_or("");
                            let before_cursor = &query[..self.cursor_pos];
                            let after_cursor = &query[self.cursor_pos..];
                            ListItem::new(Line::from(vec![
                                Span::styled("/ ", Color::LightYellow),
                                Span::styled(before_cursor.to_string(), Color::Cyan),
                                Span::styled("|", Color::White),
                                Span::styled(after_cursor.to_string(), Color::Cyan),
                            ]))
                        } else if let Some(query) = self.search_query {
                            if !query.is_empty() {
                                // Show filter indicator when query is non-empty but not active
                                ListItem::new(Line::from(vec![
                                    Span::styled("/ [", Color::LightYellow),
                                    Span::styled(query.to_string(), Color::Cyan),
                                    Span::styled("]", Color::LightYellow),
                                ]))
                            } else {
                                ListItem::new(d.clone()).fg(self.root_color)
                            }
                        } else {
                            ListItem::new(d.clone()).fg(self.root_color)
                        }
                    }
                    SchemaInfo::Primitive {
                        display: d, name, ..
                    } => {
                        if let Some(positions) = match_positions.get(&idx) {
                            // Has match positions - render with highlighting
                            let line = build_highlighted_line(
                                d,
                                name,
                                positions,
                                self.primitive_color,
                                self.selected_color, // Use selected_color (yellow) for match highlighting
                                is_selected,
                                self.selected_color,
                            );
                            ListItem::new(line)
                        } else {
                            let mut item = ListItem::new(d.clone()).fg(self.primitive_color);
                            if is_selected {
                                item = item.bg(self.selected_color).fg(Color::Black);
                            }
                            item
                        }
                    }
                    SchemaInfo::Group { display: d, .. } => {
                        ListItem::new(d.clone()).fg(self.group_color)
                    }
                }
            })
            .collect();

        let mut block = Block::bordered()
            .title(Line::from(self.title.fg(self.title_color).bold()).centered())
            .border_set(self.border_style);

        if self.show_legend {
            let mut legend_vec = vec![
                "Leaf".fg(self.primitive_color),
                ", ".into(),
                "Group".fg(self.group_color),
            ];

            if self.selected_index > 0 {
                legend_vec.extend(vec![", ".into(), "Selected".bold().fg(self.selected_color)]);
            }

            let legend = Line::from(legend_vec);
            block = block.title_bottom(legend.centered());
        }

        let list = List::new(items).block(block);
        list.render(area, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_highlighted_line_basic() {
        // Test highlighting "user" in "   ├─ user_id"
        let display = "   ├─ user_id";
        let name = "user_id";
        let positions = vec![0, 1, 2, 3]; // "user" positions

        let line = build_highlighted_line(
            display,
            name,
            &positions,
            Color::White,
            Color::Yellow,
            false,
            Color::Yellow,
        );

        // Line should have multiple spans
        assert!(!line.spans.is_empty());

        // First span should be the prefix "   ├─ "
        assert_eq!(line.spans[0].content, "   ├─ ");
    }

    #[test]
    fn test_build_highlighted_line_full_match() {
        // Test highlighting all of "email"
        let display = "   └─ email";
        let name = "email";
        let positions = vec![0, 1, 2, 3, 4]; // all positions

        let line = build_highlighted_line(
            display,
            name,
            &positions,
            Color::White,
            Color::Yellow,
            false,
            Color::Yellow,
        );

        assert!(!line.spans.is_empty());
        // Should have prefix span and highlighted span
        assert!(line.spans.len() >= 2);
    }

    #[test]
    fn test_build_highlighted_line_fuzzy_match() {
        // Test highlighting "u", "i", "d" in "user_id" (fuzzy match for "uid")
        let display = "   ├─ user_id";
        let name = "user_id";
        let positions = vec![0, 5, 6]; // u, i, d positions

        let line = build_highlighted_line(
            display,
            name,
            &positions,
            Color::White,
            Color::Yellow,
            false,
            Color::Yellow,
        );

        // Should have multiple spans: prefix, u (highlighted), ser_ (normal), id (highlighted)
        assert!(line.spans.len() >= 3);
    }

    #[test]
    fn test_build_highlighted_line_no_match_positions() {
        // Test with empty positions (should still render, just no highlighting)
        let display = "   ├─ column";
        let name = "column";
        let positions: Vec<usize> = vec![];

        let line = build_highlighted_line(
            display,
            name,
            &positions,
            Color::White,
            Color::Yellow,
            false,
            Color::Yellow,
        );

        // Should still produce a valid line
        assert!(!line.spans.is_empty());
    }

    #[test]
    fn test_build_highlighted_line_name_not_found() {
        // Test when name is not found in display (edge case)
        let display = "   ├─ something";
        let name = "other";
        let positions = vec![0, 1];

        let line = build_highlighted_line(
            display,
            name,
            &positions,
            Color::White,
            Color::Yellow,
            false,
            Color::Yellow,
        );

        // Should fall back to plain display
        assert!(!line.spans.is_empty());
    }

    #[test]
    fn test_build_highlighted_line_selected() {
        let display = "   ├─ user_id";
        let name = "user_id";
        let positions = vec![0, 1, 2, 3];

        let line = build_highlighted_line(
            display,
            name,
            &positions,
            Color::White,
            Color::Yellow,
            true, // selected
            Color::Yellow,
        );

        // Line should have a style applied for selection
        assert!(line.style.bg.is_some());
    }

    #[test]
    fn test_build_highlighted_line_unicode() {
        // Test with unicode characters in the display
        let display = "   ├─ naïve_column";
        let name = "naïve_column";
        let positions = vec![0, 1]; // "na" positions

        let line = build_highlighted_line(
            display,
            name,
            &positions,
            Color::White,
            Color::Yellow,
            false,
            Color::Yellow,
        );

        // Should handle unicode correctly
        assert!(!line.spans.is_empty());
    }

    #[test]
    fn test_build_highlighted_line_alternating_highlights() {
        // Test with alternating highlighted positions: a_b_c with positions 0, 2, 4
        let display = "   ├─ a_b_c";
        let name = "a_b_c";
        let positions = vec![0, 2, 4]; // a, b, c

        let line = build_highlighted_line(
            display,
            name,
            &positions,
            Color::White,
            Color::Yellow,
            false,
            Color::Yellow,
        );

        // Should have many spans due to alternating
        // prefix, "a" (highlighted), "_" (normal), "b" (highlighted), "_" (normal), "c" (highlighted)
        assert!(line.spans.len() >= 5);
    }
}
