use crate::file::schema::SchemaInfo;
use crate::search::filter_schema_indices;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Stylize},
    symbols::border,
    text::{Line, Span},
    widgets::{Block, List, ListItem, Widget},
};

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

impl<'a> Widget for SchemaTreeComponent<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Filter schema items if search query is present
        let filtered_indices: Vec<usize> = match self.search_query {
            Some(query) if !query.is_empty() => filter_schema_indices(self.schema_columns, query),
            _ => (0..self.schema_columns.len()).collect(),
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
                    SchemaInfo::Primitive { display: d, .. } => {
                        let mut item = ListItem::new(d.clone()).fg(self.primitive_color);
                        if is_selected {
                            item = item.bg(self.selected_color).fg(Color::Black);
                        }
                        item
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
