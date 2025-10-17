use crate::file::schema::SchemaInfo;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Stylize},
    symbols::border,
    text::Line,
    widgets::{Block, List, ListItem, Widget},
};

pub struct SchemaTreeComponent<'a> {
    pub schema_columns: &'a Vec<SchemaInfo>,
    pub selected_index: usize,
    pub title: String,
    pub title_color: Color,
    pub root_color: Color,
    pub primitive_color: Color,
    pub group_color: Color,
    pub selected_color: Color,
    pub border_style: border::Set,
    pub show_legend: bool,
}

impl<'a> SchemaTreeComponent<'a> {
    pub fn new(schema_columns: &'a Vec<SchemaInfo>) -> Self {
        Self {
            schema_columns,
            selected_index: 0,
            title: "Schema Tree".to_string(),
            title_color: Color::Yellow,
            root_color: Color::DarkGray,
            primitive_color: Color::Blue,
            group_color: Color::Green,
            selected_color: Color::Yellow,
            border_style: border::ROUNDED,
            show_legend: true,
        }
    }

    pub fn with_selected_index(mut self, index: usize) -> Self {
        self.selected_index = index;
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
}

impl<'a> Widget for SchemaTreeComponent<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Create a mapping from primitive column index to schema tree index
        let primitive_to_schema_map: Vec<usize> = self
            .schema_columns
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| matches!(line, SchemaInfo::Primitive { .. }).then_some(idx))
            .collect();

        let items: Vec<ListItem> = self
            .schema_columns
            .iter()
            .enumerate()
            .map(|(idx, line)| {
                let is_selected = if self.selected_index > 0 {
                    // Convert primitive index (1-based) to schema tree index
                    primitive_to_schema_map
                        .get(self.selected_index - 1)
                        .map_or(false, |&schema_idx| idx == schema_idx)
                } else {
                    false
                };

                match line {
                    SchemaInfo::Root { display: ref d, .. } => {
                        ListItem::new(d.clone()).fg(self.root_color)
                    }
                    SchemaInfo::Primitive { display: ref d, .. } => {
                        let mut item = ListItem::new(d.clone()).fg(self.primitive_color);
                        if is_selected {
                            item = item.bg(self.selected_color).fg(Color::Black);
                        }
                        item
                    }
                    SchemaInfo::Group { display: ref d, .. } => {
                        ListItem::new(d.clone()).fg(self.group_color)
                    }
                }
            })
            .collect();

        // highlight the color

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
