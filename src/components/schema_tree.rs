use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Stylize},
    symbols::border,
    text::Line,
    widgets::{Block, List, ListItem, Widget},
};
use crate::schema::SchemaColumnType;

pub struct SchemaTreeComponent {
    pub schema_columns: Vec<SchemaColumnType>,
    pub selected_index: Option<usize>,
    pub title: String,
    pub title_color: Color,
    pub root_color: Color,
    pub primitive_color: Color,
    pub group_color: Color,
    pub selected_color: Color,
    pub border_style: border::Set,
    pub show_legend: bool,
}

impl SchemaTreeComponent {
    pub fn new(schema_columns: Vec<SchemaColumnType>) -> Self {
        Self {
            schema_columns,
            selected_index: None,
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

    pub fn with_selected_index(mut self, index: Option<usize>) -> Self {
        self.selected_index = index;
        self
    }

    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }

    pub fn with_colors(mut self, root: Color, primitive: Color, group: Color, selected: Color) -> Self {
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

impl Widget for SchemaTreeComponent {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let items: Vec<ListItem> = self.schema_columns
            .iter()
            .enumerate()
            .map(|(idx, line)| {
                match line {
                    SchemaColumnType::Root { display: ref d, .. } => {
                        ListItem::new(d.clone()).fg(self.root_color)
                    }
                    SchemaColumnType::Primitive { display: ref d, .. } => {
                        let mut item = ListItem::new(d.clone()).fg(self.primitive_color);
                        if let Some(selected_index) = self.selected_index {
                            if idx == selected_index {
                                item = item.fg(self.selected_color).bold();
                            }
                        }
                        item
                    }
                    SchemaColumnType::Group { display: ref d, .. } => {
                        let mut item = ListItem::new(d.clone()).fg(self.group_color);
                        if let Some(selected_index) = self.selected_index {
                            if idx == selected_index {
                                item = item.fg(self.selected_color).bold();
                            }
                        }
                        item
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
            
            if self.selected_index.is_some() {
                legend_vec.extend(vec![", ".into(), "Selected".bold().fg(self.selected_color)]);
            }
            
            let legend = Line::from(legend_vec);
            block = block.title_bottom(legend.centered());
        }

        let list = List::new(items).block(block);
        list.render(area, buf);
    }
}