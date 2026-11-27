use crate::file::Renderable;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Stylize;
use ratatui::text::Line;
use ratatui::widgets::Tabs;
use ratatui::widgets::Widget;

use crate::tabs::Tab;
use crate::tabs::metadata::MetadataTab;
use crate::tabs::row_groups::RowGroupsTab;
use crate::tabs::schema::SchemaTab;
use crate::tabs::visualize::VisualizeTab;

pub struct TabManager {
    pub tabs: Vec<Box<dyn Tab>>,
    pub active_tab: usize,
    pub title: String,
}

impl TabManager {
    pub fn new(
        num_columns: usize,
        num_row_groups: usize,
        sample_data_rows: usize,
        num_properties: usize,
        sample_data_columns: &[String],
    ) -> Self {
        Self {
            tabs: vec![
                Box::new(
                    VisualizeTab::new()
                        .with_column_names(sample_data_columns)
                        .with_max_rows(sample_data_rows),
                ),
                Box::new(
                    MetadataTab::new().with_max_vertical_scroll(num_properties.saturating_sub(1)),
                ),
                Box::new(SchemaTab::new().with_max_vertical_scroll(num_columns)),
                Box::new(
                    RowGroupsTab::new()
                        .with_max_horizontal_scroll(num_row_groups - 1)
                        .with_max_vertical_scroll(num_columns),
                ),
            ],
            active_tab: 0,
            title: "Tabs".to_string(),
        }
    }

    pub fn next(&mut self) {
        self.active_tab = (self.active_tab + 1) % self.tabs.len();
    }

    pub fn prev(&mut self) {
        if self.active_tab == 0 {
            self.active_tab = self.tabs.len() - 1;
        } else {
            self.active_tab = (self.active_tab.saturating_sub(1)) % self.tabs.len();
        }
    }

    #[allow(clippy::borrowed_box)]
    pub fn active_tab(&self) -> &Box<dyn Tab> {
        &self.tabs[self.active_tab]
    }

    /// Width of the tab titles, their padding, and the spaces between tabs.
    pub fn content_width(&self) -> usize {
        self.tabs
            .iter()
            .map(|tab| Line::from(tab.to_string()).width() + 2)
            .sum::<usize>()
            + self.tabs.len().saturating_sub(1)
    }

    pub fn render_instructions(&self, escape_action: Option<&str>, area: Rect, buf: &mut Buffer) {
        let instructions = [
            Line::from(vec!["[Tab]".green(), " Toggle Mode".into()]),
            Line::from(vec!["[?]".yellow(), " Help".into()]),
            Line::from(vec!["[Q]".blue(), "uit".into()]),
        ];
        let escape = escape_action
            .map(|action| Line::from(vec!["[Esc]".yellow(), format!(" {action}").into()]))
            .filter(|line| line.width() + 3 + 6 <= usize::from(area.width));
        // Reserve Esc and quit first, keeping Esc leftmost among fitted commands.
        let instructions_width =
            usize::from(area.width) - escape.as_ref().map_or(0, |line| line.width() + 3);

        // Keep the longest suffix of complete commands that fits the footer.
        let mut line = Line::default();
        for instruction in instructions.into_iter().rev() {
            let separator = if line.spans.is_empty() { "" } else { " | " };
            if instruction.width() + separator.len() + line.width() > instructions_width {
                break;
            }
            let mut spans = instruction.spans;
            if !separator.is_empty() {
                spans.push(separator.into());
            }
            spans.extend(line.spans);
            line = Line::from(spans);
        }
        if let Some(mut escape) = escape {
            escape.spans.push(" | ".into());
            escape.spans.extend(line.spans);
            line = escape;
        }

        if area.width < 6 {
            line = Line::from(if area.width >= 3 { "[q]" } else { "q" }.blue());
        }

        line.right_aligned().render(area, buf);
    }
}

impl Renderable for TabManager {
    fn render_content(&self, area: Rect, buf: &mut Buffer) {
        let tab_titles: Vec<Line> = self
            .tabs
            .iter()
            .map(|t| Line::from(t.to_string()))
            .collect();
        let tabs_widget: Tabs<'_> = Tabs::new(tab_titles)
            .select(self.active_tab)
            .padding(" ", " ")
            .divider(" ");

        tabs_widget.render(area, buf);
    }
}
