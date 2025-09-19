use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    prelude::Color,
    style::Stylize,
    widgets::Widget,
    Frame,
};

use crate::file::Renderable;
use crate::{app::AppRenderView, components::ColumnSizesButterflyChart};
use crate::{components::FileSchemaTable, components::SchemaTreeComponent, tabs::TabType};

pub fn render_app<'a, 'b>(app: &'b AppRenderView<'a>, frame: &mut Frame)
where
    'b: 'a,
{
    frame.render_widget(AppWidget(app), frame.area());
}

struct AppWidget<'a>(&'a AppRenderView<'a>);

impl<'a> AppWidget<'a> {
    fn render_tabs_view(&self, area: Rect, buf: &mut Buffer) {
        let file_name_length = self.0.file_name().len() as u16;
        let [tabs_area, file_name_area] =
            Layout::horizontal([Constraint::Min(0), Constraint::Length(file_name_length)])
                .areas(area);
        self.0.tabs().render_content(tabs_area, buf);
        self.0.file_name().green().render(file_name_area, buf);
    }

    fn render_footer_view(&self, area: Rect, buf: &mut Buffer) {
        self.0.title.bold().fg(Color::Green).render(area, buf);
    }

    fn render_metadata_view(&self, area: Rect, buf: &mut Buffer) {
        // render the metadata
        self.0.parquet_ctx.metadata.render_content(area, buf);
    }

    fn render_schema_view(&self, area: Rect, buf: &mut Buffer) {
        // render the schema tree
        let tree_width = self.0.parquet_ctx.schema.tree_width() as u16;
        let [tree_area, central_area] =
            Layout::horizontal([Constraint::Length(tree_width + 1), Constraint::Fill(1)])
                .areas(area);
        self.render_schema_tree(tree_area, buf);

        // Render the schema table with selection highlighting
        FileSchemaTable::new(&self.0.parquet_ctx.schema)
            .with_selected_index(*self.0.column_selected())
            .with_horizontal_scroll(self.0.horizontal_scroll)
            .render(central_area, buf);
    }

    fn render_schema_tree(&self, area: Rect, buf: &mut Buffer) {
        SchemaTreeComponent::new(&self.0.parquet_ctx.schema.columns)
            .with_title("Schema Tree".to_string())
            .with_selected_index(*self.0.column_selected())
            .render(area, buf);
    }

    fn render_visualize_view(&self, area: Rect, buf: &mut Buffer) {
        // render the visualize view
        "Visualize".render(area, buf);
    }

    fn render_row_groups_view(&self, area: Rect, buf: &mut Buffer) {
        // render the schema tree
        let tree_width = self.0.parquet_ctx.schema.tree_width() as u16;
        let [tree_area, sizes_chart_area, _central_area] = Layout::horizontal([
            Constraint::Length(tree_width),
            Constraint::Fill(1),
            Constraint::Fill(3),
        ])
        .areas(area);
        self.render_schema_tree(tree_area, buf);

        ColumnSizesButterflyChart::new(&self.0.parquet_ctx.schema).render(sizes_chart_area, buf);
    }
}

impl<'a> Widget for AppWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let app = self.0;

        let vertical = Layout::vertical([
            Constraint::Length(3),
            Constraint::Fill(1),
            Constraint::Length(1),
        ]);
        let [header_area, inner_area, footer_area] = vertical.areas(area);

        self.render_tabs_view(header_area, buf);
        self.render_footer_view(footer_area, buf);

        match app.tabs().active_tab() {
            TabType::Metadata => {
                self.render_metadata_view(inner_area, buf);
            }
            TabType::Schema => {
                self.render_schema_view(inner_area, buf);
            }
            TabType::RowGroups => {
                self.render_row_groups_view(inner_area, buf);
            }
            TabType::Visualize => {
                self.render_visualize_view(inner_area, buf);
            }
        }
    }
}
