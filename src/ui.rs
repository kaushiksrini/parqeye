use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    prelude::Color,
    style::{Style, Stylize},
    widgets::{Block, BorderType, Borders, Widget},
    Frame,
};

use crate::{app::AppRenderView, components::RowGroupColumnMetadataComponent};
use crate::{
    components::DataTable, components::FileSchemaTable, components::RowGroupMetadata,
    components::SchemaTreeComponent,
};
use crate::{components::RowGroupProgressBar, file::Renderable};

pub fn render_app<'a, 'b>(app: &'b AppRenderView<'a>, frame: &mut Frame)
where
    'b: 'a,
{
    frame.render_widget(AppWidget(app), frame.area());
}

struct AppWidget<'a>(&'a AppRenderView<'a>);

impl<'a> AppWidget<'a> {
    fn render_tabs_view(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::LightYellow));
        let inner_area = block.inner(area);
        block.render(area, buf);

        let file_name_length = self.0.file_name().len() as u16;

        let [tabs_area, file_name_area] =
            Layout::horizontal([Constraint::Min(0), Constraint::Length(file_name_length)])
                .areas(inner_area);
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
            .with_selected_index(self.0.state().vertical_offset())
            .with_horizontal_scroll(self.0.state().horizontal_offset())
            .render(central_area, buf);
    }

    fn render_schema_tree(&self, area: Rect, buf: &mut Buffer) {
        SchemaTreeComponent::new(&self.0.parquet_ctx.schema.columns)
            .with_title("Schema Tree".to_string())
            .with_selected_index(self.0.state().vertical_offset())
            .render(area, buf);
    }

    fn render_row_groups_view(&self, area: Rect, buf: &mut Buffer) {
        // render the schema tree
        let tree_width = self.0.parquet_ctx.schema.tree_width() as u16;
        let [tree_area, main_area] = Layout::horizontal([
            Constraint::Length(tree_width),
            // Constraint::Fill(1),
            Constraint::Fill(1),
        ])
        .areas(area);
        self.render_schema_tree(tree_area, buf);

        let [rg_progress, central_area] =
            Layout::vertical([Constraint::Length(3), Constraint::Fill(1)]).areas(main_area);

        RowGroupProgressBar::new(
            &self.0.parquet_ctx.row_groups.row_groups,
            self.0.state().horizontal_offset(),
        )
        .render(rg_progress, buf);

        if self.0.state().vertical_offset() > 0 {
            RowGroupColumnMetadataComponent::new(
                &self.0.parquet_ctx.row_groups.row_groups[self.0.state().horizontal_offset()]
                    .column_metadata[self.0.state().vertical_offset() - 1],
            )
            .render(central_area, buf);
        } else {
            // Display row group level statistics and charts when no column is selected
            RowGroupMetadata::new(
                &self.0.parquet_ctx.row_groups.row_groups,
                &self.0.parquet_ctx.row_groups.avg_median_stats,
                self.0.state().horizontal_offset(),
            )
            .render(central_area, buf);
        }
    }

    fn render_visualize_view(&self, area: Rect, buf: &mut Buffer) {
        if let Some(ref sample_data) = self.0.parquet_ctx.sample_data {
            DataTable::new(sample_data)
                .with_horizontal_scroll(self.0.state().horizontal_offset())
                .render(area, buf);
        } else {
            "No sample data available - failed to read parquet file data.".render(area, buf);
        }
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

        match app.tabs().active_tab().to_owned().to_string().as_str() {
            "Metadata" => self.render_metadata_view(inner_area, buf),
            "Schema" => self.render_schema_view(inner_area, buf),
            "Row Groups" => self.render_row_groups_view(inner_area, buf),
            "Visualize" => self.render_visualize_view(inner_area, buf),
            _ => {}
        }
    }
}
