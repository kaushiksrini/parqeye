pub mod metadata;
pub mod parquet_ctx;
pub mod row_groups;
pub mod sample_data;
pub mod schema;
pub mod utils;

use ratatui::{buffer::Buffer, layout::Rect};

pub trait Renderable {
    fn render_content(&self, area: Rect, buf: &mut Buffer);
}