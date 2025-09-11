pub mod metadata;
pub mod parquet_ctx;
pub mod row_groups;
pub mod schema;

use ratatui::{buffer::Buffer, layout::Rect, widgets::Block, widgets::Widget};

pub trait Renderable {
    fn render_content(&self, area: Rect, buf: &mut Buffer);
}

pub struct RenderWrapper<'a, T: Renderable + 'a> {
    content: &'a T,
    title: Option<String>,
    bordered: bool,
}

impl<'a, T: Renderable> RenderWrapper<'a, T> {
    pub fn new(content: &'a T) -> Self {
        Self {
            content,
            title: None,
            bordered: true,
        }
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn without_border(mut self) -> Self {
        self.bordered = false;
        self
    }
}

impl<'a, T: Renderable> Widget for RenderWrapper<'a, T> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if self.bordered {
            let mut block = Block::bordered();
            if let Some(title) = self.title {
                block = block.title(title);
            }
            let inner = block.inner(area);
            block.render(area, buf);
            self.content.render_content(inner, buf);
        } else {
            self.content.render_content(area, buf);
        }
    }
}
