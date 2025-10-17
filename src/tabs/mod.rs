pub mod manager;
pub mod visualize;
pub mod metadata;
pub mod schema;
pub mod row_groups;

pub use manager::TabManager;
pub use visualize::VisualizeTab;
pub use metadata::MetadataTab;
pub use schema::SchemaTab;

use crate::app::AppState;
use crossterm::event::KeyEvent;
use std::io;

pub trait Tab {
    fn on_event(&self, key_event: KeyEvent, state: &mut AppState) -> Result<(), io::Error>;
    fn instructions(&self) -> String;
    fn to_string(&self) -> String;
}
