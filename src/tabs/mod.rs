pub mod manager;
pub mod metadata;
pub mod row_groups;
pub mod schema;
pub mod visualize;

pub use manager::TabManager;
pub use metadata::MetadataTab;
pub use schema::SchemaTab;
pub use visualize::VisualizeTab;

use crate::app::AppState;
use crossterm::event::KeyEvent;
use std::io;

pub trait Tab {
    fn on_event(&self, key_event: KeyEvent, state: &mut AppState) -> Result<(), io::Error>;
    fn instructions(&self) -> String;
    fn to_string(&self) -> String;
}
