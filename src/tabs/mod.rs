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
use ratatui::text::Span;
use std::io;

pub trait Tab {
    fn on_event(&self, key_event: KeyEvent, state: &mut AppState) -> Result<(), io::Error>;
    fn instructions(&self) -> Vec<Span<'static>>;
    fn to_string(&self) -> String;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyModifiers};

    fn paging_tabs() -> Vec<Box<dyn Tab>> {
        vec![
            Box::new(MetadataTab::new().with_max_vertical_scroll(11)),
            Box::new(SchemaTab::new().with_max_vertical_scroll(12)),
            Box::new(row_groups::RowGroupsTab::new().with_max_vertical_scroll(12)),
            Box::new(VisualizeTab::new().with_max_rows(12)),
        ]
    }

    #[test]
    fn test_page_keys_move_selection_and_viewport_in_each_tab() {
        for tab in paging_tabs() {
            let mut state = AppState::new();
            state.set_visible_data_rows(5);
            state.right();

            for (key, selected, scroll) in [
                (KeyCode::PageUp, 0, 0),
                (KeyCode::PageDown, 5, 1),
                (KeyCode::PageDown, 10, 6),
                (KeyCode::PageDown, 11, 7),
                (KeyCode::PageDown, 11, 7),
                (KeyCode::PageUp, 6, 6),
                (KeyCode::PageUp, 1, 1),
                (KeyCode::PageUp, 0, 0),
                (KeyCode::PageUp, 0, 0),
            ] {
                tab.on_event(KeyEvent::new(key, KeyModifiers::NONE), &mut state)
                    .unwrap();
                let context = format!("{}: {key:?}", tab.to_string());
                assert_eq!(state.vertical_offset(), selected, "{context}");
                assert_eq!(state.data_vertical_scroll(), scroll, "{context}");
                assert_eq!(state.horizontal_offset(), 1, "{context}");
            }
        }
    }

    #[test]
    fn test_page_keys_use_current_viewport_height_in_each_tab() {
        for tab in paging_tabs() {
            let mut state = AppState::new();
            state.set_visible_data_rows(3);
            tab.on_event(
                KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE),
                &mut state,
            )
            .unwrap();
            assert_eq!(state.vertical_offset(), 3, "{}", tab.to_string());
            assert_eq!(state.data_vertical_scroll(), 1, "{}", tab.to_string());

            state.set_visible_data_rows(2);
            tab.on_event(
                KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE),
                &mut state,
            )
            .unwrap();
            assert_eq!(state.vertical_offset(), 5, "{}", tab.to_string());
            assert_eq!(state.data_vertical_scroll(), 4, "{}", tab.to_string());

            tab.on_event(
                KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE),
                &mut state,
            )
            .unwrap();
            assert_eq!(state.vertical_offset(), 3, "{}", tab.to_string());
            assert_eq!(state.data_vertical_scroll(), 3, "{}", tab.to_string());
        }
    }

    #[test]
    fn test_visualize_page_shortcuts_match_page_keys() {
        let tab = VisualizeTab::new().with_max_rows(12);

        for (down, up) in [('d', 'u'), ('D', 'U')] {
            let mut state = AppState::new();
            state.set_visible_data_rows(5);
            let mut page_state = AppState::new();
            page_state.set_visible_data_rows(5);

            for (shortcut, key) in [
                (down, KeyCode::PageDown),
                (down, KeyCode::PageDown),
                (down, KeyCode::PageDown),
                (up, KeyCode::PageUp),
                (up, KeyCode::PageUp),
                (up, KeyCode::PageUp),
            ] {
                tab.on_event(
                    KeyEvent::new(KeyCode::Char(shortcut), KeyModifiers::NONE),
                    &mut state,
                )
                .unwrap();
                tab.on_event(KeyEvent::new(key, KeyModifiers::NONE), &mut page_state)
                    .unwrap();

                assert_eq!(state.vertical_offset(), page_state.vertical_offset());
                assert_eq!(
                    state.data_vertical_scroll(),
                    page_state.data_vertical_scroll()
                );
            }
        }
    }
}
