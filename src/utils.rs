/// Convert a byte count into a human-readable string (e.g. "2.3 MB").
pub fn human_readable_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{:.0} {}", size, UNITS[unit])
    } else {
        format!("{:.2} {}", size, UNITS[unit])
    }
}

/// Convert a plain count into a human-readable string with K / M / B suffixes.
pub fn human_readable_count(n: u64) -> String {
    const UNITS: [&str; 4] = ["", "K", "M", "B"]; // up to billions
    let mut unit = 0;
    let mut value = n as f64;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n}")
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

pub fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() > max {
        let truncated: String = s.chars().take(max - 1).collect();
        format!("{truncated}…")
    } else {
        s.to_string()
    }
}

pub fn commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

pub fn adjust_scroll_for_viewport(column_selected: Option<usize>, viewport_height: usize) -> usize {
    let mut scroll_offset: usize = 0;
    if let Some(selected_idx) = column_selected {
        // If root (index 0) is selected, no scrolling needed
        if selected_idx == 0 {
            scroll_offset = 0;
        }

        // For items after root, adjust scroll considering root is always visible
        let effective_viewport = viewport_height.saturating_sub(1); // Account for root
        let relative_selected = selected_idx - 1; // Relative to items after root

        // Check if selection is above visible area (scroll up to show it)
        if relative_selected < scroll_offset {
            scroll_offset = relative_selected;
        }
        // Check if selection is at or below the last visible position (scroll down)
        // Only scroll when selection goes beyond the last visible item
        else if relative_selected > scroll_offset + effective_viewport - 1 {
            scroll_offset = relative_selected.saturating_sub(effective_viewport - 1);
        }
    }
    
    return  scroll_offset;
}

pub fn adjust_scroll_for_selection(column_selected: Option<usize>, schema_tree_height: usize) -> usize {
    let mut scroll_offset: usize = 0;
    if let Some(_selected_idx) = column_selected {
        // Set the viewport height from the schema tree height
        let viewport_height = schema_tree_height;
        scroll_offset = adjust_scroll_for_viewport(column_selected, viewport_height);
    }
    return scroll_offset;
}