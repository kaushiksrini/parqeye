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

pub fn truncate_str(s: &str, width: usize) -> String {
    if s.chars().count() > width {
        let truncated: String = s.chars().take(width - 1).collect();
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
