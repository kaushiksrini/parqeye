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

/// Format byte size into human-readable format
pub fn format_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_human_readable_bytes() {
        assert_eq!(human_readable_bytes(0), "0 B");
        assert_eq!(human_readable_bytes(500), "500 B");
        assert_eq!(human_readable_bytes(1024), "1.00 KiB");
        assert_eq!(human_readable_bytes(1536), "1.50 KiB");
        assert_eq!(human_readable_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(human_readable_bytes(1024 * 1024 * 1024), "1.00 GiB");
        assert_eq!(human_readable_bytes(1024 * 1024 * 1024 * 1024), "1.00 TiB");
        assert_eq!(human_readable_bytes(2500 * 1024 * 1024), "2.44 GiB");
    }

    #[test]
    fn test_human_readable_count() {
        assert_eq!(human_readable_count(0), "0");
        assert_eq!(human_readable_count(500), "500");
        assert_eq!(human_readable_count(999), "999");
        assert_eq!(human_readable_count(1000), "1.0 K");
        assert_eq!(human_readable_count(1500), "1.5 K");
        assert_eq!(human_readable_count(1_000_000), "1.0 M");
        assert_eq!(human_readable_count(1_500_000), "1.5 M");
        assert_eq!(human_readable_count(1_000_000_000), "1.0 B");
        assert_eq!(human_readable_count(2_500_000_000), "2.5 B");
    }

    #[test]
    fn test_truncate_str() {
        assert_eq!(truncate_str("hello", 10), "hello");
        assert_eq!(truncate_str("hello", 5), "hello");
        assert_eq!(truncate_str("hello world", 8), "hello w…");
        assert_eq!(truncate_str("hello world", 6), "hello…");
        assert_eq!(truncate_str("", 5), "");
        assert_eq!(truncate_str("a", 1), "a");
        assert_eq!(truncate_str("ab", 1), "…");
        assert_eq!(truncate_str("hello", 3), "he…");
    }

    #[test]
    fn test_commas() {
        assert_eq!(commas(0), "0");
        assert_eq!(commas(100), "100");
        assert_eq!(commas(999), "999");
        assert_eq!(commas(1000), "1,000");
        assert_eq!(commas(1234), "1,234");
        assert_eq!(commas(1234567), "1,234,567");
        assert_eq!(commas(1_000_000), "1,000,000");
        assert_eq!(commas(1_234_567_890), "1,234,567,890");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1536), "1.5 KB");
        assert_eq!(format_size(1024 * 1024), "1.0 MB");
        assert_eq!(format_size(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(format_size(1024 * 1024 * 1024 * 1024), "1.0 TB");
    }

    #[test]
    fn test_edge_cases() {
        // Test maximum values
        assert!(human_readable_bytes(u64::MAX).contains("TiB"));
        assert!(human_readable_count(u64::MAX).contains("B"));

        // Test empty string truncation
        assert_eq!(truncate_str("", 0), "");

        // Test single digit comma formatting
        assert_eq!(commas(1), "1");
    }

    #[test]
    fn test_unicode_truncation() {
        // Test with unicode characters
        assert_eq!(truncate_str("hello 🌍 world", 10), "hello 🌍 w…");
        assert_eq!(truncate_str("日本語", 2), "日…");
    }
}
