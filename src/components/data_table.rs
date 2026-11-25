use crate::file::sample_data::ParquetSampleData;
use crate::tabs::visualize::SelectionMode;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    prelude::{Color, Position},
    style::Modifier,
    symbols::{border, line},
    text::Span,
    widgets::Widget,
};
use std::borrow::Cow;
use std::cell::{Ref, RefCell};

use crate::file::Renderable;
use crate::file::utils::commas;

const NUM_SPACES_BETWEEN_COLUMNS: u16 = 2;
const NUM_SPACES_AFTER_LINE_NUMBER: u16 = 2;
const MIN_ROW_NUM_DIGITS: u16 = 4;
/// Header text line plus the separator line above the first data row.
const HEADER_HEIGHT: u16 = 2;
const MAX_COLUMN_WIDTH: usize = 25;

/// Only the most recent viewport is retained. Shared by layout and rendering so
/// moving the cursor or redrawing a prompt does not remeasure the same cells.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ColumnWidthCache {
    viewport: Option<(usize, u16, usize, usize)>,
    projection: Option<Vec<usize>>,
    widths: Vec<u16>,
    #[cfg(test)]
    calculations: usize,
}

/// Stop measuring once a string fills the column; long values and names should
/// cost no more than short ones when both will be clipped to the same width.
fn capped_width(text: &str, limit: usize) -> usize {
    if text.len() <= limit {
        return Span::raw(text).width().min(limit);
    }
    if let Some(prefix) = text.get(..limit)
        && prefix.is_ascii()
        && Span::raw(prefix).width() == limit
    {
        return limit;
    }
    let span = Span::raw(text);
    let mut width = 0;
    for grapheme in span.styled_graphemes(ratatui::style::Style::default()) {
        width += Span::raw(grapheme.symbol).width();
        if width >= limit {
            return limit;
        }
    }
    width
}

/// Truncate by terminal display width without splitting UTF-8 characters.
pub(crate) fn truncate_cell(text: &str, width: u16) -> Cow<'_, str> {
    if width == 0 {
        return Cow::Borrowed("");
    }
    let limit = usize::from(width);
    if text.len() <= limit {
        return Cow::Borrowed(text);
    }
    if let Some(prefix) = text.get(..limit + 1)
        && prefix.is_ascii()
        && Span::raw(prefix).width() == limit + 1
    {
        return Cow::Owned(format!("{}…", &prefix[..limit - 1]));
    }
    let span = Span::raw(text);
    let mut prefix = String::new();
    let mut used = 0;
    let mut ellipsis_end = 0;
    for grapheme in span.styled_graphemes(ratatui::style::Style::default()) {
        used += Span::raw(grapheme.symbol).width();
        if used > usize::from(width) {
            prefix.truncate(ellipsis_end);
            prefix.push('…');
            return Cow::Owned(prefix);
        }
        prefix.push_str(grapheme.symbol);
        if used < usize::from(width) {
            ellipsis_end = prefix.len();
        }
    }
    Cow::Borrowed(text)
}

/// Digits needed for the largest row number currently in view.
fn row_num_digits(vertical_scroll: usize, rows_height: u16, total_rows: usize) -> u16 {
    let last_row_num = vertical_scroll
        .saturating_add(rows_height as usize)
        .min(total_rows)
        .max(1);
    (last_row_num.ilog10() as u16 + 1).max(MIN_ROW_NUM_DIGITS)
}

/// Number of columns starting at `start` whose widths fit in `available_width`
/// (at least 1, so an over-wide column still shows, clipped).
fn columns_fitting(widths: &[u16], start: usize, available_width: u16) -> usize {
    let mut acc = 0u16;
    let mut count = 0usize;
    for &w in widths.iter().skip(start) {
        if count > 0 && acc.saturating_add(w) > available_width {
            break;
        }
        acc = acc.saturating_add(w);
        count += 1;
    }
    count.max(1)
}

/// Smallest scroll offset at which the trailing columns fill `available_width`
/// (i.e. the last column is flush right). Accumulates from the right so the end
/// is always reachable regardless of column widths.
fn max_scroll_from_widths(widths: &[u16], available_width: u16) -> usize {
    let mut acc = 0u16;
    let mut fit = 0usize;
    for &w in widths.iter().rev() {
        if fit > 0 && acc.saturating_add(w) > available_width {
            break;
        }
        acc = acc.saturating_add(w);
        fit += 1;
    }
    widths.len().saturating_sub(fit.max(1))
}

pub struct DataTable<'a> {
    pub data: &'a ParquetSampleData,
    projection: Option<&'a [usize]>,
    width_cache: Option<&'a RefCell<ColumnWidthCache>>,
    local_width_cache: RefCell<ColumnWidthCache>,
    pub title: String,
    pub title_color: Color,
    pub border_style: border::Set,
    pub horizontal_scroll: usize,
    pub vertical_scroll: usize,
    pub selected_row: Option<usize>,
    pub selected_column: Option<usize>,
    pub selection_mode: SelectionMode,
    pub selected_color: Color,
    pub border_color: Color,
}

impl<'a> DataTable<'a> {
    /// Number of data rows that fit below the table header.
    pub fn visible_rows(area_height: u16) -> u16 {
        area_height.saturating_sub(HEADER_HEIGHT)
    }

    pub fn new(data: &'a ParquetSampleData) -> Self {
        Self {
            data,
            projection: None,
            width_cache: None,
            local_width_cache: RefCell::default(),
            title: format!("Data Preview ({} rows)", commas(data.total_rows as u64)),
            title_color: Color::Cyan,
            border_style: border::ROUNDED,
            horizontal_scroll: 0,
            vertical_scroll: 0,
            selected_row: None,
            selected_column: None,
            selection_mode: SelectionMode::Row,
            selected_color: Color::Rgb(60, 60, 60),
            border_color: Color::DarkGray,
        }
    }

    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }

    pub fn with_projection(mut self, projection: Option<&'a [usize]>) -> Self {
        self.projection = projection;
        self
    }

    pub(crate) fn with_width_cache(mut self, cache: &'a RefCell<ColumnWidthCache>) -> Self {
        self.width_cache = Some(cache);
        self
    }

    fn source_columns(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.column_count())
            .map(|index| self.projection.map_or(index, |indices| indices[index]))
    }

    fn column_count(&self) -> usize {
        self.projection
            .map_or(self.data.total_columns, <[usize]>::len)
    }

    pub fn with_colors(mut self, title: Color, selected: Color) -> Self {
        self.title_color = title;
        self.selected_color = selected;
        self
    }

    pub fn with_border_style(mut self, border_style: border::Set) -> Self {
        self.border_style = border_style;
        self
    }

    pub fn with_horizontal_scroll(mut self, offset: usize) -> Self {
        self.horizontal_scroll = offset;
        self
    }

    pub fn with_vertical_scroll(mut self, offset: usize) -> Self {
        self.vertical_scroll = offset;
        self
    }

    pub fn with_selected_row(mut self, row: Option<usize>) -> Self {
        self.selected_row = row;
        self
    }

    pub fn with_selection(mut self, mode: SelectionMode, column: usize) -> Self {
        self.selection_mode = mode;
        self.selected_column = Some(column);
        self
    }

    pub fn scroll_left(&mut self) {
        if self.horizontal_scroll > 0 {
            self.horizontal_scroll -= 1;
        }
    }

    pub fn scroll_right(&mut self) {
        let max_scroll = self.get_max_scroll();
        if self.horizontal_scroll < max_scroll {
            self.horizontal_scroll += 1;
        }
    }

    pub fn get_max_scroll(&self) -> usize {
        // Calculate how many columns we can show at reasonable width
        let available_width = 120; // Assume reasonable terminal width
        let min_column_width = 12; // Minimum width for readability
        let max_visible_columns = available_width / min_column_width;

        // Total columns minus visible columns
        self.column_count().saturating_sub(max_visible_columns)
    }

    /// Rendered width of every column (from the rows in view). Basis for both the
    /// scroll bound and how many columns are drawn, so they stay in agreement.
    fn all_column_widths(&self, area_height: u16) -> Ref<'_, [u16]> {
        let win = self.data.loaded();
        let rows_height = Self::visible_rows(area_height);
        let key = (self.vertical_scroll, rows_height, win.start, win.rows.len());
        let cell = self.width_cache.unwrap_or(&self.local_width_cache);
        let cache = cell.borrow();
        let valid = cache.viewport == Some(key)
            && cache.projection.as_deref() == self.projection
            && cache.widths.len() == self.column_count();
        drop(cache);
        if !valid {
            let start = self
                .vertical_scroll
                .saturating_sub(win.start)
                .min(win.rows.len());
            let end = start
                .saturating_add(usize::from(rows_height))
                .min(win.rows.len());
            let widths = self.calculate_column_widths(&win.rows[start..end]);
            let mut cache = cell.borrow_mut();
            cache.viewport = Some(key);
            cache.projection = self.projection.map(<[usize]>::to_vec);
            cache.widths = widths;
            #[cfg(test)]
            {
                cache.calculations += 1;
            }
        }
        Ref::map(cell.borrow(), |cache| cache.widths.as_slice())
    }

    /// Digits in the row number gutter for a render area `area_height` tall.
    fn row_num_digits(&self, area_height: u16) -> u16 {
        row_num_digits(
            self.vertical_scroll,
            Self::visible_rows(area_height),
            self.data.total_rows,
        )
    }

    /// Maximum horizontal scroll offset for a render area; the last column is
    /// always reachable.
    pub fn max_horizontal_scroll(&self, area_width: u16, area_height: u16) -> usize {
        let row_num_section_width =
            self.row_num_digits(area_height) + 2 * NUM_SPACES_AFTER_LINE_NUMBER + 1;
        let available_width = area_width.saturating_sub(row_num_section_width);
        max_scroll_from_widths(&self.all_column_widths(area_height), available_width)
    }

    /// Keep the selected column fully visible, allowing one over-wide column
    /// to be clipped in terminals too narrow to show it in full.
    pub fn horizontal_scroll_for_selection(
        &self,
        area_width: u16,
        area_height: u16,
        selected_column: usize,
    ) -> usize {
        let gutter = self.row_num_digits(area_height) + 2 * NUM_SPACES_AFTER_LINE_NUMBER + 1;
        let available_width = area_width.saturating_sub(gutter);
        let widths = self.all_column_widths(area_height);
        let max_scroll = max_scroll_from_widths(&widths, available_width);
        let selected_column = selected_column.min(widths.len().saturating_sub(1));
        let mut scroll = self.horizontal_scroll.min(max_scroll).min(selected_column);
        while scroll < max_scroll
            && selected_column >= scroll + columns_fitting(&widths, scroll, available_width)
        {
            scroll += 1;
        }
        scroll
    }

    fn calculate_column_widths(&self, visible_rows: &[Vec<String>]) -> Vec<u16> {
        let mut widths = Vec::new();

        for col_idx in self.source_columns() {
            let header = &self.data.flattened_columns[col_idx];
            let mut max_width = capped_width(header, MAX_COLUMN_WIDTH);

            // Check content width for this column
            for row in visible_rows {
                if max_width == MAX_COLUMN_WIDTH {
                    break;
                }
                if let Some(cell) = row.get(col_idx) {
                    max_width = max_width.max(capped_width(cell, MAX_COLUMN_WIDTH));
                }
            }

            // Use minimum width of 8 and maximum of 25 for readability, add spacing
            widths.push((max_width.max(8) as u16) + NUM_SPACES_BETWEEN_COLUMNS);
        }

        widths
    }

    fn render_header_separator(&self, buf: &mut Buffer, area: Rect, x_row_separator: u16, y: u16) {
        let border_style = ratatui::style::Style::default().fg(self.border_color);

        // Draw horizontal line
        for x in 0..area.width {
            if let Some(cell) = buf.cell_mut(Position::new(x, y - 1)) {
                cell.set_symbol(line::HORIZONTAL).set_style(border_style);
            }
        }

        // Intersection with row number separator
        if let Some(cell) = buf.cell_mut(Position::new(x_row_separator - 1, y - 1)) {
            cell.set_symbol(line::HORIZONTAL_DOWN)
                .set_style(border_style);
        }
    }

    fn render_row_numbers(&self, buf: &mut Buffer, area: Rect, rows: &[Vec<&str>]) {
        let mut y = area.y;

        for (row_idx, _) in rows.iter().take(usize::from(area.height)).enumerate() {
            let actual_row_num = row_idx + self.vertical_scroll + 1;
            let is_selected = self
                .selected_row
                .is_some_and(|selected| row_idx + self.vertical_scroll == selected);

            let row_num_formatted = format!("{}", actual_row_num);
            let mut style: ratatui::prelude::Style =
                ratatui::style::Style::default().fg(Color::DarkGray);
            if is_selected && self.selection_mode != SelectionMode::Column {
                style = style
                    .add_modifier(Modifier::BOLD)
                    .add_modifier(Modifier::UNDERLINED);
            }
            let span = Span::styled(row_num_formatted, style);
            buf.set_span(0, y, &span, area.width);
            y += 1;
            if y >= area.bottom() {
                break;
            }
        }
    }

    fn render_header(
        &self,
        buf: &mut Buffer,
        x_start: u16,
        y: u16,
        headers: &[&str],
        column_widths: &[u16],
        max_width: u16,
    ) {
        let mut x_offset = x_start;

        for (column_idx, (header, &width)) in headers.iter().zip(column_widths).enumerate() {
            if x_offset >= max_width {
                break;
            }

            let effective_width = width.saturating_sub(NUM_SPACES_BETWEEN_COLUMNS);
            let truncated = truncate_cell(header, effective_width);
            let padding = usize::from(width).saturating_sub(Span::raw(truncated.as_ref()).width());

            let mut style = ratatui::style::Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD);
            if self.selection_mode == SelectionMode::Column
                && self.selected_column == Some(self.horizontal_scroll + column_idx)
            {
                style = style.bg(self.selected_color);
            }
            let span = Span::styled(format!("{truncated}{}", " ".repeat(padding)), style);

            buf.set_span(x_offset, y, &span, width.min(max_width - x_offset));
            x_offset += width;
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn render_data_row(
        &self,
        buf: &mut Buffer,
        x_start: u16,
        y: u16,
        row_data: &[&str],
        column_widths: &[u16],
        is_selected: bool,
        max_width: u16,
    ) {
        let mut x_offset = x_start;

        for (column_idx, (cell_data, &width)) in row_data.iter().zip(column_widths).enumerate() {
            if x_offset >= max_width {
                break;
            }

            let column_selected = self.selected_column == Some(self.horizontal_scroll + column_idx);
            let highlighted = match self.selection_mode {
                SelectionMode::Row => is_selected,
                SelectionMode::Column => column_selected,
                SelectionMode::Cell => is_selected && column_selected,
            };
            let style = if highlighted {
                ratatui::style::Style::default()
                    .bg(self.selected_color)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD)
            } else {
                ratatui::style::Style::default()
            };
            let effective_width = width.saturating_sub(NUM_SPACES_BETWEEN_COLUMNS);
            let truncated = truncate_cell(cell_data, effective_width);

            // Pad with spaces to fill the column width
            let padding = usize::from(width).saturating_sub(Span::raw(truncated.as_ref()).width());
            let padded = format!("{truncated}{}", " ".repeat(padding));
            let span = Span::styled(padded, style);

            buf.set_span(x_offset, y, &span, width.min(max_width - x_offset));
            x_offset += width;
        }
    }

    fn render_row_number_separator(
        &self,
        buf: &mut Buffer,
        x_row_separator: u16,
        y_start: u16,
        height: u16,
    ) {
        let border_style = ratatui::style::Style::default().fg(self.border_color);

        // Draw vertical line after row numbers
        for y in y_start..(y_start + height) {
            if let Some(cell) = buf.cell_mut(Position::new(x_row_separator - 1, y)) {
                cell.set_symbol(line::VERTICAL).set_style(border_style);
            }
        }
    }
}

impl<'a> Widget for DataTable<'a> {
    fn render(mut self, area: Rect, buf: &mut Buffer) {
        if area.width == 0 || area.height < HEADER_HEIGHT {
            return;
        }

        let win = self.data.loaded();

        // Calculate row number section width
        let max_row_num_length = self.row_num_digits(area.height);
        let row_num_section_width = max_row_num_length + 2 * NUM_SPACES_AFTER_LINE_NUMBER + 1;
        let x_row_separator = max_row_num_length + NUM_SPACES_AFTER_LINE_NUMBER + 1;

        // Calculate available width for data columns
        let available_width = area.width.saturating_sub(row_num_section_width);

        // Clamp scroll to the actual end (last column flush right), then show as
        // many columns as truly fit from there. Sizing by real column widths
        // (not a flat estimate) keeps wide columns from pushing the last ones
        // off-screen / out of reach.
        self.horizontal_scroll = self
            .horizontal_scroll
            .min(self.max_horizontal_scroll(area.width, area.height));
        let all_widths = self.all_column_widths(area.height);
        let horizontal_scroll = self.horizontal_scroll;
        let max_visible_columns = columns_fitting(&all_widths, horizontal_scroll, available_width);

        // Get visible columns
        let columns: Vec<_> = self
            .source_columns()
            .skip(horizontal_scroll)
            .take(max_visible_columns)
            .collect();
        let visible_headers: Vec<&str> = columns
            .iter()
            .map(|&index| self.data.flattened_columns[index].as_str())
            .collect();

        // Get visible data for each row (apply vertical scroll). Rows live in a
        // bounded window; index into it relative to the window start.
        let row_skip = self.vertical_scroll.saturating_sub(win.start);
        let visible_rows: Vec<Vec<&str>> = win
            .rows
            .iter()
            .skip(row_skip)
            .take(usize::from(Self::visible_rows(area.height)))
            .map(|row| {
                columns
                    .iter()
                    .map(|&index| row.get(index).map_or("", String::as_str))
                    .collect()
            })
            .collect();

        // Calculate column widths
        let column_widths = &all_widths[horizontal_scroll..];

        // Header area: header text + separator
        let y_header = area.y;
        let y_first_record = area.y + HEADER_HEIGHT;

        // Row area: including row numbers and row content
        let rows_area = Rect::new(
            area.x,
            y_first_record,
            area.width,
            Self::visible_rows(area.height),
        );

        // Render row numbers
        self.render_row_numbers(buf, rows_area, &visible_rows);

        // Render header
        self.render_header(
            buf,
            row_num_section_width,
            y_header,
            &visible_headers,
            column_widths,
            area.width,
        );

        // Render header separator (horizontal line below headers)
        self.render_header_separator(buf, area, x_row_separator, y_first_record);

        // Render data rows
        for (y_offset, (row_idx, row_data)) in
            (y_first_record..).zip(visible_rows.iter().enumerate())
        {
            if y_offset >= rows_area.bottom() {
                break;
            }
            let actual_row_num = row_idx + self.vertical_scroll;
            let is_selected = self
                .selected_row
                .is_some_and(|selected| actual_row_num == selected);

            self.render_data_row(
                buf,
                row_num_section_width,
                y_offset,
                row_data,
                column_widths,
                is_selected,
                area.width,
            );
        }

        // Render vertical separator after row numbers
        self.render_row_number_separator(buf, x_row_separator, y_first_record, rows_area.height);
    }
}

impl Renderable for ParquetSampleData {
    fn render_content(&self, area: Rect, buf: &mut Buffer) {
        let table_component = DataTable::new(self);
        table_component.render(area, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widths_ignore_offscreen_rows_and_cache_across_layout_and_rendering() {
        let data = ParquetSampleData::from_rows(
            &["a", "b", "c"],
            vec![
                vec!["short".into(), "short".into(), "short".into()],
                vec!["longer_visible_value".into(), "x".into(), "x".into()],
                vec!["x".repeat(1_000_000), "x".into(), "x".into()],
            ],
        );
        let cache = RefCell::new(ColumnWidthCache::default());
        let projection = [0, 2];
        let table = DataTable::new(&data)
            .with_projection(Some(&projection))
            .with_width_cache(&cache);
        assert_eq!(&*table.all_column_widths(3), &[10, 10]);
        assert_eq!(table.max_horizontal_scroll(80, 3), 0);
        assert_eq!(table.horizontal_scroll_for_selection(80, 3, 1), 0);
        let area = Rect::new(0, 0, 80, 3);
        table.render(area, &mut Buffer::empty(area));
        assert_eq!(cache.borrow().calculations, 1);

        let table = DataTable::new(&data)
            .with_projection(Some(&projection))
            .with_width_cache(&cache)
            .with_selection(SelectionMode::Cell, 1)
            .with_selected_row(Some(0))
            .with_horizontal_scroll(1);
        // Horizontal resizing and cursor movement reuse widths for the same rows.
        assert_eq!(table.max_horizontal_scroll(20, 3), 1);
        assert_eq!(cache.borrow().calculations, 1);
        assert_eq!(table.all_column_widths(4)[0], 22);
        assert_eq!(cache.borrow().calculations, 2);
        let reordered = DataTable::new(&data)
            .with_projection(Some(&[2, 0]))
            .with_width_cache(&cache);
        assert_eq!(&*reordered.all_column_widths(4), &[10, 22]);
        assert_eq!(cache.borrow().calculations, 3);
        let table = table.with_vertical_scroll(2);
        assert_eq!(table.all_column_widths(3)[0], 27);
        assert_eq!(cache.borrow().calculations, 4);
        let table = table.with_projection(Some(&[1]));
        assert_eq!(&*table.all_column_widths(3), &[10]);
        assert_eq!(cache.borrow().calculations, 5);
        let table = table.with_projection(None);
        assert_eq!(&*table.all_column_widths(3), &[27, 10, 10]);
        assert_eq!(cache.borrow().calculations, 6);
    }

    #[test]
    fn width_cache_recomputes_after_the_reader_window_changes() {
        let path = format!(
            "{}/alltypes_tiny_pages.parquet",
            crate::file::parquet_test_data()
        );
        let data = ParquetSampleData::open(&path, 7300).unwrap();
        let cache = RefCell::default();
        let table = DataTable::new(&data).with_width_cache(&cache);
        table.max_horizontal_scroll(80, 6);
        assert_eq!(cache.borrow().calculations, 1);
        data.ensure_loaded(6000, 4);
        table.max_horizontal_scroll(80, 6);
        assert_eq!(cache.borrow().calculations, 2);
    }

    #[test]
    fn capped_measurement_and_clipping_preserve_unicode_graphemes() {
        for text in [
            "",
            "abcdef",
            "資料📊",
            "e\u{301}clair",
            "👩‍💻 family 👨‍👩‍👧‍👦",
            "x\nlong",
        ] {
            for width in 0..15 {
                assert_eq!(
                    capped_width(text, width),
                    Span::raw(text).width().min(width)
                );
                let clipped = truncate_cell(text, width as u16);
                assert!(Span::raw(clipped.as_ref()).width() <= width);
            }
        }
        assert_eq!(truncate_cell("abcdef", 4), "abc…");
        assert_eq!(truncate_cell("資料", 3), "資…");
        assert_eq!(truncate_cell("👩‍💻xyz", 3), "👩‍💻…");
        assert_eq!(truncate_cell("e\u{301}clair", 3), "e\u{301}c…");
    }

    #[test]
    fn projection_keeps_headers_values_and_highlights_aligned() {
        let data = sample_data();
        let projection = [1, 4];
        for mode in [
            SelectionMode::Row,
            SelectionMode::Column,
            SelectionMode::Cell,
        ] {
            let area = Rect::new(0, 0, 80, 6);
            let mut buf = Buffer::empty(area);
            let table = DataTable::new(&data)
                .with_projection(Some(&projection))
                .with_selected_row(Some(1))
                .with_selection(mode, 1);
            let widths = table.all_column_widths(area.height);
            assert_eq!(widths.len(), 2);
            let second_x = 9 + widths[0];
            drop(widths);
            table.render(area, &mut buf);
            let line = |y| (0..80).map(|x| buf[(x, y)].symbol()).collect::<String>();
            assert!(line(0).contains(&data.flattened_columns[1]));
            assert!(line(0).contains(&data.flattened_columns[4]));
            assert!(!line(0).contains(&data.flattened_columns[0]));
            let win = data.loaded();
            let rendered_value: String = (second_x..80).map(|x| buf[(x, 3)].symbol()).collect();
            assert!(rendered_value.starts_with(&win.rows[1][4]));
            assert_eq!(buf[(second_x, 3)].bg, Color::Rgb(60, 60, 60));
            assert_eq!(
                buf[(second_x, 0)].bg == Color::Rgb(60, 60, 60),
                mode == SelectionMode::Column
            );
        }
    }

    fn sample_data() -> ParquetSampleData {
        let path = format!(
            "{}/alltypes_plain.parquet",
            crate::file::parquet_test_data()
        );
        ParquetSampleData::open(&path, 8).unwrap()
    }

    #[test]
    fn highlights_only_the_selected_row_column_or_cell_after_horizontal_scrolling() {
        let data = sample_data();
        let selected_color = Color::Rgb(60, 60, 60);
        for scroll in [0, 1] {
            // The first two columns are ten terminal cells wide, after a nine-cell gutter.
            let selected_x = if scroll == 0 { 19 } else { 9 };
            let other_x = if scroll == 0 { 9 } else { 19 };
            for mode in [
                SelectionMode::Row,
                SelectionMode::Column,
                SelectionMode::Cell,
            ] {
                let area = Rect::new(0, 0, 80, 6);
                let mut buf = Buffer::empty(area);
                DataTable::new(&data)
                    .with_horizontal_scroll(scroll)
                    .with_selected_row(Some(1))
                    .with_selection(mode, 1)
                    .render(area, &mut buf);

                assert_eq!(
                    buf[(selected_x, 3)].bg,
                    selected_color,
                    "{mode:?}, {scroll}"
                );
                assert_eq!(buf[(other_x, 2)].bg, Color::Reset);
                assert_eq!(
                    buf[(selected_x, 2)].bg == selected_color,
                    mode == SelectionMode::Column
                );
                assert_eq!(
                    buf[(other_x, 3)].bg == selected_color,
                    mode == SelectionMode::Row
                );
                assert_eq!(
                    buf[(selected_x, 0)].bg == selected_color,
                    mode == SelectionMode::Column
                );
            }
        }
    }

    #[test]
    fn selected_column_stays_visible_for_variable_widths_and_resizing() {
        let data = sample_data();
        let selected_column = data.total_columns - 1;
        let mut scroll = 0;
        for width in [100, 40, 20, 80] {
            scroll = DataTable::new(&data)
                .with_horizontal_scroll(scroll)
                .horizontal_scroll_for_selection(width, 6, selected_column);
            let area = Rect::new(0, 0, width, 6);
            let mut buf = Buffer::empty(area);
            DataTable::new(&data)
                .with_horizontal_scroll(scroll)
                .with_selected_row(Some(0))
                .with_selection(SelectionMode::Cell, selected_column)
                .render(area, &mut buf);
            assert!((9..width).any(|x| buf[(x, 2)].bg == Color::Rgb(60, 60, 60)));
        }
        assert_eq!(
            DataTable::new(&data)
                .with_horizontal_scroll(scroll)
                .horizontal_scroll_for_selection(40, 6, 0),
            0
        );
    }

    #[test]
    fn selection_handles_unicode_headers_and_tiny_or_empty_tables() {
        let mut data = sample_data();
        data.flattened_columns[0] = "資料📊".repeat(20);
        for width in 0..35 {
            for height in 0..5 {
                let area = Rect::new(0, 0, width, height);
                let mut buf = Buffer::empty(area);
                DataTable::new(&data)
                    .with_selection(SelectionMode::Column, 0)
                    .render(area, &mut buf);
            }
        }
        let mut buf = Buffer::empty(Rect::new(0, 0, 40, 6));
        DataTable::new(&ParquetSampleData::default())
            .with_selection(SelectionMode::Cell, 0)
            .render(buf.area, &mut buf);
    }

    #[test]
    fn test_columns_fitting_counts_what_fits() {
        let widths = [10, 10, 10, 10];
        // 10 + 10 = 20 <= 25; a third column would overflow.
        assert_eq!(columns_fitting(&widths, 0, 25), 2);
        // Exact fit.
        assert_eq!(columns_fitting(&widths, 0, 20), 2);
        // Counting from a non-zero offset.
        assert_eq!(columns_fitting(&widths, 2, 25), 2);
    }

    #[test]
    fn test_columns_fitting_shows_at_least_one() {
        // A single column wider than the whole area is still shown (clipped),
        // never zero columns.
        assert_eq!(columns_fitting(&[40], 0, 10), 1);
        assert_eq!(columns_fitting(&[40, 40], 0, 10), 1);
    }

    #[test]
    fn test_max_scroll_is_zero_when_all_columns_fit() {
        assert_eq!(max_scroll_from_widths(&[10, 10, 10], 100), 0);
    }

    #[test]
    fn test_max_scroll_reaches_the_last_column() {
        // Five 20-wide columns in 45 cells: two fit, so the last column becomes
        // flush-right at offset 3. A flat "width / 12" estimate capped this at 2
        // and left the final columns unreachable.
        let widths = [20, 20, 20, 20, 20];
        let max = max_scroll_from_widths(&widths, 45);
        assert_eq!(max, 3);
        // At the max offset the trailing columns (including the last) all fit.
        assert_eq!(max + columns_fitting(&widths, max, 45), widths.len());
    }

    #[test]
    fn test_max_scroll_with_a_wide_trailing_column() {
        // Only the last (over-wide) column fits from the right.
        assert_eq!(max_scroll_from_widths(&[10, 10, 50], 30), 2);
    }

    #[test]
    fn test_row_num_digits_has_a_minimum() {
        assert_eq!(row_num_digits(0, 40, 0), 4);
        assert_eq!(row_num_digits(0, 40, 7), 4);
        // Top of a huge file: only rows 1..=40 are on screen.
        assert_eq!(row_num_digits(0, 40, 60_000_000), 4);
    }

    #[test]
    fn test_row_num_digits_follows_the_last_row_in_view() {
        // Rows 9,961..=10,000 are on screen: 10,000 needs 5 digits.
        assert_eq!(row_num_digits(9_960, 40, 60_000_000), 5);
        // One row up, the last visible row is 9,999.
        assert_eq!(row_num_digits(9_959, 40, 60_000_000), 4);
        assert_eq!(row_num_digits(30_000_000, 40, 60_000_000), 8);
    }

    #[test]
    fn test_row_num_digits_is_capped_by_total_rows() {
        // Bottom of the file: the view extends past the last row, which is 99,999.
        assert_eq!(row_num_digits(99_990, 40, 99_999), 5);
        assert_eq!(row_num_digits(59_999_960, 40, 60_000_000), 8);
        assert_eq!(row_num_digits(999_999_990, 40, 1_000_000_000), 10);
    }
}
