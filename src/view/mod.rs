//! Maps visible row positions to original, zero-based file rows.
use roaring::RoaringTreemap;
use std::sync::Arc;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RowIndex {
    #[default]
    All,
    Filtered(Arc<RoaringTreemap>),
}
impl RowIndex {
    pub fn same_rows(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::All, Self::All) => true,
            (Self::Filtered(a), Self::Filtered(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
    pub fn len(&self, total: usize) -> usize {
        match self {
            Self::All => total,
            Self::Filtered(rows) => rows.len() as usize,
        }
    }
    pub fn is_empty(&self, total: usize) -> bool {
        self.len(total) == 0
    }
    pub fn file_row(&self, position: usize) -> Option<usize> {
        match self {
            Self::All => Some(position),
            Self::Filtered(rows) => rows.select(position as u64).map(|r| r as usize),
        }
    }
    /// Position of the first visible file row >= `row`, or None after the end.
    pub fn position_at_or_after(&self, row: usize, total: usize) -> Option<usize> {
        let position = match self {
            Self::All => row,
            Self::Filtered(rows) => {
                if row == 0 {
                    0
                } else {
                    rows.rank(row as u64 - 1) as usize
                }
            }
        };
        (position < self.len(total)).then_some(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rank_select_preserves_file_rows_and_handles_empty_and_end() {
        let rows = RowIndex::Filtered(Arc::new(
            [2, 5, 9, u32::MAX as u64 + 3].into_iter().collect(),
        ));
        assert_eq!(rows.len(usize::MAX), 4);
        for (position, file) in [2, 5, 9, u32::MAX as usize + 3].into_iter().enumerate() {
            assert_eq!(rows.file_row(position), Some(file));
            assert_eq!(rows.position_at_or_after(file, usize::MAX), Some(position));
        }
        assert_eq!(rows.position_at_or_after(3, usize::MAX), Some(1));
        assert_eq!(rows.file_row(4), None);
        assert_eq!(
            rows.position_at_or_after(u32::MAX as usize + 4, usize::MAX),
            None
        );
        assert_eq!(RowIndex::All.position_at_or_after(10, 10), None);
        assert_eq!(
            RowIndex::Filtered(Arc::default()).position_at_or_after(0, 10),
            None
        );
    }
}
