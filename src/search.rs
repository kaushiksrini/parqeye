use crate::file::schema::SchemaInfo;
use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use std::collections::{BTreeSet, HashMap};

/// Result of fuzzy search including match positions for highlighting
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Indices of matching schema items (preserves tree structure)
    pub indices: Vec<usize>,
    /// Match positions for each matching primitive (schema_index -> positions in name)
    pub match_positions: HashMap<usize, Vec<usize>>,
}

/// Filter schema items, returning indices of matches with match positions for highlighting.
/// Preserves tree structure by including parent groups when primitives match.
pub fn filter_schema_with_positions(items: &[SchemaInfo], query: &str) -> SearchResult {
    if query.is_empty() {
        return SearchResult {
            indices: (0..items.len()).collect(),
            match_positions: HashMap::new(),
        };
    }

    let matcher = SkimMatcherV2::default();
    let mut result = BTreeSet::new();
    let mut match_positions: HashMap<usize, Vec<usize>> = HashMap::new();
    result.insert(0); // Always include root

    let mut current_groups: Vec<usize> = Vec::new();

    for (idx, item) in items.iter().enumerate() {
        match item {
            SchemaInfo::Root { .. } => {
                current_groups.clear();
            }
            SchemaInfo::Group { .. } => {
                current_groups.push(idx);
            }
            SchemaInfo::Primitive { name, .. } => {
                if let Some((_, positions)) = matcher.fuzzy_indices(name, query) {
                    result.insert(idx);
                    match_positions.insert(idx, positions);
                    for &g in &current_groups {
                        result.insert(g);
                    }
                }
            }
        }
    }

    SearchResult {
        indices: result.into_iter().collect(),
        match_positions,
    }
}

/// Filter schema items, returning indices of matches.
/// Preserves tree structure by including parent groups when primitives match.
pub fn filter_schema_indices(items: &[SchemaInfo], query: &str) -> Vec<usize> {
    filter_schema_with_positions(items, query).indices
}

/// Get filtered primitive column indices (1-based, matching vertical_offset convention).
/// Returns indices that can be used for navigation when filtering is active.
pub fn get_filtered_primitive_indices(items: &[SchemaInfo], query: &str) -> Vec<usize> {
    if query.is_empty() {
        // Return all primitive indices (1-based)
        return items
            .iter()
            .enumerate()
            .filter(|(_, item)| matches!(item, SchemaInfo::Primitive { .. }))
            .enumerate()
            .map(|(prim_idx, _)| prim_idx + 1) // 1-based
            .collect();
    }

    let matcher = SkimMatcherV2::default();
    let mut primitive_idx = 0usize;
    let mut result = Vec::new();

    for item in items.iter() {
        if let SchemaInfo::Primitive { name, .. } = item {
            primitive_idx += 1; // Increment for each primitive (1-based)
            if matcher.fuzzy_match(name, query).is_some() {
                result.push(primitive_idx);
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> Vec<SchemaInfo> {
        vec![
            SchemaInfo::Root {
                name: "root".to_string(),
                display: "└─ root".to_string(),
            },
            SchemaInfo::Primitive {
                name: "user_id".to_string(),
                display: "   ├─ user_id".to_string(),
                info: Box::new(crate::file::schema::ColumnSchemaInfo {
                    name: "user_id".to_string(),
                    repetition: "REQUIRED".to_string(),
                    physical: "INT64".to_string(),
                    logical: "".to_string(),
                    codec: "SNAPPY".to_string(),
                    converted_type: "".to_string(),
                    encoding: "PLAIN".to_string(),
                    dictionary_values: None,
                }),
                stats: crate::file::schema::ColumnStats {
                    min: None,
                    max: None,
                    nulls: 0,
                    distinct: None,
                    total_compressed_size: 0,
                    total_uncompressed_size: 0,
                },
            },
            SchemaInfo::Primitive {
                name: "username".to_string(),
                display: "   ├─ username".to_string(),
                info: Box::new(crate::file::schema::ColumnSchemaInfo {
                    name: "username".to_string(),
                    repetition: "OPTIONAL".to_string(),
                    physical: "BYTE_ARRAY".to_string(),
                    logical: "".to_string(),
                    codec: "SNAPPY".to_string(),
                    converted_type: "".to_string(),
                    encoding: "PLAIN".to_string(),
                    dictionary_values: None,
                }),
                stats: crate::file::schema::ColumnStats {
                    min: None,
                    max: None,
                    nulls: 0,
                    distinct: None,
                    total_compressed_size: 0,
                    total_uncompressed_size: 0,
                },
            },
            SchemaInfo::Primitive {
                name: "email".to_string(),
                display: "   └─ email".to_string(),
                info: Box::new(crate::file::schema::ColumnSchemaInfo {
                    name: "email".to_string(),
                    repetition: "OPTIONAL".to_string(),
                    physical: "BYTE_ARRAY".to_string(),
                    logical: "".to_string(),
                    codec: "SNAPPY".to_string(),
                    converted_type: "".to_string(),
                    encoding: "PLAIN".to_string(),
                    dictionary_values: None,
                }),
                stats: crate::file::schema::ColumnStats {
                    min: None,
                    max: None,
                    nulls: 0,
                    distinct: None,
                    total_compressed_size: 0,
                    total_uncompressed_size: 0,
                },
            },
        ]
    }

    #[test]
    fn test_empty_query_returns_all() {
        let schema = create_test_schema();
        let indices = filter_schema_indices(&schema, "");
        assert_eq!(indices, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_exact_match() {
        let schema = create_test_schema();
        let indices = filter_schema_indices(&schema, "email");
        assert!(indices.contains(&0)); // root
        assert!(indices.contains(&3)); // email
        assert!(!indices.contains(&1)); // user_id
        assert!(!indices.contains(&2)); // username
    }

    #[test]
    fn test_fuzzy_match() {
        let schema = create_test_schema();
        let indices = filter_schema_indices(&schema, "user");
        assert!(indices.contains(&0)); // root
        assert!(indices.contains(&1)); // user_id
        assert!(indices.contains(&2)); // username
        assert!(!indices.contains(&3)); // email
    }

    #[test]
    fn test_no_match() {
        let schema = create_test_schema();
        let indices = filter_schema_indices(&schema, "zzz");
        assert_eq!(indices, vec![0]); // only root
    }

    // Tests for filter_schema_with_positions

    #[test]
    fn test_with_positions_empty_query() {
        let schema = create_test_schema();
        let result = filter_schema_with_positions(&schema, "");
        assert_eq!(result.indices, vec![0, 1, 2, 3]);
        assert!(result.match_positions.is_empty());
    }

    #[test]
    fn test_with_positions_exact_match() {
        let schema = create_test_schema();
        let result = filter_schema_with_positions(&schema, "email");

        // Check indices
        assert!(result.indices.contains(&0)); // root
        assert!(result.indices.contains(&3)); // email

        // Check match positions for "email" - should match all characters
        let positions = result.match_positions.get(&3).unwrap();
        assert_eq!(positions, &vec![0, 1, 2, 3, 4]); // e-m-a-i-l
    }

    #[test]
    fn test_with_positions_partial_match() {
        let schema = create_test_schema();
        let result = filter_schema_with_positions(&schema, "user");

        // Both user_id and username should match
        assert!(result.match_positions.contains_key(&1)); // user_id
        assert!(result.match_positions.contains_key(&2)); // username

        // Check positions for user_id - "user" matches first 4 chars
        let positions = result.match_positions.get(&1).unwrap();
        assert_eq!(positions, &vec![0, 1, 2, 3]);

        // Check positions for username - "user" matches first 4 chars
        let positions = result.match_positions.get(&2).unwrap();
        assert_eq!(positions, &vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_with_positions_fuzzy_match() {
        let schema = create_test_schema();
        // "uid" should fuzzy match "user_id" at positions u(0), i(5), d(6)
        let result = filter_schema_with_positions(&schema, "uid");

        assert!(result.match_positions.contains_key(&1)); // user_id
        let positions = result.match_positions.get(&1).unwrap();
        // Fuzzy matcher finds u, i, d in "user_id"
        assert!(positions.contains(&0)); // 'u'
        assert!(positions.contains(&5)); // 'i'
        assert!(positions.contains(&6)); // 'd'
    }

    #[test]
    fn test_with_positions_no_match() {
        let schema = create_test_schema();
        let result = filter_schema_with_positions(&schema, "zzz");

        assert_eq!(result.indices, vec![0]); // only root
        assert!(result.match_positions.is_empty());
    }

    // Tests for get_filtered_primitive_indices

    #[test]
    fn test_primitive_indices_empty_query() {
        let schema = create_test_schema();
        let indices = get_filtered_primitive_indices(&schema, "");

        // Should return all primitive indices (1-based)
        assert_eq!(indices, vec![1, 2, 3]); // user_id=1, username=2, email=3
    }

    #[test]
    fn test_primitive_indices_single_match() {
        let schema = create_test_schema();
        let indices = get_filtered_primitive_indices(&schema, "email");

        // Only email matches, which is the 3rd primitive (1-based)
        assert_eq!(indices, vec![3]);
    }

    #[test]
    fn test_primitive_indices_multiple_matches() {
        let schema = create_test_schema();
        let indices = get_filtered_primitive_indices(&schema, "user");

        // user_id (1) and username (2) should match
        assert_eq!(indices, vec![1, 2]);
    }

    #[test]
    fn test_primitive_indices_no_match() {
        let schema = create_test_schema();
        let indices = get_filtered_primitive_indices(&schema, "zzz");

        assert!(indices.is_empty());
    }

    #[test]
    fn test_primitive_indices_with_groups() {
        // Create schema with a group
        let schema = vec![
            SchemaInfo::Root {
                name: "root".to_string(),
                display: "└─ root".to_string(),
            },
            SchemaInfo::Group {
                name: "address".to_string(),
                display: "   ├─ address".to_string(),
                repetition: "OPTIONAL".to_string(),
            },
            SchemaInfo::Primitive {
                name: "street".to_string(),
                display: "   │  ├─ street".to_string(),
                info: Box::new(crate::file::schema::ColumnSchemaInfo {
                    name: "street".to_string(),
                    repetition: "OPTIONAL".to_string(),
                    physical: "BYTE_ARRAY".to_string(),
                    logical: "".to_string(),
                    codec: "SNAPPY".to_string(),
                    converted_type: "".to_string(),
                    encoding: "PLAIN".to_string(),
                    dictionary_values: None,
                }),
                stats: crate::file::schema::ColumnStats {
                    min: None,
                    max: None,
                    nulls: 0,
                    distinct: None,
                    total_compressed_size: 0,
                    total_uncompressed_size: 0,
                },
            },
            SchemaInfo::Primitive {
                name: "city".to_string(),
                display: "   │  └─ city".to_string(),
                info: Box::new(crate::file::schema::ColumnSchemaInfo {
                    name: "city".to_string(),
                    repetition: "OPTIONAL".to_string(),
                    physical: "BYTE_ARRAY".to_string(),
                    logical: "".to_string(),
                    codec: "SNAPPY".to_string(),
                    converted_type: "".to_string(),
                    encoding: "PLAIN".to_string(),
                    dictionary_values: None,
                }),
                stats: crate::file::schema::ColumnStats {
                    min: None,
                    max: None,
                    nulls: 0,
                    distinct: None,
                    total_compressed_size: 0,
                    total_uncompressed_size: 0,
                },
            },
            SchemaInfo::Primitive {
                name: "email".to_string(),
                display: "   └─ email".to_string(),
                info: Box::new(crate::file::schema::ColumnSchemaInfo {
                    name: "email".to_string(),
                    repetition: "OPTIONAL".to_string(),
                    physical: "BYTE_ARRAY".to_string(),
                    logical: "".to_string(),
                    codec: "SNAPPY".to_string(),
                    converted_type: "".to_string(),
                    encoding: "PLAIN".to_string(),
                    dictionary_values: None,
                }),
                stats: crate::file::schema::ColumnStats {
                    min: None,
                    max: None,
                    nulls: 0,
                    distinct: None,
                    total_compressed_size: 0,
                    total_uncompressed_size: 0,
                },
            },
        ];

        // Empty query should return all primitives (groups don't count)
        let indices = get_filtered_primitive_indices(&schema, "");
        assert_eq!(indices, vec![1, 2, 3]); // street=1, city=2, email=3

        // Filter for "city" should return only index 2
        let indices = get_filtered_primitive_indices(&schema, "city");
        assert_eq!(indices, vec![2]);

        // Filter for "e" should match street and email
        let indices = get_filtered_primitive_indices(&schema, "e");
        assert_eq!(indices, vec![1, 3]); // street has 'e', email has 'e'
    }
}
