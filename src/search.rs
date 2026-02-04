use crate::file::schema::SchemaInfo;
use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use std::collections::BTreeSet;

/// Filter schema items, returning indices of matches.
/// Preserves tree structure by including parent groups when primitives match.
pub fn filter_schema_indices(items: &[SchemaInfo], query: &str) -> Vec<usize> {
    if query.is_empty() {
        return (0..items.len()).collect();
    }

    let matcher = SkimMatcherV2::default();
    let mut result = BTreeSet::new();
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
                if matcher.fuzzy_match(name, query).is_some() {
                    result.insert(idx);
                    for &g in &current_groups {
                        result.insert(g);
                    }
                }
            }
        }
    }

    result.into_iter().collect()
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
}
