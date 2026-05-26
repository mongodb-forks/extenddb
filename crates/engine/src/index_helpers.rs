// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for secondary index operations in the engine layer.

use extenddb_core::types::{IndexInfo, Item, KeySchemaElement, ProjectionType};

/// Build the combined key schema for `LastEvaluatedKey` extraction.
///
/// For index queries/scans, the LEK includes both the base table key attributes
/// and the index key attributes (deduplicated), matching real `DynamoDB` behavior.
pub fn combined_lek_key_schema(
    base_key_schema: &[KeySchemaElement],
    index_info: Option<&IndexInfo>,
) -> Vec<KeySchemaElement> {
    let Some(idx) = index_info else {
        return base_key_schema.to_vec();
    };
    let mut combined = base_key_schema.to_vec();
    for ks in &idx.key_schema {
        if !combined
            .iter()
            .any(|k| k.attribute_name == ks.attribute_name)
        {
            combined.push(ks.clone());
        }
    }
    combined
}

/// Filter an item to only the attributes projected into a secondary index.
///
/// For `ProjectionType::All`, returns the item unchanged.
/// For `KeysOnly`, retains only the base table and index key attributes.
/// For `Include`, retains keys plus the explicitly included `NonKeyAttributes`.
pub fn apply_index_projection(
    item: &Item,
    index_info: &IndexInfo,
    base_key_schema: &[KeySchemaElement],
) -> Item {
    match index_info.projection.projection_type {
        ProjectionType::All => item.clone(),
        ProjectionType::KeysOnly | ProjectionType::Include => {
            let mut allowed: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
            for ks in base_key_schema {
                allowed.insert(&ks.attribute_name);
            }
            for ks in &index_info.key_schema {
                allowed.insert(&ks.attribute_name);
            }
            if let Some(ref non_key) = index_info.projection.non_key_attributes {
                for attr in non_key {
                    allowed.insert(attr);
                }
            }
            item.iter()
                .filter(|(k, _)| allowed.contains(k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
    }
}

/// Validate that an `ExclusiveStartKey` contains the required key elements for Scan.
///
/// For base table scans, uses the long DynamoDB error message.
/// For index scans, uses the short message (matching real DynamoDB behavior).
pub fn validate_scan_exclusive_start_key(
    start_key: &Item,
    key_info: &extenddb_core::types::TableKeyInfo,
    index_info: Option<&IndexInfo>,
) -> Result<(), extenddb_core::error::DynamoDbError> {
    let required = combined_lek_key_schema(&key_info.key_schema, index_info);
    for ks in &required {
        if !start_key.contains_key(&ks.attribute_name) {
            let msg = if index_info.is_some() {
                "The provided starting key is invalid".to_owned()
            } else {
                "The provided starting key is invalid: The provided key element does not match the schema".to_owned()
            };
            return Err(extenddb_core::error::DynamoDbError::ValidationException(
                msg,
            ));
        }
    }
    Ok(())
}
