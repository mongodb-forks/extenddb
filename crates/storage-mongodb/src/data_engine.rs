// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DataEngine` trait implementation for `MongoEngine`.

use bson::{Document, doc};
use futures::future::BoxFuture;
use mongodb::options::{FindOneAndDeleteOptions, FindOneAndReplaceOptions, ReturnDocument};

use extenddb_core::expression::{
    self, Expr, ExpressionMaps, KeyCondition, SortKeyCondition, UpdateAction,
};
use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType, StreamEventName,
    StreamRecord, StreamRecordData, TableKeyInfo, extract_key, item_size_bytes,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    composite_pk_to_text, encode_netstring_composite, pk_to_text, sk_info,
};
use extenddb_storage::{
    DataEngine, ItemPairResult, QueryResult, StreamCapture, StreamEngine, TransactGetOp,
    TransactWriteOp,
};

use crate::MongoEngine;
use crate::condition::condition_to_filter;
use crate::data::{
    data_collection_name, document_to_item, item_to_document, pk_filter, sk_field_name,
};

use extenddb_core::types::{AttributeDefinition, Projection, ProjectionType};

impl DataEngine for MongoEngine {
    fn put_item(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let item = item.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            self.put_item_impl(
                &key_info,
                item,
                return_old,
                condition.as_ref(),
                &maps,
                stream.as_ref(),
            )
            .await
        })
    }

    fn get_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        Box::pin(async move { self.get_item_impl(&key_info, &key).await })
    }

    fn delete_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            self.delete_item_impl(
                &key_info,
                &key,
                return_old,
                condition.as_ref(),
                &maps,
                stream.as_ref(),
            )
            .await
        })
    }

    fn update_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, ItemPairResult> {
        let key_info = key_info.clone();
        let key = key.clone();
        let actions = actions.to_vec();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            self.update_item_impl(
                &key_info,
                &key,
                &actions,
                return_old,
                return_new,
                condition.as_ref(),
                &maps,
                stream.as_ref(),
            )
            .await
        })
    }

    fn query(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        let key_info = key_info.clone();
        let key_condition = key_condition.clone();
        let maps = maps.clone();
        let exclusive_start_key = exclusive_start_key.cloned();
        let index_name = index_name.map(std::string::ToString::to_string);
        Box::pin(async move {
            self.query_impl(
                &key_info,
                &key_condition,
                &maps,
                forward,
                limit,
                exclusive_start_key.as_ref(),
                index_name.as_deref(),
            )
            .await
        })
    }

    fn scan(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        let key_info = key_info.clone();
        let exclusive_start_key = exclusive_start_key.cloned();
        let index_name = index_name.map(std::string::ToString::to_string);
        Box::pin(async move {
            self.scan_impl(
                &key_info,
                limit,
                exclusive_start_key.as_ref(),
                segment,
                total_segments,
                index_name.as_deref(),
            )
            .await
        })
    }

    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        let ops_data: Vec<_> = ops
            .iter()
            .map(|op| (op.key_info.clone(), op.key.clone()))
            .collect();
        Box::pin(async move { self.transact_get_items_impl(&ops_data).await })
    }

    fn transact_write_items(
        &self,
        ops: &[TransactWriteOp<'_>],
        token: Option<(&str, &str)>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let ops_owned: Vec<_> = ops.iter().map(clone_transact_write_op).collect();
        let token_owned = token.map(|(t, f)| (t.to_owned(), f.to_owned()));
        Box::pin(async move {
            self.transact_write_items_impl(
                &ops_owned,
                token_owned.as_ref().map(|(t, f)| (t.as_str(), f.as_str())),
            )
            .await
        })
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            let coll = self.data_db.collection::<Document>("idempotency_tokens");
            let cutoff = time::OffsetDateTime::now_utc()
                - std::time::Duration::from_secs(max_age_seconds as u64);
            let cutoff_bson = mongodb::bson::DateTime::from_millis(cutoff.unix_timestamp() * 1000);
            let result = coll
                .delete_many(doc! { "created_at": { "$lt": cutoff_bson } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(result.deleted_count)
        })
    }
}

impl MongoEngine {
    async fn write_stream_inline(
        &self,
        key_info: &TableKeyInfo,
        capture: &StreamCapture,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
    ) -> Result<(), StorageError> {
        use extenddb_core::types::StreamViewType;

        let source_item = new_item.or(old_item);
        let Some(source) = source_item else {
            return Ok(());
        };

        let event = match (old_item, new_item) {
            (None, Some(_)) => StreamEventName::Insert,
            (Some(_), Some(_)) => StreamEventName::Modify,
            (Some(_), None) => StreamEventName::Remove,
            (None, None) => return Ok(()),
        };

        let keys: std::collections::BTreeMap<String, AttributeValue> = key_info
            .key_schema
            .iter()
            .filter_map(|ks| {
                source
                    .get(&ks.attribute_name)
                    .map(|v| (ks.attribute_name.clone(), v.clone()))
            })
            .collect();

        let new_image = match capture.view_type {
            StreamViewType::NewImage | StreamViewType::NewAndOldImages => new_item.cloned(),
            _ => None,
        };
        let old_image = match capture.view_type {
            StreamViewType::OldImage | StreamViewType::NewAndOldImages => old_item.cloned(),
            _ => None,
        };

        let size = source_item.map_or(0, |i| i64::try_from(item_size_bytes(i)).unwrap_or(i64::MAX));

        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_str = source
            .get(pk_name)
            .map(|v| match v {
                AttributeValue::S(s) => s.clone(),
                AttributeValue::N(n) => n.clone(),
                AttributeValue::B(b) => {
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b)
                }
                _ => String::new(),
            })
            .unwrap_or_default();

        let shard_id = self
            .assign_shard(&key_info.account_id, &key_info.table_name, &pk_str)
            .await?;
        let seq = self.next_sequence_number(&shard_id).await?;

        let record = StreamRecord {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_name: event,
            event_version: "1.1".to_owned(),
            event_source: "aws:dynamodb".to_owned(),
            aws_region: capture.region.to_string(),
            dynamodb: StreamRecordData {
                approximate_creation_date_time: i64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                )
                .unwrap_or(i64::MAX),
                keys,
                new_image,
                old_image,
                sequence_number: seq,
                size_bytes: size,
                stream_view_type: capture.view_type,
            },
            user_identity: capture.user_identity.clone(),
        };

        self.write_stream_record(
            &key_info.account_id,
            &record,
            &shard_id,
            &key_info.table_name,
        )
        .await
    }

    async fn put_item_impl(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let new_doc =
            item_to_document(&item, &key_info.key_schema, &key_info.attribute_definitions)?;
        let key_filter = pk_filter(&item, &key_info.key_schema, &key_info.attribute_definitions)?;

        // Always get old item for GSI sync
        let old_item: Option<Item>;
        let return_val: Option<Item>;

        if let Some(cond) = condition {
            let existing_doc = coll
                .find_one(key_filter.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if let Some(ref existing) = existing_doc {
                let existing_item = document_to_item(existing)?;
                let passed = expression::evaluate_condition(cond, &existing_item, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if !passed {
                    return Err(StorageError::ConditionFailed(Some(existing_item)));
                }
                let opts = FindOneAndReplaceOptions::builder()
                    .return_document(ReturnDocument::Before)
                    .build();
                let old_doc = coll
                    .find_one_and_replace(key_filter, new_doc)
                    .with_options(opts)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                old_item = old_doc.as_ref().map(document_to_item).transpose()?;
                return_val = if return_old { old_item.clone() } else { None };
            } else {
                let empty = std::collections::BTreeMap::new();
                let passed = expression::evaluate_condition(cond, &empty, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if !passed {
                    return Err(StorageError::ConditionFailed(None));
                }
                if let Err(e) = coll.insert_one(new_doc).await {
                    if e.to_string().contains("E11000") {
                        let winner = coll
                            .find_one(key_filter)
                            .await
                            .map_err(|e2| StorageError::Internal(e2.to_string()))?
                            .map(|d| document_to_item(&d))
                            .transpose()?;
                        return Err(StorageError::ConditionFailed(winner));
                    }
                    return Err(StorageError::Internal(e.to_string()));
                }
                old_item = None;
                return_val = None;
            }
        } else {
            // Unconditional put: always use findOneAndReplace to get old item for GSI
            let opts = FindOneAndReplaceOptions::builder()
                .upsert(true)
                .return_document(ReturnDocument::Before)
                .build();
            let old_doc = coll
                .find_one_and_replace(key_filter, new_doc)
                .with_options(opts)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            old_item = old_doc.as_ref().map(document_to_item).transpose()?;
            return_val = if return_old { old_item.clone() } else { None };
        }

        // Sync GSI collections
        self.sync_indexes(key_info, old_item.as_ref(), Some(&item))
            .await?;

        // Write stream record
        if let Some(capture) = stream {
            self.write_stream_inline(key_info, capture, old_item.as_ref(), Some(&item))
                .await?;
        }

        Ok(return_val)
    }

    async fn get_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let filter = pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)?;
        let doc = coll
            .find_one(filter)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        doc.as_ref().map(document_to_item).transpose()
    }

    async fn delete_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let key_filter = pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)?;

        let deleted_item: Option<Item>;

        if let Some(cond) = condition {
            let existing_doc = coll
                .find_one(key_filter.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if let Some(ref existing) = existing_doc {
                let existing_item = document_to_item(existing)?;
                let passed = expression::evaluate_condition(cond, &existing_item, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if !passed {
                    return Err(StorageError::ConditionFailed(Some(existing_item)));
                }
                coll.delete_one(key_filter)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                deleted_item = Some(existing_item);
            } else {
                let empty = std::collections::BTreeMap::new();
                let passed = expression::evaluate_condition(cond, &empty, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if !passed {
                    return Err(StorageError::ConditionFailed(None));
                }
                deleted_item = None;
            }
        } else {
            // Always use findOneAndDelete to get old item for GSI sync
            let old_doc = coll
                .find_one_and_delete(key_filter)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            deleted_item = old_doc.as_ref().map(document_to_item).transpose()?;
        }

        // Sync GSI collections (remove old entry)
        if deleted_item.is_some() {
            self.sync_indexes(key_info, deleted_item.as_ref(), None)
                .await?;
        }

        // Write stream record
        if let Some(capture) = stream {
            self.write_stream_inline(key_info, capture, deleted_item.as_ref(), None)
                .await?;
        }

        Ok(if return_old { deleted_item } else { None })
    }

    #[allow(clippy::too_many_arguments)]
    async fn update_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<(Option<Item>, Option<Item>), StorageError> {
        let coll_name = data_collection_name(&key_info.table_id);
        let coll = self.data_db.collection::<Document>(&coll_name);

        let key_filter = pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)?;

        // Optimistic concurrency: retry on version conflict
        for _attempt in 0..50 {
            let existing_doc = coll
                .find_one(key_filter.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let current_version = existing_doc
                .as_ref()
                .and_then(|d| d.get_i64("_v").ok())
                .unwrap_or(0);

            let existing_item = if let Some(doc) = existing_doc.as_ref() {
                document_to_item(doc)?
            } else {
                key.clone()
            };

            if let Some(cond) = condition {
                let passed = expression::evaluate_condition(cond, &existing_item, maps)
                    .map_err(|e| StorageError::Validation(e.to_string()))?;
                if !passed {
                    return Err(StorageError::ConditionFailed(Some(existing_item)));
                }
            }

            let need_old = return_old || stream.is_some();
            let old_item = if need_old {
                Some(existing_item.clone())
            } else {
                None
            };

            let mut new_item = existing_item;
            expression::apply_update(actions, &mut new_item, maps)
                .map_err(|e| StorageError::Validation(e.to_string()))?;

            let mut new_doc = item_to_document(
                &new_item,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )?;
            let new_version = current_version + 1;
            new_doc.insert("_v", new_version);

            if existing_doc.is_some() {
                // Conditional replace: match key + version
                let mut versioned_filter = key_filter.clone();
                if current_version == 0 {
                    // Document may not have _v field yet (written by PutItem)
                    versioned_filter.insert("_v", doc! { "$not": { "$gt": 0_i64 } });
                } else {
                    versioned_filter.insert("_v", current_version);
                }
                let result = coll
                    .replace_one(versioned_filter, new_doc)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if result.matched_count == 0 {
                    // Version conflict — retry with jittered backoff
                    let base_us = 100u64.saturating_mul(1u64 << _attempt.min(10));
                    let jitter = rand::random_range(0..=base_us);
                    tokio::time::sleep(std::time::Duration::from_micros(jitter)).await;
                    continue;
                }
            } else {
                // Insert (upsert for new item)
                let opts = mongodb::options::ReplaceOptions::builder()
                    .upsert(true)
                    .build();
                coll.replace_one(key_filter.clone(), new_doc)
                    .with_options(opts)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }

            // Sync GSI collections
            self.sync_indexes(key_info, old_item.as_ref(), Some(&new_item))
                .await?;

            // Write stream record
            if let Some(capture) = stream {
                self.write_stream_inline(key_info, capture, old_item.as_ref(), Some(&new_item))
                    .await?;
            }

            let old_item_result = if return_old { old_item } else { None };
            let new_item_result = if return_new { Some(new_item) } else { None };
            return Ok((old_item_result, new_item_result));
        }

        Err(StorageError::Internal(
            "UpdateItem: too many version conflicts, giving up".to_owned(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_impl(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        use futures::TryStreamExt;

        // Determine collection and effective key schema for the query target
        let (coll_name, effective_key_schema) = if let Some(idx_name) = index_name {
            let idx_info = self
                .index_info_by_table_id_impl(&key_info.table_id, idx_name)
                .await?;
            (
                data_collection_name(&idx_info.index_id),
                idx_info.key_schema.clone(),
            )
        } else {
            (
                data_collection_name(&key_info.table_id),
                key_info.key_schema.clone(),
            )
        };
        let coll = self.data_db.collection::<Document>(&coll_name);

        // Build the query filter — handle multi-part HASH keys
        let pk_text = if key_condition.extra_pk_conditions.is_empty() {
            let pk_value = resolve_key_expr(&key_condition.pk_value, maps)?;
            pk_to_text(&pk_value)
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .into_owned()
        } else {
            let mut parts = Vec::with_capacity(1 + key_condition.extra_pk_conditions.len());
            let first_val = resolve_key_expr(&key_condition.pk_value, maps)?;
            parts.push(
                pk_to_text(&first_val)
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                    .into_owned(),
            );
            for (_, value) in &key_condition.extra_pk_conditions {
                let val = resolve_key_expr(value, maps)?;
                parts.push(
                    pk_to_text(&val)
                        .map_err(|e| StorageError::Internal(e.to_string()))?
                        .into_owned(),
                );
            }
            encode_netstring_composite(&parts)
        };

        let mut filter = doc! { "pk": &pk_text };

        // Determine sort key field using effective key schema
        let sk_field = sk_field_name(&effective_key_schema, &key_info.attribute_definitions);

        // Apply sort key condition
        if let Some(ref sk_cond) = key_condition.sk_condition {
            if let Some(sk_f) = sk_field {
                let sk_filter = build_sk_filter(sk_cond, sk_f, maps)?;
                for (k, v) in sk_filter {
                    filter.insert(k, v);
                }
            }
        }

        // Apply exclusive_start_key pagination
        if let Some(start_key) = exclusive_start_key {
            if let Some(sk_f) = sk_field {
                // Get the sort key value from the start key
                if let Some((sk_name, sk_type)) =
                    sk_info(&effective_key_schema, &key_info.attribute_definitions)
                {
                    if let Some(sk_val) = start_key.get(sk_name) {
                        let sk_bson = sk_to_bson(sk_val, sk_type)?;
                        if forward {
                            filter.insert(sk_f, doc! { "$gt": sk_bson });
                        } else {
                            filter.insert(sk_f, doc! { "$lt": sk_bson });
                        }
                    }
                }
            }
        }

        // Build sort direction
        let sort_direction = if forward { 1 } else { -1 };
        let sort_doc = if let Some(sk_f) = sk_field {
            doc! { sk_f: sort_direction }
        } else {
            doc! { "pk": sort_direction }
        };

        // Apply limit (fetch one extra for pagination)
        let fetch_limit = limit.map(|l| l + 1);

        let collation_opt = if sk_field == Some("sk_s") {
            Some(
                mongodb::options::Collation::builder()
                    .locale("simple".to_string())
                    .build(),
            )
        } else {
            None
        };

        let opts = mongodb::options::FindOptions::builder()
            .sort(sort_doc)
            .limit(fetch_limit)
            .collation(collation_opt)
            .build();

        let cursor = coll
            .find(filter)
            .with_options(opts)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let docs: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut items: Vec<Item> = docs
            .iter()
            .map(document_to_item)
            .collect::<Result<Vec<_>, _>>()?;

        // Handle pagination
        let last_evaluated_key = if let Some(l) = limit {
            #[allow(clippy::cast_sign_loss)]
            let l_usize = l as usize;
            if items.len() > l_usize {
                items.truncate(l_usize);
                items
                    .last()
                    .map(|item| extract_key(item, &key_info.key_schema))
            } else {
                None
            }
        } else {
            None
        };

        Ok((items, last_evaluated_key))
    }

    async fn scan_impl(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        use futures::TryStreamExt;

        let coll_name = if let Some(idx_name) = index_name {
            let idx_info = self
                .index_info_by_table_id_impl(&key_info.table_id, idx_name)
                .await?;
            data_collection_name(&idx_info.index_id)
        } else {
            data_collection_name(&key_info.table_id)
        };
        let coll = self.data_db.collection::<Document>(&coll_name);

        let mut filter = Document::new();

        // Apply exclusive_start_key for pagination (using _id for scan ordering)
        if let Some(start_key) = exclusive_start_key {
            let start_pk = composite_pk_to_text(start_key, &key_info.key_schema)?;
            if let Some((sk_name, sk_type)) =
                sk_info(&key_info.key_schema, &key_info.attribute_definitions)
            {
                if let Some(sk_val) = start_key.get(sk_name) {
                    let sk_text = match sk_val {
                        AttributeValue::S(s) => s.clone(),
                        AttributeValue::N(n) => n.clone(),
                        AttributeValue::B(b) => {
                            use base64::Engine;
                            base64::engine::general_purpose::STANDARD.encode(b)
                        }
                        _ => return Err(StorageError::Internal("invalid sk type".to_string())),
                    };
                    let start_id = format!("{start_pk}#{sk_text}");
                    filter.insert("_id", doc! { "$gt": start_id });
                }
            } else {
                filter.insert("_id", doc! { "$gt": &start_pk });
            }
        }

        // Parallel scan segment filtering
        // segment/total_segments use CRC32 hash of pk modulo total_segments
        let apply_segment_filter = segment.is_some() && total_segments.is_some();

        // Apply limit
        let fetch_limit = limit.map(|l| {
            let extra = l + 1;
            if apply_segment_filter {
                extra * total_segments.unwrap_or(1)
            } else {
                extra
            }
        });

        let opts = mongodb::options::FindOptions::builder()
            .sort(doc! { "_id": 1 })
            .limit(fetch_limit)
            .build();

        let cursor = coll
            .find(filter)
            .with_options(opts)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let docs: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut items: Vec<Item> = Vec::new();
        for doc in &docs {
            let item = document_to_item(doc)?;

            // Apply segment filter if needed
            if let (Some(seg), Some(total)) = (segment, total_segments) {
                let pk_text = composite_pk_to_text(&item, &key_info.key_schema)?;
                let hash = crc32fast::hash(pk_text.as_bytes());
                #[allow(clippy::cast_sign_loss)]
                let total_u = total as u32;
                #[allow(clippy::cast_sign_loss)]
                let seg_u = seg as u32;
                if hash % total_u != seg_u {
                    continue;
                }
            }

            items.push(item);

            // Check if we have enough items
            if let Some(l) = limit {
                #[allow(clippy::cast_sign_loss)]
                if items.len() > l as usize {
                    break;
                }
            }
        }

        // Handle pagination
        let last_evaluated_key = if let Some(l) = limit {
            #[allow(clippy::cast_sign_loss)]
            let l_usize = l as usize;
            if items.len() > l_usize {
                items.truncate(l_usize);
                items
                    .last()
                    .map(|item| extract_key(item, &key_info.key_schema))
            } else {
                None
            }
        } else {
            None
        };

        Ok((items, last_evaluated_key))
    }

    // ── GSI Sync ──────────────────────────────────────────────────────

    async fn sync_indexes(
        &self,
        key_info: &TableKeyInfo,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
    ) -> Result<(), StorageError> {
        use futures::TryStreamExt;

        // Fast path: skip catalog query if we know this table has no GSIs
        if let Some(entry) = self.gsi_cache.get(&key_info.table_id) {
            if !*entry {
                return Ok(());
            }
        }

        let indexes_coll = self.catalog_db.collection::<Document>("indexes");
        let mut cursor = indexes_coll
            .find(doc! { "_id.table_id": &key_info.table_id })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut found_any = false;
        while let Some(idx_doc) = cursor
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            found_any = true;
            let index_id = match idx_doc.get_str("index_id") {
                Ok(id) => id.to_string(),
                Err(_) => continue,
            };
            let idx_key_schema: Vec<KeySchemaElement> = match idx_doc.get("key_schema") {
                Some(ks) => bson::from_bson(ks.clone()).unwrap_or_default(),
                None => continue,
            };
            let projection: Projection = match idx_doc.get("projection") {
                Some(p) => bson::from_bson(p.clone()).unwrap_or(Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                }),
                None => Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                },
            };

            let idx_coll_name = data_collection_name(&index_id);
            let idx_coll = self.data_db.collection::<Document>(&idx_coll_name);

            // Delete old index entry
            if let Some(old) = old_item {
                if item_has_index_keys(old, &idx_key_schema) {
                    let old_filter =
                        pk_filter(old, &idx_key_schema, &key_info.attribute_definitions)?;
                    let _ = idx_coll.delete_one(old_filter).await;
                }
            }

            // Insert new index entry
            if let Some(new) = new_item {
                if item_has_index_keys(new, &idx_key_schema) {
                    let projected =
                        project_item(new, &idx_key_schema, &key_info.key_schema, &projection);
                    let idx_doc = item_to_document(
                        &projected,
                        &idx_key_schema,
                        &key_info.attribute_definitions,
                    )?;
                    let filter =
                        pk_filter(&projected, &idx_key_schema, &key_info.attribute_definitions)?;
                    let opts = mongodb::options::ReplaceOptions::builder()
                        .upsert(true)
                        .build();
                    idx_coll
                        .replace_one(filter, idx_doc)
                        .with_options(opts)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                }
            }
        }

        self.gsi_cache.insert(key_info.table_id.clone(), found_any);
        Ok(())
    }

    // ── Transactions ──────────────────────────────────────────────────

    async fn transact_get_items_impl(
        &self,
        ops: &[(TableKeyInfo, Item)],
    ) -> Result<Vec<Option<Item>>, StorageError> {
        use extenddb_core::types::CancellationReason;
        use extenddb_core::validation;

        // Validate key types before starting transaction
        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        let mut any_failed = false;
        for (key_info, key) in ops {
            match validation::validate_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            ) {
                Ok(()) => reasons.push(CancellationReason::none()),
                Err(e) => {
                    any_failed = true;
                    reasons.push(CancellationReason::validation_error(e.to_string()));
                }
            }
        }
        if any_failed {
            return Err(StorageError::TransactionCanceled(reasons));
        }

        // Use a MongoDB session with snapshot read concern for consistent reads
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tx_options = mongodb::options::TransactionOptions::builder()
            .read_concern(mongodb::options::ReadConcern::snapshot())
            .build();

        session
            .start_transaction()
            .with_options(tx_options)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut results = Vec::with_capacity(ops.len());
        for (key_info, key) in ops {
            let coll_name = data_collection_name(&key_info.table_id);
            let coll = self.data_db.collection::<Document>(&coll_name);
            let filter = pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)?;
            let doc = coll
                .find_one(filter)
                .session(&mut session)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let item = doc.as_ref().map(document_to_item).transpose()?;
            results.push(item);
        }

        session
            .commit_transaction()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(results)
    }

    async fn transact_write_items_impl(
        &self,
        ops: &[OwnedTransactWriteOp],
        token: Option<(&str, &str)>,
    ) -> Result<(), StorageError> {
        use extenddb_core::types::CancellationReason;
        use extenddb_core::validation;

        // Start a MongoDB multi-document transaction
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tx_options = mongodb::options::TransactionOptions::builder()
            .read_concern(mongodb::options::ReadConcern::snapshot())
            .write_concern(
                mongodb::options::WriteConcern::builder()
                    .w(mongodb::options::Acknowledgment::Majority)
                    .build(),
            )
            .build();

        session
            .start_transaction()
            .with_options(tx_options)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Check idempotency token
        if let Some((tok, fp)) = token {
            let idem_coll = self.data_db.collection::<Document>("idempotency_tokens");
            let existing = idem_coll
                .find_one(doc! { "token": tok })
                .session(&mut session)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if let Some(existing_doc) = existing {
                let stored_fp = existing_doc.get_str("fingerprint").unwrap_or_default();
                if stored_fp == fp {
                    session
                        .abort_transaction()
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    return Err(StorageError::IdempotentReplay);
                }
                session
                    .abort_transaction()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                return Err(StorageError::IdempotentMismatch);
            }

            // Store the token
            idem_coll
                .insert_one(doc! {
                    "token": tok,
                    "fingerprint": fp,
                    "created_at": mongodb::bson::DateTime::now(),
                })
                .session(&mut session)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        let mut any_failed = false;

        for op in ops {
            let reason = self
                .execute_transact_write_op_in_session(op, &mut session)
                .await;
            match reason {
                Ok(()) => reasons.push(CancellationReason::none()),
                Err(TransactOpError::Cancel(r)) => {
                    any_failed = true;
                    reasons.push(r);
                }
                Err(TransactOpError::Storage(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }

        if any_failed {
            let _ = session.abort_transaction().await;
            return Err(StorageError::TransactionCanceled(reasons));
        }

        session
            .commit_transaction()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn execute_transact_write_op_in_session(
        &self,
        op: &OwnedTransactWriteOp,
        session: &mut mongodb::ClientSession,
    ) -> Result<(), TransactOpError> {
        use extenddb_core::types::CancellationReason;
        use extenddb_core::validation;

        match op {
            OwnedTransactWriteOp::Put {
                key_info,
                item,
                condition,
                maps,
                ..
            } => {
                validation::validate_item_keys(
                    item,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                let coll_name = data_collection_name(&key_info.table_id);
                let coll = self.data_db.collection::<Document>(&coll_name);
                let key_filter =
                    pk_filter(item, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                let existing_doc = coll
                    .find_one(key_filter.clone())
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                if let Some(cond) = condition {
                    let existing_item = if let Some(doc) = existing_doc.as_ref() {
                        document_to_item(doc).map_err(TransactOpError::Storage)?
                    } else {
                        Item::new()
                    };
                    let passed = expression::evaluate_condition(cond, &existing_item, maps)
                        .map_err(|e| {
                            TransactOpError::Cancel(CancellationReason::validation_error(
                                e.to_string(),
                            ))
                        })?;
                    if !passed {
                        return Err(TransactOpError::Cancel(
                            CancellationReason::condition_check_failed_with_item(None),
                        ));
                    }
                }

                let new_doc =
                    item_to_document(item, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                let opts = mongodb::options::ReplaceOptions::builder()
                    .upsert(true)
                    .build();
                coll.replace_one(key_filter, new_doc)
                    .with_options(opts)
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                Ok(())
            }
            OwnedTransactWriteOp::Delete {
                key_info,
                key,
                condition,
                maps,
                ..
            } => {
                validation::validate_key_only(
                    key,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                let coll_name = data_collection_name(&key_info.table_id);
                let coll = self.data_db.collection::<Document>(&coll_name);
                let key_filter =
                    pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                if let Some(cond) = condition {
                    let existing_doc = coll
                        .find_one(key_filter.clone())
                        .session(&mut *session)
                        .await
                        .map_err(|e| {
                            TransactOpError::Storage(StorageError::Internal(e.to_string()))
                        })?;

                    let existing_item = if let Some(doc) = existing_doc.as_ref() {
                        document_to_item(doc).map_err(TransactOpError::Storage)?
                    } else {
                        Item::new()
                    };
                    let passed = expression::evaluate_condition(cond, &existing_item, maps)
                        .map_err(|e| {
                            TransactOpError::Cancel(CancellationReason::validation_error(
                                e.to_string(),
                            ))
                        })?;
                    if !passed {
                        return Err(TransactOpError::Cancel(
                            CancellationReason::condition_check_failed_with_item(None),
                        ));
                    }
                }

                coll.delete_one(key_filter)
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                Ok(())
            }
            OwnedTransactWriteOp::Update {
                key_info,
                key,
                actions,
                condition,
                maps,
                ..
            } => {
                validation::validate_key_only(
                    key,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                let coll_name = data_collection_name(&key_info.table_id);
                let coll = self.data_db.collection::<Document>(&coll_name);
                let key_filter =
                    pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                let existing_doc = coll
                    .find_one(key_filter.clone())
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                let mut item = if let Some(doc) = existing_doc.as_ref() {
                    document_to_item(doc).map_err(TransactOpError::Storage)?
                } else {
                    key.clone()
                };

                if let Some(cond) = condition {
                    let condition_item = if existing_doc.is_some() {
                        &item
                    } else {
                        &std::collections::BTreeMap::new()
                    };
                    let passed = expression::evaluate_condition(cond, condition_item, maps)
                        .map_err(|e| {
                            TransactOpError::Cancel(CancellationReason::validation_error(
                                e.to_string(),
                            ))
                        })?;
                    if !passed {
                        return Err(TransactOpError::Cancel(
                            CancellationReason::condition_check_failed_with_item(None),
                        ));
                    }
                }

                expression::apply_update(actions, &mut item, maps).map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                let new_doc =
                    item_to_document(&item, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                let opts = mongodb::options::ReplaceOptions::builder()
                    .upsert(true)
                    .build();
                coll.replace_one(key_filter, new_doc)
                    .with_options(opts)
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                Ok(())
            }
            OwnedTransactWriteOp::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                ..
            } => {
                validation::validate_key_only(
                    key,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;

                let coll_name = data_collection_name(&key_info.table_id);
                let coll = self.data_db.collection::<Document>(&coll_name);
                let key_filter =
                    pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions)
                        .map_err(TransactOpError::Storage)?;

                let existing_doc = coll
                    .find_one(key_filter)
                    .session(&mut *session)
                    .await
                    .map_err(|e| TransactOpError::Storage(StorageError::Internal(e.to_string())))?;

                let existing_item = if let Some(doc) = existing_doc.as_ref() {
                    document_to_item(doc).map_err(TransactOpError::Storage)?
                } else {
                    Item::new()
                };

                let passed = expression::evaluate_condition(condition, &existing_item, maps)
                    .map_err(|e| {
                        TransactOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                    })?;
                if !passed {
                    return Err(TransactOpError::Cancel(
                        CancellationReason::condition_check_failed_with_item(None),
                    ));
                }

                Ok(())
            }
        }
    }
}

// ── Transaction helper types ──────────────────────────────────────────

enum TransactOpError {
    Cancel(extenddb_core::types::CancellationReason),
    Storage(StorageError),
}

/// Owned version of `TransactWriteOp` to allow moving into async blocks.
enum OwnedTransactWriteOp {
    Put {
        key_info: TableKeyInfo,
        item: Item,
        condition: Option<Expr>,
        maps: ExpressionMaps,
    },
    Delete {
        key_info: TableKeyInfo,
        key: Item,
        condition: Option<Expr>,
        maps: ExpressionMaps,
    },
    Update {
        key_info: TableKeyInfo,
        key: Item,
        actions: Vec<UpdateAction>,
        condition: Option<Expr>,
        maps: ExpressionMaps,
    },
    ConditionCheck {
        key_info: TableKeyInfo,
        key: Item,
        condition: Expr,
        maps: ExpressionMaps,
    },
}

fn clone_transact_write_op(op: &TransactWriteOp<'_>) -> OwnedTransactWriteOp {
    match op {
        TransactWriteOp::Put {
            key_info,
            item,
            condition,
            maps,
            ..
        } => OwnedTransactWriteOp::Put {
            key_info: (*key_info).clone(),
            item: (*item).clone(),
            condition: condition.cloned(),
            maps: (*maps).clone(),
        },
        TransactWriteOp::Delete {
            key_info,
            key,
            condition,
            maps,
            ..
        } => OwnedTransactWriteOp::Delete {
            key_info: (*key_info).clone(),
            key: (*key).clone(),
            condition: condition.cloned(),
            maps: (*maps).clone(),
        },
        TransactWriteOp::Update {
            key_info,
            key,
            actions,
            condition,
            maps,
            ..
        } => OwnedTransactWriteOp::Update {
            key_info: (*key_info).clone(),
            key: (*key).clone(),
            actions: actions.to_vec(),
            condition: condition.cloned(),
            maps: (*maps).clone(),
        },
        TransactWriteOp::ConditionCheck {
            key_info,
            key,
            condition,
            maps,
            ..
        } => OwnedTransactWriteOp::ConditionCheck {
            key_info: (*key_info).clone(),
            key: (*key).clone(),
            condition: (*condition).clone(),
            maps: (*maps).clone(),
        },
    }
}

/// Resolve a key expression (Placeholder) to an `AttributeValue`.
fn resolve_key_expr(expr: &Expr, maps: &ExpressionMaps) -> Result<AttributeValue, StorageError> {
    match expr {
        Expr::Placeholder(name) => maps
            .resolve_value(name)
            .cloned()
            .map_err(|e| StorageError::Validation(e.to_string())),
        _ => Err(StorageError::Internal(
            "expected placeholder in key condition".to_owned(),
        )),
    }
}

/// Build a `MongoDB` filter for a sort key condition.
fn build_sk_filter(
    sk_cond: &SortKeyCondition,
    sk_field: &str,
    maps: &ExpressionMaps,
) -> Result<Document, StorageError> {
    match sk_cond {
        SortKeyCondition::Compare { op, value, .. } => {
            let av = resolve_key_expr(value, maps)?;
            let sk_type = infer_sk_type_from_field(sk_field);
            let bson_val = sk_to_bson(&av, sk_type)?;

            let filter = match op {
                extenddb_core::expression::CompareOp::Eq => doc! { sk_field: bson_val },
                extenddb_core::expression::CompareOp::Lt => doc! { sk_field: { "$lt": bson_val } },
                extenddb_core::expression::CompareOp::Le => doc! { sk_field: { "$lte": bson_val } },
                extenddb_core::expression::CompareOp::Gt => doc! { sk_field: { "$gt": bson_val } },
                extenddb_core::expression::CompareOp::Ge => doc! { sk_field: { "$gte": bson_val } },
                extenddb_core::expression::CompareOp::Ne => doc! { sk_field: { "$ne": bson_val } },
            };
            Ok(filter)
        }
        SortKeyCondition::Between { low, high, .. } => {
            let sk_type = infer_sk_type_from_field(sk_field);
            let low_av = resolve_key_expr(low, maps)?;
            let high_av = resolve_key_expr(high, maps)?;
            let low_bson = sk_to_bson(&low_av, sk_type)?;
            let high_bson = sk_to_bson(&high_av, sk_type)?;
            Ok(doc! { sk_field: { "$gte": low_bson, "$lte": high_bson } })
        }
        SortKeyCondition::BeginsWith { prefix, .. } => {
            let prefix_av = resolve_key_expr(prefix, maps)?;
            match prefix_av {
                AttributeValue::S(ref p) => {
                    // For begins_with on string sort keys: sk_s >= prefix AND sk_s < prefix + max_char
                    let upper = increment_string(p);
                    Ok(doc! { sk_field: { "$gte": p.as_str(), "$lt": &upper } })
                }
                AttributeValue::B(ref b) => {
                    let upper = increment_bytes(b);
                    let low_bin = bson::Binary {
                        subtype: bson::spec::BinarySubtype::Generic,
                        bytes: b.clone(),
                    };
                    let high_bin = bson::Binary {
                        subtype: bson::spec::BinarySubtype::Generic,
                        bytes: upper,
                    };
                    Ok(doc! { sk_field: { "$gte": low_bin, "$lt": high_bin } })
                }
                _ => Err(StorageError::Validation(
                    "begins_with requires string or binary sort key".to_string(),
                )),
            }
        }
    }
}

/// Convert an `AttributeValue` sort key to the appropriate BSON type.
fn sk_to_bson(
    av: &AttributeValue,
    sk_type: ScalarAttributeType,
) -> Result<bson::Bson, StorageError> {
    match (sk_type, av) {
        (ScalarAttributeType::S, AttributeValue::S(s)) => Ok(bson::Bson::String(s.clone())),
        (ScalarAttributeType::N, AttributeValue::N(n)) => match n.parse::<bson::Decimal128>() {
            Ok(d) => Ok(bson::Bson::Decimal128(d)),
            Err(_) => {
                if let Ok(f) = n.parse::<f64>() {
                    Ok(bson::Bson::Double(f))
                } else {
                    Err(StorageError::Internal(format!(
                        "cannot parse numeric sort key: {n}"
                    )))
                }
            }
        },
        (ScalarAttributeType::B, AttributeValue::B(b)) => Ok(bson::Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: b.clone(),
        })),
        _ => Err(StorageError::Internal("sort key type mismatch".to_string())),
    }
}

/// Infer the `ScalarAttributeType` from the sort key field name.
fn infer_sk_type_from_field(field: &str) -> ScalarAttributeType {
    if field.ends_with("_n") {
        ScalarAttributeType::N
    } else if field.ends_with("_b") {
        ScalarAttributeType::B
    } else {
        ScalarAttributeType::S
    }
}

/// Increment a string to get the exclusive upper bound for `begins_with`.
fn increment_string(s: &str) -> String {
    // Append the maximum Unicode code point
    let mut result = s.to_string();
    result.push(char::MAX);
    result
}

/// Increment bytes to get the exclusive upper bound for `begins_with` on binary.
fn increment_bytes(b: &[u8]) -> Vec<u8> {
    let mut result = b.to_vec();
    // Increment the last byte, with carry
    let mut i = result.len();
    while i > 0 {
        i -= 1;
        if result[i] < 255 {
            result[i] += 1;
            return result;
        }
        result[i] = 0;
    }
    // All bytes were 0xFF; prepend a 0x01 byte (makes it longer)
    result.insert(0, 1);
    result
}

fn item_has_index_keys(item: &Item, idx_key_schema: &[KeySchemaElement]) -> bool {
    idx_key_schema
        .iter()
        .all(|ks| item.contains_key(&ks.attribute_name))
}

fn project_item(
    item: &Item,
    idx_key_schema: &[KeySchemaElement],
    base_key_schema: &[KeySchemaElement],
    projection: &Projection,
) -> Item {
    match projection.projection_type {
        ProjectionType::All => item.clone(),
        ProjectionType::KeysOnly => {
            let mut projected = Item::new();
            for ks in idx_key_schema.iter().chain(base_key_schema.iter()) {
                if let Some(v) = item.get(&ks.attribute_name) {
                    projected.insert(ks.attribute_name.clone(), v.clone());
                }
            }
            projected
        }
        ProjectionType::Include => {
            let mut projected = Item::new();
            // Always include key attributes
            for ks in idx_key_schema.iter().chain(base_key_schema.iter()) {
                if let Some(v) = item.get(&ks.attribute_name) {
                    projected.insert(ks.attribute_name.clone(), v.clone());
                }
            }
            // Include non-key attributes from projection
            if let Some(ref attrs) = projection.non_key_attributes {
                for attr in attrs {
                    if let Some(v) = item.get(attr) {
                        projected.insert(attr.clone(), v.clone());
                    }
                }
            }
            projected
        }
    }
}
