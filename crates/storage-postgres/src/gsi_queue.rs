// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Persistent GSI update queue (D-4, D-5, D-6).
//!
//! Base table writes insert a row into `gsi_pending` within the same
//! transaction as the item mutation. Worker tasks claim rows using
//! `SELECT ... FOR UPDATE SKIP LOCKED`, apply index updates, and delete
//! processed rows atomically. Pending updates survive process crash/restart.
//!
//! Index metadata is cached in memory per `table_id` to avoid repeated
//! catalog queries on the hot path, matching the old in-memory queue's
//! zero-catalog-query performance.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use extenddb_core::types::{AttributeDefinition, Item, KeySchemaElement, Projection};
use extenddb_storage::error::StorageError;
use sqlx::PgPool;
use tokio::sync::{Mutex, Notify};

use crate::data::{
    all_sort_key_info, delete_index_row_multi, index_table_name, insert_index_row_multi,
    item_has_index_keys, project_item_for_index,
};

/// Number of worker tasks consuming from the persistent queue.
const NUM_WORKERS: u64 = 4;

/// Maximum rows to claim per batch per worker.
const BATCH_SIZE: i64 = 100;

/// Cache TTL for index metadata. Catalog changes (CreateTable, UpdateTable)
/// are rare; hitting the catalog on every row would negate the benefit of
/// the persistent queue.
const CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// PostgreSQL SQLSTATE code for "undefined_table" (relation does not exist).
const PG_UNDEFINED_TABLE: &str = "42P01";

fn is_undefined_table(err: &StorageError) -> bool {
    match err {
        StorageError::Internal(msg) => msg.contains(PG_UNDEFINED_TABLE),
        _ => false,
    }
}

/// Cached index metadata for a single table.
struct CachedTableMeta {
    base_key_schema: Vec<KeySchemaElement>,
    attr_defs: Vec<AttributeDefinition>,
    indexes: Vec<IndexMeta>,
    fetched_at: std::time::Instant,
}

/// Per-index metadata needed to apply a GSI update.
struct IndexMeta {
    index_id: String,
    key_schema: Vec<KeySchemaElement>,
    projection: Projection,
    effective_delay_ms: u64,
}

/// Persistent GSI propagation queue backed by the `gsi_pending` table.
pub struct GsiQueue {
    data_pool: PgPool,
    catalog_pool: PgPool,
    notify: Arc<Notify>,
    gsi_default_delay_ms: Arc<AtomicU64>,
    cache: Mutex<HashMap<String, CachedTableMeta>>,
}

impl GsiQueue {
    /// Create the queue and spawn worker tasks.
    pub fn spawn(
        data_pool: PgPool,
        catalog_pool: PgPool,
        gsi_default_delay_ms: Arc<AtomicU64>,
    ) -> Arc<Self> {
        let notify = Arc::new(Notify::new());
        let q = Arc::new(Self {
            data_pool,
            catalog_pool,
            notify,
            gsi_default_delay_ms,
            cache: Mutex::new(HashMap::new()),
        });

        for worker_id in 0..NUM_WORKERS {
            let q = Arc::clone(&q);
            tokio::spawn(async move {
                worker(worker_id, q).await;
            });
        }

        q
    }

    /// Wake workers after a write inserts into `gsi_pending`.
    pub fn notify_workers(&self) {
        self.notify.notify_waiters();
    }

    /// Get or refresh cached metadata for a table.
    async fn get_table_meta(
        &self,
        table_id: &str,
    ) -> Result<Option<Arc<CachedTableMeta>>, StorageError> {
        // Fast path: check cache under lock.
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(table_id) {
                if entry.fetched_at.elapsed() < CACHE_TTL {
                    // SAFETY: We need to return owned data outside the lock.
                    // Clone the relevant data into an Arc to avoid holding the lock.
                    let meta = Arc::new(CachedTableMeta {
                        base_key_schema: entry.base_key_schema.clone(),
                        attr_defs: entry.attr_defs.clone(),
                        indexes: entry
                            .indexes
                            .iter()
                            .map(|i| IndexMeta {
                                index_id: i.index_id.clone(),
                                key_schema: i.key_schema.clone(),
                                projection: i.projection.clone(),
                                effective_delay_ms: i.effective_delay_ms,
                            })
                            .collect(),
                        fetched_at: entry.fetched_at,
                    });
                    return Ok(Some(meta));
                }
            }
        }

        // Slow path: query catalog.
        let table_row: Option<(serde_json::Value, serde_json::Value)> = sqlx::query_as(
            "SELECT key_schema, attribute_definitions FROM tables WHERE table_id = $1",
        )
        .bind(table_id)
        .fetch_optional(&self.catalog_pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((ks_json, ad_json)) = table_row else {
            // Table deleted — evict from cache.
            self.cache.lock().await.remove(table_id);
            return Ok(None);
        };

        let base_key_schema: Vec<KeySchemaElement> =
            serde_json::from_value(ks_json).map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs: Vec<AttributeDefinition> =
            serde_json::from_value(ad_json).map_err(|e| StorageError::Internal(e.to_string()))?;

        let index_rows: Vec<(
            String,
            String,
            serde_json::Value,
            serde_json::Value,
            Option<i32>,
        )> = sqlx::query_as(
            "SELECT index_id, index_type, key_schema, projection, propagation_delay_ms \
                 FROM indexes WHERE table_id = $1",
        )
        .bind(table_id)
        .fetch_all(&self.catalog_pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let system_delay = self
            .gsi_default_delay_ms
            .load(std::sync::atomic::Ordering::Relaxed);

        let mut indexes = Vec::new();
        for (index_id, index_type, ks_json, proj_json, per_gsi_delay) in index_rows {
            if index_type == "LSI" {
                continue;
            }
            let effective_delay = match per_gsi_delay {
                Some(0) => 0u64,
                Some(ms) if ms > 0 => ms as u64,
                _ => system_delay,
            };
            if effective_delay == 0 {
                continue;
            }
            let key_schema: Vec<KeySchemaElement> = serde_json::from_value(ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let projection: Projection = serde_json::from_value(proj_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            indexes.push(IndexMeta {
                index_id,
                key_schema,
                projection,
                effective_delay_ms: effective_delay,
            });
        }

        let entry = CachedTableMeta {
            base_key_schema: base_key_schema.clone(),
            attr_defs: attr_defs.clone(),
            indexes: indexes
                .iter()
                .map(|i| IndexMeta {
                    index_id: i.index_id.clone(),
                    key_schema: i.key_schema.clone(),
                    projection: i.projection.clone(),
                    effective_delay_ms: i.effective_delay_ms,
                })
                .collect(),
            fetched_at: std::time::Instant::now(),
        };

        let result = Arc::new(CachedTableMeta {
            base_key_schema,
            attr_defs,
            indexes,
            fetched_at: entry.fetched_at,
        });

        self.cache.lock().await.insert(table_id.to_owned(), entry);
        Ok(Some(result))
    }
}

/// Insert a pending GSI update within an existing transaction.
///
/// `delay_ms` is the effective propagation delay — the row becomes eligible
/// for processing after `NOW() + delay_ms`.
pub(crate) async fn enqueue_gsi_pending(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table_id: &str,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    delay_ms: u64,
) -> Result<(), StorageError> {
    let old_json = old_item
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let new_json = new_item
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    let delay_interval = delay_ms as f64 / 1000.0;
    sqlx::query(
        "INSERT INTO gsi_pending (table_id, old_item, new_item, ready_at) \
         VALUES ($1, $2, $3, NOW() + make_interval(secs => $4))",
    )
    .bind(table_id)
    .bind(old_json)
    .bind(new_json)
    .bind(delay_interval)
    .execute(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(())
}

/// Worker loop. Claims rows using FOR UPDATE SKIP LOCKED.
async fn worker(worker_id: u64, q: Arc<GsiQueue>) {
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

    tracing::debug!("GSI worker {worker_id} started");

    loop {
        match process_batch(worker_id, &q).await {
            Ok(0) => {
                tokio::time::timeout(POLL_INTERVAL, q.notify.notified())
                    .await
                    .ok();
            }
            Ok(_) => continue,
            Err(e) => {
                tracing::error!("GSI worker {worker_id}: batch error: {e}");
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    }
}

/// Claim and process a batch of ready rows. Returns the number processed.
///
/// Only rows with `ready_at <= NOW()` are eligible — the propagation delay
/// is enforced by the timestamp set at enqueue time, not by sleeping.
///
/// Each row is processed in its own transaction:
///   1. DELETE FROM gsi_pending WHERE ... RETURNING (atomic claim)
///   2. Write GSI updates
///   3. COMMIT
///
/// If crash after DELETE but before COMMIT → rollback → row reappears.
/// Workers never block each other (DELETE only touches ready rows by PK).
async fn process_batch(worker_id: u64, q: &GsiQueue) -> Result<usize, StorageError> {
    // Claim a batch of ready rows atomically.
    let rows: Vec<(
        i64,
        String,
        Option<serde_json::Value>,
        Option<serde_json::Value>,
    )> = sqlx::query_as(
        "DELETE FROM gsi_pending \
         WHERE id IN ( \
             SELECT id FROM gsi_pending \
             WHERE ready_at <= NOW() \
             ORDER BY id \
             LIMIT $1 \
             FOR UPDATE SKIP LOCKED \
         ) \
         RETURNING id, table_id, old_item, new_item",
    )
    .bind(BATCH_SIZE)
    .fetch_all(&q.data_pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    if rows.is_empty() {
        return Ok(0);
    }

    let count = rows.len();

    for (id, table_id, old_json, new_json) in rows {
        if let Err(e) = apply_row(worker_id, q, id, &table_id, old_json, new_json).await {
            tracing::error!("GSI worker {worker_id}: failed id={id} table={table_id}: {e}");
        }
    }

    Ok(count)
}

/// Apply GSI updates for a single claimed row.
async fn apply_row(
    worker_id: u64,
    q: &GsiQueue,
    id: i64,
    table_id: &str,
    old_json: Option<serde_json::Value>,
    new_json: Option<serde_json::Value>,
) -> Result<(), StorageError> {
    let old_item: Option<Item> = old_json
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let new_item: Option<Item> = new_json
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    let Some(meta) = q.get_table_meta(table_id).await? else {
        tracing::debug!("GSI worker {worker_id}: table {table_id} deleted, skipping id={id}");
        return Ok(());
    };

    let mut tx = q
        .data_pool
        .begin()
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    for idx in &meta.indexes {
        let idx_table = index_table_name(&idx.index_id);
        let idx_sks = all_sort_key_info(&idx.key_schema, &meta.attr_defs);
        let base_sks = all_sort_key_info(&meta.base_key_schema, &meta.attr_defs);

        if let Some(ref old) = old_item {
            if item_has_index_keys(old, &idx.key_schema) {
                delete_index_row_multi(
                    &mut tx,
                    &idx_table,
                    old,
                    &meta.base_key_schema,
                    &meta.attr_defs,
                    &base_sks,
                )
                .await
                .or_else(|e| {
                    if is_undefined_table(&e) {
                        Ok(())
                    } else {
                        Err(e)
                    }
                })?;
            }
        }

        if let Some(ref new) = new_item {
            if item_has_index_keys(new, &idx.key_schema) {
                let projected = project_item_for_index(
                    new,
                    &idx.key_schema,
                    &meta.base_key_schema,
                    &idx.projection,
                );
                insert_index_row_multi(
                    &mut tx,
                    &idx_table,
                    new,
                    &projected,
                    &idx.key_schema,
                    &meta.base_key_schema,
                    &meta.attr_defs,
                    &idx_sks,
                    &base_sks,
                )
                .await
                .or_else(|e| {
                    if is_undefined_table(&e) {
                        Ok(())
                    } else {
                        Err(e)
                    }
                })?;
            }
        }
    }

    tx.commit()
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    tracing::trace!("GSI worker {worker_id}: processed id={id} table={table_id}");
    Ok(())
}
