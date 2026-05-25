-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Persistent queue for async GSI propagation.
-- Inserted atomically within the base write transaction, consumed by
-- background workers. Survives process crash/restart.

CREATE TABLE IF NOT EXISTS gsi_pending (
    id BIGSERIAL PRIMARY KEY,
    table_id TEXT NOT NULL,
    old_item JSONB,
    new_item JSONB,
    ready_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gsi_pending_ready
    ON gsi_pending (ready_at);
