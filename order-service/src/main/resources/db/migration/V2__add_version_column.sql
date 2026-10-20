-- V2__add_version_column.sql
-- Add optimistic locking version column to orders and reservations tables

ALTER TABLE orders ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE reservations ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;
