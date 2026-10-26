-- Flyway migration V3: Add retry backoff and status columns to outbox_events to prevent Head-of-Line blocking (Scenario 02 / REM-05)
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS status VARCHAR(32) NOT NULL DEFAULT 'PENDING';

CREATE INDEX IF NOT EXISTS idx_outbox_eligible ON outbox_events (next_retry_at, created_at)
WHERE published_at IS NULL AND status = 'PENDING';
