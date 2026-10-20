-- V1__init_schema.sql
-- FlashSale Engine initial schema baseline

CREATE TABLE IF NOT EXISTS sales (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    total_capacity INTEGER NOT NULL CHECK (total_capacity >= 0),
    status VARCHAR(32) NOT NULL DEFAULT 'DRAFT',
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_sales_dates CHECK (ends_at > starts_at),
    CONSTRAINT chk_sales_status CHECK (status IN ('DRAFT', 'OPEN', 'PAUSED', 'CLOSED'))
);

CREATE INDEX IF NOT EXISTS idx_sales_status_dates ON sales (status, starts_at, ends_at);

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    sale_id UUID NOT NULL REFERENCES sales(id) ON DELETE RESTRICT,
    shopper_id VARCHAR(128) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    reservation_id UUID NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    payment_reference VARCHAR(128),
    failure_code VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_order_status CHECK (
        status IN ('PENDING', 'PROCESSING', 'CONFIRMED', 'PAYMENT_FAILED', 'EXPIRED', 'CANCELLED')
    ),
    CONSTRAINT uq_orders_idempotency UNIQUE (sale_id, shopper_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_orders_sale_id ON orders (sale_id);
CREATE INDEX IF NOT EXISTS idx_orders_shopper_id ON orders (shopper_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_reservation_id ON orders (reservation_id);

CREATE TABLE IF NOT EXISTS reservations (
    id UUID PRIMARY KEY,
    sale_id UUID NOT NULL REFERENCES sales(id) ON DELETE RESTRICT,
    order_id UUID REFERENCES orders(id) ON DELETE SET NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_reservation_status CHECK (
        status IN ('ACTIVE', 'COMMITTED', 'RELEASED', 'EXPIRED')
    )
);

CREATE INDEX IF NOT EXISTS idx_reservations_sale_status ON reservations (sale_id, status);
CREATE INDEX IF NOT EXISTS idx_reservations_expires_at ON reservations (expires_at) WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed ON outbox_events (created_at) 
WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_name VARCHAR(64) NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_name, event_id)
);
