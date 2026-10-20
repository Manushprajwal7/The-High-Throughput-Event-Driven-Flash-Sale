# FlashSale Engine --- Data Model Baseline

This document specifies the durable PostgreSQL relational schema, indexes, constraints, Flyway migration conventions, and Redis in-memory key storage designs.

---

## 1. PostgreSQL Relational Schema

PostgreSQL 16 serves as the source of truth for durable business records, order lifecycle history, outbox events, and consumer idempotency locks.

### 1.1 Table: `sales`
Stores flash sale campaigns and master capacity constraints.

```sql
CREATE TABLE sales (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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

CREATE INDEX idx_sales_status_dates ON sales (status, starts_at, ends_at);
```

---

### 1.2 Table: `orders`
Stores durable purchase records, state progression, and idempotency fingerprints.

```sql
CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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

CREATE INDEX idx_orders_sale_id ON orders (sale_id);
CREATE INDEX idx_orders_shopper_id ON orders (shopper_id);
CREATE INDEX idx_orders_status ON orders (status);
CREATE INDEX idx_orders_reservation_id ON orders (reservation_id);
```

---

### 1.3 Table: `reservations`
Durable mirror of in-memory reservations for reconciliation and crash auditing.

```sql
CREATE TABLE reservations (
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

CREATE INDEX idx_reservations_sale_status ON reservations (sale_id, status);
CREATE INDEX idx_reservations_expires_at ON reservations (expires_at) WHERE status = 'ACTIVE';
```

---

### 1.4 Table: `outbox_events`
Stores pending events committed in the same ACID transaction as order updates. Polled by the Outbox Relay worker.

```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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

-- Crucial partial index for high-speed relay polling
CREATE INDEX idx_outbox_unprocessed ON outbox_events (created_at) 
WHERE published_at IS NULL;
```

---

### 1.5 Table: `processed_events`
Guarantees consumer-side idempotency across Kafka redeliveries.

```sql
CREATE TABLE processed_events (
    consumer_name VARCHAR(64) NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_name, event_id)
);
```

---

## 2. Redis Key Structure & Data Strategy

Redis 7 operates in-memory to provide microsecond atomic stock manipulation.

### 2.1 Key Namespaces

| Key Pattern | Redis Type | TTL | Purpose |
| :--- | :--- | :--- | :--- |
| `flashsale:stock:{saleId}` | Integer (String) | None (cleared on close) | Available inventory count for atomic decrement/increment. |
| `flashsale:res:{saleId}:{resId}` | Hash | 120 seconds | Active reservation metadata: `{shopperId, quantity, status, expiresAt}`. |
| `flashsale:idemp:{saleId}:{shopperId}:{key}` | String | 300 seconds | Short-term fast idempotency cache for duplicate rejection before DB lookup. |
| `flashsale:ratelimit:{ipOrClientId}` | Integer (String) | 1 second / 60 seconds | Token bucket rate-limiter counter. |

---

## 3. Redis Lua Script Specifications

To guarantee atomic concurrency, all mutations to available stock and reservations are executed exclusively via Lua scripts.

### 3.1 Script 1: `reserve_stock.lua`
* **Keys:**
  * `KEYS[1]`: Stock key (`flashsale:stock:{saleId}`)
  * `KEYS[2]`: Reservation key (`flashsale:res:{saleId}:{resId}`)
* **Args:**
  * `ARGV[1]`: Requested quantity (integer, e.g. `1`)
  * `ARGV[2]`: TTL in seconds (e.g. `120`)
  * `ARGV[3]`: Shopper ID
* **Logic:**
  1. Read available stock from `KEYS[1]`.
  2. If stock is nil or `tonumber(stock) < tonumber(quantity)`, return `0` (`SOLD_OUT`).
  3. Decrement stock: `redis.call('DECRBY', KEYS[1], quantity)`.
  4. Create reservation hash at `KEYS[2]` with fields `shopperId`, `quantity`, `status='ACTIVE'`.
  5. Set TTL on `KEYS[2]`: `redis.call('EXPIRE', KEYS[2], ARGV[2])`.
  6. Return `1` (Success).

---

### 3.2 Script 2: `release_stock.lua` (Idempotent Compensation)
* **Keys:**
  * `KEYS[1]`: Stock key (`flashsale:stock:{saleId}`)
  * `KEYS[2]`: Reservation key (`flashsale:res:{saleId}:{resId}`)
* **Args:**
  * `ARGV[1]`: Release reason (e.g. `PAYMENT_FAILED`)
* **Logic:**
  1. Check if reservation exists at `KEYS[2]`.
  2. If reservation does not exist or status is not `ACTIVE`, return `0` (Already released or expired; do not double-restore stock).
  3. Read `quantity` from reservation hash.
  4. Set status field to `RELEASED`.
  5. Increment stock: `redis.call('INCRBY', KEYS[1], quantity)`.
  6. Delete or set low TTL on reservation key.
  7. Return `1` (Stock successfully restored).

---

### 3.3 Script 3: `commit_stock.lua`
* **Keys:**
  * `KEYS[1]`: Reservation key (`flashsale:res:{saleId}:{resId}`)
* **Logic:**
  1. Check if reservation exists at `KEYS[1]`.
  2. If status is `ACTIVE`, set status to `COMMITTED` and delete the key or retain for audit.
  3. Return `1`.
