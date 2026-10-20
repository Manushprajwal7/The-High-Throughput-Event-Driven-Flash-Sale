# FlashSale Engine --- Failure Scenarios & Boundary Crash Analysis

In a distributed flash sale architecture, partial failures, network partitions, and process crashes will occur. Because there is no distributed 2-phase commit (2PC) spanning Redis, PostgreSQL, and Kafka, the system relies on bounded sagas, idempotency locks, transactional outboxes, and reconciliation to remain safe and consistent.

---

## 1. Boundary Crash Analysis Matrix

```
[Client] ──(1)──> [Gateway] ──(2)──> [Order Service] ──(3)──> [Inventory/Redis]
                                           │
                                          (4)
                                           ▼
                                    [PostgreSQL Outbox]
                                           │
                                          (5)
                                           ▼
                                        [Kafka]
                                           │
                                          (6)
                                           ▼
                                    [Payment Worker] ──(7)──> [Inventory/Redis]
```

| Window | Boundary / Event | Failure Event | Consequence if Unhandled | Detection Mechanism | Automated Recovery / Mitigation |
| :---: | :--- | :--- | :--- | :--- | :--- |
| **W1** | Client $\rightarrow$ Gateway $\rightarrow$ Order Svc | Client connection drop or timeout during processing. | Client retries request, potentially reserving duplicate inventory. | `Idempotency-Key` header with request fingerprinting. | Deduplication filter checks existing key in PostgreSQL. Returns cached response without re-decrementing stock. |
| **W2** | Order Svc $\rightarrow$ Inventory (Redis) | Inventory Service or Redis is unavailable during reservation. | Request hangs or throws 500 error. | HTTP client connection timeout (500ms) + Resilience4j Circuit Breaker. | Fail-fast: return `503 Service Degraded` or `409 Sold Out`. Zero stock mutated. |
| **W3** | **Orphaned Reservation Window** (Redis reserved $\rightarrow$ DB commit) | Order Service reserves stock in Redis, but crashes **before** committing the order & outbox in PostgreSQL. | Stock was decremented in Redis, but no order exists in PostgreSQL. Inventory leaked (undercounted). | 1. Redis reservation TTL (120s).<br/>2. Periodic Reconciliation Job. | **Self-Healing:** Active reservation in Redis has TTL. If not committed within TTL, background cleaner releases uncommitted stock back to available pool. |
| **W4** | PostgreSQL $\rightarrow$ Outbox Relay $\rightarrow$ Kafka | Broker is down when Outbox Relay polls for pending events. | Events accumulate in `outbox_events` table. | Outbox Relay catches `KafkaException`; increments `attempt_count` with backoff. | Outbox events remain safely persisted in PostgreSQL. Relay resumes publishing immediately when broker recovers. |
| **W5** | **Duplicate Publication Window** (Kafka ACK $\rightarrow$ DB update) | Relay publishes to Kafka, receives ACK, but crashes before updating `published_at` in PostgreSQL. | On restart, relay re-reads the event and publishes a duplicate message to Kafka. | Kafka consumers inspect `processed_events` table. | **Consumer Idempotency:** Payment worker processes within DB transaction with `uq_processed_events(consumer_name, event_id)`. Duplicate discarded. |
| **W6** | Kafka $\rightarrow$ Payment Worker | Payment worker crashes while simulating payment processing. | Kafka partition offset not committed. | Kafka consumer group rebalance upon heartbeat timeout. | Rebalance assigns partition to healthy consumer. Message re-delivered and re-executed idempotently. |
| **W7** | Payment Worker $\rightarrow$ Compensating Release | Payment is declined, but Inventory Service call to restore stock fails (Redis down). | Inventory undercounted; declined stock never returned to available pool. | Worker writes compensating outbox record or scheduled retry queue. | Outbox/saga retry mechanism ensures compensating release is retried with exponential backoff until acknowledged. |
| **W8** | **Cold Redis Restart / State Wipe** | Redis container crashes or restarts without durable snapshot. | Redis memory empty; available stock counter lost. | Health check detects cold start / missing stock key. | **Reconciliation Protocol:** Reconciliation worker queries PostgreSQL for sale capacity and active/confirmed counts, rebuilding Redis keys atomically. |

---

## 2. In-Depth Recovery Protocols

### 2.1 The Orphaned Reservation Protocol (Window W3)
* **The Problem:** The order service completes `reserve_stock.lua` in Redis, but suffers an ungraceful OOM crash, SIGKILL, or network cut before the corresponding `INSERT INTO orders` and `outbox_events` transaction commits to PostgreSQL.
* **The Invariant:** Available inventory in Redis is 1 unit lower than actual durable reality.
* **Resolution:**
  1. Every reservation in Redis contains a mandatory expiration TTL (default: 120 seconds).
  2. In PostgreSQL, durable reservations are created in `reservations` table only when the order transaction succeeds.
  3. A scheduled **Reconciliation Cleaner** runs every 60 seconds:
     * Scans Redis for reservations older than 120 seconds that were never committed via `commit_stock.lua`.
     * Cross-checks with PostgreSQL: does an order exist with this `reservation_id`?
     * If no order exists, the cleaner executes `release_stock.lua`, restoring the reserved count to `flashsale:stock:{saleId}`.

---

### 2.2 Cold Redis Crash & State Rebuild Protocol (Window W8)
* **The Problem:** In a catastrophic failure, Redis restarts cleanly with blank state.
* **The Invariant:** PostgreSQL is the ultimate durable authority; Redis is the transient concurrency authority.
* **Rebuild Algorithm:**
  1. Operator or boot hook invokes `/api/v1/admin/demo/rebuild-stock/{saleId}` (or automated on cold boot).
  2. Worker queries PostgreSQL:
     $$\text{Confirmed Units} = \text{COUNT}(*) \text{ from } \texttt{orders} \text{ where } \texttt{status} = \text{'CONFIRMED'}$$
     $$\text{Active Units} = \text{COUNT}(*) \text{ from } \texttt{reservations} \text{ where } \texttt{status} = \text{'ACTIVE' and } \texttt{expires\_at} > \text{NOW()}$$
     $$\text{Remaining Available Stock} = \text{total\_capacity} - (\text{Confirmed Units} + \text{Active Units})$$
  3. If $\text{Remaining Available Stock} < 0$, alert administrator (critical anomaly).
  4. Write `Remaining Available Stock` atomically to `flashsale:stock:{saleId}` in Redis using `SET ... NX`.

---

## 3. Resilience & Degradation Rules

1. **PostgreSQL Down:**
   * Order creation fails fast with `503 Service Unavailable`.
   * Fast reservation in Redis is aborted or reverted.
2. **Kafka Down:**
   * Orders are still accepted! Since orders and outbox events are persisted in PostgreSQL, the HTTP endpoint returns `202 Accepted`.
   * Outbox Relay buffers messages in PostgreSQL and backlogs until Kafka returns.
3. **Redis Down:**
   * Inventory reservations cannot be atomically verified. Order creation fails fast with `503 Service Unavailable`.
   * PostgreSQL is shielded from catastrophic unthrottled load spikes.
