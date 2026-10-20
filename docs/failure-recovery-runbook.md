# Failure and Recovery Runbook

* **System:** FlashSale Engine
* **Component Focus:** Payment Simulation, Asynchronous Fulfillment, Expiry, and Reconciliation
* **Last Updated:** 2026-10-19

---

## 1. Overview and Core Philosophy

In a high-concurrency flash sale platform, distributed operations across **Redis** (in-memory stock counter), **PostgreSQL** (ACID durable state), and **Apache Kafka** (event backbone) cannot participate in a single atomic two-phase commit (2PC) without introducing severe performance bottlenecks and deadlock vulnerabilities.

Instead, the system relies on **Separation of Concerns with Compensating Sagas & Transactional Outbox**:
1. **Redis** serves as the fast, in-memory reservation authority with strict TTL (120 seconds).
2. **PostgreSQL** acts as the durable business authority, recording orders, state transitions, and outbox events in local ACID transactions.
3. **Kafka** streams ordered events partitioned by `orderId`.
4. **Payment Fulfillment Service** applies idempotent state transitions, simulates payment outcomes, executes compensating stock releases, and periodically reconciles discrepancies.

---

## 2. Distributed State Machine & Saga Flow

```
                     +-------------------+
                     |      PENDING      |
                     +---------+---------+
                               |
                   Kafka Event | (ORDER_RESERVED)
                               v
                     +-------------------+
                     |    PROCESSING     |
                     +----+---------+----+
                          |         |
         Payment Success  |         | Payment Declined / Timeout
                          v         v
         +------------------+     +--------------------+
         |    CONFIRMED     |     |   PAYMENT_FAILED   |
         +------------------+     +--------------------+
         (Commit Stock)           (Compensating Release)
```

### Alternative Terminal Transitions:
- `PENDING` $\to$ `EXPIRED`: Triggered if reservation TTL passes before payment processing commences.
- `PENDING` $\to$ `CANCELLED`: Triggered upon user-initiated cancellation.
- `PROCESSING` $\to$ `CANCELLED`: Permitted only if payment has not settled.

---

## 3. Failure Scenarios and Automated Recovery

### Scenario 1: Payment Declined by Simulated Gateway
* **Symptom:** Shopper card declined, fraud rule triggered, or simulator configured with `ALWAYS_DECLINE`.
* **State Transition:** Order transitions `PENDING` $\to$ `PROCESSING` $\to$ `PAYMENT_FAILED` with `failure_code = 'INSUFFICIENT_FUNDS'` or `'CARD_DECLINED'`.
* **Compensating Action:**
  1. Relational reservation marked `RELEASED`.
  2. Worker invokes `POST /api/v1/sales/{saleId}/reservations/{reservationId}/release`.
  3. Redis executes `release_stock.lua` which atomically increments `available_stock` and deletes the reservation hash key.
* **Invariant Guarantee:** Stock is released exactly once. Repeated attempts return 0 in Redis without increasing stock twice.

### Scenario 2: Transient Payment Simulator Timeout
* **Symptom:** External simulated gateway latency exceeds timeout threshold (`ALWAYS_TIMEOUT` mode).
* **Automatic Handling:**
  1. Worker throws `PaymentTimeoutException`.
  2. Spring Kafka's `DefaultErrorHandler` intercepts the failure and initiates bounded retries (3 attempts with 500ms backoff).
  3. If transient issues resolve, payment completes normally.
  4. If all retries are exhausted, the record is routed to Dead-Letter Topic: `flashsale.orders.dlq.v1`.
* **Operational Action:** Operators inspect DLQ records using Kafka CLI or dashboard, resolve dependency issues, and re-publish to `flashsale.orders.v1`.

### Scenario 3: Reservation Expiration Before Payment Processing
* **Symptom:** Worker was backlogged or offline, and reservation TTL (120s) expired while event waited in Kafka.
* **Automatic Handling:**
  1. Worker checks `expiresAt.isBefore(Instant.now())` prior to processing.
  2. Order is transitioned directly to `EXPIRED` (`failure_code = 'RESERVATION_EXPIRED'`).
  3. Reservation is marked `EXPIRED`.
  4. Compensating stock release is invoked in Redis.
* **Invariant Guarantee:** An expired reservation can never be confirmed. Subsequent payment success attempts are deterministically rejected.

### Scenario 4: Worker Crash Mid-Saga (Orphaned Reservation)
* **Symptom:** Payment fulfillment worker process terminates after order creation but before event consumption.
* **Automatic Recovery via Background Workers:**
  1. **`ReservationExpiryWorker`** runs every 5 seconds:
     - Queries `SELECT * FROM reservations WHERE status = 'ACTIVE' AND expires_at < NOW()`.
     - Marks reservation as `EXPIRED` and linked order as `EXPIRED`.
     - Issues `release_stock.lua` call to Redis to restore inventory.
  2. **`OrderReconciliationWorker`** runs every 30 seconds:
     - Scans for `PENDING` orders older than 5 minutes.
     - Detects mismatches (e.g. order `PAYMENT_FAILED` or `EXPIRED` but reservation still `ACTIVE`).
     - Safely aligns state by marking reservation `RELEASED`/`EXPIRED` and restoring stock.

### Scenario 5: Duplicate Kafka Delivery (At-Least-Once Delivery)
* **Symptom:** Broker re-delivers the same `ORDER_RESERVED` event due to consumer rebalance or network glitch.
* **Automatic Handling:**
  1. Worker queries table `processed_events` for key `(consumer_name, event_id)`.
  2. If found, message is immediately acknowledged and discarded without re-running payment simulation or stock operations.
  3. If order is already in a terminal state (`CONFIRMED`, `PAYMENT_FAILED`, `EXPIRED`), duplicate events are safely ignored.

---

## 4. Operational Runbook & Commands

### 4.1 Inspecting and Changing Payment Simulation Mode
* **Query Current Mode:**
  ```bash
  curl -X GET http://localhost:8083/api/v1/simulation/payment
  ```
* **Switch to Always Success (Default Happy Path):**
  ```bash
  curl -X PUT http://localhost:8083/api/v1/simulation/payment \
       -H "Content-Type: application/json" \
       -d '{"mode": "ALWAYS_SUCCESS"}'
  ```
* **Switch to Always Decline (Failure Demonstration):**
  ```bash
  curl -X PUT http://localhost:8083/api/v1/simulation/payment \
       -H "Content-Type: application/json" \
       -d '{"mode": "ALWAYS_DECLINE"}'
  ```
* **Switch to Rule-Based (Deterministic by Shopper Name):**
  ```bash
  curl -X PUT http://localhost:8083/api/v1/simulation/payment \
       -H "Content-Type: application/json" \
       -d '{"mode": "RULE_BASED"}'
  ```
  - Shopper IDs containing `"decline"` or `"fail"` $\to$ Declined.
  - Shopper IDs containing `"timeout"` $\to$ Transient Timeout.
  - All other shoppers $\to$ Confirmed.

### 4.2 Inspecting Dead-Letter Queue (DLQ)
* **View DLQ Messages using Kafka Console Consumer:**
  ```bash
  docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic flashsale.orders.dlq.v1 \
    --from-beginning \
    --max-messages 10
  ```

### 4.3 Triggering Manual Reconciliation
* Under ordinary circumstances, reconciliation runs automatically on background schedules.
* For emergency maintenance, the operator can verify database state via PostgreSQL:
  ```sql
  -- Identify orphaned active reservations that expired
  SELECT id, sale_id, order_id, expires_at, status 
  FROM reservations 
  WHERE status = 'ACTIVE' AND expires_at < NOW();

  -- Identify stuck pending orders older than 5 minutes
  SELECT id, sale_id, shopper_id, created_at, status 
  FROM orders 
  WHERE status = 'PENDING' AND created_at < NOW() - INTERVAL '5 minutes';
  ```
