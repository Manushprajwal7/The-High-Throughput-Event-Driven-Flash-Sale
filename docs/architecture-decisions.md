# FlashSale Engine — Architecture Decision Records (ADRs)

This document aggregates and standardizes the key architectural decisions governing the distributed design, data consistency models, fault tolerance, and cloud deployment of the FlashSale Engine.

---

## Table of Contents
1. [ADR-001: Distributed Architecture & Consistency Model](#adr-001-distributed-architecture--consistency-model)
2. [ADR-002: Dual-Write Mitigation via Transactional Outbox Pattern](#adr-002-dual-write-mitigation-via-transactional-outbox-pattern)
3. [ADR-003: Redis Leased Reservation & Crash-Window Reconciliation](#adr-003-redis-leased-reservation--crash-window-reconciliation)
4. [ADR-004: Payment Idempotency, Concurrency Control & Saga Compensation Preconditions](#adr-004-payment-idempotency-concurrency-control--saga-compensation-preconditions)
5. [ADR-005: Aurora PostgreSQL Connection Management via AWS RDS Proxy](#adr-005-aurora-postgresql-connection-management-via-aws-rds-proxy)
6. [ADR-006: Partitioning Strategy, Consumer Lag Defense & Dead-Letter Isolation](#adr-006-partitioning-strategy-consumer-lag-defense--dead-letter-isolation)

---

## ADR-001: Distributed Architecture & Consistency Model

* **Status:** Accepted (Updated with Stage 00 Audit Findings)
* **Date:** 2026-10-13 (Updated 2026-10-20)
* **Context:** FlashSale Engine (Distributed High-Concurrency Ticketing Platform)

### Context & Problem Statement
A flash sale system experiences sudden, extreme bursts of concurrent traffic (e.g., 10,000 shoppers competing for 100 units within seconds). The platform must:
1. Prevent inventory overselling under high concurrency.
2. Return low-latency responses (under 50ms) to buyers.
3. Durably persist order states and audit trails without risk of data loss.
4. Process payment and fulfillment asynchronously without holding synchronous HTTP threads.

Coordinating state across **Redis** (in-memory counter), **PostgreSQL** (relational store), and **Apache Kafka** (message broker) presents a classic distributed systems dilemma: no single atomic transaction can span all three systems without crippling throughput and introducing catastrophic lock vulnerability.

### Decision
Adopt a **tri-tier decoupled architecture**:
1. **Tier 1 (Reservation Layer):** Redis with atomic Lua scripts (`DECRBY` with reservation hash lease) to absorb burst traffic with sub-millisecond execution.
2. **Tier 2 (Durable State & Outbox Layer):** PostgreSQL with ACID transactions committing `orders`, `reservations`, and `outbox_events` in a single local transaction.
3. **Tier 3 (Asynchronous Event & Settlement Layer):** Apache Kafka event stream with an asynchronous Outbox Relay Worker and idempotent Payment & Fulfillment Consumers.

### Consequences
* **Positive:** In-memory isolation absorbs burst traffic without exhausting relational database connections; ACID transactions protect relational order integrity.
* **Negative:** Introduces a distributed "crash window" between Redis reservation and database persistence, requiring explicit active reconciliation loops.

---

## ADR-002: Dual-Write Mitigation via Transactional Outbox Pattern

* **Status:** Accepted
* **Date:** 2026-10-20
* **Context:** Avoiding Dual-Write Discrepancies between PostgreSQL and Apache Kafka

### Context & Problem Statement
When an order is created, the system must both update the relational database and emit an event to Kafka topic `flashsale.orders.v1`.
If the application performs a direct dual-write:
```
// ANTI-PATTERN:
database.save(order);
kafkaTemplate.send(topic, event);
```
Two fatal failure modes emerge:
1. The database commit succeeds, but the network to Kafka fails or the JVM crashes. The order exists in DB but is never published to Kafka; the shopper is never charged, and the order hangs forever.
2. The Kafka message is sent, but the database transaction rolls back. An event is published for an order that does not exist in the database, causing phantom fulfillment.

### Considered Alternatives
1. **Distributed Two-Phase Commit (2PC / XA):**
   * *Rejected:* Kafka does not participate in XA transactions; 2PC across network boundaries causes extreme latency, database lock contention, and coordinator single-points-of-failure.
2. **Direct Kafka Send inside `@Transactional` block:**
   * *Rejected:* Kafka network I/O extends database transaction time, holding database connections and risking commit failure after message dispatch.
3. **Transactional Outbox Pattern (Selected):**
   * Persist the domain state and an outbox event in the same local database transaction. An asynchronous process relays the outbox records to Kafka.

### Decision
Implement the **Transactional Outbox Pattern** with **At-Least-Once Delivery Guarantees**:
1. Commit `orders`, `reservations`, and `outbox_events` in a single local PostgreSQL ACID transaction.
2. Run an `OutboxRelayWorker` that polls unpublished events (`published_at IS NULL`), sends them to Kafka with `acks=all`, and records `published_at = NOW()`.
3. To eliminate **Head-of-Line (HoL) Blocking** during Kafka outages (identified in Audit Scenario 02):
   - Add `next_retry_at TIMESTAMP` and `attempt_count INT` columns to `outbox_events`.
   - Apply exponential backoff with jitter on failed publish attempts.
   - Query only eligible records: `WHERE published_at IS NULL AND (next_retry_at IS NULL OR next_retry_at <= NOW())`.
   - Wrap the relay in a Resilience4j Circuit Breaker to suspend polling when Kafka is down.
4. Mandate consumer deduplication via `processed_events` table in all Kafka consumers.

### Consequences
* **Positive:** Zero risk of dual-write inconsistency; database transactions complete in sub-millisecond local scope.
* **Negative:** Requires asynchronous background workers; duplicate deliveries can occur when publisher crashes after Kafka ACK (mitigated by consumer deduplication).

---

## ADR-003: Redis Leased Reservation & Crash-Window Reconciliation

* **Status:** Accepted
* **Date:** 2026-10-20
* **Context:** Preventing Permanent Inventory Leakage during Node/Pod Crashes

### Context & Problem Statement
In the checkout flow:
1. `inventory-service` executes `reserve_stock.lua`, calling `DECRBY flashsale:stock:{saleId} qty`.
2. `order-service` receives the reservation and prepares to save the order to PostgreSQL.

If the application crashes, the host reboots, or network drops between step 1 and step 2, Redis has decremented the stock counter, but PostgreSQL has no record of the reservation.
Currently, `ReservationExpiryWorker` queries PostgreSQL `reservations` table (which has 0 records). When the Redis key expires (120s TTL), Redis passively evicts the key without restoring the counter, causing a **permanent stock leak** (Audit Scenario 01).

### Decision
Implement a **Redis-Side Active Lease & Reconciliation Protocol**:
1. In `reserve_stock.lua`, when decrementing stock, add the `reservationId` and `expiresAt` timestamp to a Redis Sorted Set:
   `ZADD flashsale:res_expirations:{saleId} <expiresAtTimestamp> <reservationId>`.
2. Introduce a **Redis Inventory Reaper** (Lua script or scheduled worker):
   - Periodically queries `ZRANGEBYSCORE flashsale:res_expirations:{saleId} 0 <nowTimestamp>`.
   - For each expired reservation ID, verifies its state. If the reservation hash was never transitioned to `COMMITTED`, atomically executes `INCRBY flashsale:stock:{saleId} qty` and removes the member from the Sorted Set.
3. In `OrderService`, pass the client's `Idempotency-Key` to `reserve_stock.lua`. Store `flashsale:res_idempotency:{saleId}:{idempotencyKey} -> reservationId` in Redis. If a concurrent duplicate request arrives, return the existing reservation ID without decrementing stock (mitigating Audit Scenario 05).

### Consequences
* **Positive:** Guarantees zero stock leaks even during total application crashes during the reservation window; eliminates false sold-out errors on concurrent idempotent requests.
* **Negative:** Adds slight memory overhead for Redis Sorted Sets; requires maintenance of an active Redis reaper process.

---

## ADR-004: Payment Idempotency, Concurrency Control & Saga Compensation Preconditions

* **Status:** Accepted
* **Date:** 2026-10-20
* **Context:** Preventing Double Charging, Race Conditions, and Inadvertent Overselling during Fulfillment

### Context & Problem Statement
During asynchronous payment fulfillment:
1. Duplicate Kafka messages can be processed concurrently by multiple threads or pods, invoking `processPayment()` twice before either commits the transaction (Audit Scenario 04).
2. If payment takes longer than 120 seconds, `ReservationExpiryWorker` expires the reservation and releases the stock in Redis. The payment thread finishes late, overwrites the order to `CONFIRMED`, and ignores the Redis commit failure, resulting in **two confirmed orders for the same item** (Audit Scenario 07).
3. If payment succeeds but the worker crashes before database commit, the order is expired and stock is given away while the customer is debited (Audit Scenario 06).

### Decision
Enforce strict **Preconditions, Optimistic Locking, and Compensating Refund Sagas**:
1. **Optimistic Locking on Entities:**
   Add `@Version private Long version;` to `OrderEntity` and `ReservationEntity`. If an expiry worker modifies the order concurrently, the payment worker's save will fail with `OptimisticLockingFailureException`.
2. **Deterministic Payment Idempotency Keys:**
   Always pass `orderId.toString()` as the idempotency key to external payment processors. If duplicate calls occur, the gateway returns the prior authorization without double debiting.
3. **Atomic Two-Phase Stock Commitment:**
   - In `InventoryClient.commitStock()`, do **not** swallow errors. If Redis `commit_stock.lua` returns 0 (indicating the reservation is no longer ACTIVE because it was released or expired), throw `StockCommitFailedException`.
4. **Compensating Refund Saga:**
   If `commitStock()` fails because the reservation expired while payment was in-flight:
   - Immediately trigger an automated **Payment Refund** via `paymentSimulationService.refundPayment(paymentReference)`.
   - Transition order status to `SETTLEMENT_ABORTED_EXPIRED` with failure code `RESERVATION_EXPIRED_DURING_PAYMENT`.
   - The zero-overselling invariant is preserved.

### State Transition Matrix
| Current State | Event | Target State | Action Required |
|---|---|---|---|
| `PENDING` | Kafka Event Received | `PROCESSING` | Update DB with version check |
| `PROCESSING` | Payment Success & Stock Commit OK | `CONFIRMED` | Commit stock in Redis; save payment ref |
| `PROCESSING` | Payment Success & Stock Commit FAILED | `SETTLEMENT_ABORTED_EXPIRED` | **Refund payment immediately**; log critical alarm |
| `PROCESSING` | Payment Declined | `PAYMENT_FAILED` | Release stock in Redis |
| `PENDING` / `PROCESSING` | TTL Exceeded (>120s) | `EXPIRED` | Release stock in Redis |

---

## ADR-005: Aurora PostgreSQL Connection Management via AWS RDS Proxy

* **Status:** Accepted
* **Date:** 2026-10-20
* **Context:** Preventing Aurora Connection Pool Exhaustion during Flash Sale Spikes

### Context & Problem Statement
In a flash sale, ECS Fargate tasks autoscale rapidly (e.g. 20 `order-service` pods + 20 `payment-fulfillment-service` pods).
With current Hikari configuration (`maximum-pool-size: 30` and `20`), total open JDBC connections would reach 1,000+.
Aurora PostgreSQL instances have physical connection limits (e.g., `db.t4g.medium` allows ~400 connections; `db.r6g.large` allows ~1,000).
Exhausting connection slots causes `SQLTransientConnectionException`, freezes application threads, and causes cascading HTTP 500/504 errors across the ingress gateway (Audit Scenario 10).

### Decision
Deploy **AWS RDS Proxy** between application containers and Amazon Aurora PostgreSQL:
1. **Connection Multiplexing:** RDS Proxy pools and shares database connections across thousands of application threads, holding open only a small, fixed pool of actual Aurora backend connections (e.g. 50–100 connections).
2. **Hikari Pool Right-Sizing:**
   - Set `order-service` Hikari `maximum-pool-size: 10`, `minimum-idle: 2`.
   - Set `payment-fulfillment-service` Hikari `maximum-pool-size: 10`, `minimum-idle: 2`.
3. **Transaction Pinning Minimization:** Keep `@Transactional` methods concise and free of remote network I/O to allow RDS Proxy to multiplex connections between transactions.
4. **Seamless Failover:** RDS Proxy intercepts database failover events, queuing incoming queries during an Aurora replica promotion, reducing failover downtime from 30+ seconds to under 3 seconds with zero application connection drops.

### Consequences
* **Positive:** Prevents connection exhaustion during 10x traffic spikes; reduces database memory overhead; stabilizes failover.
* **Negative:** Adds minimal proxy latency (~1ms per query); requires IAM role configuration and VPC endpoint routing.

---

## ADR-006: Partitioning Strategy, Consumer Lag Defense & Dead-Letter Isolation

* **Status:** Accepted
* **Date:** 2026-10-20
* **Context:** Scalable Kafka Streaming and Consumer Lag Mitigation

### Context & Problem Statement
1. The local Kafka topic `flashsale.orders.v1` defaults to a single partition. Concurrency is limited to 1 active consumer thread regardless of configured listeners (Audit Scenario 12).
2. If 10,000 orders arrive in 10 seconds, a single partition consumer taking 50ms/order requires 500 seconds to drain the queue. Because reservation TTL is 120 seconds, the majority of orders will expire before they are ever consumed.
3. Unprocessable "poison pill" messages cause continuous retry loops that block partition progression.

### Decision
Implement **Multi-Partition Keying, CloudWatch Lag Autoscaling, and SQS/Kafka Dead-Letter Isolation**:
1. **Partitioning Topology:**
   - Pre-provision topic `flashsale.orders.v1` with **12 partitions** (matching expected consumer task scale).
   - Key messages by `aggregateId` (`orderId`). This guarantees that all state transitions for a given order are strictly ordered while allowing 12 parallel consumer workers to process independent orders simultaneously.
2. **Consumer Scaling & Lag Defense:**
   - Scale ECS Fargate consumer tasks based on the CloudWatch metric `Kafka.ConsumerLag` or `EstimatedMaxTimeLag`.
   - If lag exceeds 60 seconds, alert operations and dynamically trigger consumer task scale-out.
3. **Dead-Letter Handling (DLQ):**
   - For unrecoverable deserialization errors or database poison pills, route messages to `flashsale.orders.dlq.v1` after 3 failed retries (500ms fixed backoff).
   - In AWS production, mirror failed Kafka DLQ messages to an **Amazon SQS Dead-Letter Queue** with CloudWatch alarms to allow manual inspection and replay without holding open Kafka consumer offsets.

### Consequences
* **Positive:** Scales fulfillment throughput to 2,000+ orders/second across 12 partitions; drains flash-sale spikes well within the 120s TTL window; isolates poison pills.
* **Negative:** Requires careful partition sizing; dynamic topic partition increases must preserve key hashing distributions.
