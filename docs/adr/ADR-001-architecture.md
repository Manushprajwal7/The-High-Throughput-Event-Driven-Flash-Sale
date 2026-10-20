# ADR-001: Distributed Architecture & Consistency Model

* **Status:** Accepted
* **Date:** 2026-10-13
* **Context:** FlashSale Engine (Distributed High-Concurrency Ticketing Platform)

---

## Context and Problem Statement

A flash sale system experiences sudden, extreme bursts of concurrent traffic (e.g., 10,000 shoppers competing for 100 units within seconds). The system must:
1. Prevent inventory overselling under high concurrency.
2. Return low-latency responses (under 50ms) to buyers.
3. Durably persist order states and audit trails without risk of data loss.
4. Process payment and fulfillment asynchronously.

The core distributed systems challenge is coordinating state across three distinct infrastructure layers: **Redis** (in-memory caching/counter), **PostgreSQL** (ACID relational database), and **Apache Kafka** (event broker). There is no native atomic transaction (2PC / XA) that can span all three systems simultaneously without crippling throughput and introducing catastrophic lock vulnerability.

---

## Decision Drivers

* **Zero Overselling:** Physical capacity must never be exceeded ($\text{Confirmed} + \text{Active Reservations} \le \text{Capacity}$).
* **Throughput & Latency:** In-memory stock evaluation to absorb traffic spikes without overwhelming relational database connection pools.
* **Durability:** Orders, audit timestamps, and state transitions must be durably stored in PostgreSQL.
* **Fault Tolerance:** Temporary broker or worker outages must not cause data loss or block order acceptance.
* **Engineering Honesty:** No claims of impossible guarantees (e.g. "exactly-once" end-to-end without consumer deduplication, or zero dual-write windows without compensation).

---

## Considered Alternatives

1. **Alternative 1: Relational Row Locking (`SELECT ... FOR UPDATE` in PostgreSQL)**
   * *Pros:* Simple single-database ACID transaction.
   * *Cons:* Severe database connection pool starvation and lock contention under thousands of concurrent requests. Database response times degrade from milliseconds to seconds.
   * *Verdict:* **Rejected** for the high-contention reservation path.

2. **Alternative 2: Distributed Two-Phase Commit (2PC / XA)**
   * *Pros:* Theoretical strict serializability across Redis, DB, and Kafka.
   * *Cons:* Not supported natively by Redis or Kafka; introduces distributed blocking locks; catastrophic failure mode if coordinator crashes.
   * *Verdict:* **Rejected**.

3. **Alternative 3: Direct Kafka Publishing from HTTP Thread (No Outbox)**
   * *Pros:* Lower latency to message broker.
   * *Cons:* Dual-write failure window: if database commits but Kafka publishing fails, the order is stranded without being fulfilled. If Kafka publishes but database rollback occurs, phantom orders are fulfilled.
   * *Verdict:* **Rejected**.

---

## Decision

We adopt a **Separation of Concerns with Compensating Sagas & Transactional Outbox**:

1. **Redis as the Fast Reservation Authority:**
   * High-throughput atomic stock decrement executed via Redis Lua script (`reserve_stock.lua`).
   * When stock reaches zero, requests are rejected immediately with `409 Sold Out` without touching PostgreSQL.
   * Reservations carry an explicit TTL (120 seconds).
2. **PostgreSQL as the Durable Business Authority:**
   * Successful reservations are durably recorded in PostgreSQL with status `PENDING`.
   * State transitions follow an explicit finite state machine.
   * PostgreSQL unique constraints on `(sale_id, shopper_id, idempotency_key)` serve as the final backstop against race conditions.
3. **Transactional Outbox Pattern for Kafka Streaming:**
   * Order records and outbox event records are committed in the same PostgreSQL ACID transaction.
   * A dedicated Outbox Relay worker polls uncommitted outbox events, publishes them to Kafka with `orderId` partition key, and marks them `PUBLISHED` only after receiving broker ACK.
4. **Consumer Idempotency & Compensating Sagas:**
   * Kafka consumers deduplicate events via a `processed_events` table.
   * If payment fails or times out, the worker issues an idempotent compensating call to Inventory Service (`release_stock.lua`) to restore stock.
5. **Periodic Reconciliation Worker:**
   * A scheduled background job reconciles Redis counters against PostgreSQL durable states, identifying any orphaned reservations from crashes and safely restoring inventory.

---

## Consequences

### Positive
* High-concurrency stock checks take single-digit milliseconds in Redis.
* PostgreSQL connection pools are shielded from excessive lock contention.
* Zero overselling invariant is strictly preserved by Redis Lua scripts and database unique constraints.
* Crash resilience: if Kafka is down, orders are still durably accepted and buffered in the outbox.

### Negative / Trade-offs
* Eventual consistency between Redis in-memory counters and PostgreSQL order records.
* Requires background reconciliation to clean up orphaned reservations caused by mid-flight service crashes.
* Consumers must handle at-least-once delivery duplicates using `processed_events`.
