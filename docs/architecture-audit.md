# FlashSale Engine — Comprehensive Architecture Audit

**Auditor:** Senior Java Distributed-Systems Architect & AWS Solutions Architect  
**Scope:** FlashSale Engine Codebase (`flashsale-common`, `flashsale-gateway`, `inventory-service`, `order-service`, `payment-fulfillment-service`)  
**Target Environment:** AWS-Hosted, High-Throughput, Multi-AZ Flash Sale Platform  
**Target Invariant:** Zero-Overselling Guarantee under Peak Traffic and Arbitrary Distributed Failures  
**Status:** Audit Completed — Stage 00 Review  

---

## 1. Executive Summary

The FlashSale Engine is a distributed, reactive, and event-driven microservices platform engineered to handle extreme traffic spikes during flash-sale events. The system leverages Spring Cloud Gateway for ingress rate limiting, Spring WebFlux and Redis Lua scripts for non-blocking atomic inventory reservation, Spring Boot and PostgreSQL for durable relational order persistence with a Transactional Outbox pattern, Apache Kafka for asynchronous order event propagation, and a background Payment & Fulfillment service.

### Overall Assessment
The core design demonstrates high architectural maturity:
- **Separation of Concerns:** High-speed in-memory reservations are decoupled from relational persistence and payment processing.
- **Local ACID Boundaries:** Order persistence and outbox event creation share a single relational database transaction.
- **Asynchronous Decoupling:** Long-latency payment gateway integrations are isolated from client-facing checkout latency.

However, a rigorous architectural audit of the code and configuration reveals **critical distributed-systems failure windows** where the primary system invariant—**strict zero-overselling**—is breached during real-world failure modes (e.g. process crashes, network latency, payment timeouts, and database connection exhaustion).

This document details the architectural findings, distinguishes verified defects from architectural risks, provides mathematical and transactional proofs of failure modes, and outlines the required remediations without modifying application code during this audit stage.

---

## 2. Core Architectural Foundations & Distributed Dynamics

### 2.1 The Crash Window Between Redis Reservation and Durable Order Creation

In a high-throughput flash sale, the system attempts to achieve sub-10ms response times for order submissions by performing an initial atomic decrement in Redis, followed by durable persistence in PostgreSQL.

#### The Code Mechanism
In `order-service/src/main/java/com/flashsale/order/service/OrderService.java`:
```java
// Step 1: Remote HTTP/REST call to Inventory Service (backed by Redis Lua script)
ReservationResponse reservation = inventoryClient.reserveStock(
    request.saleId(),
    request.shopperId(),
    request.quantity(),
    correlationId
); // Line 101-106

// [CRASH WINDOW OCCURS HERE]

// Step 2: Durable relational commit in PostgreSQL
orderPersistenceHelper.persistOrderAndReservation(orderEntity, reservationEntity, outboxEvent); // Line 165
```

In `inventory-service/src/main/resources/scripts/reserve_stock.lua`:
```lua
-- Decrement stock in Redis
redis.call('DECRBY', KEYS[1], requestedQty) -- Line 33

-- Create reservation hash with TTL
redis.call('HSET', KEYS[2], 'shopperId', ARGV[3], 'quantity', requestedQty, 'status', 'ACTIVE', 'expiresAt', ARGV[4])
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2])) -- Line 42
```

#### The Distributed Invariant Gap
Redis and PostgreSQL are two distinct, independent distributed systems. There is **no distributed transaction (2PC / XA)** linking `reserve_stock.lua` with `orderPersistenceHelper.persistOrderAndReservation`.

The non-atomic interval between the moment Redis executes `DECRBY` (and returns HTTP 200 to `OrderService`) and the moment PostgreSQL commits the transaction to the Write-Ahead Log (WAL) is the **Crash Window**.

```
Client                  OrderService             Inventory(Redis)           PostgreSQL
  │                          │                          │                        │
  │─── POST /orders ────────>│                          │                        │
  │                          │─── reserveStock() ──────>│                        │
  │                          │                          │── DECRBY stock         │
  │                          │                          │── EXPIRE res_key 120s  │
  │                          │<── 200 OK (resId) ───────│                        │
  │                          │                                                   │
  │                     [💀 CRASH / KILL / TIMEOUT / OOM]                        │
  │                          │                                                   │
  │                          x (persistOrder never executed)                    │
  │                                                                              │
  │  After 120s: Redis key evicts passively. Stock counter remains decremented!  │
  │  PostgreSQL reservations table has 0 rows -> ReservationExpiryWorker finds 0! │
```

#### Failure Consequences
1. If the `order-service` process is terminated (SIGKILL, container OOM, pod eviction, host hardware failure, or PostgreSQL connection timeout) during this window:
   - Redis has decremented the stock counter (`DECRBY`).
   - PostgreSQL has no record of the order or reservation.
2. The background reaper (`payment-fulfillment-service/src/main/java/com/flashsale/payment/worker/ReservationExpiryWorker.java` line 61-62) queries:
   `reservationRepository.findByStatusAndExpiresAtBefore(ReservationStatus.ACTIVE, now)`.
   Because the reservation was never saved to PostgreSQL, this query returns 0 rows.
3. In Redis, the reservation hash key `flashsale:res:{saleId}:{reservationId}` expires after 120 seconds. Redis key expiration merely evicts the hash; **Redis does not automatically invoke an `INCRBY` or trigger a Lua callback on eviction**.
4. **Permanent Inventory Leak:** The stock units remain decremented in Redis forever. If 50 containers crash during a 10,000-request burst, 50 sale items are lost to phantom reservations and will never be sold.

---

### 2.2 Why Kafka and PostgreSQL Are Not One Atomic Transaction

A frequent distributed systems misconception is attempting to make a relational database commit and a Kafka message publication participate in a single atomic transaction.

#### The Fallacy of Distributed 2PC (XA)
To make PostgreSQL and Kafka commit atomically would require an XA Distributed Transaction Coordinator implementing Two-Phase Commit (2PC):
1. **Prepare Phase:** PostgreSQL writes prepare locks; Kafka initiates transactional producer sequence.
2. **Commit Phase:** Coordinator contacts both systems to commit.

In a high-throughput flash sale, distributed 2PC across PostgreSQL and Kafka is an **architectural anti-pattern** for four fundamental reasons:
1. **Throughput Collapse & Lock Contention:** 2PC requires multiple synchronous network round-trips over the network. Database locks are held across network calls to Kafka brokers, reducing system throughput from 10,000+ writes/second to fewer than 100 writes/second.
2. **Blocking Coordinator Failure:** If the coordinator or network fails during the "in-doubt" window, database rows remain locked indefinitely, freezing table partitions.
3. **Availability Coupling:** If a Kafka broker experiences a transient partition rebalance or network glitch, all database writes in `order-service` will fail, violating fault isolation.
4. **Cloud-Native Incompatibility:** Managed cloud services (AWS Aurora PostgreSQL and Amazon MSK) do not natively support distributed XA transactions across heterogeneous engines.

#### The Correct Distributed Solution: The Transactional Outbox Pattern
The FlashSale Engine correctly chooses the **Transactional Outbox Pattern**:
- The domain entities (`orders`, `reservations`) and the outbox event (`outbox_events`) are committed within a **single local ACID transaction** in PostgreSQL.
- An asynchronous relay (`OutboxRelayWorker.java` or Debezium CDC) reads committed events from PostgreSQL and publishes them to Kafka.
- Publication is guaranteed **at-least-once**.
- **Consumer Deduplication:** Because network retries can publish duplicate messages to Kafka, consumers must be idempotent (`processed_events` table).

---

### 2.3 The Durable Meaning of an Accepted Order

When a shopper submits an order request, `order-service` responds with `HTTP 202 Accepted` (or `HTTP 200 OK` with status `PENDING`).

In distributed systems semantics, what does an "Accepted" order guarantee?

#### 1. What an Accepted Order IS NOT:
- It is **not** a finalized sale.
- It is **not** a guarantee that the customer's card has been charged.
- It is **not** an irrevocable guarantee that the goods will be shipped.

#### 2. What an Accepted Order IS:
- **A Temporary Inventory Lease:** The shopper has acquired a time-bounded lease (120 seconds) on the requested stock quantity.
- **A Durable Intent-to-Purchase:** The platform has durably persisted the order in PostgreSQL under state `PENDING` with an assigned `reservationId` and `expiresAt` timestamp.
- **An Asynchronous Commitment to Attempt Settlement:** The system guarantees that as long as the payment is settled before the lease expires, the order will transition to `CONFIRMED`.

#### State Machine Invariants
```
                      ┌────────────────────────────────────────┐
                      │                PENDING                 │
                      │  (Stock leased in Redis, saved in DB)   │
                      └───────────────────┬────────────────────┘
                                          │
                                   [Kafka Delivery]
                                          ▼
                      ┌────────────────────────────────────────┐
                      │               PROCESSING               │
                      │       (Payment call in-flight)         │
                      └───────┬──────────────────────┬─────────┘
                              │                      │
             [Payment Success │                      │ [Payment Decline /
              & Stock Commit] │                      │  Lease Expiration /
                              ▼                      ▼  User Cancel]
         ┌─────────────────────────┐    ┌───────────────────────────┐
         │        CONFIRMED        │    │ PAYMENT_FAILED / EXPIRED  │
         │  (DURABLE FINAL STATE)  │    │  (COMPENSATED & RELEASED) │
         │   Stock permanently sold│    │   Stock returned to pool  │
         └─────────────────────────┘    └───────────────────────────┘
```
- **Durable Finality** occurs **only** when the state transitions to `CONFIRMED` in PostgreSQL **and** the stock lease is committed in Redis.

---

## 3. Detailed Audit of the 14 Failure Scenarios

This section categorizes findings into **Verified Defects** (reproducible logic and transaction bugs in existing code) and **Architectural & Infrastructure Risks** (vulnerabilities arising when deployed at AWS scale).

---

### Scenario 01: Redis reserves inventory, but the application crashes before persisting an order
- **Status:** **VERIFIED DEFECT**
- **Severity:** `CRITICAL`
- **Affected Files:**
  - `order-service/src/main/java/com/flashsale/order/service/OrderService.java` (lines 101–165)
  - `inventory-service/src/main/resources/scripts/reserve_stock.lua` (lines 33–42)
  - `payment-fulfillment-service/src/main/java/com/flashsale/payment/worker/ReservationExpiryWorker.java` (lines 60–63)
- **Code Evidence:**
  In `OrderService.java`:
  ```java
  ReservationResponse reservation = inventoryClient.reserveStock(...); // Remote Redis DECRBY
  // ... JVM crash or network failure here ...
  orderPersistenceHelper.persistOrderAndReservation(orderEntity, reservationEntity, outboxEvent);
  ```
  In `ReservationExpiryWorker.java`:
  ```java
  List<ReservationEntity> expiredReservations =
      reservationRepository.findByStatusAndExpiresAtBefore(ReservationStatus.ACTIVE, now);
  ```
- **Failure Consequences:**
  Because the reservation was never inserted into PostgreSQL's `reservations` table, the relational database is unaware of its existence. When the 120s TTL expires in Redis, Redis evicts the key without restoring the stock counter `flashsale:stock:{saleId}`. Stock is leaked permanently.
- **Proposed Fix:**
  1. Record reservation expiry timestamps in a Redis Sorted Set (`ZADD flashsale:expirations:{saleId} <timestamp> <reservationId>`).
  2. Implement a Redis-side scheduled Lua reaper that inspects the Sorted Set for timestamps older than `now`, validates whether the reservation was committed or released, and if still orphaned, executes `INCRBY` on the stock key.
- **Proof Test:**
  `OrphanedRedisReservationLeakTest`: Execute `reserveStock`, bypass PostgreSQL persistence, advance mock time past TTL, and assert that Redis available stock returns to original capacity.

---

### Scenario 02: PostgreSQL commits an order, but Kafka is unavailable
- **Status:** **VERIFIED DEFECT**
- **Severity:** `HIGH`
- **Affected Files:**
  - `order-service/src/main/java/com/flashsale/order/relay/OutboxRelayWorker.java` (lines 89–150)
  - `order-service/src/main/java/com/flashsale/order/entity/OutboxEventEntity.java`
- **Code Evidence:**
  ```java
  List<OutboxEventEntity> pendingEvents = outboxEventRepository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();
  // ...
  kafkaTemplate.send(topic, key, event.getPayload()).get(5, TimeUnit.SECONDS); // Fails
  // ...
  if (attempts >= MAX_PUBLISH_ATTEMPTS) {
      routeToDeadLetterTopic(event, errMsg); // Also calls kafkaTemplate.send(), which fails!
  } else {
      outboxEventRepository.save(event); // publishedAt is STILL NULL
  }
  ```
- **Failure Consequences:**
  1. **Head-of-Line (HoL) Blocking:** The query `findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()` continuously selects the *exact same 100 failed events* on every 200ms scheduled tick. Even if Kafka recovers for other topics, these 100 events starve the entire queue.
  2. **Expiration During Outage:** If Kafka remains unavailable for > 120s, orders remain `PENDING` in PostgreSQL until `ReservationExpiryWorker` expires them and releases inventory. When Kafka returns, stale expired orders are processed.
- **Proposed Fix:**
  1. Add `next_retry_at` and `status` to `outbox_events`.
  2. Implement exponential backoff jitter: `next_retry_at = now() + (2 ^ attempts) * 500ms`.
  3. Filter relay query: `WHERE published_at IS NULL AND (next_retry_at IS NULL OR next_retry_at <= :now)`.
  4. Wrap Kafka calls with a Resilience4j Circuit Breaker to pause outbox polling during outages.
- **Proof Test:**
  `OutboxRelayKafkaOutageHeadOfLineTest`: Insert 100 failing events with Kafka disabled; introduce 10 new valid events; verify backoff schedule prevents HoL starvation.

---

### Scenario 03: Kafka accepts an event, but publisher crashes before recording publication success
- **Status:** **VERIFIED DEFECT**
- **Severity:** `MEDIUM`
- **Affected Files:**
  - `order-service/src/main/java/com/flashsale/order/relay/OutboxRelayWorker.java` (lines 108–112)
- **Code Evidence:**
  ```java
  kafkaTemplate.send(topic, key, event.getPayload()).get(5, TimeUnit.SECONDS); // ACK received by broker
  // [Crash occurs here before DB update]
  event.setPublishedAt(Instant.now());
  outboxEventRepository.save(event);
  ```
- **Failure Consequences:**
  The event exists durably in Kafka topic `flashsale.orders.v1`. In PostgreSQL, `published_at` is still `NULL`. Upon worker restart, the event is re-queried and published to Kafka a second time.
- **Proposed Fix:**
  Acknowledge that Outbox Relay provides **at-least-once delivery**. Consumer microservices must enforce idempotency via the `processed_events` table with unique constraint on `(consumer_name, event_id)`.
- **Proof Test:**
  `OutboxPublisherCrashDuplicateDeliveryTest`: Force duplicate outbox publication and verify consumer processes the message payload only once.

---

### Scenario 04: A Kafka event is delivered multiple times
- **Status:** **VERIFIED DEFECT**
- **Severity:** `HIGH`
- **Affected Files:**
  - `payment-fulfillment-service/src/main/java/com/flashsale/payment/service/FulfillmentService.java` (lines 90–158)
- **Code Evidence:**
  ```java
  if (processedEventRepository.isEventProcessed(CONSUMER_NAME, eventId)) { return; } // Line 90
  // ...
  PaymentResult paymentResult = paymentSimulationService.processPayment(orderId, shopperId, quantity); // Line 143
  // ...
  processedEventRepository.save(new ProcessedEventEntity(CONSUMER_NAME, eventId)); // Line 158
  ```
- **Failure Consequences:**
  If two consumer threads (or separate consumer containers) receive duplicate Kafka deliveries concurrently, both pass the check at line 90 before either has committed line 158. Both invoke `processPayment()`, executing duplicate financial charges against the customer.
- **Proposed Fix:**
  1. Pass a deterministic idempotency key (`orderId.toString()`) to the payment provider.
  2. Insert `ProcessedEventEntity` or acquire a pessimistic write lock (`SELECT ... FOR UPDATE` on `OrderEntity`) at the start of the transaction before invoking the payment gateway.
- **Proof Test:**
  `ConcurrentDuplicateKafkaDeliveryTest`: Simulate 2 threads consuming the same `ORDER_RESERVED` envelope concurrently; assert payment gateway is invoked only once.

---

### Scenario 05: Two simultaneous requests use the same idempotency key
- **Status:** **VERIFIED DEFECT**
- **Severity:** `HIGH`
- **Affected Files:**
  - `order-service/src/main/java/com/flashsale/order/service/OrderService.java` (lines 82–184)
  - `inventory-service/src/main/resources/scripts/reserve_stock.lua`
- **Code Evidence:**
  In `OrderService.java`:
  ```java
  Optional<OrderEntity> existingOpt = orderRepository.findBySaleIdAndShopperIdAndIdempotencyKey(...); // Line 82
  // ... Both threads find empty ...
  ReservationResponse reservation = inventoryClient.reserveStock(...); // Line 101 - Redis DECRBY called TWICE!
  // ...
  orderPersistenceHelper.persistOrderAndReservation(...); // Second thread throws DataIntegrityViolationException
  ```
- **Failure Consequences:**
  1. **False Sold-Out:** If available stock is 1, and two identical requests arrive concurrently, the second call to `reserve_stock.lua` fails with `SOLD_OUT` before reaching PostgreSQL. The user receives a 409 Sold Out error on an idempotent retry!
  2. If stock is available, Redis decrements stock twice. The second thread catches the DB unique constraint and calls `releaseStock()`, causing stock jitter and unnecessary reservations.
- **Proposed Fix:**
  Include `idempotencyKey` in the Redis reservation request and Lua script. `reserve_stock.lua` checks `flashsale:res_idempotency:{saleId}:{idempotencyKey}`. If present, it returns the existing `reservationId` without decrementing stock.
- **Proof Test:**
  `ConcurrentSameIdempotencyKeyRaceTest`: Send 50 concurrent requests with the identical idempotency key on a sale with stock = 1; assert 1 reservation is made, 0 false sold-out errors occur, and stock = 0.

---

### Scenario 06: Payment succeeds, but the worker crashes before recording success
- **Status:** **VERIFIED DEFECT**
- **Severity:** `CRITICAL`
- **Affected Files:**
  - `payment-fulfillment-service/src/main/java/com/flashsale/payment/service/FulfillmentService.java` (lines 143–158)
  - `payment-fulfillment-service/src/main/java/com/flashsale/payment/worker/ReservationExpiryWorker.java` (lines 75–83)
- **Code Evidence:**
  In `FulfillmentService.java`:
  ```java
  PaymentResult paymentResult = paymentSimulationService.processPayment(orderId, shopperId, quantity); // Paid!
  // [Crash occurs here before orderRepository.save()]
  order.transitionTo(OrderStatus.CONFIRMED);
  orderRepository.save(order);
  ```
  In `ReservationExpiryWorker.java`:
  ```java
  if (order.getStatus() == OrderStatus.PENDING || order.getStatus() == OrderStatus.PROCESSING) {
      order.transitionTo(OrderStatus.EXPIRED);
      inventoryClient.releaseStock(...);
  }
  ```
- **Failure Consequences:**
  The customer has been debited. Because the worker crashed, PostgreSQL retains the order as `PENDING` or `PROCESSING`. When `ReservationExpiryWorker` runs, it marks the order `EXPIRED` and restores stock to Redis. The item is resold to someone else, resulting in an unauthorized debit and lost goods.
- **Proposed Fix:**
  1. Record an intermediate `PAYMENT_SUBMITTED` record with payment transaction ID before charging.
  2. Update `ReservationExpiryWorker` and `OrderReconciliationWorker`: before expiring any order in `PROCESSING` status, call the payment gateway status API using `orderId` to verify settlement status. If captured, auto-confirm rather than expire.
- **Proof Test:**
  `PaymentSuccessWorkerCrashReconciliationTest`: Simulate worker crash after payment; trigger reconciliation; assert order recovers to `CONFIRMED`.

---

### Scenario 07: A reservation expires while payment processing is in progress
- **Status:** **VERIFIED DEFECT**
- **Severity:** `CRITICAL`
- **Affected Files:**
  - `payment-fulfillment-service/src/main/java/com/flashsale/payment/worker/ReservationExpiryWorker.java` (lines 75–93)
  - `payment-fulfillment-service/src/main/java/com/flashsale/payment/service/FulfillmentService.java` (lines 148–158)
  - `payment-fulfillment-service/src/main/java/com/flashsale/payment/client/InventoryClient.java` (lines 30–44)
  - `inventory-service/src/main/resources/scripts/commit_stock.lua` (lines 14–16)
- **Code Evidence:**
  1. `ReservationExpiryWorker` marks order `EXPIRED` and calls `releaseStock()`, returning stock to Redis.
  2. `FulfillmentService.java`:
     ```java
     order.transitionTo(OrderStatus.CONFIRMED); // Transitions dirty in-memory entity!
     orderRepository.save(order);               // Overwrites EXPIRED in DB without version check!
     inventoryClient.commitStock(saleId, reservationId, correlationId);
     ```
  3. `InventoryClient.java:41-43`:
     ```java
     } catch (Exception ex) {
         log.error("Failed to commit stock for reservation {}...: {}", reservationId, saleId, ex.getMessage());
     } // Swallows commit failure!
     ```
- **Failure Consequences:**
  **Direct Double-Selling / Overselling:**
  The item is released in Redis at $T=120$s and purchased by User B.
  At $T=123$s, User A's slow payment completes. `FulfillmentService` blindly overwrites User A's order to `CONFIRMED` and swallows the Redis commit failure. Both User A and User B possess confirmed orders for the exact same inventory item!
- **Proposed Fix:**
  1. Add `@Version private Long version;` to `OrderEntity` and `ReservationEntity` to trigger optimistic locking conflict on concurrent expiration.
  2. In `InventoryClient.java`, do not swallow commit errors; throw `StockCommitFailedException`.
  3. If commit fails, initiate an automated **Compensating Refund Saga**: refund payment, transition order to `SETTLEMENT_ABORTED_EXPIRED`, and notify customer.
- **Proof Test:**
  `ReservationExpiryDuringPaymentRaceTest`: Concurrently trigger `expireOrphanedReservations` while payment is running; verify order does not transition to `CONFIRMED` and compensation is initiated.

---

### Scenario 08: A retry attempts to release the same reservation twice
- **Status:** **VERIFIED SAFE (with minor caveat)**
- **Severity:** `LOW`
- **Affected Files:**
  - `inventory-service/src/main/resources/scripts/release_stock.lua` (lines 8–29)
- **Code Evidence:**
  ```lua
  local status = redis.call('HGET', KEYS[2], 'status')
  if status ~= 'ACTIVE' then
      return 0 -- Already RELEASED, COMMITTED, or EXPIRED. Do not double-restore stock!
  end
  redis.call('HSET', KEYS[2], 'status', 'RELEASED', 'releaseReason', ARGV[1])
  redis.call('EXPIRE', KEYS[2], 60)
  redis.call('INCRBY', KEYS[1], qty)
  ```
- **Failure Consequences:**
  The first release restores stock and sets status to `RELEASED` with a 60s TTL. Any subsequent retry within 60s finds status `RELEASED` and returns 0 without calling `INCRBY`. Retries after 60s find key missing (`EXISTS == 0`) and return 0.
  **Caveat:** If a release is requested > 60s after natural TTL eviction, Redis does not know whether it was ever released.
- **Proposed Fix:**
  Maintain the 60s tombstone and ensure PostgreSQL is the ultimate source of truth for reservation states.
- **Proof Test:**
  `IdempotentStockReleaseRetryTest`: Execute 5 sequential release calls on the same active reservation; verify Redis stock counter increases by exactly `quantity` on call 1 and 0 on calls 2–5.

---

### Scenario 09: Redis restarts or loses recently acknowledged reservation state
- **Status:** **VERIFIED DEFECT / INFRA RISK**
- **Severity:** `HIGH`
- **Affected Files:**
  - `docker-compose.yml` (line 38)
  - `inventory-service/src/main/java/com/flashsale/inventory/service/SaleService.java`
- **Code Evidence:**
  `docker-compose.yml`:
  `["redis-server", "--appendonly", "yes", "--save", "60", "1"]`
  No stock rehydration endpoint or startup reconciliation exists in `inventory-service`.
- **Failure Consequences:**
  In a crash, reboot, or ElastiCache failover with asynchronous replication, acknowledged reservations can be lost. If stock is reloaded from initial catalog stock, all previously sold units are re-sold, violating zero-overselling.
- **Proposed Fix:**
  Implement a `StockRehydrationService` in `inventory-service`:
  $\text{AvailableStock} = \text{InitialStock} - \sum(\text{ActiveReservations} + \text{CommittedOrders})$.
  Query PostgreSQL source of truth to rehydrate Redis keys on startup or failover.
- **Proof Test:**
  `RedisFailoverStateLossRehydrationTest`: Wipe Redis with 20 active reservations in DB; execute rehydration; verify stock key reflects exact unreserved quantity.

---

### Scenario 10: Aurora reaches its connection limit during a traffic burst
- **Status:** **VERIFIED DEFECT / SCALING RISK**
- **Severity:** `HIGH`
- **Affected Files:**
  - `order-service/src/main/resources/application.yml` (line 15: `maximum-pool-size: 30`)
  - `payment-fulfillment-service/src/main/resources/application.yml` (line 14: `maximum-pool-size: 20`)
- **Code Evidence:**
  Direct JDBC connections from application pods to PostgreSQL without an intermediate connection pooler or proxy.
- **Failure Consequences:**
  If ECS Fargate scales to 20 `order-service` pods and 20 `payment-fulfillment-service` pods during peak flash sale, total requested connections = $(20 \times 30) + (20 \times 20) = 1,000$. An Aurora `db.t4g.medium` or `db.r6g.large` instance hits connection limits, throwing `SQLTransientConnectionException`, blocking all checkouts.
- **Proposed Fix:**
  1. Deploy **AWS RDS Proxy** in front of Aurora PostgreSQL for connection multiplexing.
  2. Reduce pod pool sizes to `maximum-pool-size: 10`, allowing thousands of pods to multiplex over 100 Aurora backend connections.
- **Proof Test:**
  `AuroraConnectionPoolSaturationTest`: Simulate 200 concurrent threads executing transactions; assert RDS Proxy prevents connection exhaustion errors.

---

### Scenario 11: Lambda workers scale faster than downstream services can process work
- **Status:** **ARCHITECTURAL RISK**
- **Severity:** `HIGH`
- **Affected Files:**
  - `payment-fulfillment-service/src/main/java/com/flashsale/payment/service/FulfillmentService.java`
- **Code Evidence:**
  Consumer Lambdas or high-concurrency Fargate tasks scale instantaneously upon detecting high queue depth in MSK or SQS.
- **Failure Consequences:**
  A sudden scale-out of 1,000 concurrent consumers overwhelms downstream third-party payment APIs (HTTP 429 Too Many Requests) and exhausts Inventory Service REST connection pools.
- **Proposed Fix:**
  1. Configure `MaximumConcurrentExecutions` or `ReservedConcurrentExecutions` on AWS Lambda consumers.
  2. Implement client-side rate limiting (Token Bucket) before payment gateway invocations.
- **Proof Test:**
  `DownstreamConsumerBackpressureTest`: Inject 10,000 events into MSK; assert consumer concurrency does not exceed configured 200 RPS downstream limit.

---

### Scenario 12: A Kafka partition becomes unavailable or consumer lag increases
- **Status:** **VERIFIED DEFECT / SCALING RISK**
- **Severity:** `MEDIUM`
- **Affected Files:**
  - `payment-fulfillment-service/src/main/java/com/flashsale/payment/config/KafkaConsumerConfig.java` (lines 40–87)
  - `payment-fulfillment-service/src/main/resources/application.yml` (line 22)
- **Code Evidence:**
  `flashsale.orders.v1` defaults to 1 partition in local setup. In `application.yml`, `listener.concurrency: 4`.
- **Failure Consequences:**
  1. **Idle Consumers:** With 1 partition, only 1 thread can consume; 3 threads are permanently idle.
  2. **TTL Expiration in Queue:** Under a 10,000-order burst, processing lag can exceed 120 seconds. Orders sitting in the Kafka topic will expire in PostgreSQL before the consumer dequeues them.
- **Proposed Fix:**
  1. Pre-create Kafka topics with 12–24 partitions keyed by `aggregateId`.
  2. Configure CloudWatch Metric Alarms on MSK `EstimatedMaxTimeLag` to scale consumer tasks.
- **Proof Test:**
  `KafkaPartitionLagExceedingTtlTest`: Publish 1,000 events to 12 partitions; verify even distribution across consumers and lag draining within 15 seconds.

---

### Scenario 13: An AWS Availability Zone becomes unavailable
- **Status:** **INFRASTRUCTURE RISK**
- **Severity:** `HIGH`
- **Affected Files:**
  - `docker-compose.yml` (single-node local definitions)
- **Code Evidence:**
  Current system lacks Multi-AZ topology configuration and relies on single endpoints.
- **Failure Consequences:**
  An AZ outage causes complete outage of services hosted in that AZ, losing uncommitted in-flight data.
- **Proposed Fix:**
  Deploy across 3 AZs:
  - Multi-AZ ALB distributing to ECS Fargate across 3 subnets.
  - Aurora PostgreSQL Multi-AZ (1 Primary Writer, 2 Read Replicas) with RDS Proxy.
  - Amazon ElastiCache for Redis Multi-AZ with Auto-Failover.
  - Amazon MSK 3-Broker Multi-AZ (`replication.factor=3`, `min.insync.replicas=2`).
- **Proof Test:**
  `MultiAzFailoverResilienceSimulationTest`: Simulate AZ network failure; assert ALB and RDS Proxy reroute traffic within 5 seconds with zero data loss.

---

### Scenario 14: A secret or credential is exposed through configuration or logs
- **Status:** **VERIFIED DEFECT**
- **Severity:** `HIGH`
- **Affected Files:**
  - `order-service/src/main/resources/application.yml` (line 10)
  - `payment-fulfillment-service/src/main/resources/application.yml` (line 10)
  - `docker-compose.yml` (lines 14, 21)
  - `payment-fulfillment-service/src/main/java/com/flashsale/payment/consumer/OrderReservedConsumer.java` (line 34)
- **Code Evidence:**
  Hardcoded passwords: `password: ${SPRING_DATASOURCE_PASSWORD:flashsale_secret}`.
  Unmasked consumer logging of shopper IDs and order IDs.
- **Failure Consequences:**
  Credential exposure in repository history; PII leakage in CloudWatch Logs violating PCI-DSS compliance.
- **Proposed Fix:**
  1. Replace hardcoded default passwords with AWS Secrets Manager references in ECS task definitions.
  2. Configure Logback `PatternLayout` with masking regex for PII (shopper IDs, emails, payment tokens).
  3. Secure Spring Boot Actuator endpoints with Spring Security.
- **Proof Test:**
  `SecurityCredentialExposureScanTest`: Automated CI/CD script scanning repository source and log outputs for unencrypted credentials.

---

## 4. System Invariant Evaluation

| Invariant | Target Requirement | Current Codebase Status | Primary Vulnerability | Remediated Status |
|---|---|---|---|---|
| **Zero Overselling** | $\sum \text{Confirmed} \le \text{Allocated}$ | **FAILED** | Scenarios 01, 07, 09 (Swallowed commit errors, expiry during payment) | Enforce Redis Lua commit validation, `@Version` optimistic locking, and Compensating Refund Saga |
| **At-Most-Once Payment** | Max 1 charge per `orderId` | **FAILED** | Scenarios 04, 06 (Concurrent Kafka deliveries execute `processPayment` before DB commit) | Payment gateway idempotency keys + database pessimistic locking prior to payment |
| **Outbox Non-Blocking** | Relay progresses despite Kafka outages | **FAILED** | Scenario 02 (Stuck top 100 events cause infinite HoL blocking) | `next_retry_at` exponential backoff + Circuit Breaker on Outbox Relay |
| **Idempotent Ingress** | Identical request returns identical order | **FAILED** | Scenario 05 (Concurrent same-key requests hit Redis twice, false SOLD_OUT) | Redis idempotency cache inside `reserve_stock.lua` |
| **Database Pool Resilience** | Pool handles burst without 500s | **FAILED** | Scenario 10 (Direct Hikari connections exceed Aurora `max_connections`) | AWS RDS Proxy connection multiplexing |

---

## 5. Security, Observability, and Operations Audit

### Security
1. **Actuator Exposure:** All Actuator endpoints (`/actuator/**`) are open without authentication in `flashsale-gateway`. Anyone can view application metrics and configuration. Must be locked down with Spring Security and internal VPC ALB routing.
2. **Secrets Management:** Microservices fall back to plaintext defaults (`flashsale_secret`). Production must inject credentials directly from AWS Secrets Manager via ECS task definition `secrets`.

### Observability & Tracing
1. **Correlation IDs:** `CorrelationIdFilter.java` correctly propagates `X-Correlation-Id`. However, the correlation ID is lost in some background worker threads (`ReservationExpiryWorker.java`).
2. **Distributed Tracing:** Current services lack AWS X-Ray or OpenTelemetry instrumentation. Under asynchronous Kafka processing, tracing spans are broken between the HTTP gateway, outbox relay, and consumer workers.
3. **Metrics Gap:** Missing CloudWatch alarms for:
   - Kafka Consumer Lag (`EstimatedMaxTimeLag` > 30s)
   - Redis Memory Fragmentation Ratio & CPU Utilization
   - Aurora Database Connections & Deadlock Counts
   - Outbox Head-of-Line Retry Rate

---

## 6. Conclusion and Next Steps

The existing FlashSale Engine has established a solid architectural foundation with Spring Cloud Gateway, Redis Lua reservations, and the Transactional Outbox pattern. However, the system currently contains **critical distributed race conditions and crash windows** that break the zero-overselling guarantee and can double-charge customers during network partitions, worker crashes, or high consumer lag.

No application code was altered during this audit stage. The verified defects and architectural recommendations documented above form the basis of the **Remediation Plan** (`docs/remediation-plan.md`) and **AWS Target Architecture** (`docs/aws-target-architecture.md`).
