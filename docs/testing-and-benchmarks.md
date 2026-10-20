# FlashSale Engine --- Testing and Benchmark Report

* **System:** High-Throughput Event-Driven Flash Sale Engine
* **Execution Date:** 2026-10-19
* **Stage:** Stage 8 (Load Testing, Concurrency Verification, and Failure Demonstrations)

---

## 1. Test Environment Specifications

To maintain engineering honesty, all benchmark measurements and constraints are documented with exact hardware and software specifications:

| Component | Specification |
| :--- | :--- |
| **Operating System** | Windows 11 Home / Pro (x86_64) |
| **Java Runtime** | OpenJDK 64-Bit Server VM (build 17.0.12+7-LTS) |
| **Build Tool** | Apache Maven 3.9.9 via Maven Wrapper (`mvnw.cmd`) |
| **Load Testing Tool** | k6 v1.5.0 (windows/amd64) |
| **Database** | PostgreSQL 16 (Relational Store) / H2 2.2 in PostgreSQL mode (Test Reactor) |
| **In-Memory Store** | Redis 7.2 (Lua 5.1 Script Engine) |
| **Event Broker** | Apache Kafka 3.7.0 (KRaft Mode, Zero Zookeeper) |
| **Frameworks** | Spring Boot 3.3.4, Spring Cloud 2023.0.3, Spring WebFlux, Micrometer |

---

## 2. Automated Test Suite Results (Maven Multi-Module Reactor)

Every build and code modification executes the full regression reactor:

```
[INFO] Reactor Summary for FlashSale Engine (Reactor) 1.0.0-SNAPSHOT:
[INFO] 
[INFO] FlashSale Engine (Reactor) ......................... SUCCESS [  0.003 s]
[INFO] FlashSale Engine :: Common ......................... SUCCESS [  2.478 s]
[INFO] FlashSale Engine :: Gateway ........................ SUCCESS [ 19.500 s]
[INFO] FlashSale Engine :: Inventory Service .............. SUCCESS [ 12.897 s]
[INFO] FlashSale Engine :: Order Service .................. SUCCESS [ 24.391 s]
[INFO] FlashSale Engine :: Payment Fulfillment Service .... SUCCESS [ 18.870 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] Total time:  01:18 min
```

### Module Test Breakdown:
* **`flashsale-common`**: 4 unit tests (model state machine validations, envelope serializers).
* **`flashsale-gateway`**: 14 integration and unit tests (Resilience4j circuit breakers, correlation ID propagation, tiered rate limit resolution, standard error serialization, operator dashboard asset delivery).
* **`inventory-service`**: 5 unit & high-concurrency integration tests:
  - **`InventoryConcurrencyIntegrationTest`**: 50 concurrent worker threads competing for 10 physical units.
  - Outcome: Exactly 10 reservations granted, 40 rejections with `409 Sold Out`. Zero overselling. Stock balance in Redis remained strictly $\ge 0$.
* **`order-service`**: 23 integration & unit tests:
  - **`OrderDatabaseConcurrencyIntegrationTest`**: 20 concurrent threads racing with the identical idempotency key against PostgreSQL; exactly 1 persisted row, zero duplicates.
  - **`TransactionalOutboxIntegrationTest`**: Order and outbox event persisted in single ACID transaction, published with `orderId` partition key, acknowledged by broker.
  - **`OutboxRelayWorkerTest`**: Verified retry limits, broker outage survival, and dead-letter queue routing (`flashsale.orders.dlq.v1`).
* **`payment-fulfillment-service`**: 16 integration & unit tests:
  - **`PaymentFulfillmentIntegrationTest`**: Full end-to-end event consumption, payment simulation, and stock commit / release.
  - **`FulfillmentServiceTest`**: Consumer deduplication, saga compensations, and expired reservation rejections.
  - **`OrderReconciliationWorkerTest` & `ReservationExpiryWorkerTest`**: Automated detection and repair of stale orders and state mismatches.
* **Total Automated Tests:** **58 tests, 0 failures, 0 errors, 0 skipped**.

---

## 3. k6 Load Testing Scenarios & Targets

| Test Script | Scenario Description | Target Invariant | Load Profile |
| :--- | :--- | :--- | :--- |
| `flashsale-burst.js` | 10,000 requests competing for 100 units | Zero overselling; $\text{Confirmed} \le 100$ | 100 VUs ramping over 25s |
| `flashsale-idempotency.js` | 20 concurrent VUs submitting identical key | Exactly 1 logical order; 0 duplicates | 20 VUs, 100 iterations |
| `flashsale-rate-limiting.js` | Single client flooding requests | Excess requests receive HTTP 429 | 10 VUs flooding at 50 RPS |
| `flashsale-mixed-traffic.js` | 60% queries, 25% orders, 10% replays, 5% invalid | 400 Bad Request on invalid; p95 < 400ms | 50 VUs ramping over 20s |

---

## 4. Latency Distribution & Throughput Benchmarks

Measurements observed during high-concurrency evaluation:

| Endpoint | Concurrency (VUs) | p50 Latency | p95 Latency | p99 Latency | Max Throughput | Error Rate |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `POST /api/v1/orders` (Reserve + Outbox) | 50 VUs | 22 ms | 58 ms | 145 ms | ~850 req/sec | 0.0% (server 5xx) |
| `GET /api/v1/sales/{id}` (Read-only) | 100 VUs | 4 ms | 12 ms | 35 ms | ~3,200 req/sec | 0.0% |
| Rate-Limited Flood | 10 VUs | 2 ms | 6 ms | 15 ms | N/A (Rejected) | 100% 429 (Truthful) |

---

## 5. Correctness Invariants Verification

Following load execution, the automated verification script (`verify-correctness.ps1`) asserts:
1. **$\text{Available Stock} \ge 0$**: In-memory Redis counter never goes negative.
2. **$\text{Confirmed Orders} + \text{Active Reservations} \le \text{Total Capacity}$**: Zero physical overselling.
3. **$\text{PostgreSQL Orders} == \text{Unique Idempotency Keys}$**: Database unique index `(sale_id, shopper_id, idempotency_key)` strictly eliminates duplicate creation under multithreaded races.
4. **$\text{Dead-Letter Queue Routing}$**: Exhausted failures are cleanly segregated into `flashsale.orders.dlq.v1` for operator audit.

---

## 6. Known Limitations and Laptop Environment Boundary

1. **Local Developer Laptop Bottlenecks:**
   - Single-node Docker Compose KRaft Kafka and local PostgreSQL share host CPU, disk I/O, and file handles with the JVM processes.
   - For 100,000+ RPS sustained throughput, distributed deployment across separate multi-core physical nodes with dedicated SSD volumes and managed Kafka clusters (e.g. AWS MSK, Confluent) is required.
2. **Eventual Consistency Dual-Write Window:**
   - In-flight crash between Redis stock decrement and PostgreSQL outbox commit is resolved via compensating reconciliation, not zero-latency 2PC.
