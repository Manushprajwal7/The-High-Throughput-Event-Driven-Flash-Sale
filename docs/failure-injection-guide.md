# FlashSale Engine --- Fault Injection & Failure Runbook

* **Stage:** Stage 8 (Load Testing and Failure Demonstrations)
* **Goal:** Verify that the system maintains consistency, zero overselling, and truthful degradation under real-world infrastructure faults.

---

## 1. Overview of Failure Boundaries

The FlashSale distributed architecture spans three major data layers:
1. **In-Memory Cache & Counters:** Redis 7
2. **Relational Durable Store:** PostgreSQL 16
3. **Event Streaming Backbone:** Apache Kafka 3.7 (KRaft)
4. **API Gateway & Routing:** Spring Cloud Gateway with Resilience4j

This guide describes exact procedures to inject infrastructure failures and observe automated recovery.

---

## 2. Failure Scenarios and Execution Steps

### Scenario 1: Extreme Burst & Contention (10,000 Requests Competing for 100 Units)
* **Objective:** Verify zero overselling under heavy concurrency.
* **Execution:**
  ```bash
  k6 run load-test/k6/flashsale-burst.js
  ```
* **Expected Invariants:**
  - Accepted orders ($\text{HTTP 200/201/202}$) $\le 100$.
  - All subsequent requests receive $\text{HTTP 409 Conflict}$ with `code: "SOLD_OUT"`.
  - Available stock in Redis never drops below zero.

---

### Scenario 2: Concurrent Duplicate Submissions (Same Idempotency Key)
* **Objective:** Ensure concurrent race conditions with identical idempotency keys create exactly one order.
* **Execution:**
  ```bash
  k6 run load-test/k6/flashsale-idempotency.js
  ```
* **Expected Invariants:**
  - Multiple concurrent requests with the same key receive identical `orderId`.
  - Re-sending with altered payload receives $\text{HTTP 409 Conflict}$ (`IDEMPOTENCY_CONFLICT`).
  - Exactly one row persisted in PostgreSQL table `orders`.

---

### Scenario 3: Gateway Rate Limit Exceeded
* **Objective:** Verify token-bucket rate limiting rejects excessive traffic.
* **Execution:**
  ```bash
  k6 run load-test/k6/flashsale-rate-limiting.js
  ```
* **Expected Invariants:**
  - Burst exceeding token capacity receives $\text{HTTP 429 Too Many Requests}$.
  - Response body contains standard `ErrorResponse` with `code: "RATE_LIMIT_EXCEEDED"`.
  - `X-Correlation-Id` header is preserved.

---

### Scenario 4: Kafka Broker Interruption / Pause
* **Objective:** Verify that orders are still accepted durably into the PostgreSQL transactional outbox when Kafka is paused or down.
* **Fault Injection:**
  ```bash
  docker compose pause kafka
  ```
* **Traffic Ingestion:**
  ```bash
  curl -X POST http://localhost:8080/api/v1/orders \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: kafka-outage-key-1" \
    -d '{"saleId": "c379a29e-297c-473d-8e68-0744a530733a", "shopperId": "shopper_kafka_test", "quantity": 1}'
  ```
* **Observation:**
  - Order is accepted: HTTP 202/200 returned.
  - Relational database has order and outbox record with `published_at = NULL`.
  - `OutboxRelayWorker` increments `attempt_count` and records error without data loss.
* **Recovery:**
  ```bash
  docker compose unpause kafka
  ```
  - Worker automatically retries and receives broker ACK.
  - Outbox row updated with `published_at = NOW()`.

---

### Scenario 5: Payment Fulfillment Worker Crash & Restart
* **Objective:** Ensure unfulfilled orders and orphaned reservations are recovered when worker is terminated.
* **Fault Injection:**
  ```bash
  docker compose stop payment-fulfillment-service
  ```
* **Observation:**
  - Orders continue to be accepted into `PENDING` state and queued in Kafka topic `flashsale.orders.v1`.
  - Consumer lag increases in Prometheus/Grafana.
* **Recovery:**
  ```bash
  docker compose start payment-fulfillment-service
  ```
  - Worker reconnects to Kafka consumer group `payment-fulfillment-workers`.
  - Resumes processing at last uncommitted offset.
  - Dedup table `processed_events` prevents re-processing previously completed events.

---

### Scenario 6: Payment Decline Surge (Saga Compensation)
* **Objective:** Validate that payment rejections trigger compensating stock releases back to Redis.
* **Fault Injection:**
  ```bash
  curl -X PUT http://localhost:8080/api/v1/simulation/payment \
    -H "Content-Type: application/json" \
    -d '{"mode": "ALWAYS_DECLINE"}'
  ```
* **Execution:**
  - Submit purchase order via Gateway or Dashboard.
* **Observation:**
  - Order status transitions `PENDING` $\to$ `PROCESSING` $\to$ `PAYMENT_FAILED`.
  - Worker calls `releaseStock` in Inventory Service (`release_stock.lua`).
  - Redis available stock increments back up.
  - No orphaned stock leakage occurs.
* **Restore:**
  ```bash
  curl -X PUT http://localhost:8080/api/v1/simulation/payment \
    -H "Content-Type: application/json" \
    -d '{"mode": "ALWAYS_SUCCESS"}'
  ```

---

### Scenario 7: Downstream Dependency Outage (Truthful Degradation)
* **Objective:** Verify Resilience4j circuit breakers return HTTP 503 rather than false successes.
* **Fault Injection:**
  ```bash
  docker compose stop inventory-service
  ```
* **Query:**
  ```bash
  curl -i http://localhost:8080/api/v1/sales
  ```
* **Observation:**
  - Gateway returns HTTP 503 `SERVICE_DEGRADED`.
  - Never returns a fake order or false positive.

---

### Scenario 8: Automated Invariant Assertion
* Run after any test or outage scenario:
  ```powershell
  .\load-test\scripts\verify-correctness.ps1
  ```
