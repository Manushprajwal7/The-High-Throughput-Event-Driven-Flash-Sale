# FlashSale Engine --- Live Demo & Portfolio Showcase Guide

> **Target Audience:** Systems Architects, Senior Backend Reviewers, Technical Interviewers.  
> **Estimated Demonstration Time:** 10--15 minutes.  
> **Key Focus Areas:** Concurrency control, dual-store consistency, transactional outbox pattern, idempotent event processing, self-healing compensation sagas, and real-time observability.

---

## 1. Demo Narrative & System Story

Flash sales present one of the most demanding challenges in distributed systems engineering: **extreme contention on a single shared counter coupled with the requirement for strict ACID durability and financial consistency**.

Traditional relational locking patterns (e.g. `SELECT FOR UPDATE`) rapidly cause database thread starvation, deadlocks, and severe latency spikes. Conversely, pure in-memory caching solutions risk catastrophic overselling upon node crashes or network partitions.

**The FlashSale Engine solves this dilemma via a dual-store, event-driven architecture:**
1. **In-Memory Concurrency Tier (Redis 7 + Lua):** Atomically claims stock reservations in under 2ms using a single-threaded Lua script, bounding contention before requests touch the relational database.
2. **Durable Transactional Tier (PostgreSQL 16 + Transactional Outbox):** Persists logical orders and outbox events in a single atomic database transaction with database-enforced idempotency constraints.
3. **Asynchronous Fulfillment Tier (Apache Kafka + Consumer Sagas):** Offloads payment processing and order confirmation into a non-blocking background pipeline protected by idempotency tables and automated compensating actions.

---

## 2. Pre-Demo Setup & Environment Verification

Execute the setup commands from the root directory:

```powershell
# 1. Start Docker Compose infrastructure (Postgres, Redis, Kafka, Prometheus, Grafana)
.\scripts\start-infra.ps1

# 2. Build reactor & verify 58 automated unit/integration tests
.\mvnw.cmd test

# 3. Start the microservices (in separate terminals or IDE runs):
# Terminal 1: Gateway
.\mvnw.cmd spring-boot:run -pl flashsale-gateway
# Terminal 2: Inventory Service
.\mvnw.cmd spring-boot:run -pl inventory-service
# Terminal 3: Order Service
.\mvnw.cmd spring-boot:run -pl order-service
# Terminal 4: Payment Fulfillment Worker
.\mvnw.cmd spring-boot:run -pl payment-fulfillment-service
```

Verify service readiness via the Gateway Actuator:
```bash
curl -s http://localhost:8080/actuator/health
```

---

## 3. Step-by-Step Live Demo Walkthrough

### Step 1: Open the Operator Dashboard
Navigate to `http://localhost:8080/index.html` in your browser.
* Explain the clean, non-blocking UI connected to live Spring Boot Actuator endpoints.
* Note the initial state: `System Health: UP`, `0 Active Reservations`.

### Step 2: Seed a Flash Sale (100 Scarce Units)
Create and open a new flash sale item:
```bash
# Automated helper:
.\scripts\seed-data.ps1

# Or manual cURL:
curl -X POST http://localhost:8080/api/v1/admin/sales \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Super Phone 16 Limited Drop",
    "totalCapacity": 100,
    "startsAt": "2026-10-19T00:00:00Z",
    "endsAt": "2026-10-20T00:00:00Z"
  }'

# Open sale (replace SALE_ID):
curl -X POST http://localhost:8080/api/v1/admin/sales/{SALE_ID}/open
```
* **What to observe:** `availableStock = 100`, `reservedStock = 0`, status changes from `DRAFT` to `OPEN`.

### Step 3: Trigger High-Concurrency Contention Burst
Run the k6 burst scenario competing for the 100 units:
```bash
k6 run load-test/k6/flashsale-burst.js -e SALE_ID={SALE_ID}
```
* **What happens behind the scenes:**
  1. 100 virtual users bombard `POST /api/v1/orders`.
  2. Redis executes `reserve_stock.lua` atomically.
  3. The first 100 requests reserve stock and receive HTTP `202 Accepted`.
  4. Subsequent requests immediately receive HTTP `409 Conflict` with `{"code":"SOLD_OUT"}` in < 5ms without overwhelming PostgreSQL.
  5. Gateway metrics record accepted vs sold-out counters.

### Step 4: Prove Zero-Overselling Invariant
Run the automated verification script:
```powershell
.\load-test\scripts\verify-correctness.ps1 -SaleId {SALE_ID}
```
* **Asserted Proof:**
  $$\text{Redis Available Stock} \ge 0$$
  $$\text{PostgreSQL Confirmed Orders} + \text{Active Reservations} \le \text{Total Capacity (100)}$$
* Point out that under zero circumstances did stock decrement to -1 or confirm 101 units.

### Step 5: Demonstrate Idempotency Replay
Show that shoppers who double-click or experience network drops do not buy twice:
```bash
# Request 1:
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-repeat-999" \
  -H "X-Shopper-Id: shopper_charlie" \
  -d '{"saleId":"{SALE_ID}","shopperId":"shopper_charlie","quantity":1}'

# Request 2 (Identical Idempotency-Key & Payload):
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-repeat-999" \
  -H "X-Shopper-Id: shopper_charlie" \
  -d '{"saleId":"{SALE_ID}","shopperId":"shopper_charlie","quantity":1}'
```
* **What to observe:** Both requests return the identical `orderId` and status `200 OK` on replay, with zero additional stock decremented.

### Step 6: Demonstrate Self-Healing Compensations & Failure Injection
Demonstrate that when payment fails, stock is automatically returned to the sale pool:
```bash
# 1. Configure Payment Simulator to decline payments
curl -X POST http://localhost:8080/api/v1/simulation/mode?mode=ALWAYS_DECLINE

# 2. Place an order
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: fail-test-key-01" \
  -d '{"saleId":"{SALE_ID}","shopperId":"shopper_decline","quantity":1}'

# 3. Wait 3 seconds, query order status:
curl http://localhost:8080/api/v1/orders/{ORDER_ID}
# Output: status = PAYMENT_FAILED

# 4. Check inventory stock:
curl http://localhost:8080/api/v1/sales/{SALE_ID}
# Output: The 1 unit was automatically released via compensating saga back into availableStock!
```

### Step 7: Inspect Real-Time Telemetry in Grafana
Open `http://localhost:3000` (admin / admin):
* Show the **FlashSale Engine - Observability Dashboard**:
  - Request throughput (RPS).
  - p95/p99 latency trends.
  - Active reservation counts.
  - Kafka consumer lag (draining to 0).
  - Circuit breaker states (`CLOSED`).

---

## 4. Key Code Sections to Highlight During Interviews

1. **Atomic Redis Lua Scripting:**  
   [`inventory-service/src/main/resources/scripts/reserve_stock.lua`](file:///c:/Users/Manus/OneDrive/Desktop/projecs/The%20High-Throughput%20Event-Driven%20Flash%20Sale/inventory-service/src/main/resources/scripts/reserve_stock.lua)  
   *Explain why in-memory Lua guarantees atomicity without distributed lock overhead.*

2. **Transactional Outbox Persistence:**  
   [`order-service/src/main/java/com/flashsale/order/service/OrderPersistenceHelper.java`](file:///c:/Users/Manus/OneDrive/Desktop/projecs/The%20High-Throughput%20Event-Driven%20Flash%20Sale/order-service/src/main/java/com/flashsale/order/service/OrderPersistenceHelper.java)  
   *Highlight `Propagation.REQUIRES_NEW` ensuring atomic order + outbox persistence.*

3. **Consumer Idempotency Deduplication:**  
   [`payment-fulfillment-service/src/main/java/com/flashsale/payment/service/FulfillmentService.java`](file:///c:/Users/Manus/OneDrive/Desktop/projecs/The%20High-Throughput%20Event-Driven%20Flash%20Sale/payment-fulfillment-service/src/main/java/com/flashsale/payment/service/FulfillmentService.java)  
   *Explain the `processed_events` table eliminating duplicate processing upon Kafka rebalances.*

4. **Self-Healing Reconciliation Worker:**  
   [`payment-fulfillment-service/src/main/java/com/flashsale/payment/worker/OrderReconciliationWorker.java`](file:///c:/Users/Manus/OneDrive/Desktop/projecs/The%20High-Throughput%20Event-Driven%20Flash%20Sale/payment-fulfillment-service/src/main/java/com/flashsale/payment/worker/OrderReconciliationWorker.java)  
   *Demonstrates defensive systems design: detecting orphaned reservations and repairing inconsistencies.*
