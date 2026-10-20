# FlashSale Engine --- Staged Implementation Plan

This document establishes the verified implementation roadmap for FlashSale Engine across 10 progressive stages (Stage 0 to Stage 9). In accordance with the project PRD, each stage must be implemented independently, thoroughly tested, and reviewed at its explicit exit gate before beginning the next stage.

---

## Stage Matrix & Progress Tracker

| Stage | Name | Key Deliverables | Status |
| :---: | :--- | :--- | :---: |
| **0** | **Repository Discovery & Design** | Architecture docs, ADRs, contracts, data model, failure matrix | **In Progress** |
| **1** | **Scaffold & Infrastructure** | Maven multi-module, Java 21, Docker Compose (PG, Redis, Kafka) | Ready to Start |
| **2** | **Sale Management & Atomic Inventory** | Redis Lua reservation engine, inventory service, concurrency tests | Pending Stage 1 |
| **3** | **Idempotent Order API & Persistence** | PostgreSQL order schema, state machine, idempotency filters | Pending Stage 2 |
| **4** | **Transactional Outbox & Kafka Backbone** | Outbox schema, relay worker, Kafka topics, idempotent consumers | Pending Stage 3 |
| **5** | **Payment Simulation & Compensation** | Deterministic payment simulation, saga compensations, reconciliation | Pending Stage 4 |
| **6** | **Gateway, Rate Limiting & Resilience** | Spring Cloud Gateway, Redis token-bucket limiter, Resilience4j | Pending Stage 5 |
| **7** | **Observability & Operator Dashboard** | Micrometer, Prometheus, Grafana dashboards, operator web UI | Pending Stage 6 |
| **8** | **Load Testing & Chaos Scenarios** | k6 benchmark scripts, failure injection runbooks, benchmark report | Pending Stage 7 |
| **9** | **Hardening & Portfolio Release** | Portfolio README, setup runbooks, demo recordings, final artifacts | Pending Stage 8 |

---

## Detailed Stage Specifications

### Stage 0: Repository Discovery and Design
* **Goal:** Establish complete architectural specifications, contracts, data models, failure matrices, and ADRs prior to any code generation.
* **Deliverables:**
  * `README.md` project overview.
  * `docs/architecture.md` detailed architecture and Mermaid diagrams.
  * `docs/implementation-plan.md` this roadmap.
  * `docs/api-contracts.md` complete REST API specification and DTO definitions.
  * `docs/data-model.md` relational schema (PostgreSQL) and Redis key specifications.
  * `docs/event-schemas.md` Kafka messaging formats and partition strategies.
  * `docs/failure-scenarios.md` boundary crash analysis and recovery runbook.
  * `docs/adr/ADR-001-architecture.md` formal ADR on architecture decisions.
* **Exit Gate:** Architectural review approved. All cross-boundary crash windows analyzed. Zero application code written.

---

### Stage 1: Build Scaffold and Local Infrastructure
* **Goal:** Create runnable Maven multi-module project baseline and containerized infrastructure.
* **Deliverables:**
  * Root `pom.xml` with dependency management for Spring Boot 3.3.x, Spring Cloud 2023.x, Java 21.
  * Maven wrapper (`mvnw`, `mvnw.cmd`, `.mvn/wrapper/`).
  * Submodules: `flashsale-common`, `flashsale-gateway`, `inventory-service`, `order-service`, `payment-fulfillment-service`.
  * `docker-compose.yml` for PostgreSQL 16, Redis 7, and Apache Kafka (KRaft mode) with persistent volumes and health checks.
  * `.env.example` environment configuration template.
  * Base profile configuration (`application.yml` and `application-local.yml`).
* **Verification Commands:**
  * `./mvnw clean compile` (reactor succeeds across all modules).
  * `docker compose up -d` (all services report healthy via docker healthcheck).
* **Exit Gate:** Clean reactor build from root, local container infrastructure starts cleanly, no hardcoded secrets.

---

### Stage 2: Sale Management and Atomic Inventory
* **Goal:** Implement the reactive inventory reservation engine with atomic Redis Lua scripts.
* **Deliverables:**
  * `inventory-service` module implementation with Spring WebFlux.
  * Redis Lua scripts for atomic stock decrement, reservation creation, and safe stock release.
  * Sale management API endpoints: Create Sale, Open Sale, Pause Sale, Close Sale.
  * Reservation API: `POST /api/v1/sales/{saleId}/reservations`.
  * Unit tests for Lua scripts and service layers.
  * Concurrency tests using Testcontainers / embedded Redis ensuring zero negative inventory under race conditions.
* **Verification Commands:**
  * `./mvnw test -pl inventory-service`
* **Exit Gate:** High-concurrency test (e.g., 50 concurrent threads competing for 10 units) verifies exactly 10 reservations succeeded, remaining stock is 0, and no overselling occurred.

---

### Stage 3: Idempotent Order API and Durable Order State
* **Goal:** Implement order submission, idempotency key validation, and ACID PostgreSQL persistence.
* **Deliverables:**
  * Flyway database migrations for `sales`, `orders`, and `reservations` tables.
  * `order-service` implementation with Spring Boot MVC / JPA.
  * `Idempotency-Key` validation filter and request fingerprinting.
  * Order lifecycle finite state machine (`PENDING`, `PROCESSING`, `CONFIRMED`, `PAYMENT_FAILED`, `EXPIRED`, `CANCELLED`).
  * Endpoints: `POST /api/v1/orders`, `GET /api/v1/orders/{orderId}`.
  * Standardized `ErrorResponse` model and global exception handler.
* **Verification Commands:**
  * `./mvnw test -pl order-service`
* **Exit Gate:** Integration tests pass: identical request returns cached result; altered payload with same key returns 409 Conflict; concurrent requests with same key produce exactly one order.

---

### Stage 4: Transactional Outbox and Kafka Event Backbone
* **Goal:** Implement atomic state persistence with Kafka event streaming via the Transactional Outbox pattern.
* **Deliverables:**
  * Flyway migration for `outbox_events` and `processed_events` tables.
  * Outbox publishing service within the primary database transaction.
  * Scheduled Outbox Relay poller with broker ACK tracking.
  * Kafka producer configuration with idempotence enabled (`acks=all`).
  * Dead-letter topic (`flashsale.orders.dlq.v1`) handling and retry policy with exponential backoff.
* **Verification Commands:**
  * `./mvnw test -pl order-service` (Testcontainers Kafka integration).
* **Exit Gate:** Killing the relay or simulating broker unavailability does not lose uncommitted events; events resume publishing upon recovery.

---

### Stage 5: Payment Simulation, Compensation, and Expiry
* **Goal:** Implement the asynchronous consumer, deterministic payment simulation, compensating release sagas, and reconciliation.
* **Deliverables:**
  * `payment-fulfillment-service` consuming `flashsale.orders.v1`.
  * Deterministic payment simulation modes: `SUCCESS`, `DECLINED`, `TRANSIENT_TIMEOUT`.
  * Compensating stock release trigger to Inventory Service upon terminal failure.
  * Background reservation expiry cleaner for uncommitted reservations.
  * Scheduled reconciliation job checking PostgreSQL durable state against Redis counters.
* **Verification Commands:**
  * `./mvnw test -pl payment-fulfillment-service`
* **Exit Gate:** End-to-end saga test confirms failed payment transitions order to `PAYMENT_FAILED` and restores stock in Redis.

---

### Stage 6: Gateway, Rate Limiting, and Resilience
* **Goal:** Provide unified API gateway with distributed token-bucket rate limiting and fault tolerance.
* **Deliverables:**
  * `flashsale-gateway` using Spring Cloud Gateway.
  * Redis-backed rate limiting per IP/Client.
  * Correlation ID generation (`X-Correlation-Id`) and tracing header propagation.
  * Resilience4j circuit breakers and fallback responses on downstream service calls.
* **Verification Commands:**
  * `./mvnw test -pl flashsale-gateway`
* **Exit Gate:** Traffic exceeding configured RPS threshold receives deterministic `429 Too Many Requests`; downstream outage results in `503 Service Unavailable` with valid error JSON.

---

### Stage 7: Observability and Dashboard
* **Goal:** Instrument all services with metrics and provide Grafana and operator dashboards.
* **Deliverables:**
  * Actuator and Micrometer instrumentation for reservation latency, order counts, Kafka lag, and error rates.
  * Prometheus configuration scraping all services.
  * Grafana dashboards (JSON provisioning) for real-time operations.
  * Operator web dashboard UI: live inventory counts, order search, failure injection controls.
* **Verification Commands:**
  * Start stack via Docker Compose, query Prometheus metrics `/actuator/prometheus`.
* **Exit Gate:** Grafana dashboard displays real-time metrics during active orders. Operator dashboard updates live inventory without mock data.

---

### Stage 8: Load Testing and Failure Demonstrations
* **Goal:** Validate performance limits and fault recovery under extreme concurrent traffic.
* **Deliverables:**
  * k6 load-testing scripts for:
    * Baseline burst: 10,000 requests competing for 100 items.
    * High duplicate rate: identical idempotency keys concurrently submitted.
    * Mixed invalid/valid traffic.
  * Fault injection scripts (broker restart, worker kill, database pause).
  * Benchmark results report (`docs/testing-and-benchmarks.md`).
* **Verification Commands:**
  * `k6 run load-test/k6/flashsale-burst.js`
* **Exit Gate:** Zero overselling verified after 10,000 requests. Benchmark metrics documented (p50/p95/p99 latency, RPS, throughput).

---

### Stage 9: Hardening and Portfolio Release
* **Goal:** Package the repository with clean documentation, runbooks, and demo materials for showcase review.
* **Deliverables:**
  * Polished root `README.md` with architectural highlights, badges, and quickstart commands.
  * Step-by-step local demo script and walk-through guide.
  * Troubleshooting runbooks for common developer issues.
  * Recorded demo scenario artifacts or logs.
* **Exit Gate:** Project builds from a clean git clone on any compatible developer machine with a single setup command.
