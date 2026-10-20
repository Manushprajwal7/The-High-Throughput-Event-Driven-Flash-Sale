# Product Requirements Document (PRD)

## FlashSale Engine --- High-Throughput Event-Driven Flash Sale & Ticketing Platform

**Document version:** 1.0\
**Status:** Implementation baseline for Antigravity\
**Date:** 13 October 2026\
**Primary implementation language:** Java\
**Target:** Portfolio-quality, locally runnable distributed-systems
project

------------------------------------------------------------------------

## 1. Executive Summary

FlashSale Engine is a demonstrable high-concurrency ticketing and
limited-inventory commerce platform. It simulates a flash sale in which
many clients attempt to purchase a scarce item at nearly the same time.
The system must prevent overselling, handle retries safely, persist
order state, process work asynchronously, and recover from partial
failures.

The project is intended to demonstrate practical backend engineering:
concurrency control, event-driven architecture, durable messaging,
idempotency, eventual consistency, fault isolation, observability, and
load testing. It is not to be described as production-ready or as having
"zero data loss" unless the implementation and tests justify those
claims.

Implementation must proceed in reviewed stages. Antigravity should first
inspect the repository and write the architecture and plan, then
implement a single verifiable stage at a time. It must run tests and
report evidence before moving on.

### Product vision

A developer can start the entire system locally, create a sale with a
fixed inventory, generate thousands of concurrent purchase attempts,
observe safe reservation and order-processing behavior, deliberately
interrupt infrastructure, and inspect the system's recovery and
performance through a dashboard and documented test reports.

### Success statement

Given an item with 100 units and a test workload of 10,000 independent
one-unit purchase attempts, the system never allows durable or active
inventory to become negative, never confirms more than 100 purchases in
the absence of replenishment, and safely handles duplicate requests and
recoverable failures. Exact throughput and latency are measured and
reported, not assumed.

------------------------------------------------------------------------

## 2. Goals and Non-Goals

### 2.1 Goals

1.  Prevent inventory overselling under concurrent requests.
2.  Support idempotent purchase submission and safe retry behavior.
3.  Return a fast, clear response when an order is accepted for
    asynchronous processing.
4.  Persist order lifecycle transitions in PostgreSQL.
5.  Publish and consume order events through Kafka with a documented
    durability strategy.
6.  Recover from duplicate events, worker restarts, temporary broker
    outages, and payment failures.
7.  Apply request rate limits and protect services with bounded
    concurrency and timeouts.
8.  Expose health, metrics, logs, and consumer lag.
9.  Provide a polished operational dashboard suitable for a live project
    demonstration.
10. Provide repeatable automated correctness, integration, resilience,
    and load tests.
11. Provide clear setup documentation, architecture decisions, API
    contracts, and a benchmark report.

### 2.2 Non-goals for the initial release

-   Real payment processing or handling card/UPI credentials.
-   Real ticket issuance, email delivery, or external commerce
    integrations.
-   Kubernetes, multi-region deployment, cloud provisioning, or service
    mesh.
-   ScyllaDB; PostgreSQL is the initial durable store.
-   Claims of linearizability across Redis, Kafka, and PostgreSQL.
-   Guaranteed "exactly once" business outcomes from Kafka configuration
    alone.
-   Unbounded load or a promise to handle millions of requests on
    arbitrary hardware.
-   User accounts, OAuth, social login, or a complex admin permission
    system.
-   Production security certification or compliance.
-   A real-money purchasing flow.

------------------------------------------------------------------------

## 3. Users and Key Use Cases

### Primary user: interviewer / reviewer

Wants to inspect the design, run the project locally, review tests, and
understand trade-offs.

### Secondary user: developer / operator

Wants to create a sale, observe live metrics, inspect orders, and
diagnose failures.

### Simulated shopper

Wants to submit a purchase request, receive an order ID, and query the
status without creating duplicate orders when retrying.

### Core user journeys

1.  Operator creates a sale item with a defined stock quantity.
2.  Operator opens the sale.
3.  Many shoppers submit purchase requests concurrently.
4.  Inventory service atomically reserves available stock or rejects the
    request as sold out.
5.  The order workflow durably records the request and schedules
    asynchronous processing.
6.  Shopper receives an order ID and a status such as `PENDING`,
    `CONFIRMED`, `PAYMENT_FAILED`, or `EXPIRED`.
7.  Operator sees accepted/rejected counts, remaining stock, order-state
    counts, processing latency, and Kafka consumer lag.
8.  Operator triggers a controlled failure scenario and observes
    documented recovery.

------------------------------------------------------------------------

## 4. Product Principles and Invariants

These invariants are acceptance-critical. Tests must validate them
directly.

### 4.1 Inventory invariants

-   Inventory must never be negative.
-   Active reservations plus confirmed units must not exceed the sale's
    available capacity, excluding explicitly released/expired
    reservations.
-   One idempotency key for one shopper/request scope must not consume
    stock more than once.
-   Releasing or expiring a reservation must be idempotent.
-   A reservation must have a unique identifier and an expiry time.
-   Reconciliation must detect differences between Redis's fast
    reservation state and durable order state.

### 4.2 Order invariants

-   Every order has a stable unique ID.
-   Every state transition is validated against an explicit state
    machine.
-   Duplicate events cannot cause duplicate durable orders, payment
    attempts, or stock releases.
-   An order cannot become `CONFIRMED` unless reservation and simulated
    payment prerequisites are satisfied.
-   Failed or expired orders must not remain indefinitely in an active
    reservation state.
-   Durable database constraints are the final safeguard against
    duplicate order records.

### 4.3 Messaging invariants

-   Event delivery may be repeated; consumers must be idempotent.
-   A Kafka acknowledgement alone does not mean that the whole business
    workflow is complete.
-   If PostgreSQL is the source of truth for an order event, use a
    transactional outbox so the state change and event record commit in
    the same database transaction.
-   Document how Redis reservation state is linked to the durable
    order/outbox workflow. Do not imply Redis and PostgreSQL participate
    in one atomic transaction.
-   Events include event ID, aggregate/order ID, event type, schema
    version, creation timestamp, and correlation ID.
-   Failed messages are retried with limits and then routed to a
    dead-letter topic or an explicitly documented equivalent.

### 4.4 Availability and honesty

-   Health checks distinguish process health from dependency readiness.
-   Performance numbers must come from saved test output.
-   The UI must label simulated payments and simulated failure controls
    clearly.
-   A degraded dependency must produce a bounded, explainable
    response---not a false success.

------------------------------------------------------------------------

## 5. Recommended Architecture

### 5.1 Logical components

1.  **FlashSale Gateway**
    -   Spring Cloud Gateway.
    -   Routes public API requests.
    -   Applies request-size limits, request validation where
        appropriate, and Redis-backed rate limiting.
    -   Propagates request/correlation IDs.
    -   Does not contain business logic or access inventory directly.
2.  **Inventory Service**
    -   Spring Boot with Spring WebFlux.
    -   Owns sale configuration and reservation APIs.
    -   Uses Redis Lua scripts for atomic stock reservation, release,
        and idempotency checks.
    -   Stores reservation identifiers and TTL metadata.
    -   Exposes metrics for reserve attempts, successes, sold-out
        responses, and script errors.
    -   Documents Redis persistence, restart, and reconciliation
        assumptions.
3.  **Order Service**
    -   Validates purchase requests and idempotency keys.
    -   Coordinates the order lifecycle and reservation workflow.
    -   Persists durable order state and outbox events in PostgreSQL.
    -   Publishes events through an outbox relay to Kafka.
    -   Provides order-status and operator query APIs.
    -   Does not return "confirmed" merely because a request was
        accepted.
4.  **Payment & Fulfillment Worker**
    -   Consumes order-processing events from Kafka.
    -   Simulates payment outcomes; no real payment provider is used.
    -   Applies idempotent state transitions.
    -   Retries transient failures with bounded backoff.
    -   Routes exhausted failures to a dead-letter topic.
    -   Updates durable order state and creates follow-up outbox events
        as needed.
5.  **Observability**
    -   Spring Actuator and Micrometer metrics.
    -   Prometheus scrapes application metrics.
    -   Grafana dashboards visualize throughput, latency, error rate,
        JVM metrics, reservation outcomes, order states, and Kafka lag.
    -   Structured logs carry order ID and correlation ID.
6.  **Dashboard**
    -   A lightweight web UI for sale overview, order search, live
        metrics, and controlled demo actions.
    -   Dashboard is an operator/portfolio interface, not a source of
        business truth.
    -   All values come from APIs or are clearly labelled as
        illustrative demo data; do not silently fabricate live metrics.

### 5.2 Data and event flow

1.  Client submits a purchase request with a unique `Idempotency-Key`.
2.  Gateway applies rate limiting and routes the request.
3.  Order workflow validates the request and attempts to reserve
    inventory through the Inventory Service.
4.  Inventory Service executes an atomic Redis Lua script. It returns a
    reservation ID or a deterministic rejection.
5.  The order workflow persists a pending order and an outbox event in a
    PostgreSQL transaction, following the documented
    reservation-recovery protocol.
6.  An outbox relay publishes the event to Kafka.
7.  The worker consumes the event, simulates payment, and applies an
    idempotent database state transition.
8.  Client polls `GET /api/v1/orders/{orderId}` or uses a future
    optional push mechanism.
9.  Expiry/reconciliation jobs detect abandoned reservations, stale
    pending orders, and mismatches.

### 5.3 Important architecture decision: Redis + PostgreSQL + Kafka

There is no single atomic transaction spanning Redis, PostgreSQL, and
Kafka in this design. The implementation must select and document a
recovery protocol. At minimum:

-   Give every reservation a unique ID and bounded TTL.
-   Make reservation and release operations idempotent.
-   Persist order and outbox records transactionally in PostgreSQL.
-   Use retries and reconciliation to repair a crash between reservation
    and durable order creation.
-   Do not release inventory merely because an HTTP client timed out;
    first determine the durable order/reservation state.
-   Add fault-injection tests for crashes at each boundary.
-   Explain whether Redis is the fast reservation authority while
    PostgreSQL is the durable order authority, and how their state is
    reconciled after a Redis restart.

For a first implementation, prefer a simple documented saga with
compensating release over a complex distributed transaction. Do not
claim that this removes every possible failure window; demonstrate how
the selected protocol detects and repairs them.

### 5.4 Deployment topology

Docker Compose should run:

-   Gateway
-   Inventory Service
-   Order Service
-   Payment/Fulfillment Worker
-   PostgreSQL
-   Redis
-   Kafka in a local-development configuration
-   Prometheus
-   Grafana
-   Dashboard

Provide health checks, persistent volumes for stateful services,
explicit ports, resource notes, and a one-command startup path. Keep the
initial setup runnable on a developer laptop.

------------------------------------------------------------------------

## 6. Technology and Version Policy

Use Java 21 LTS as the default baseline unless the workspace or
compatible framework requirements justify another supported LTS release.
Use Maven multi-module structure. Select a stable Spring Boot release
and its compatible Spring Cloud release train from official
compatibility guidance. Pin dependency versions and commit the Maven
wrapper.

Recommended components:

-   Java 21 LTS
-   Spring Boot
-   Spring WebFlux
-   Spring Cloud Gateway
-   Spring for Apache Kafka
-   Apache Kafka
-   Redis
-   PostgreSQL
-   Flyway
-   Resilience4j
-   Spring Boot Actuator
-   Micrometer + Prometheus
-   Grafana
-   JUnit 5
-   Testcontainers
-   WireMock where useful
-   k6 for load testing
-   Docker Compose

Avoid mixing blocking JDBC calls into WebFlux event-loop threads. Either
isolate JDBC work on a bounded executor or select R2DBC after
documenting its operational and transactional trade-offs. Do not add
both JDBC and R2DBC without a clear reason.

------------------------------------------------------------------------

## 7. Functional Requirements

### FR-01: Sale management

-   Create a sale/item with a unique ID, name, total capacity, sale
    start/end time, and status.
-   Open, pause, and close a sale through an operator endpoint.
-   Reject invalid capacity, invalid time ranges, and illegal state
    transitions.
-   Sale configuration must be durable.
-   Initial version may expose operator APIs without full
    authentication, but clearly document this as local-demo-only.

### FR-02: Inventory reservation

-   Reserve a requested quantity, defaulting to one.
-   Use an atomic Redis Lua script to check stock, decrement available
    quantity, and create a reservation.
-   Reject a request when stock is insufficient.
-   Enforce a maximum quantity per request.
-   Support reservation expiry and explicit release.
-   Return stable error codes for sold out, sale closed, rate limited,
    invalid request, and temporary dependency failure.
-   Include reservation ID and expiry timestamp in successful
    reservation responses.

### FR-03: Idempotent purchase API

-   Require `Idempotency-Key` for purchase creation.
-   Scope keys to the appropriate shopper/client and sale.
-   Persist the association between key and order/result.
-   Repeating a request with the same key and same payload returns the
    original logical result.
-   Reusing a key with a different payload returns a conflict response.
-   Concurrent duplicate requests must not create duplicate reservations
    or orders.

### FR-04: Order lifecycle

Define and document a state machine. Suggested states:

`PENDING` → `PROCESSING` → `CONFIRMED`

Alternative terminal paths:

`PENDING` → `PAYMENT_FAILED`\
`PENDING` → `EXPIRED`\
`PROCESSING` → `PAYMENT_FAILED`\
`PENDING` / `PROCESSING` → `CANCELLED` only when cancellation rules
allow it.

-   Persist state transitions with timestamps.
-   Validate every transition.
-   Record failure codes and safe-to-display reasons.
-   Do not let client requests directly set arbitrary order states.
-   Make all compensation operations idempotent.

### FR-05: Event processing

-   Publish versioned order events to Kafka.
-   Use a stable key (such as order ID) where ordering for an order
    matters.
-   Configure consumer groups and bounded concurrency.
-   Handle duplicate deliveries.
-   Implement bounded retry and dead-letter handling.
-   Expose consumer lag and processing outcomes.
-   Document retention and local Kafka configuration.

### FR-06: Durable outbox

-   Store business changes and outbox rows in the same PostgreSQL
    transaction.
-   Relay pending outbox events to Kafka.
-   Track publication attempts and timestamps.
-   Make relay restart-safe.
-   Avoid marking an event published before broker acknowledgement.
-   Expect possible duplicate publication and make consumers idempotent.
-   Test process interruption before and after publication
    acknowledgement.

### FR-07: Payment simulation

-   Provide deterministic modes for success, decline, and transient
    failure.
-   Do not collect real payment information.
-   Use an idempotency identifier for simulated payment processing.
-   Apply timeouts and bounded retries to transient failures.
-   Release inventory on terminal payment failure according to the saga
    rules.
-   Display the simulation mode in the dashboard.

### FR-08: Rate limiting and resilience

-   Apply configurable per-client rate limits at the gateway.
-   Configure timeouts for downstream calls.
-   Use Resilience4j circuit breakers and bulkheads where they add
    value.
-   Provide bounded queues and reject excess work gracefully.
-   Avoid unlimited retries.
-   Return `429` for rate-limit rejection and appropriate `503`
    responses for temporary unavailability.
-   Make fallback responses truthful; never return a fake successful
    order.

### FR-09: Reservation expiry and reconciliation

-   Expire reservations after a configurable duration.
-   Use a scheduled worker or reliable queue-based expiry strategy.
-   Repeated expiry processing must not release stock twice.
-   Reconcile stale reservations, pending orders, and Redis/DB count
    differences.
-   Emit metrics and structured logs for detected mismatches.
-   Document operator recovery actions for unresolved mismatches.

### FR-10: Observability

Expose: - Request count and rate - Request latency histogram - HTTP
error count - Reservation attempts/successes/rejections - Current
available inventory - Order counts by state - Outbox backlog and
oldest-event age - Kafka consumer lag and processing failures - Retry
and dead-letter counts - Circuit-breaker state - JVM heap, garbage
collection, thread, and connection-pool metrics

Use correlation IDs across gateway, services, logs, and events. Avoid
high-cardinality labels such as raw order IDs in Prometheus metrics.

### FR-11: Dashboard

Provide: - Sale selector and sale status - Initial, available, reserved,
confirmed, failed, and expired inventory counts - Purchase request rate
and result breakdown - Order status lookup by order ID - Order lifecycle
timeline - Kafka/outbox processing status - API health and dependency
readiness - Latency and error-rate charts - A controlled load-test
launch/instructions panel - Explicit labels for simulated payment and
demo-only controls

The dashboard must be usable on desktop and tablet sizes. It must show
loading, empty, error, and stale-data states. Do not embed credentials
or connect directly to infrastructure from the browser.

### FR-12: Developer experience

-   `./mvnw test` runs appropriate automated tests.
-   `docker compose up --build` starts the documented local stack,
    subject to prerequisites.
-   Provide `.env.example`.
-   Provide database migrations.
-   Provide sample curl commands and seeded demo sale instructions.
-   Provide a troubleshooting guide.
-   Add CI checks for compilation, tests, and formatting/linting if
    configured.

------------------------------------------------------------------------

## 8. API Contract Baseline

All endpoints should use `/api/v1`. Exact DTO fields and error schema
must be written in `docs/api-contracts.md` before implementation.

### Public/client endpoints

-   `GET /api/v1/sales` --- list active or visible sales.
-   `GET /api/v1/sales/{saleId}` --- sale details and safe inventory
    summary.
-   `POST /api/v1/sales/{saleId}/reservations` --- reserve inventory.
-   `POST /api/v1/orders` --- create an order; requires
    `Idempotency-Key`.
-   `GET /api/v1/orders/{orderId}` --- query order status.
-   `POST /api/v1/orders/{orderId}/cancel` --- request cancellation if
    the current state allows it.

### Operator/demo endpoints

-   `POST /api/v1/admin/sales` --- create a sale.
-   `POST /api/v1/admin/sales/{saleId}/open` --- open a sale.
-   `POST /api/v1/admin/sales/{saleId}/pause` --- pause a sale.
-   `POST /api/v1/admin/sales/{saleId}/close` --- close a sale.
-   `GET /api/v1/admin/metrics/summary` --- aggregate operational
    summary.
-   `GET /api/v1/admin/orders` --- paginated order search.
-   `POST /api/v1/admin/demo/payment-mode` --- set a local simulated
    payment mode; disabled outside local demo profile.
-   `POST /api/v1/admin/demo/failure/{scenario}` --- enable a documented
    failure scenario only in local demo mode.

### Operational endpoints

-   Spring Actuator health/readiness/liveness endpoints.
-   Prometheus scrape endpoint.
-   Optional API documentation via OpenAPI.

### HTTP behavior

-   `202 Accepted` means a durable order request has been accepted for
    asynchronous processing, not that a ticket is confirmed.
-   `200 OK` returns a successful read or idempotent replay.
-   `201 Created` may be used for synchronously created resources where
    appropriate.
-   `400 Bad Request` for malformed/invalid input.
-   `409 Conflict` for idempotency-key payload mismatch or illegal state
    conflict.
-   `429 Too Many Requests` for rate limiting.
-   `503 Service Unavailable` for temporary dependency or overload
    conditions.
-   `404 Not Found` for unknown resource IDs.
-   Return a consistent error body containing timestamp, status, code,
    message, path, and correlation ID. Do not expose stack traces or
    secrets.

------------------------------------------------------------------------

## 9. Data Model Baseline

Use Flyway migrations and explicit constraints. Final schema should be
documented in `docs/data-model.md`.

### `sales`

-   `id` UUID primary key
-   `name`
-   `total_capacity` integer, check \>= 0
-   `status` (DRAFT, OPEN, PAUSED, CLOSED)
-   `starts_at`, `ends_at`
-   `created_at`, `updated_at`
-   Version field if optimistic concurrency is used

### `orders`

-   `id` UUID primary key
-   `sale_id` foreign key
-   `client_id` or demo shopper identifier
-   `idempotency_key`
-   `request_fingerprint`
-   `reservation_id`
-   `quantity`
-   `status`
-   `payment_reference` for simulated payment only
-   `failure_code`
-   `created_at`, `updated_at`
-   Unique constraint on the chosen idempotency scope

### `reservations` (durable tracking/reconciliation)

-   `id` UUID primary key
-   `sale_id`
-   `order_id` when associated
-   `quantity`
-   `status` (ACTIVE, COMMITTED, RELEASED, EXPIRED)
-   `expires_at`
-   `created_at`, `updated_at`
-   Unique constraints that prevent duplicate release/association
    operations

### `outbox_events`

-   `id` UUID primary key
-   `aggregate_type`
-   `aggregate_id`
-   `event_type`
-   `schema_version`
-   `payload` JSON/JSONB
-   `created_at`
-   `published_at`
-   `attempt_count`
-   `last_error` with sensitive details excluded

### `processed_events`

-   `consumer_name`
-   `event_id`
-   `processed_at`
-   Unique constraint on `(consumer_name, event_id)`

### Redis keys

Document a namespaced key strategy, sale stock key, reservation key,
idempotency key, and TTL policy. Lua scripts must validate all inputs
and use a single consistent key-slot strategy if Redis Cluster
compatibility is claimed. A single Redis instance in Docker Compose is
sufficient for the first version; do not claim Redis Cluster behavior
until it is actually tested.

------------------------------------------------------------------------

## 10. Non-Functional Requirements

### Correctness

-   Zero negative inventory in all correctness tests.
-   No more confirmed units than sale capacity.
-   Duplicate request tests yield one logical order/reservation.
-   Duplicate event tests yield one logical business effect.
-   Compensation and expiry can safely run more than once.

### Performance

-   Establish a baseline on documented local hardware.
-   Measure throughput, p50/p95/p99 latency, error rate, CPU, memory,
    and Kafka lag.
-   Run at least 3 repeatable load-test iterations for published
    benchmark results.
-   Separate warm-up from measured duration.
-   Test multiple concurrency levels and stock ratios.
-   No predetermined throughput guarantee. Set targets after measuring
    the baseline.

### Reliability

-   Services restart without corrupting durable order state.
-   Outbox relay resumes after restart.
-   Consumers recover from redelivery.
-   A temporary dependency outage does not cause unbounded
    thread/connection growth.
-   Reconciliation identifies deliberately introduced inconsistent
    states.

### Security (local demo scope)

-   Validate all input.
-   Restrict failure injection and admin endpoints to local demo
    profile.
-   Keep secrets out of source control and logs.
-   Use safe CORS defaults.
-   Avoid exposing Actuator environment/configuration endpoints
    publicly.
-   Clearly document that unauthenticated operator endpoints are not
    internet-safe.

### Maintainability

-   Layered, testable code; small classes and clear interfaces.
-   No business logic inside controllers.
-   Shared module contains only genuinely shared contracts/utilities.
-   No circular module dependencies.
-   Architecture decisions recorded in ADRs.
-   Code and docs updated together when API or event contracts change.

------------------------------------------------------------------------

## 11. Implementation Stages and Exit Gates

Antigravity must complete each stage, run the specified checks,
summarize changed files and evidence, and stop for review before
proceeding. Never implement all stages in a single generation pass.

### Stage 0 --- Repository discovery and design

**Deliverables** - `README.md` skeleton - `docs/architecture.md` -
`docs/implementation-plan.md` - `docs/api-contracts.md` -
`docs/data-model.md` - `docs/failure-scenarios.md` -
`docs/adr/ADR-001-architecture.md` - Mermaid diagrams for request flow,
order state machine, outbox flow, and reservation compensation

**Tasks** - Inspect existing files and preserve useful work. - Confirm
module boundaries, versions, ports, configuration, event schemas, and
local infrastructure. - Identify every crash window in reservation →
order persistence → outbox → Kafka → worker processing. - Document the
selected saga/reconciliation protocol.

**Exit gate** A human can review the design and understand the
failure-recovery plan before application code is generated. No business
implementation yet.

### Stage 1 --- Build scaffold and local infrastructure

**Deliverables** - Maven multi-module repository - Maven wrapper -
Compatible dependency management - Docker Compose for PostgreSQL, Redis,
and Kafka - Health checks and persistent volumes - `.env.example` -
Basic CI workflow if appropriate - Configuration profiles for local
development and tests

**Tests** - Maven reactor compiles. - Infrastructure starts from a clean
checkout. - Each dependency is reachable from the relevant container. -
No credentials are hardcoded.

**Exit gate** A documented command starts dependencies and all modules
compile.

### Stage 2 --- Sale management and atomic inventory

**Deliverables** - Sale schema and Flyway migrations - Sale CRUD/state
transition endpoints - Redis Lua scripts for reserve/release -
Reservation IDs, TTL, and key naming documentation - Inventory service
unit and integration tests

**Tests** - Concurrent requests cannot reserve more than capacity. -
Stock never becomes negative. - Insufficient stock is rejected
deterministically. - Repeated release does not increase stock twice. -
Invalid quantity and closed sale requests are rejected. - Script errors
do not produce false success.

**Exit gate** Concurrency tests pass and show the invariant from both
Redis state and service responses.

### Stage 3 --- Idempotent order API and durable order state

**Deliverables** - Order state machine - Order and reservation tracking
tables - Create/query/cancel APIs as appropriate - Idempotency-key
validation and request fingerprinting - Consistent API error model -
State-transition audit timestamps

**Tests** - Same key + same payload returns the same logical result. -
Same key + different payload returns `409`. - Concurrent duplicate
submissions create one logical order. - Illegal state transitions are
rejected. - Database unique constraints enforce idempotency under races.

**Exit gate** Order and idempotency tests pass against PostgreSQL using
integration tests.

### Stage 4 --- Transactional outbox and Kafka event backbone

**Deliverables** - Versioned event contracts - Outbox table and relay
worker - Kafka topics and keys - Consumer group configuration - Retry
policy and dead-letter topic - Event schema documentation

**Tests** - Outbox rows survive application restart. - Relay republishes
pending events. - Duplicate publication does not duplicate business
effects. - Consumer restarts safely. - Broker outage is observable and
does not silently drop durable outbox events. - Ordering assumptions are
documented and tested where required.

**Exit gate** An accepted durable order can be processed asynchronously
after a broker interruption, according to the documented recovery
protocol.

### Stage 5 --- Payment simulation, compensation, and expiry

**Deliverables** - Deterministic simulated payment adapter - Success,
decline, and transient-error modes - Reservation-expiry process -
Idempotent release/compensation - Reconciliation worker - Failure and
recovery runbook

**Tests** - Payment decline reaches a terminal failure state. - Stock is
released exactly once logically. - Repeated failure/expiry events do not
double-release. - Expired reservations are not later confirmed
incorrectly. - Crash injection at each workflow boundary is detected and
repaired. - Reconciliation reports mismatches and applies only safe,
documented repairs.

**Exit gate** All lifecycle tests pass and recovery outcomes are visible
in logs and durable records.

### Stage 6 --- Gateway, rate limiting, and resilience

**Deliverables** - Spring Cloud Gateway routes - Redis-backed rate
limiter - Timeouts and bounded connection pools - Resilience4j circuit
breakers/bulkheads where justified - Standard correlation IDs and error
responses

**Tests** - Excess traffic receives `429`. - Unavailable dependencies
result in bounded failures, not false success. - Circuit breaker opens
and recovers under a controlled test. - No unbounded retries or thread
growth. - Correlation IDs are present in logs across service boundaries.

**Exit gate** Traffic control and dependency failure tests pass.

### Stage 7 --- Observability and dashboard

**Deliverables** - Actuator and Micrometer instrumentation - Prometheus
configuration - Grafana dashboard JSON/provisioning - Operator
dashboard - Order search/status timeline - Live health and metrics
panels - Loading/error/stale-data states

**Tests** - Prometheus scrapes every intended service. - Dashboard
values match API/Prometheus data. - Error states render correctly when
dependencies are stopped. - Metrics avoid high-cardinality IDs. - Demo
controls cannot be used outside the local demo profile.

**Exit gate** A reviewer can start the stack, run a scenario, and see
its effects through the dashboard.

### Stage 8 --- Load testing and failure demonstrations

**Deliverables** - k6 scripts for normal, burst, contention, duplicate,
and rate-limited traffic - Reproducible test instructions - Benchmark
report template and actual results - Failure-injection runbook - Saved
correctness assertions and output artifacts

**Scenarios** 1. 10,000 requests competing for 100 units. 2. Same
idempotency key repeated concurrently. 3. Mixed valid and invalid
requests. 4. Kafka paused or unavailable. 5. Worker killed and
restarted. 6. PostgreSQL temporarily unavailable. 7. Payment declines
and reservation expirations. 8. Gateway rate limit exceeded.

**Metrics** - Requests/sec - p50, p95, p99 latency - Error and rejection
rate - CPU and memory - Reservation success/rejection counts - Confirmed
orders and final inventory - Outbox backlog and oldest event age - Kafka
consumer lag - Recovery duration

**Exit gate** Results are repeatable, correctness assertions pass, and
the report includes hardware/software configuration and limitations. No
benchmark values are fabricated.

### Stage 9 --- Hardening and portfolio release

**Deliverables** - Complete README and architecture diagram -
Setup/troubleshooting guide - API examples - Sample `.env.example` -
Architecture Decision Records - CI verification - Demo script and short
screen-recording plan - Known limitations and future roadmap

**Exit gate** A reviewer can clone the repository, follow the
instructions, start the stack, run tests, reproduce the key demo, and
understand the design trade-offs.

------------------------------------------------------------------------

## 12. Test Strategy

### Unit tests

Test Lua script wrappers, validation, state-machine transitions, request
fingerprinting, retry decisions, and compensation rules.

### Integration tests

Use Testcontainers where practical for PostgreSQL, Redis, and Kafka.
Avoid relying only on mocks for concurrency or transaction guarantees.

### Concurrency tests

Use synchronized starts and repeated runs. Assert final stock,
successful reservations, and unique logical orders. Tests must not rely
only on the number of HTTP `200` responses.

### Fault-injection tests

Terminate or pause components at documented workflow boundaries. Verify
durable state after restart and run reconciliation. Include a test where
an outbox event is published but the relay fails before recording the
acknowledgement, causing duplicate publication.

### Load tests

Use k6 or an equivalent repeatable tool. Separate test traffic from
correctness assertions. Record environment details and avoid running
unbounded tests on a laptop.

### Regression policy

Every fixed correctness bug receives a regression test. Stage exit gates
must run before Antigravity starts the next stage.

------------------------------------------------------------------------

## 13. UX and Demo Requirements

The demo should tell a clear story:

1.  Display a sale with 100 remaining units.
2.  Launch a burst of concurrent purchase attempts.
3.  Show accepted reservations and sold-out rejections.
4.  Show order states progressing asynchronously.
5.  Show final counts and verify no overselling.
6.  Repeat a request with the same idempotency key and show that it does
    not consume another unit.
7.  Enable a simulated payment decline and show the compensation
    lifecycle.
8.  Interrupt Kafka or the worker, then restart it and show recovery.
9.  Open Grafana to inspect latency, throughput, consumer lag, and error
    metrics.

All simulated behavior must be labelled. Dashboard animations must not
substitute for real backend results. A load-test panel may provide
instructions or launch a local script, but a browser must not be allowed
to execute arbitrary shell commands.

------------------------------------------------------------------------

## 14. Antigravity Agent Instructions

Paste these operating instructions into Antigravity before
implementation:

> You are the senior engineer implementing FlashSale Engine from this
> PRD. Read the entire PRD and inspect the existing workspace first.
>
> Work in stages. Implement only the current stage and its exit gate. Do
> not begin the next stage without user approval.
>
> Before editing, summarize the files you intend to create or change.
> Preserve existing useful work. Prefer small, reviewable changes.
>
> After implementation, run the relevant build, unit tests, integration
> tests, and static checks that are available. If a test cannot run
> because a dependency or environment is missing, state that precisely;
> do not claim it passed.
>
> Review your diff for security issues, blocking work on reactive event
> loops, missing database constraints, race conditions, duplicate-event
> behavior, retry loops, secrets, and unhandled crash windows.
>
> Update the docs and tests with every contract change. Never fabricate
> API responses, benchmark results, test outcomes, or
> production-readiness claims.
>
> For each stage report: (1) implemented features, (2) files changed,
> (3) commands executed, (4) test results, (5) known limitations, (6)
> unresolved risks, and (7) the exact next step. Stop at the stage exit
> gate and wait for approval.

### Required first prompt

> Begin Stage 0 only. Inspect the workspace, then produce the
> architecture, module boundaries, API contracts, data model, event
> schemas, state machine, failure matrix, and implementation plan.
> Include a Mermaid diagram of the main request path and identify crash
> windows across Redis, PostgreSQL, and Kafka. Do not generate
> application implementation code yet. Stop and ask for review.

### Required stage prompt template

> Implement Stage \[N\] from `docs/PRD.md` only. Review the stage's
> deliverables and exit gate. First list the files to change. Make the
> smallest coherent implementation, add tests for every requirement, run
> the relevant checks, update docs, review the diff, report evidence and
> limitations, and stop for review. Do not start Stage \[N+1\].

------------------------------------------------------------------------

## 15. Repository Structure

Recommended target structure:

``` text
flashsale-engine/
├── pom.xml
├── mvnw
├── mvnw.cmd
├── docker-compose.yml
├── .env.example
├── README.md
├── docs/
│   ├── PRD.md
│   ├── architecture.md
│   ├── implementation-plan.md
│   ├── api-contracts.md
│   ├── data-model.md
│   ├── event-schemas.md
│   ├── failure-scenarios.md
│   ├── testing-and-benchmarks.md
│   ├── runbooks/
│   └── adr/
├── flashsale-common/
├── flashsale-gateway/
├── inventory-service/
├── order-service/
├── payment-fulfillment-service/
├── dashboard/
├── observability/
│   ├── prometheus/
│   └── grafana/
├── load-test/
│   └── k6/
└── scripts/
```

Keep the shared module small. Avoid placing persistence entities or
service-specific business logic in a shared library merely to reduce
code duplication.

------------------------------------------------------------------------

## 16. Definition of Done

The project is ready for a portfolio release only when:

-   [ ] All modules build from a clean checkout.
-   [ ] The local infrastructure can be started using documented
    commands.
-   [ ] Database migrations apply cleanly.
-   [ ] Inventory never goes negative in concurrency tests.
-   [ ] Confirmed quantity never exceeds sale capacity.
-   [ ] Idempotency is enforced under concurrent duplicate submissions.
-   [ ] Outbox relay and Kafka consumers recover from restart and
    duplicate delivery.
-   [ ] Payment failure, reservation expiry, and compensation are
    tested.
-   [ ] Reconciliation can detect and safely handle documented
    inconsistencies.
-   [ ] Rate limiting, timeouts, and overload behavior are tested.
-   [ ] Metrics and logs make the demo observable.
-   [ ] Dashboard data is sourced from the running application.
-   [ ] Load-test results include environment and methodology.
-   [ ] No unverified performance claims are in the README or resume
    description.
-   [ ] Secrets and demo-only failure controls are handled safely.
-   [ ] Documentation includes architecture trade-offs and known
    limitations.

------------------------------------------------------------------------

## 17. Risks and Decisions to Resolve During Stage 0

1.  **Redis as reservation authority:** define persistence settings,
    restart behavior, and the reconciliation source of truth.
2.  **Reservation/order creation boundary:** select a saga protocol and
    test crashes before and after durable order persistence.
3.  **Reactive stack versus blocking persistence:** isolate JDBC or
    choose R2DBC deliberately.
4.  **Kafka local configuration:** use a simple supported single-node
    development setup; do not imply production broker redundancy.
5.  **Dashboard scope:** keep the UI focused on demonstrating
    correctness and operations, not unrelated commerce features.
6.  **Load-test limits:** calibrate workloads to the machine and report
    the test environment.
7.  **Security:** protect demo/admin operations and keep local-only
    failure injection disabled in non-demo profiles.

Stage 0 must document decisions, assumptions, alternatives, and
consequences. Where a decision is not yet justified, record it as open
instead of silently guessing.

------------------------------------------------------------------------

## 18. Final Product Outcome

The completed FlashSale Engine should be a reproducible engineering
demonstration, not merely a collection of microservices. Its strongest
evidence will be:

-   a concurrency test proving inventory invariants;
-   an idempotency test proving retries do not duplicate purchases;
-   an outbox/Kafka recovery test proving accepted work is eventually
    processed under the stated assumptions;
-   a compensation test proving failed purchases release inventory
    safely;
-   a load-test report with measured latency and throughput;
-   and an observability dashboard that shows the real behavior of the
    running system.

The project should prioritize correctness, transparent trade-offs, and
repeatable evidence over the number of technologies or microservices.
