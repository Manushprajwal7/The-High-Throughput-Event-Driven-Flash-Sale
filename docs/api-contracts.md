# FlashSale Engine --- API Contract Baseline

All endpoints are versioned with the prefix `/api/v1`. This document defines the exact HTTP request/response schemas, DTOs, headers, status codes, and error formats.

---

## 1. Global Headers & Conventions

### 1.1 HTTP Headers
* **`X-Correlation-Id`** (Optional on ingress, Required on egress): String (UUID). Tracing identifier propagated across gateway, services, outbox events, and structured logs.
* **`Idempotency-Key`** (Required for `POST /api/v1/orders`): String (max 128 chars). Client-generated unique token scoped per client and sale to guarantee idempotent processing.
* **`Content-Type`**: `application/json`

### 1.2 Standard Error Response Model
All non-2xx responses adhere to the following schema:

```json
{
  "timestamp": "2026-10-13T10:15:30.123Z",
  "status": 409,
  "error": "Conflict",
  "code": "SOLD_OUT",
  "message": "Available stock is exhausted for this sale.",
  "path": "/api/v1/orders",
  "correlationId": "8f2d5d88-3482-411a-85b3-3a131b0c9502",
  "details": null
}
```

#### Standard Error Codes:
* `VALIDATION_FAILED` (400): Request payload validation failure.
* `RESOURCE_NOT_FOUND` (404): Sale or Order identifier does not exist.
* `SOLD_OUT` (409): Sale capacity is fully exhausted.
* `IDEMPOTENCY_CONFLICT` (409): Same idempotency key re-used with an altered payload.
* `ILLEGAL_STATE_TRANSITION` (409): Order or sale is in an invalid state for the requested action.
* `RATE_LIMIT_EXCEEDED` (429): Token bucket limit exceeded at the gateway.
* `SERVICE_DEGRADED` (503): Downstream service unavailable or circuit breaker open.

---

## 2. Public / Client Endpoints

### 2.1 List Sales
* **Method:** `GET`
* **Path:** `/api/v1/sales`
* **Description:** Retrieve all visible or active flash sales.
* **Status Code:** `200 OK`
* **Response Body:**
```json
[
  {
    "saleId": "c379a29e-297c-473d-8e68-0744a530733a",
    "name": "PlayStation 5 Pro Flash Sale",
    "totalCapacity": 100,
    "availableStock": 42,
    "status": "OPEN",
    "startsAt": "2026-10-13T10:00:00Z",
    "endsAt": "2026-10-13T11:00:00Z"
  }
]
```

---

### 2.2 Get Sale Details
* **Method:** `GET`
* **Path:** `/api/v1/sales/{saleId}`
* **Description:** Query inventory details for a specific sale.
* **Status Codes:** `200 OK`, `404 Not Found`
* **Response Body:**
```json
{
  "saleId": "c379a29e-297c-473d-8e68-0744a530733a",
  "name": "PlayStation 5 Pro Flash Sale",
  "totalCapacity": 100,
  "availableStock": 42,
  "status": "OPEN",
  "startsAt": "2026-10-13T10:00:00Z",
  "endsAt": "2026-10-13T11:00:00Z",
  "createdAt": "2026-10-13T09:30:00Z"
}
```

---

### 2.3 Reserve Inventory (Direct / Internal)
* **Method:** `POST`
* **Path:** `/api/v1/sales/{saleId}/reservations`
* **Description:** Atomically decrements available stock and creates a reservation in Redis.
* **Request Body:**
```json
{
  "quantity": 1,
  "shopperId": "user_98472",
  "reservationTtlSeconds": 120
}
```
* **Status Codes:**
  * `201 Created`: Successfully reserved.
  * `409 Conflict`: Sold out or sale not active (`SOLD_OUT`, `SALE_NOT_OPEN`).
  * `400 Bad Request`: Invalid quantity.
* **Response Body (201 Created):**
```json
{
  "reservationId": "a90bb154-159d-4766-b33a-d68f2caea100",
  "saleId": "c379a29e-297c-473d-8e68-0744a530733a",
  "quantity": 1,
  "expiresAt": "2026-10-13T10:17:30.123Z"
}
```

---

### 2.4 Create Purchase Order
* **Method:** `POST`
* **Path:** `/api/v1/orders`
* **Required Header:** `Idempotency-Key: <unique-string>`
* **Description:** Primary entry point for shoppers submitting an order. Reserves inventory and durably records a pending order with outbox event.
* **Request Body:**
```json
{
  "saleId": "c379a29e-297c-473d-8e68-0744a530733a",
  "shopperId": "user_98472",
  "quantity": 1
}
```
* **Status Codes:**
  * `202 Accepted`: Order accepted for asynchronous payment processing.
  * `200 OK`: Idempotent replay of an existing order submission.
  * `409 Conflict`: Sold out (`SOLD_OUT`) or idempotency payload mismatch (`IDEMPOTENCY_CONFLICT`).
  * `429 Too Many Requests`: Rate limit throttled.
* **Response Body (202 Accepted / 200 OK):**
```json
{
  "orderId": "e1f14841-f421-4f11-9f20-dc4166258aa4",
  "saleId": "c379a29e-297c-473d-8e68-0744a530733a",
  "shopperId": "user_98472",
  "quantity": 1,
  "status": "PENDING",
  "reservationId": "a90bb154-159d-4766-b33a-d68f2caea100",
  "createdAt": "2026-10-13T10:15:30.123Z",
  "message": "Order accepted for processing."
}
```

---

### 2.5 Query Order Status
* **Method:** `GET`
* **Path:** `/api/v1/orders/{orderId}`
* **Description:** Check order progress and terminal status.
* **Status Codes:** `200 OK`, `404 Not Found`
* **Response Body:**
```json
{
  "orderId": "e1f14841-f421-4f11-9f20-dc4166258aa4",
  "saleId": "c379a29e-297c-473d-8e68-0744a530733a",
  "shopperId": "user_98472",
  "quantity": 1,
  "status": "CONFIRMED",
  "paymentReference": "sim_pay_8a72b0c1",
  "failureCode": null,
  "createdAt": "2026-10-13T10:15:30.123Z",
  "updatedAt": "2026-10-13T10:15:31.050Z"
}
```

---

### 2.6 Cancel Order
* **Method:** `POST`
* **Path:** `/api/v1/orders/{orderId}/cancel`
* **Description:** Request explicit order cancellation if still in non-terminal state.
* **Status Codes:** `200 OK`, `409 Conflict` (if already confirmed)
* **Response Body:**
```json
{
  "orderId": "e1f14841-f421-4f11-9f20-dc4166258aa4",
  "status": "CANCELLED",
  "message": "Order successfully cancelled and stock released."
}
```

---

## 3. Operator & Demo Endpoints (`/api/v1/admin`)

*Note: Demo and failure-injection endpoints are strictly restricted to the `demo` or `local` profile.*

### 3.1 Create Sale
* **Method:** `POST`
* **Path:** `/api/v1/admin/sales`
* **Request Body:**
```json
{
  "name": "PlayStation 5 Pro Flash Sale",
  "totalCapacity": 100,
  "startsAt": "2026-10-13T10:00:00Z",
  "endsAt": "2026-10-13T11:00:00Z"
}
```
* **Status Code:** `201 Created`
* **Response Body:** Full sale object.

### 3.2 Sale State Controls
* `POST /api/v1/admin/sales/{saleId}/open` -> Status `200 OK` (Sale status changed to `OPEN`)
* `POST /api/v1/admin/sales/{saleId}/pause` -> Status `200 OK` (Sale status changed to `PAUSED`)
* `POST /api/v1/admin/sales/{saleId}/close` -> Status `200 OK` (Sale status changed to `CLOSED`)

---

### 3.3 Set Demo Payment Simulation Mode
* **Method:** `POST`
* **Path:** `/api/v1/admin/demo/payment-mode`
* **Request Body:**
```json
{
  "mode": "ALWAYS_SUCCESS"
}
```
* **Supported Modes:** `ALWAYS_SUCCESS`, `ALWAYS_DECLINE`, `TRANSIENT_FAILURE_THEN_SUCCESS`, `RANDOM_90_PERCENT`.
* **Status Code:** `200 OK`

---

### 3.4 Inject Demo Failure Scenario
* **Method:** `POST`
* **Path:** `/api/v1/admin/demo/failure/{scenario}`
* **Scenarios:**
  * `pause-outbox-relay`
  * `resume-outbox-relay`
  * `simulate-broker-lag`
  * `trigger-reconciliation`
* **Status Code:** `200 OK`

---

## 4. Internal Service-to-Service Endpoints

These endpoints are used for inter-service compensation and finalization:

### 4.1 Commit Reservation
* **Method:** `POST`
* **Path:** `/api/v1/sales/{saleId}/reservations/{reservationId}/commit`
* **Description:** Called when payment succeeds to convert active reservation to permanently consumed capacity in Redis.
* **Status Code:** `200 OK`

### 4.2 Release Reservation (Compensating Action)
* **Method:** `POST`
* **Path:** `/api/v1/sales/{saleId}/reservations/{reservationId}/release`
* **Description:** Called when payment declines or fails terminally to return stock back to Redis available counter.
* **Status Code:** `200 OK`
