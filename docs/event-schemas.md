# FlashSale Engine --- Kafka Event Schemas & Messaging Baseline

This document defines the Apache Kafka topics, partition strategies, event envelopes, versioning semantics, and retry/dead-letter queue (DLQ) rules.

---

## 1. Topic Topology & Partitioning

| Topic Name | Partitions (Local) | Retention | Purpose | Key Strategy |
| :--- | :---: | :---: | :--- | :--- |
| `flashsale.orders.v1` | 4 | 7 days | Primary order lifecycle events (`ORDER_RESERVED`, `ORDER_CONFIRMED`, `ORDER_FAILED`). | `aggregateId` (`orderId`) |
| `flashsale.orders.dlq.v1` | 4 | 14 days | Unrecoverable failed events after retry exhaustion. | `aggregateId` (`orderId`) |

### Partition Key Policy:
* **Strict Per-Order Ordering:** Every event published to `flashsale.orders.v1` MUST set the Kafka message key to the `orderId` (UUID string).
* This guarantees that all lifecycle state transitions for a single order land on the exact same Kafka partition and are processed strictly sequentially by consumers.

---

## 2. Standard Event Envelope

Every event produced by the Outbox Relay follows a unified versioned envelope:

```json
{
  "eventId": "18b96582-7484-486d-9657-611ff7c2f82c",
  "aggregateType": "ORDER",
  "aggregateId": "e1f14841-f421-4f11-9f20-dc4166258aa4",
  "eventType": "ORDER_RESERVED",
  "schemaVersion": 1,
  "timestamp": "2026-10-13T10:15:30.150Z",
  "correlationId": "8f2d5d88-3482-411a-85b3-3a131b0c9502",
  "payload": { ... }
}
```

---

## 3. Event Payloads (Schema Version 1)

### 3.1 Event: `ORDER_RESERVED`
Emitted by `order-service` when stock is reserved in Redis and the order is persisted with status `PENDING`. Consumed by `payment-fulfillment-service` to simulate payment.

```json
{
  "eventId": "18b96582-7484-486d-9657-611ff7c2f82c",
  "aggregateType": "ORDER",
  "aggregateId": "e1f14841-f421-4f11-9f20-dc4166258aa4",
  "eventType": "ORDER_RESERVED",
  "schemaVersion": 1,
  "timestamp": "2026-10-13T10:15:30.150Z",
  "correlationId": "8f2d5d88-3482-411a-85b3-3a131b0c9502",
  "payload": {
    "orderId": "e1f14841-f421-4f11-9f20-dc4166258aa4",
    "saleId": "c379a29e-297c-473d-8e68-0744a530733a",
    "shopperId": "user_98472",
    "quantity": 1,
    "reservationId": "a90bb154-159d-4766-b33a-d68f2caea100",
    "expiresAt": "2026-10-13T10:17:30.123Z"
  }
}
```

---

### 3.2 Event: `ORDER_CONFIRMED`
Emitted when the payment worker successfully processes payment and commits the order.

```json
{
  "eventId": "7a3536bf-1e9d-4ee8-b2a8-fce5e6f65809",
  "aggregateType": "ORDER",
  "aggregateId": "e1f14841-f421-4f11-9f20-dc4166258aa4",
  "eventType": "ORDER_CONFIRMED",
  "schemaVersion": 1,
  "timestamp": "2026-10-13T10:15:31.050Z",
  "correlationId": "8f2d5d88-3482-411a-85b3-3a131b0c9502",
  "payload": {
    "orderId": "e1f14841-f421-4f11-9f20-dc4166258aa4",
    "saleId": "c379a29e-297c-473d-8e68-0744a530733a",
    "reservationId": "a90bb154-159d-4766-b33a-d68f2caea100",
    "paymentReference": "sim_pay_8a72b0c1",
    "confirmedAt": "2026-10-13T10:15:31.045Z"
  }
}
```

---

### 3.3 Event: `ORDER_FAILED`
Emitted when simulated payment declines or fails terminally.

```json
{
  "eventId": "f5168e37-9759-45ce-844c-2234cf7f28ab",
  "aggregateType": "ORDER",
  "aggregateId": "e1f14841-f421-4f11-9f20-dc4166258aa4",
  "eventType": "ORDER_FAILED",
  "schemaVersion": 1,
  "timestamp": "2026-10-13T10:15:31.200Z",
  "correlationId": "8f2d5d88-3482-411a-85b3-3a131b0c9502",
  "payload": {
    "orderId": "e1f14841-f421-4f11-9f20-dc4166258aa4",
    "saleId": "c379a29e-297c-473d-8e68-0744a530733a",
    "reservationId": "a90bb154-159d-4766-b33a-d68f2caea100",
    "failureCode": "PAYMENT_DECLINED",
    "reason": "Simulated card authorization failed",
    "failedAt": "2026-10-13T10:15:31.195Z"
  }
}
```

---

## 4. Consumer Semantics & Idempotency Rules

1. **At-Least-Once Delivery Handling:** Kafka consumers must assume events may be received multiple times. Consumers must query `processed_events` in PostgreSQL using `(consumer_name, event_id)` prior to executing side effects.
2. **Atomic State & Deduplication:** When processing an event, the state transition in `orders` and the insertion in `processed_events` occur in a single database transaction.
3. **Transient Failure Retry:**
   * Transient network or database errors trigger a local retry with exponential backoff: initial interval 500ms, multiplier 2.0, max attempts: 3.
4. **Dead-Letter Queue Routing:**
   * If a message repeatedly causes unrecoverable deserialization or business exceptions after 3 attempts, it is routed to `flashsale.orders.dlq.v1` with failure headers (`x-exception-message`, `x-original-topic`, `x-failure-timestamp`).
   * The consumer offset is committed to prevent partition blocking.
