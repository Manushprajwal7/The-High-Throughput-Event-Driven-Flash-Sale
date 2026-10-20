# FlashSale Engine --- Developer Troubleshooting Runbook

This guide covers common development, infrastructure, and runtime issues encountered when operating the FlashSale Engine locally.

---

## 1. Port Conflicts

The platform uses several standard local ports:

| Service / Infrastructure | Default Port | Environment Variable Override |
| :--- | :--- | :--- |
| **FlashSale Gateway** | `8080` | `GATEWAY_PORT` |
| **Order Service** | `8081` | `ORDER_SERVICE_PORT` |
| **Inventory Service** | `8082` | `INVENTORY_SERVICE_PORT` |
| **Payment Fulfillment** | `8083` | `PAYMENT_SERVICE_PORT` |
| **PostgreSQL** | `5432` | `POSTGRES_PORT` |
| **Redis** | `6379` | `REDIS_PORT` |
| **Apache Kafka** | `9092` | `KAFKA_PORT` |
| **Prometheus** | `9090` | `PROMETHEUS_PORT` |
| **Grafana** | `3000` | `GRAFANA_PORT` |

### Symptom: `BindException: Address already in use`
**Diagnosis & Resolution:**
```powershell
# Identify process holding the port (e.g. 5432):
Get-NetTCPConnection -LocalPort 5432 | Select-Object OwningProcess

# Or modify .env to assign an alternate port:
POSTGRES_PORT=5433
REDIS_PORT=6380
```

---

## 2. Docker & Container Issues

### Symptom: Kafka fails to start (`CLUSTER_ID` error or formatting failure)
**Diagnosis:** Stale Kafka metadata logs in the mounted Docker volume `flashsale_kafka_data`.
**Resolution:**
```powershell
docker compose down -v
docker compose up -d kafka
```

### Symptom: PostgreSQL container unhealthy
**Diagnosis:** Check PostgreSQL logs for authentication or collation issues:
```powershell
docker logs flashsale-postgres
```
Verify that `.env` password matches Spring datasource configuration:
`POSTGRES_PASSWORD=flashsale_secret`

---

## 3. Database & Flyway Migration Issues

### Symptom: `FlywayException: Validate failed: Migrations have failed validation`
**Diagnosis:** A migration file (`V1__init_schema.sql`) was modified after being applied to the local database, resulting in a checksum mismatch.
**Resolution:**
In development, recreate the database cleanly:
```powershell
docker compose down -v
docker compose up -d postgres
```

---

## 4. Kafka Consumer Lag & Message Stuck in Outbox

### Symptom: Order remains in `PENDING` status indefinitely
**Diagnosis Flow:**
1. **Check Outbox Table:**
   ```sql
   SELECT id, aggregate_id, event_type, attempt_count, last_error 
   FROM outbox_events WHERE published_at IS NULL;
   ```
   If `attempt_count > 0` with `last_error` populated, check Kafka connectivity from `order-service`.
2. **Check Dead-Letter Queue:**
   If `attempt_count >= 5`, the event was automatically routed to `flashsale.orders.dlq.v1` to prevent head-of-line blocking.
3. **Verify Kafka Broker Connectivity:**
   Ensure `KAFKA_BOOTSTRAP_SERVERS` is set to `localhost:9092` (when running services on the host machine).

---

## 5. Circuit Breaker Stuck in `OPEN` State

### Symptom: Gateway returns `503 Service Unavailable (SERVICE_DEGRADED)`
**Diagnosis:**
Resilience4j tripped the circuit breaker after downstream service errors exceeded the 50% threshold.
**Resolution:**
1. Inspect gateway logs for downstream connection failures.
2. The circuit breaker automatically transitions to `HALF-OPEN` after 5 seconds (`wait-duration-in-open-state: 5s`).
3. Ensure the downstream service (e.g. `order-service` on port 8081) is running and responsive:
   ```bash
   curl http://localhost:8081/actuator/health
   ```

---

## 6. k6 Load Test Issues

### Symptom: k6 reports `connection refused` or `high rate of 429`
**Diagnosis:**
* **Rate Limiting:** The Gateway enforces a token bucket rate limiter (20 RPS replenish, 40 burst). When running k6 burst tests, test against the Gateway with distributed `X-Shopper-Id` headers or target the service endpoint directly if benchmarking raw service throughput.
* **k6 Installation:** Verify k6 is in your system PATH by running `k6 version`.

---

## 7. Clean State Reset Procedure

To reset the entire system back to an immaculate initial state:

```powershell
# 1. Stop containers and purge volumes
docker compose down -v

# 2. Re-create containers with pristine storage
.\scripts\start-infra.ps1

# 3. Seed fresh demonstration data
.\scripts\seed-data.ps1
```
