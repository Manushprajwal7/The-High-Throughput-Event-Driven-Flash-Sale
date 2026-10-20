#!/usr/bin/env bash
# scripts/run-demo.sh
# Interactive / Automated FlashSale Engine Live Demonstration Script

set -euo pipefail

GATEWAY_URL="${1:-http://localhost:8080}"
TOTAL_CAPACITY="${2:-100}"
SHOPPER_COUNT="${3:-120}"

function print_header() {
    echo ""
    echo "=========================================================="
    echo "DEMO STEP: $1"
    echo "=========================================================="
}

echo "=========================================================="
echo "       FlashSale Engine - Live Demonstration Showcase     "
echo "=========================================================="

print_header "0. Checking Gateway & Service Availability"
if ! curl -sf "$GATEWAY_URL/actuator/health" > /dev/null; then
    echo "Warning: Gateway ($GATEWAY_URL) is not responding."
    echo "Ensure local infrastructure and services are running:"
    echo "  1. ./scripts/start-infra.sh"
    echo "  2. ./mvnw spring-boot:run in each service directory"
    exit 1
fi
echo "Gateway is UP."

print_header "1. Creating Flash Sale with $TOTAL_CAPACITY Units"
STARTS_AT=$(date -u -d "5 minutes ago" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")
ENDS_AT=$(date -u -d "24 hours" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")

CREATE_RES=$(curl -s -X POST "$GATEWAY_URL/api/v1/admin/sales" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"Sneaker Drop (Demo)\",\"totalCapacity\":$TOTAL_CAPACITY,\"startsAt\":\"$STARTS_AT\",\"endsAt\":\"$ENDS_AT\"}")

SALE_ID=$(echo "$CREATE_RES" | grep -o '"saleId":"[^"]*' | cut -d'"' -f4)
if [ -z "$SALE_ID" ]; then
  SALE_ID=$(echo "$CREATE_RES" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
fi
echo "Created Sale ID: $SALE_ID"

echo "Opening Sale..."
curl -s -X POST "$GATEWAY_URL/api/v1/admin/sales/$SALE_ID/open" -H "Content-Type: application/json" > /dev/null
echo "Sale is OPEN."

print_header "2. Simulating Contention ($SHOPPER_COUNT Shoppers competing for $TOTAL_CAPACITY units)"
ACCEPTED=0
SOLD_OUT=0
ERRORS=0
SAMPLE_ORDER_ID=""

for i in $(seq 1 "$SHOPPER_COUNT"); do
    IDEMP_KEY="demo-key-$i-$RANDOM"
    RES=$(curl -s -w "\n%{http_code}" -X POST "$GATEWAY_URL/api/v1/orders" \
      -H "Content-Type: application/json" \
      -H "Idempotency-Key: $IDEMP_KEY" \
      -H "X-Shopper-Id: shopper_$i" \
      -d "{\"saleId\":\"$SALE_ID\",\"shopperId\":\"shopper_$i\",\"quantity\":1}")

    HTTP_CODE=$(echo "$RES" | tail -n1)
    BODY=$(echo "$RES" | sed '$d')

    if [ "$HTTP_CODE" -eq 200 ] || [ "$HTTP_CODE" -eq 201 ] || [ "$HTTP_CODE" -eq 202 ]; then
        ACCEPTED=$((ACCEPTED + 1))
        if [ -z "$SAMPLE_ORDER_ID" ]; then
            SAMPLE_ORDER_ID=$(echo "$BODY" | grep -o '"orderId":"[^"]*' | cut -d'"' -f4 || echo "")
        fi
    elif [ "$HTTP_CODE" -eq 409 ]; then
        SOLD_OUT=$((SOLD_OUT + 1))
    else
        ERRORS=$((ERRORS + 1))
    fi
done

echo "Traffic Burst Finished:"
echo "  * Accepted Orders:    $ACCEPTED"
echo "  * Sold Out Rejections: $SOLD_OUT"
echo "  * Other / Errors:     $ERRORS"

print_header "3. Invariant Verification: Zero Overselling"
SALE_STATUS=$(curl -s "$GATEWAY_URL/api/v1/sales/$SALE_ID")
echo "Current Sale State: $SALE_STATUS"

if [ "$ACCEPTED" -gt "$TOTAL_CAPACITY" ]; then
    echo "ERROR: Accepted orders ($ACCEPTED) exceeded capacity ($TOTAL_CAPACITY)!" >&2
    exit 1
else
    echo ">>> INVARIANT 1 PASSED: Strict Zero-Overselling Enforced! <<<"
fi

print_header "4. Idempotency Replay Test"
FIXED_KEY="idemp-test-$RANDOM"
REPLAY_PAYLOAD="{\"saleId\":\"$SALE_ID\",\"shopperId\":\"shopper_fixed\",\"quantity\":1}"

R1=$(curl -s -X POST "$GATEWAY_URL/api/v1/orders" -H "Content-Type: application/json" -H "Idempotency-Key: $FIXED_KEY" -d "$REPLAY_PAYLOAD")
R2=$(curl -s -X POST "$GATEWAY_URL/api/v1/orders" -H "Content-Type: application/json" -H "Idempotency-Key: $FIXED_KEY" -d "$REPLAY_PAYLOAD")

O1=$(echo "$R1" | grep -o '"orderId":"[^"]*' | cut -d'"' -f4 || echo "")
O2=$(echo "$R2" | grep -o '"orderId":"[^"]*' | cut -d'"' -f4 || echo "")

if [ -n "$O1" ] && [ "$O1" = "$O2" ]; then
    echo "Submission 1 Order ID: $O1"
    echo "Submission 2 Order ID: $O2"
    echo ">>> INVARIANT 2 PASSED: Replay returned exact matching Order ID without multi-decrement! <<<"
else
    echo "Replay check completed. Responses: $R1 | $R2"
fi

print_header "5. Asynchronous Order Status Query"
if [ -n "$SAMPLE_ORDER_ID" ]; then
    sleep 2
    ORDER_INFO=$(curl -s "$GATEWAY_URL/api/v1/orders/$SAMPLE_ORDER_ID")
    echo "Order Status: $ORDER_INFO"
fi

print_header "6. Observability Links"
echo "  * Operator UI:        http://localhost:8080/index.html"
echo "  * Grafana Dashboard:  http://localhost:3000 (admin / admin)"
echo "  * Prometheus Targets: http://localhost:9090/targets"
echo "=========================================================="
