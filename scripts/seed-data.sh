#!/usr/bin/env bash
# scripts/seed-data.sh
# Seeds a demo flash sale item with 100 units capacity and opens it for orders.

set -euo pipefail

GATEWAY_URL="${1:-http://localhost:8080}"
SALE_NAME="${2:-Super Phone 16 Limited Drop}"
CAPACITY="${3:-100}"

echo "=========================================================="
echo "FlashSale Engine - Seeding Demo Sale"
echo "=========================================================="

STARTS_AT=$(date -u -d "5 minutes ago" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")
ENDS_AT=$(date -u -d "24 hours" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")

CREATE_PAYLOAD=$(cat <<EOF
{
  "name": "$SALE_NAME",
  "totalCapacity": $CAPACITY,
  "startsAt": "$STARTS_AT",
  "endsAt": "$ENDS_AT"
}
EOF
)

echo "Creating sale: '$SALE_NAME' ($CAPACITY units)..."
CREATE_RES=$(curl -s -X POST "$GATEWAY_URL/api/v1/admin/sales" \
  -H "Content-Type: application/json" \
  -d "$CREATE_PAYLOAD")

SALE_ID=$(echo "$CREATE_RES" | grep -o '"saleId":"[^"]*' | cut -d'"' -f4)
if [ -z "$SALE_ID" ]; then
  SALE_ID=$(echo "$CREATE_RES" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
fi

if [ -z "$SALE_ID" ]; then
  echo "Failed to create sale. Response: $CREATE_RES" >&2
  exit 1
fi

echo "Sale created with ID: $SALE_ID"
echo "Opening sale for purchase requests..."

OPEN_RES=$(curl -s -X POST "$GATEWAY_URL/api/v1/admin/sales/$SALE_ID/open" \
  -H "Content-Type: application/json")

echo ""
echo "Sale is now OPEN and ready for flash sale traffic!"
echo "  * Sale ID: $SALE_ID"
echo "  * Details: $OPEN_RES"
echo "=========================================================="
