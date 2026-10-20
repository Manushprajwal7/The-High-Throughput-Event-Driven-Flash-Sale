import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom Micrometer / k6 Metrics
export const acceptedOrders = new Counter('flashsale_accepted_orders');
export const soldOutResponses = new Counter('flashsale_sold_out_responses');
export const rateLimitedResponses = new Counter('flashsale_rate_limited_responses');
export const unexpectedErrors = new Counter('flashsale_unexpected_errors');
export const reservationLatency = new Trend('flashsale_reservation_latency_ms');

export const options = {
  scenarios: {
    flashsale_burst: {
      executor: 'ramping-vus',
      startVUs: 10,
      stages: [
        { duration: '5s', target: 50 },  // Rapid burst of shoppers
        { duration: '15s', target: 100 }, // Peak contention
        { duration: '5s', target: 0 },    // Wind down
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<300', 'p(99)<800'], // 95% of requests under 300ms
    'flashsale_unexpected_errors': ['count<5'],       // Zero or negligible server crashes
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:8080';
const DEFAULT_SALE_ID = __ENV.SALE_ID || 'c379a29e-297c-473d-8e68-0744a530733a';

export function setup() {
  console.log(`[k6 Setup] Targeting FlashSale Gateway: ${BASE_URL}`);
  console.log(`[k6 Setup] Target Sale ID: ${DEFAULT_SALE_ID}`);
  return { saleId: DEFAULT_SALE_ID };
}

export default function (data) {
  const vuId = __VU;
  const iterId = __ITER;
  const idempotencyKey = `burst-vu${vuId}-iter${iterId}-${Math.random().toString(36).substring(2, 8)}`;
  const shopperId = `shopper_${vuId}_${iterId}`;

  const payload = JSON.stringify({
    saleId: data.saleId,
    shopperId: shopperId,
    quantity: 1,
  });

  const headers = {
    'Content-Type': 'application/json',
    'Idempotency-Key': idempotencyKey,
    'X-Shopper-Id': shopperId,
  };

  const startTime = Date.now();
  const response = http.post(`${BASE_URL}/api/v1/orders`, payload, { headers });
  reservationLatency.add(Date.now() - startTime);

  const status = response.status;

  if (status === 200 || status === 201 || status === 202) {
    acceptedOrders.add(1);
    check(response, {
      'order accepted': (r) => r.status === 200 || r.status === 201 || r.status === 202,
      'has order id': (r) => {
        try {
          const json = JSON.parse(r.body);
          return json.orderId !== undefined;
        } catch (e) {
          return false;
        }
      },
    });
  } else if (status === 409) {
    soldOutResponses.add(1);
    check(response, {
      'sold out rejection is truthful': (r) => {
        try {
          const json = JSON.parse(r.body);
          return json.code === 'SOLD_OUT' || json.code === 'SALE_NOT_OPEN';
        } catch (e) {
          return false;
        }
      },
    });
  } else if (status === 429) {
    rateLimitedResponses.add(1);
  } else {
    unexpectedErrors.add(1);
    console.error(`Unexpected HTTP ${status}: ${response.body}`);
  }

  // Small pacing between requests per VU
  sleep(0.05);
}

export function teardown(data) {
  console.log('[k6 Teardown] Flash Sale Burst test execution completed.');
}
