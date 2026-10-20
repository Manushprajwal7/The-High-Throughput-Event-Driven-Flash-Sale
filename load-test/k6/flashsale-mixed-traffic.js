import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

export const querySuccess = new Counter('flashsale_query_success');
export const orderSubmitted = new Counter('flashsale_order_submitted');
export const validationErrorsCaught = new Counter('flashsale_validation_errors_caught');

export const options = {
  scenarios: {
    mixed_traffic: {
      executor: 'ramping-vus',
      startVUs: 5,
      stages: [
        { duration: '5s', target: 30 },
        { duration: '10s', target: 50 },
        { duration: '5s', target: 0 },
      ],
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<400'],
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:8080';
const SALE_ID = __ENV.SALE_ID || 'c379a29e-297c-473d-8e68-0744a530733a';

export default function () {
  const rand = Math.random();

  if (rand < 0.60) {
    // 60% Read traffic: Sale inventory query
    const res = http.get(`${BASE_URL}/api/v1/sales`);
    check(res, { 'sale list ok': (r) => r.status === 200 });
    querySuccess.add(1);
  } else if (rand < 0.85) {
    // 25% Purchase traffic
    const key = `mixed-key-${__VU}-${__ITER}-${Math.random().toString(36).substring(2, 6)}`;
    const payload = JSON.stringify({
      saleId: SALE_ID,
      shopperId: `shopper_${__VU}`,
      quantity: 1,
    });
    const res = http.post(`${BASE_URL}/api/v1/orders`, payload, {
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    });
    check(res, { 'order response status valid': (r) => r.status === 200 || r.status === 201 || r.status === 409 });
    orderSubmitted.add(1);
  } else if (rand < 0.95) {
    // 10% Idempotent duplicate replay
    const replayKey = `replay-key-${__VU}`;
    const payload = JSON.stringify({
      saleId: SALE_ID,
      shopperId: `shopper_replay_${__VU}`,
      quantity: 1,
    });
    const res = http.post(`${BASE_URL}/api/v1/orders`, payload, {
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': replayKey },
    });
    check(res, { 'replay handled': (r) => r.status === 200 || r.status === 201 || r.status === 409 });
  } else {
    // 5% Invalid request: quantity 0 or missing idempotency key
    const payload = JSON.stringify({
      saleId: SALE_ID,
      shopperId: `shopper_invalid`,
      quantity: 0, // Invalid!
    });
    const res = http.post(`${BASE_URL}/api/v1/orders`, payload, {
      headers: { 'Content-Type': 'application/json' }, // Missing Idempotency-Key!
    });
    check(res, {
      'invalid rejected with 400': (r) => r.status === 400,
    });
    validationErrorsCaught.add(1);
  }

  sleep(0.05);
}
