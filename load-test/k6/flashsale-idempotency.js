import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

export const identicalReplays = new Counter('flashsale_identical_replays');
export const conflictRejections = new Counter('flashsale_conflict_rejections');

export const options = {
  scenarios: {
    concurrent_identical_keys: {
      executor: 'shared-iterations',
      vus: 20,
      iterations: 100,
      maxDuration: '20s',
    },
  },
  thresholds: {
    'http_req_failed': ['rate<0.05'], // Only expected errors should occur
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:8080';
const SALE_ID = __ENV.SALE_ID || 'c379a29e-297c-473d-8e68-0744a530733a';

// Fixed shared key for all concurrent iterations to trigger race
const SHARED_KEY = `shared-idempotency-key-${Date.now()}`;
const SHOPPER_ID = 'shopper_race_tester';

export default function () {
  const isConflictTest = (__ITER % 10 === 9); // Every 10th request tests altered payload

  const quantity = isConflictTest ? 2 : 1;
  const payload = JSON.stringify({
    saleId: SALE_ID,
    shopperId: SHOPPER_ID,
    quantity: quantity,
  });

  const headers = {
    'Content-Type': 'application/json',
    'Idempotency-Key': SHARED_KEY,
    'X-Shopper-Id': SHOPPER_ID,
  };

  const response = http.post(`${BASE_URL}/api/v1/orders`, payload, { headers });

  if (isConflictTest) {
    // Expect 409 Idempotency Conflict because payload altered
    check(response, {
      'altered payload returns 409 conflict': (r) => r.status === 409,
      'code is IDEMPOTENCY_CONFLICT': (r) => {
        try {
          return JSON.parse(r.body).code === 'IDEMPOTENCY_CONFLICT';
        } catch (e) {
          return false;
        }
      },
    });
    conflictRejections.add(1);
  } else {
    // Expect original logical result (same orderId)
    check(response, {
      'identical submission succeeds': (r) => r.status === 200 || r.status === 201 || r.status === 202,
      'returns valid orderId': (r) => {
        try {
          return JSON.parse(r.body).orderId !== undefined;
        } catch (e) {
          return false;
        }
      },
    });
    identicalReplays.add(1);
  }

  sleep(0.02);
}
