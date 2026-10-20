import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

export const rateLimitRejections = new Counter('flashsale_rate_limit_429');
export const normalResponses = new Counter('flashsale_normal_responses');

export const options = {
  scenarios: {
    flood_rate_limit: {
      executor: 'per-vu-iterations',
      vus: 10,
      iterations: 30, // 300 total requests in quick succession
      maxDuration: '10s',
    },
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:8080';
const SINGLE_SHOPPER = 'rate_limit_abuser_ip';

export default function () {
  const headers = {
    'X-Shopper-Id': SINGLE_SHOPPER,
    'X-Forwarded-For': '198.51.100.42',
  };

  const response = http.get(`${BASE_URL}/api/v1/sales`, { headers });

  if (response.status === 429) {
    rateLimitRejections.add(1);
    check(response, {
      'rate limited status is 429': (r) => r.status === 429,
      'error response has code RATE_LIMIT_EXCEEDED': (r) => {
        try {
          return JSON.parse(r.body).code === 'RATE_LIMIT_EXCEEDED';
        } catch (e) {
          return false;
        }
      },
      'correlation id header present': (r) => r.headers['X-Correlation-Id'] !== undefined,
    });
  } else {
    normalResponses.add(1);
  }
}
