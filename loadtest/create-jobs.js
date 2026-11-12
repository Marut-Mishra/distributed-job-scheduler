// k6 load test: creates one-off jobs at a constant arrival rate and lets the scheduler run them.
//
// Run (no install needed — k6 runs in Docker):
//   docker run --rm -i -e RATE=100 -e DURATION=2m grafana/k6 run - < loadtest/create-jobs.js
//
// On Linux add:  --add-host=host.docker.internal:host-gateway
//
// Env vars:
//   BASE_URL   default http://host.docker.internal:8080
//   RATE       jobs created per second (default 50)
//   DURATION   how long to keep creating (default 1m)
//   HANDLER    log | sleep | flaky (default sleep)
//   SLEEP_MS   handler work time for "sleep" (default 50)
//   FAIL_RATE  failure probability for "flaky" (default 0.2)

import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:18080';
const RATE = parseInt(__ENV.RATE || '50');
const HANDLER = __ENV.HANDLER || 'sleep';

export const options = {
  scenarios: {
    create_jobs: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '1m',
      preAllocatedVUs: Math.max(10, RATE),
      maxVUs: Math.max(50, RATE * 4),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],     // API must stay healthy under load
    http_req_duration: ['p(95)<250'],   // job creation is a single insert
  },
};

export function setup() {
  const startedAt = new Date().toISOString();
  console.log(`Load test window starts at ${startedAt}`);
  return { startedAt };
}

export default function () {
  const payload =
    HANDLER === 'sleep' ? { millis: parseInt(__ENV.SLEEP_MS || '50') } :
    HANDLER === 'flaky' ? { failRate: parseFloat(__ENV.FAIL_RATE || '0.2') } : {};

  const res = http.post(`${BASE_URL}/api/v1/jobs`, JSON.stringify({
    name: `load-${__VU}-${__ITER}`,
    handler: HANDLER,
    scheduleType: 'ONE_OFF',
    payload,
    maxRetries: 3,
    timeoutSeconds: 30,
  }), { headers: { 'Content-Type': 'application/json' } });

  check(res, { 'created (201)': (r) => r.status === 201 });
}

export function teardown(data) {
  console.log('');
  console.log('Wait ~30s for in-flight runs to drain, then fetch the results:');
  console.log(`  curl "${BASE_URL.replace('host.docker.internal', 'localhost')}/api/v1/stats?since=${data.startedAt}"`);
}
