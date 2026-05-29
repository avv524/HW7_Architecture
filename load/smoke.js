import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const errors = new Counter('errors');
const successLatency = new Trend('success_latency', true);

export const options = {
  scenarios: {
    publish_events: {
      executor: 'constant-vus',
      vus: 10,
      duration: __ENV.LOAD_DURATION || '45s',
      gracefulStop: '10s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.01'],
    success_latency: ['p(99)<1000'],
  },
};

const BASE_URL = __ENV.WMS_URL || 'http://localhost:8080';

const ZONES = ['ZONE-A', 'ZONE-B', 'ZONE-C'];
const EVENT_TYPES = [
  { type: 'PRODUCT_RECEIVED', weight: 0.5 },
  { type: 'PRODUCT_RESERVED', weight: 0.3 },
  { type: 'PRODUCT_RELEASED', weight: 0.2 },
];

function pickEventType() {
  const r = Math.random();
  let acc = 0;
  for (const entry of EVENT_TYPES) {
    acc += entry.weight;
    if (r <= acc) return entry.type;
  }
  return 'PRODUCT_RECEIVED';
}

function buildEvent() {
  const id = uuidv4();
  const sku = `SKU-LOAD-${(id.substring(0, 6))}`;
  const zone = ZONES[Math.floor(Math.random() * ZONES.length)];
  const qty = 1 + Math.floor(Math.random() * 10);
  return {
    event_id: `load-${id}`,
    event_type: pickEventType(),
    schema_version: 2,
    event_timestamp: new Date().toISOString(),
    product_id: sku,
    zone_id: zone,
    quantity: qty,
    supplier_id: 'SUP-LOAD',
  };
}

export default function () {
  const payload = JSON.stringify(buildEvent());
  const params = {
    headers: { 'Content-Type': 'application/json' },
    tags: { endpoint: '/events' },
  };

  const res = http.post(`${BASE_URL}/events`, payload, params);
  const ok = check(res, {
    'status is 2xx or 4xx (no server error)': (r) => r.status < 500,
    'response has kafka_metadata when 2xx': (r) =>
      r.status >= 300 || (r.json() && r.json().kafka_metadata !== undefined),
  });
  if (!ok || res.status >= 500) {
    errors.add(1);
  } else {
    successLatency.add(res.timings.duration);
  }
  sleep(0.1);
}

export function handleSummary(data) {
  return {
    stdout: textSummary(data),
    'artifacts/load-summary.json': JSON.stringify(data, null, 2),
  };
}

function textSummary(data) {
  const reqs = data.metrics.http_reqs?.values?.count ?? 0;
  const errs = data.metrics.errors?.values?.count ?? 0;
  const failedRate = data.metrics.http_req_failed?.values?.rate ?? 0;
  const p95 = data.metrics.http_req_duration?.values?.['p(95)'] ?? 0;
  const p99 = data.metrics.http_req_duration?.values?.['p(99)'] ?? 0;
  return [
    '',
    '=== k6 smoke summary ===',
    `requests:           ${reqs}`,
    `errors (5xx/local): ${errs}`,
    `http_req_failed:    ${(failedRate * 100).toFixed(2)}%`,
    `http p95 latency:   ${p95.toFixed(2)} ms`,
    `http p99 latency:   ${p99.toFixed(2)} ms`,
    '',
  ].join('\n');
}
