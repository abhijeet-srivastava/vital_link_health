// Reconnect burst: 200 devices each flush a backlog of 50-reading batches as fast as the API takes them.
// About 10% of requests are deliberate retries of an earlier batch of the same device (same body), which must
// return DUPLICATE for every reading and add no rows. Run with load/run.sh burst.
import exec from 'k6/execution';
import { BASE_URL, batch, devices, post, summary } from './lib/devices.js';

const VUS = Number(__ENV.VUS || 200);
const BATCHES = Number(__ENV.BATCHES || 22);          // requests per device, retries included (device burst is 30)
const BATCH_SIZE = Number(__ENV.BATCH_SIZE || 50);
const RETRY_RATE = Number(__ENV.RETRY_RATE || 0.1);
const BREACH_RATE = Number(__ENV.BREACH_RATE || 0.05);
const CRITICAL_SHARE = Number(__ENV.CRITICAL_SHARE || 0.1);

export const options = {
  scenarios: {
    burst: { executor: 'per-vu-iterations', vus: VUS, iterations: 1, maxDuration: '15m' },
  },
  // Targets (performance-and-scaling.md): a miss fails the run.
  thresholds: {
    'ingest_latency{kind:new}': ['p(99)<150'],     // SLO is p99 < 150 ms for a 100-reading batch; these are 50
    ingest_errors: ['rate<0.01'],
    retry_readings_accepted: ['count==0'],          // a retried batch never adds a row
    readings_rejected: ['count==0'],
    'requests_shed{status:429}': ['count>=0'],      // always passes; makes the summary show the 429/503 split
    'requests_shed{status:503}': ['count>=0'],
  },
  summaryTrendStats: ['min', 'med', 'p(95)', 'p(99)', 'max', 'count'],
};

export function setup() {
  if (devices.length < VUS) throw new Error(`need ${VUS} devices in the keys file, have ${devices.length}`);
  return { startedAt: new Date().toISOString(), base: BASE_URL };
}

export default function () {
  // Spread the VUs over the whole device list so all three device types take part.
  const step = Math.floor(devices.length / VUS);
  const device = devices[(exec.vu.idInTest - 1) * step];
  const sent = [];
  let seq = Date.now() * 10;   // device_seq only has to grow per device
  for (let i = 0; i < BATCHES; i++) {
    if (sent.length > 0 && Math.random() < RETRY_RATE) {
      post(device, sent[Math.floor(Math.random() * sent.length)], 'retry');
      continue;
    }
    const body = batch(device, BATCH_SIZE, seq, BREACH_RATE, CRITICAL_SHARE, 60);
    seq += BATCH_SIZE;
    if (post(device, body, 'new')) sent.push(body);
  }
}

export function handleSummary(data) {
  return summary('burst', data, data.setup_data.startedAt);
}
