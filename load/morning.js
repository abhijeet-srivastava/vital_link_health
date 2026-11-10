// Morning curve: 6-9 AM compressed 20x (one minute = 20 minutes of clock), shaped as an arrival rate of small
// batches from random devices. PEAK_RPS x BATCH_SIZE is the peak readings/s; the defaults give 2,800 readings/s,
// the "morning sustained" figure in performance-and-scaling.md. 20% of readings breach a threshold, 10% of those
// at CRITICAL. Run with load/run.sh morning.
import { BASE_URL, batch, devices, post, summary } from './lib/devices.js';

const PEAK_RPS = Number(__ENV.PEAK_RPS || 560);
const BATCH_SIZE = Number(__ENV.BATCH_SIZE || 5);       // "average batch of 5" in the capacity model
const BREACH_RATE = Number(__ENV.BREACH_RATE || 0.2);
const CRITICAL_SHARE = Number(__ENV.CRITICAL_SHARE || 0.1);
const MINUTE = __ENV.STAGE || '1m';                     // one stage = 20 minutes of the morning

// Share of the peak at 06:20, 06:40 ... 09:00: steep rise to the 07:00-07:20 plateau, long tail after.
const CURVE = [0.35, 0.7, 1.0, 1.0, 0.85, 0.65, 0.45, 0.3, 0.15];

export const options = {
  scenarios: {
    morning: {
      executor: 'ramping-arrival-rate',
      startRate: Math.round(PEAK_RPS * 0.1),            // 06:00
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 1000,
      stages: CURVE.map((share) => ({ target: Math.round(PEAK_RPS * share), duration: MINUTE })),
    },
  },
  thresholds: {
    'ingest_latency{kind:new}': ['p(99)<150'],
    ingest_errors: ['rate<0.01'],
    readings_rejected: ['count==0'],
    'requests_shed{status:429}': ['count>=0'],      // always passes; makes the summary show the 429/503 split
    'requests_shed{status:503}': ['count>=0'],
    dropped_iterations: ['count==0'],                   // the generator kept up with the curve
  },
  summaryTrendStats: ['min', 'med', 'p(95)', 'p(99)', 'max', 'count'],
};

export function setup() {
  return { startedAt: new Date().toISOString(), base: BASE_URL };
}

export default function () {
  const device = devices[Math.floor(Math.random() * devices.length)];
  post(device, batch(device, BATCH_SIZE, Date.now() * 10, BREACH_RATE, CRITICAL_SHARE, 30), 'new');
}

export function handleSummary(data) {
  return summary('morning', data, data.setup_data.startedAt);
}
