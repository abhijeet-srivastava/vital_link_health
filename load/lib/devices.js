// Seed devices and readings for the k6 scripts. Devices and patients come from R__dev_seed.sql
// (device i -> patient i; 1-600 BP cuffs, 601-850 glucometers, 851-1000 pulse oximeters); keys come from
// load/.keys/keys.csv, written by the seed service (load/run.sh mints it). Values are synthetic, no PHI.
import http from 'k6/http';
import { sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter, Rate, Trend } from 'k6/metrics';

const KEYS_FILE = __ENV.KEYS_FILE || '/load/.keys/keys.csv';
export const BASE_URL = __ENV.BASE_URL || 'http://ingest-api:8081';

// The demo devices (1, 2, 601, 602, 851) are not in the file, so demo.sh keeps working during a run.
export const devices = new SharedArray('devices', () =>
  open(KEYS_FILE).trim().split('\n').slice(1).map((line) => {
    const [id, key] = line.split(',');
    const deviceId = Number(id);
    return { deviceId, patientId: deviceId, key, kind: deviceId <= 600 ? 'BP' : deviceId <= 850 ? 'GLUCOSE' : 'PULSE_OX' };
  }));

export const metrics = {
  latency: new Trend('ingest_latency', true),             // 202 responses only; tagged kind=new|retry
  accepted: new Counter('readings_accepted'),
  duplicate: new Counter('readings_duplicate'),
  rejected: new Counter('readings_rejected'),
  sent: new Counter('readings_sent_unique'),              // readings in new batches; the DB should hold exactly this many
  retryAccepted: new Counter('retry_readings_accepted'),  // any ACCEPTED in a deliberate retry is a duplicate row
  shed: new Counter('requests_shed'),                     // 429 / 503 (backpressure), resent after Retry-After
  errors: new Rate('ingest_errors'),                      // non-2xx other than 429/503, or a transport error
};

const between = (lo, hi) => lo + Math.floor(Math.random() * (hi - lo + 1));

// A breaching reading is HIGH by default and CRITICAL with probability criticalShare. Normal values stay inside
// every band of both seed programs; breaching values cross one band only (thresholds in R__dev_seed.sql).
function values(kind, breach, critical) {
  switch (kind) {
    case 'BP':
      return {
        type: 'BLOOD_PRESSURE', unit: 'mmHg',
        values: { systolic: !breach ? between(110, 145) : critical ? between(182, 200) : between(162, 178),
                  diastolic: between(65, 90), pulse: between(60, 95) },
      };
    case 'GLUCOSE':
      return {
        type: 'GLUCOSE', unit: 'mg/dL',
        values: { glucose: !breach ? between(90, 160) : critical ? between(410, 450) : between(260, 390) },
      };
    default:
      return {
        type: 'PULSE_OX', unit: '%',
        values: { spo2: !breach ? between(94, 99) : critical ? between(82, 87) : between(89, 91), pulse: between(60, 95) },
      };
  }
}

// One batch body. measured_at runs backwards from now in stepSeconds, like a device flushing its buffer.
export function batch(device, size, seqStart, breachRate, criticalShare, stepSeconds) {
  const now = Date.now();
  const readings = [];
  metrics.sent.add(size);
  for (let i = 0; i < size; i++) {
    const breach = Math.random() < breachRate;
    const v = values(device.kind, breach, breach && Math.random() < criticalShare);
    readings.push({
      reading_id: crypto.randomUUID(),
      device_seq: seqStart + i,
      type: v.type,
      measured_at: new Date(now - (size - i) * stepSeconds * 1000).toISOString(),
      values: v.values,
      unit: v.unit,
    });
  }
  return JSON.stringify({ patient_id: device.patientId, readings });
}

// POST a batch; on 429/503 wait Retry-After (capped) and resend the same body, as a device would.
export function post(device, body, kind, maxAttempts = 6) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const res = http.post(`${BASE_URL}/v1/devices/${device.deviceId}/readings`, body, {
      headers: { 'Content-Type': 'application/json', 'X-Device-Key': device.key },
      tags: { name: 'ingest', kind },
      timeout: '30s',
      responseCallback: http.expectedStatuses(202),
    });
    if (res.status === 429 || res.status === 503) {
      metrics.shed.add(1, { status: String(res.status) });
      sleep(Math.min(Number(res.headers['Retry-After'] || 5), Number(__ENV.MAX_RETRY_AFTER || 30)));
      continue;
    }
    if (res.status !== 202) {
      metrics.errors.add(1, { status: String(res.status) });
      return null;
    }
    metrics.errors.add(0);
    metrics.latency.add(res.timings.duration, { kind });
    for (const r of res.json('results')) {
      if (r.status === 'ACCEPTED') {
        metrics.accepted.add(1);
        if (kind === 'retry') metrics.retryAccepted.add(1);
      } else if (r.status === 'DUPLICATE') {
        metrics.duplicate.add(1);
      } else {
        metrics.rejected.add(1, { code: String(r.code) });
      }
    }
    return res;
  }
  metrics.errors.add(1, { status: 'gave_up' });
  return null;
}

// Run window for load/report.sql, written next to k6's own summary.
export function summary(script, data, startedAt) {
  const name = __ENV.RUN_LABEL || script;
  const window = { scenario: name, started_at: startedAt, ended_at: new Date().toISOString() };
  const lines = Object.entries(data.metrics).map(([k, m]) => {
    const v = Object.entries(m.values).map(([s, x]) => `${s}=${Math.round(x * 100) / 100}`).join(' ');
    const failed = m.thresholds && Object.values(m.thresholds).some((t) => !t.ok) ? '  THRESHOLD FAILED' : '';
    return `${k}: ${v}${failed}`;
  });
  return {
    stdout: lines.join('\n') + '\n',
    [`/load/results/${name}-summary.json`]: JSON.stringify({ window, metrics: data.metrics }, null, 2),
    [`/load/results/${name}-window.json`]: JSON.stringify(window),
  };
}
