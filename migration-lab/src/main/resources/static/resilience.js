// Resilience lab. Everything drawn here is a sample the control plane measured from the running
// system (GET /api/resilience/state); the browser only lays it out on a shared clock.
'use strict';

const $ = selector => document.querySelector(selector);
const SVG = 'http://www.w3.org/2000/svg';
const PHASES = ['HYPOTHESIS', 'INJECT', 'OBSERVE', 'EXPLAIN', 'MITIGATE', 'RECOVER', 'VERIFY'];
const PHASE_LABEL = { HYPOTHESIS: 'Hypothesis', INJECT: 'Inject', OBSERVE: 'Observe', EXPLAIN: 'Explain', MITIGATE: 'Apply mitigation', RECOVER: 'Recover', VERIFY: 'Verify' };

const view = {
  samples: [], markers: [], last: 0, window: 180, hover: null,
  experiments: [], selected: null, run: null, chaos: null, load: null, runs: [], controlsLoaded: false,
};

function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value === false || value == null) continue;
    if (key === 'class') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else if (key === 'dataset') Object.assign(node.dataset, value);
    else node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat()) if (child != null && child !== false) node.append(child.nodeType ? child : String(child));
  return node;
}

function s(tag, attrs = {}) {
  const node = document.createElementNS(SVG, tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  return node;
}

const num = (v, digits = 0) => v == null || Number.isNaN(v) ? '—' : Number(v).toLocaleString('en', { maximumFractionDigits: digits, minimumFractionDigits: 0 });
const sum = values => values.reduce((a, b) => a + (b || 0), 0);
const gatewayCalls = x => x.gateway && x.gateway.attempts ? sum(Object.entries(x.gateway.attempts).filter(([k]) => k !== 'REJECTED_BY_BREAKER').map(([, v]) => v)) : null;
const outcome = (x, key) => x.traffic.outcomes[key] || 0;
const pool = (x, service, field) => x.resources[service] ? x.resources[service][field] : null;
const cpu = (x, service) => x.resources[service] ? x.resources[service].cpu : null;
const lag = group => x => x.lag ? (x.lag[group] ?? null) : null;

// The signal chain, top to bottom. Colours carry meaning (DESIGN.md): blue demand, green work done,
// amber retries, red failure, violet compensation or mitigation; neutral tones for context.
const ROWS = [
  { title: 'Traffic', note: 'Clients arriving, and the requests they sent (retries included).', unit: '/s', series: [
    { label: 'clients arriving', color: 'var(--blue)', get: x => x.traffic.offered },
    { label: 'requests sent', color: 'var(--amber)', get: x => x.traffic.attempts },
    { label: 'turned away at cap', color: 'var(--red)', get: x => x.traffic.dropped },
  ] },
  { title: 'Throughput', note: 'Requests that succeeded, and orders that finished their saga.', unit: '/s', series: [
    { label: 'requests OK', color: 'var(--green)', get: x => x.traffic.succeeded },
    { label: 'orders completed', color: 'var(--teal)', get: x => x.sagas.completed },
    { label: 'orders cancelled', color: 'var(--violet)', get: x => x.sagas.cancelled },
    { label: 'gateway calls', color: 'var(--slate)', get: gatewayCalls },
  ] },
  { title: 'Latency', note: 'Client-observed p50/p95/p99 over 5 s; order end to end; outbox → Kafka.', unit: 'ms', series: [
    { label: 'write p50', color: 'var(--blue)', dash: true, get: x => x.latency.writeP50 },
    { label: 'write p95', color: 'var(--blue)', get: x => x.latency.writeP95, faint: true },
    { label: 'write p99', color: 'var(--ink)', get: x => x.latency.writeP99 },
    { label: 'read p99', color: 'var(--teal)', get: x => x.latency.readP99 },
    { label: 'order end-to-end p99', color: 'var(--violet)', get: x => x.sagas.p99 },
    { label: 'outbox → Kafka p95', color: 'var(--amber)', dash: true, get: x => x.relay.p95 },
  ] },
  { title: 'Kafka lag', note: 'Records each consumer group has not yet committed, from the broker.', unit: 'records', series: [
    { label: 'payment-service', color: 'var(--blue)', get: lag('payment-service') },
    { label: 'inventory-service', color: 'var(--teal)', get: lag('inventory-service') },
    { label: 'shipping-service', color: 'var(--slate)', get: lag('shipping-service') },
    { label: 'order-saga', color: 'var(--ink)', get: lag('order-saga') },
    { label: 'order-projection', color: 'var(--violet)', dash: true, get: lag('order-projection') },
  ] },
  { title: 'Retries', note: 'Clients retrying, consumers redelivering, and retries a budget withheld.', unit: '/s', series: [
    { label: 'client retries', color: 'var(--amber)', get: x => x.traffic.retries },
    { label: 'consumer redeliveries', color: 'var(--ink)', get: x => x.consumers.retries },
    { label: 'duplicates skipped', color: 'var(--slate)', get: x => x.consumers.duplicates },
    { label: 'retries withheld', color: 'var(--violet)', dash: true, get: x => x.traffic.throttled },
  ] },
  { title: 'Errors', note: 'Failed attempts by cause, refusals by the edge guards, dead letters.', unit: '/s', series: [
    { label: 'timeouts', color: 'var(--red)', get: x => outcome(x, 'TIMEOUT') },
    { label: 'refused / reset', color: 'var(--red)', dash: true, get: x => outcome(x, 'CONNECTION') },
    { label: '5xx', color: 'var(--ink)', get: x => outcome(x, 'SERVER_ERROR') },
    { label: '429 rate-limited', color: 'var(--violet)', get: x => outcome(x, 'RATE_LIMITED') },
    { label: '503 shed / bulkhead', color: 'var(--violet)', dash: true, get: x => outcome(x, 'SHED') + outcome(x, 'BULKHEAD_FULL') },
    { label: 'dead-lettered', color: 'var(--amber)', get: x => x.consumers.deadLettered },
  ] },
  { title: 'Pressure', note: 'Requests inside order-service, threads waiting for a DB connection, rebalances.', unit: '', series: [
    { label: 'in flight (edge)', color: 'var(--blue)', get: x => x.edge.inFlight },
    { label: 'pool waiting, order A', color: 'var(--red)', get: x => pool(x, 'order-service', 'poolPending') },
    { label: 'pool waiting, order B', color: 'var(--red)', dash: true, get: x => pool(x, 'order-service-b', 'poolPending') },
    { label: 'unfinished orders', color: 'var(--violet)', get: x => x.sagas.active },
    { label: 'rebalances', color: 'var(--amber)', get: x => x.consumers.rebalances },
  ] },
  { title: 'Resources', note: 'Process CPU (share of the container’s CPUs) and heap.', unit: '%', series: [
    { label: 'order-service CPU', color: 'var(--blue)', get: x => cpu(x, 'order-service') },
    { label: 'order-service-b CPU', color: 'var(--blue)', dash: true, get: x => cpu(x, 'order-service-b') },
    { label: 'payment CPU', color: 'var(--teal)', get: x => cpu(x, 'payment-service') },
    { label: 'inventory CPU', color: 'var(--slate)', get: x => cpu(x, 'inventory-service') },
  ] },
  { title: 'Breaker & consumer', note: 'Circuit breaker state and whether the payment consumer is paused.', bands: [
    { label: 'breaker open', color: 'var(--red)', get: x => x.gateway.breaker === 'OPEN' },
    { label: 'breaker half-open', color: 'var(--amber)', get: x => x.gateway.breaker === 'HALF_OPEN' },
    { label: 'payment consumer paused', color: 'var(--violet)', get: x => x.gateway.consumerPaused === true },
    { label: 'service unreachable', color: 'var(--slate)', get: x => x.unreachable.length > 0 },
  ] },
];

// ---- Data ---------------------------------------------------------------------------------------

async function api(method, path, body, pending) {
  try {
    if (pending) toast(pending);
    const response = await fetch(path, { method, headers: body ? { 'Content-Type': 'application/json' } : {}, body: body ? JSON.stringify(body) : undefined });
    const text = await response.text();
    const data = text ? JSON.parse(text) : null;
    if (!response.ok) throw new Error(data && (data.detail || data.title) || `HTTP ${response.status}`);
    if (pending) toast(null);
    return data;
  } catch (e) {
    toast(e.message, true);
    throw e;
  }
}

let toastTimer;
function toast(message, error = false) {
  const t = $('#toast');
  clearTimeout(toastTimer);
  if (!message) { t.hidden = true; return; }
  t.textContent = message;
  t.className = error ? 'toast error' : 'toast';
  t.hidden = false;
  toastTimer = setTimeout(() => { t.hidden = true; }, error ? 7000 : 2500);
}

async function poll() {
  try {
    const response = await fetch(`/api/resilience/state?since=${view.last}`);
    if (!response.ok) throw new Error(response.status);
    const state = await response.json();
    for (const sample of state.samples) if (sample.t > view.last) view.samples.push(sample);
    if (view.samples.length) view.last = view.samples[view.samples.length - 1].t;
    if (view.samples.length > 900) view.samples.splice(0, view.samples.length - 900);
    const known = new Set(view.markers.map(m => `${m.t}${m.label}`));
    for (const m of state.markers) if (!known.has(`${m.t}${m.label}`)) view.markers.push(m);
    view.markers = view.markers.filter(m => m.t > Date.now() - 900_000);
    view.now = state.now;
    view.chaos = state.chaos;
    view.load = state.load;
    const runChanged = JSON.stringify(state.run) !== JSON.stringify(view.run);
    view.run = state.run;
    if (view.run && !view.selected) view.selected = view.run.experiment;
    connection(true);
    renderChain();
    renderLoad();
    renderChaos();
    if (runChanged) { renderExperiment(); renderExperimentList(); }
  } catch {
    connection(false);
  }
}

function connection(ok) {
  const c = $('#connection');
  c.classList.toggle('disconnected', !ok);
  c.lastChild.textContent = ok ? ' Live' : ' Control plane unreachable';
}

// ---- Signal chain -------------------------------------------------------------------------------

function renderChain() {
  const rows = $('#rows');
  const end = view.now || Date.now();
  const start = end - view.window * 1000;
  const samples = view.samples.filter(x => x.t >= start);
  $('#chain-empty').hidden = samples.length > 0;
  const markers = view.markers.filter(m => m.t >= start);
  const at = view.hover == null ? samples[samples.length - 1] : nearest(samples, view.hover);
  rows.replaceChildren(markerStrip(markers, start, end), ...ROWS.map(row => renderRow(row, samples, markers, start, end, at)), cursor(start, end));
}

function nearest(samples, t) {
  let best = null;
  for (const x of samples) if (!best || Math.abs(x.t - t) < Math.abs(best.t - t)) best = x;
  return best;
}

function markerStrip(markers, start, end) {
  const strip = h('div', { class: 'markers', 'aria-label': 'Changes made' });
  for (const m of markers) {
    const left = (m.t - start) / (end - start) * 100;
    strip.append(h('span', { class: m.kind, style: `left: ${left}%`, title: `${new Date(m.t).toLocaleTimeString()} · ${m.label}` }, m.label));
  }
  return strip;
}

function renderRow(row, samples, markers, start, end, at) {
  const legend = h('ul', { class: 'series' });
  const svg = s('svg', { viewBox: '0 0 1000 86', preserveAspectRatio: 'none', role: 'img', 'aria-label': row.title });
  for (const y of [2, 44, 84]) svg.append(s('line', { class: 'grid', x1: 0, x2: 1000, y1: y, y2: y }));
  const x = t => (t - start) / (end - start) * 1000;
  let max = 0;
  if (row.series) {
    for (const series of row.series) for (const p of samples) { const v = series.get(p); if (v != null && v > max) max = v; }
    max = niceCeil(max);
    for (const series of row.series) {
      let d = '';
      let drawing = false;
      for (const p of samples) {
        const v = series.get(p);
        if (v == null) { drawing = false; continue; }
        const px = x(p.t).toFixed(1);
        const py = (84 - (v / max) * 80).toFixed(1);
        d += `${drawing ? 'L' : 'M'}${px},${py}`;
        drawing = true;
      }
      const path = s('path', { class: 'line', d, stroke: series.color });
      if (series.dash) path.setAttribute('stroke-dasharray', '5 4');
      if (series.faint) path.setAttribute('opacity', '.45');
      svg.append(path);
      const value = at ? series.get(at) : null;
      legend.append(h('li', {}, h('span', {}, h('i', { class: series.dash ? 'dash' : '', style: `background:${series.color};color:${series.color}` }), series.label), h('b', {}, value == null ? '—' : `${num(value, value < 10 ? 1 : 0)}${row.unit && row.unit !== 'records' ? ` ${row.unit}` : ''}`)));
    }
  } else {
    row.bands.forEach((band, i) => {
      const top = 4 + i * 20;
      for (let k = 0; k < samples.length; k++) {
        if (!band.get(samples[k])) continue;
        const next = samples[k + 1] ? samples[k + 1].t : samples[k].t + 1000;
        svg.append(s('rect', { x: x(samples[k].t), y: top, width: Math.max(1, x(next) - x(samples[k].t)), height: 16, fill: band.color, opacity: '.75' }));
      }
      svg.append(s('line', { class: 'grid', x1: 0, x2: 1000, y1: top + 18, y2: top + 18 }));
      const on = at ? band.get(at) : false;
      legend.append(h('li', {}, h('span', {}, h('i', { style: `background:${band.color}` }), band.label), h('b', {}, at ? (on ? 'yes' : 'no') : '—')));
    });
  }
  for (const m of markers) svg.append(s('line', { class: `marker-line ${m.kind}`, x1: x(m.t), x2: x(m.t), y1: 0, y2: 86 }));
  const scale = row.series ? h('span', { class: 'axis-max' }, `${num(max)}${row.unit && row.unit !== 'records' ? ` ${row.unit}` : ''}`) : null;
  return h('div', { class: 'row' },
    h('div', { class: 'row-label' }, h('strong', {}, row.title), h('small', {}, row.note), legend),
    h('div', { class: 'plot', dataset: { plot: '1' } }, svg, scale));
}

function niceCeil(v) {
  if (v <= 1) return 1;
  const p = 10 ** Math.floor(Math.log10(v));
  for (const m of [1, 2, 2.5, 5, 10]) if (v <= m * p) return m * p;
  return 10 * p;
}

function cursor(start, end) {
  if (view.hover == null) return h('span', { hidden: true });
  const plot = document.querySelector('.plot');
  if (!plot) return h('span', { hidden: true });
  const rows = $('#rows').getBoundingClientRect();
  const box = plot.getBoundingClientRect();
  const left = box.left - rows.left + (view.hover - start) / (end - start) * box.width;
  return h('div', { class: 'cursor', style: `left:${left}px` }, h('span', { class: 'cursor-time' }, new Date(view.hover).toLocaleTimeString()));
}

function trackHover() {
  const rows = $('#rows');
  rows.addEventListener('mousemove', event => {
    const plot = event.target.closest('.plot');
    if (!plot) return;
    const box = plot.getBoundingClientRect();
    const end = view.now || Date.now();
    const start = end - view.window * 1000;
    view.hover = start + (event.clientX - box.left) / box.width * (end - start);
    renderChain();
  });
  rows.addEventListener('mouseleave', () => { view.hover = null; renderChain(); });
  for (const b of document.querySelectorAll('[data-window]'))
    b.addEventListener('click', () => {
      view.window = Number(b.dataset.window);
      for (const other of document.querySelectorAll('[data-window]')) other.setAttribute('aria-pressed', other === b ? 'true' : 'false');
      renderChain();
    });
}

// ---- Experiments --------------------------------------------------------------------------------

function renderExperimentList() {
  const list = $('#experiment-list');
  list.replaceChildren(...view.experiments.map(e => {
    const running = view.run && view.run.experiment === e.id && ['running', 'waiting'].includes(view.run.status);
    return h('li', {}, h('button', { type: 'button', 'aria-current': view.selected === e.id ? 'true' : 'false', onclick: () => { view.selected = e.id; renderExperiment(); renderExperimentList(); } },
      h('strong', {}, e.title),
      h('span', { class: 'chips' }, running ? h('span', { class: 'chip info' }, view.run.status) : null, e.needsChaos ? h('span', { class: 'chip warn' }, 'Toxiproxy') : null, ...e.concepts.slice(0, 2).map(c => h('span', { class: 'chip' }, c)))));
  }));
}

function renderExperiment() {
  const panel = $('#experiment');
  const e = view.experiments.find(x => x.id === view.selected) || view.experiments[0];
  if (!e) { panel.replaceChildren(h('p', { class: 'runs-empty' }, 'Loading experiments…')); return; }
  view.selected = e.id;
  const run = view.run && view.run.experiment === e.id ? view.run : null;
  const active = view.run && ['running', 'waiting'].includes(view.run.status);
  const chaosMissing = e.needsChaos && view.chaos && !view.chaos.configured;
  const steps = e.steps.map((step, i) => {
    const result = run ? run.steps[i] : null;
    const status = result ? result.status : 'pending';
    const checks = result && result.checks.length ? result.checks : step.checks.map(claim => ({ claim, status: 'pending' }));
    return h('li', { class: status }, h('div', {},
      h('div', { class: 'step-head' }, h('span', { class: 'step-phase' }, PHASE_LABEL[step.phase]), h('strong', {}, step.title),
        status === 'running' ? h('span', { class: 'chip info' }, 'running') : status === 'failed' ? h('span', { class: 'chip bad' }, 'did not hold') : status === 'passed' ? h('span', { class: 'chip good' }, 'held') : null),
      h('div', { class: 'step-body' },
        h('p', {}, step.text),
        result && result.did ? h('p', { class: 'step-did' }, `→ ${result.did}`) : null,
        checks.length ? h('ul', { class: 'checks' }, ...checks.map(c => h('li', { class: `${c.status}${status === 'running' ? ' live' : ''}` }, h('span', {}, c.claim), h('span', { class: 'observed' }, c.observed || '')))) : null)));
  });
  const next = run && run.status === 'waiting' ? e.steps[run.current + 1] : null;
  const bar = h('div', { class: 'run-bar' },
    h('p', {}, run ? runLine(run) : active ? `${view.run.title} is running.` : `Load: ${e.load.ratePerSecond} clients/s, ${e.load.readPercent}% reads, client retry ${e.load.retry}.`),
    h('div', { class: 'buttons' },
      !active ? h('button', { type: 'button', class: 'primary-action', disabled: chaosMissing, onclick: () => start(e.id, false) }, 'Start step by step') : null,
      !active ? h('button', { type: 'button', disabled: chaosMissing, onclick: () => start(e.id, true) }, 'Run all steps') : null,
      next ? h('button', { type: 'button', class: 'primary-action', onclick: () => advance(false) }, `Next: ${PHASE_LABEL[next.phase]}`) : null,
      next ? h('button', { type: 'button', onclick: () => advance(true) }, 'Run the rest') : null,
      active ? h('button', { type: 'button', class: 'danger', onclick: abort }, 'Abort & reset') : null));
  panel.replaceChildren(...[
    h('div', { class: 'experiment-top' }, h('div', {}, h('h3', {}, e.title), h('p', {}, e.summary),
      h('div', { class: 'experiment-meta' }, ...e.concepts.map(c => h('span', { class: 'chip mitigate' }, c))))),
    chaosMissing ? h('p', { class: 'needs-chaos' }, 'This experiment breaks a network link through Toxiproxy, which runs with the chaos overlay: ', h('code', {}, 'docker compose -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.chaos.yml up -d')) : null,
    h('ol', { class: 'track' }, ...steps),
    bar].filter(Boolean));
}

function runLine(run) {
  const held = run.steps.flatMap(x => x.checks).filter(c => c.status === 'held').length;
  const failed = run.steps.flatMap(x => x.checks).filter(c => c.status === 'failed').length;
  const status = { running: 'Running', waiting: 'Waiting for you', passed: 'Every claim held', failed: 'Some claims did not hold', aborted: 'Aborted' }[run.status] || run.status;
  return `${status} · ${held} held${failed ? `, ${failed} did not` : ''}${run.finishedAt ? ' · lab reset afterwards' : ''}`;
}

async function start(id, auto) {
  view.run = await api('POST', `/api/resilience/experiments/${id}/start?auto=${auto}`, null, 'Resetting the lab and starting the load…');
  renderExperiment(); renderExperimentList();
}

async function advance(auto) {
  view.run = await api('POST', `/api/resilience/experiments/next?auto=${auto}`);
  renderExperiment();
}

async function abort() {
  view.run = await api('POST', '/api/resilience/experiments/abort', null, 'Aborting and resetting every lever…');
  renderExperiment(); renderExperimentList();
}

// ---- Levers -------------------------------------------------------------------------------------

function formValues(form) {
  const values = {};
  for (const el of form.elements) {
    if (!el.name) continue;
    values[el.name] = el.type === 'checkbox' ? el.checked : el.type === 'number' ? (el.value === '' ? null : Number(el.value)) : el.value;
  }
  return values;
}

function fill(form, values) {
  for (const [key, value] of Object.entries(values || {})) {
    const el = form.elements[key];
    if (!el || el === document.activeElement) continue;
    if (el.type === 'checkbox') el.checked = !!value; else el.value = value ?? '';
  }
}

function renderLoad() {
  const load = view.load;
  if (!load) return;
  const status = $('#load-status');
  status.textContent = load.running ? `running · ${load.profile.ratePerSecond}/s` : 'stopped';
  status.className = load.running ? 'chip info' : 'chip';
  if (!view.loadFilled) { fill($('#load-form'), load.profile); view.loadFilled = true; }
  const o = load.outcomes || {};
  const items = [
    ['arrived', load.arrivals], ['turned away', load.dropped], ['requests', load.attempts], ['retries', load.retries],
    ['succeeded', load.succeeded], ['failed', load.failed], ['waiting now', load.inFlight], ['timeouts', o.TIMEOUT],
    ['429', o.RATE_LIMITED], ['503', (o.SHED || 0) + (o.BULKHEAD_FULL || 0)], ['retry tokens', load.retryTokens],
  ];
  $('#load-totals').replaceChildren(...items.map(([k, v]) => h('div', {}, h('dt', {}, k), h('dd', {}, num(v, k === 'retry tokens' ? 1 : 0)))));
}

function renderChaos() {
  const box = $('#chaos');
  const status = $('#chaos-status');
  const chaos = view.chaos;
  if (!chaos) return;
  if (!chaos.configured) {
    status.textContent = 'not running';
    status.className = 'chip';
    box.replaceChildren(h('p', { class: 'chaos-off' }, 'Toxiproxy is part of the chaos overlay, which routes five real connections through it:',
      h('code', {}, 'docker compose -f docker-compose.yml \\\n  -f docker-compose.override.yml -f docker-compose.chaos.yml up -d')));
    return;
  }
  if (chaos.error) { status.textContent = 'unreachable'; status.className = 'chip bad'; box.replaceChildren(h('p', { class: 'chaos-off' }, chaos.error)); return; }
  const broken = chaos.links.filter(l => !(l.enabled && l.toxics.length === 0)).length;
  status.textContent = broken ? `${broken} degraded` : 'all healthy';
  status.className = broken ? 'chip bad' : 'chip good';
  if (box.contains(document.activeElement)) return;
  box.replaceChildren(
    h('label', { class: 'field' }, h('span', {}, 'Latency ms (± 20 %) · bandwidth KB/s'), h('input', { id: 'chaos-value', type: 'number', min: 1, max: 60000, value: view.chaosValue || 200, oninput: e => { view.chaosValue = Number(e.target.value); } })),
    ...chaos.links.map(link => {
      const state = !link.enabled ? 'down' : link.toxics.length ? link.toxics.map(t => `${t.type}${t.attributes.latency ? ` ${t.attributes.latency} ms` : t.attributes.rate ? ` ${t.attributes.rate} KB/s` : ''}`).join(', ') : 'healthy';
      return h('div', { class: `link${state === 'healthy' ? '' : ' broken'}` },
        h('div', { class: 'link-head' }, h('strong', {}, link.name), h('span', { class: `chip ${state === 'healthy' ? 'good' : 'bad'}` }, state), h('small', {}, link.carries)),
        h('div', { class: 'buttons' },
          ...[['latency', 'Latency'], ['bandwidth', 'Bandwidth'], ['reset', 'Reset'], ['partition', 'Partition'], ['down', 'Down']].map(([fault, label]) =>
            h('button', { type: 'button', onclick: () => chaosAct(link.name, fault) }, label)),
          h('button', { type: 'button', class: 'quiet', onclick: () => chaosAct(link.name, 'heal') }, 'Heal')));
    }));
}

async function chaosAct(link, fault) {
  const value = view.chaosValue || 200;
  const params = fault === 'latency' ? `?value=${value}&jitter=${Math.round(value / 5)}` : fault === 'bandwidth' ? `?value=${value}` : '';
  view.chaos = await api('POST', `/api/resilience/chaos/${link}/${fault}${params}`);
  renderChaos();
}

async function refreshControls() {
  try {
    const c = await (await fetch('/api/resilience/controls')).json();
    const edge = c.edge['order-service'] && !c.edge['order-service'].error ? c.edge['order-service'] : c.edge['order-service-b'];
    if (edge && edge.settings) {
      if (!view.edgeFilled) { fill($('#edge-form'), edge.settings); view.edgeFilled = true; }
      const parts = [];
      for (const [name, e] of Object.entries(c.edge)) parts.push(e.error ? `${name}: unreachable` : `${name}: ${guards(e.settings)}, ${e.inFlight} in flight, ${num(e.active && e.active.active)} unfinished orders`);
      $('#edge-state').textContent = parts.join(' · ');
    }
    const g = c.gateway;
    if (g && !g.error) {
      const breaker = $('#breaker');
      breaker.textContent = `breaker ${g.policy && !g.policy.breaker ? 'off' : g.breakerState.toLowerCase().replace('_', '-')}`;
      breaker.className = `chip ${g.breakerState === 'OPEN' ? 'bad' : g.breakerState === 'HALF_OPEN' ? 'warn' : 'good'}`;
      if (!view.policyFilled && g.policy) { fill($('#policy-form'), g.policy); view.policyFilled = true; }
      $('#gateway-state').textContent = `Gateway ${g.mode}; failure rate ${g.failureRate < 0 ? '—' : `${num(g.failureRate)} %`} over ${g.bufferedCalls} calls; ${g.notPermittedCalls} refused by the breaker; consumer paused by the breaker ${g.consumerPauses} times.`;
    }
    const p = c.payment;
    if (p && !p.error) {
      const consumer = (p.consumers || []).find(x => x.id === 'payment-service');
      const slow = (p.faults || []).find(f => f.name === 'slow-processing');
      const status = $('#consumer-status');
      status.textContent = !consumer ? 'unknown' : !consumer.running ? 'stopped' : consumer.pauseRequested || consumer.paused ? 'paused' : slow ? `slow · ${slow.mode} ms` : 'running';
      status.className = `chip ${consumer && (consumer.pauseRequested || slow) ? 'warn' : 'good'}`;
      const overrides = consumer && consumer.configOverrides ? Object.entries(consumer.configOverrides).map(([k, v]) => `${k}=${v}`).join(', ') : '';
      $('#consumer-state').textContent = consumer ? `Partitions ${consumer.assignedPartitions.join(', ') || 'none'}${overrides ? `; overrides ${overrides}` : '; default poll settings'}.` : '';
    }
  } catch { /* shown by the connection indicator */ }
}

function guards(settings) {
  const on = [];
  if (settings.rateLimit) on.push(`rate ${settings.ratePerSecond}/s`);
  if (settings.shedding) on.push(`shed > ${settings.maxActiveSagas}`);
  if (settings.bulkhead) on.push(`bulkhead ${settings.maxConcurrent}`);
  return on.length ? on.join(', ') : 'guards off';
}

async function refreshRuns() {
  try {
    const runs = await (await fetch('/api/resilience/runs?limit=12')).json();
    const box = $('#runs');
    if (!runs.length) { box.replaceChildren(h('p', { class: 'runs-empty' }, 'No experiment has finished yet.')); return; }
    const open = new Set([...box.querySelectorAll('details[open]')].map(d => d.dataset.id));
    box.replaceChildren(...runs.map(r => {
      const status = r.result.status;
      return h('details', { dataset: { id: String(r.id) }, open: open.has(String(r.id)) },
        h('summary', {}, h('span', { class: `chip ${status === 'passed' ? 'good' : status === 'aborted' ? '' : 'bad'}` }, status), h('strong', {}, r.result.title), h('time', {}, new Date(r.at).toLocaleString())),
        h('div', { class: 'run-body' }, h('p', { class: 'control-note' }, r.summary),
          h('ul', { class: 'checks' }, ...r.result.steps.flatMap(step => step.checks.map(c => h('li', { class: c.status }, h('span', {}, `${PHASE_LABEL[step.phase]}: ${c.claim}`), h('span', { class: 'observed' }, c.observed || '')))))));
    }));
  } catch { /* retried on the next tick */ }
}

function wireControls() {
  const load = $('#load-form');
  load.addEventListener('submit', async event => {
    event.preventDefault();
    view.load = await api('POST', '/api/resilience/load/start', formValues(load), 'Restocking and starting the load…');
    renderLoad();
  });
  $('#load-apply').addEventListener('click', async () => { view.load = await api('PUT', '/api/resilience/load', formValues(load)); renderLoad(); });
  $('#load-stop').addEventListener('click', async () => { view.load = await api('POST', '/api/resilience/load/stop'); renderLoad(); });
  $('#load-reset').addEventListener('click', async () => { view.load = await api('POST', '/api/resilience/load/reset'); renderLoad(); });

  const edge = $('#edge-form');
  edge.addEventListener('submit', async event => { event.preventDefault(); await api('PUT', '/api/resilience/edge', formValues(edge), 'Applying to both replicas…'); refreshControls(); });

  const policy = $('#policy-form');
  policy.addEventListener('submit', async event => { event.preventDefault(); await api('PUT', '/api/resilience/policy', formValues(policy)); refreshControls(); });
  for (const b of document.querySelectorAll('[data-gateway]'))
    b.addEventListener('click', async () => { await api('POST', `/api/resilience/gateway/${b.dataset.gateway}`); refreshControls(); });
  $('#breaker-reset').addEventListener('click', async () => { await api('POST', '/api/resilience/breaker/reset'); refreshControls(); });

  const slow = $('#slow-form');
  slow.addEventListener('submit', async event => { event.preventDefault(); await api('POST', `/api/resilience/slow?service=payment-service&ms=${formValues(slow).ms || 0}`); refreshControls(); });
  $('#slow-clear').addEventListener('click', async () => { await api('POST', '/api/resilience/slow?service=payment-service&ms=0'); refreshControls(); });
  const pollForm = $('#poll-form');
  pollForm.addEventListener('submit', async event => {
    event.preventDefault();
    const v = formValues(pollForm);
    const q = Object.entries(v).filter(([, x]) => x != null).map(([k, x]) => `${k}=${x}`).join('&');
    await api('PUT', `/api/resilience/consumers/payment/config${q ? `?${q}` : ''}`, null, 'Restarting the payment consumer…');
    refreshControls();
  });
  $('#poll-default').addEventListener('click', async () => { await api('PUT', '/api/resilience/consumers/payment/config', null, 'Restarting the payment consumer…'); refreshControls(); });
  for (const b of document.querySelectorAll('[data-consumer]'))
    b.addEventListener('click', async () => { await api('POST', `/api/resilience/consumers/payment/${b.dataset.consumer}`); refreshControls(); });

  $('#reset-lab').addEventListener('click', async () => {
    const result = await api('POST', '/api/resilience/reset', null, 'Resetting every lever and stopping the load…');
    view.edgeFilled = view.policyFilled = false;
    toast(result.failures.length ? `Reset with problems: ${result.failures.join('; ')}` : 'Lab reset: links healed, guards off, default policy, load stopped.', result.failures.length > 0);
    refreshControls();
  });
}

async function init() {
  const grafana = document.body.dataset.grafana;
  if (grafana) { const link = $('#grafana-link'); link.href = `${grafana}/d/zeroshift-resilience`; link.hidden = false; }
  trackHover();
  wireControls();
  try { view.experiments = await (await fetch('/api/resilience/experiments')).json(); } catch { view.experiments = []; }
  renderExperimentList();
  renderExperiment();
  await poll();
  refreshControls();
  refreshRuns();
  setInterval(poll, 1000);
  setInterval(refreshControls, 3000);
  setInterval(refreshRuns, 10000);
  window.addEventListener('resize', renderChain);
}

document.addEventListener('DOMContentLoaded', init);
