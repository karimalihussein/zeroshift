// Race condition lab (/race). Every mark on this page is an event the backend recorded from PostgreSQL: the
// transactions, their reads and writes, the lock waits pg_blocking_pids reported, the aborts from SQLSTATE and
// the committed state the engine read back. The browser never invents a step. What it adds is pacing: a run is
// captured first, then replayed event by event (in recorded order, on one clock for every lane) so the race can
// be watched instead of read. The ruler and the inspector always show the real elapsed time.
'use strict';

const API = '/api/race-lab';
const grafana = document.body.dataset.grafana || '';
const $ = id => document.getElementById(id);
const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)').matches;

// ---- DOM helpers ---------------------------------------------------------------------------------------
function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v === true ? '' : v);
  }
  for (const c of children.flat(Infinity)) if (c !== null && c !== undefined && c !== false) el.append(c instanceof Node ? c : String(c));
  return el;
}
const SVG = 'http://www.w3.org/2000/svg';
function s(tag, attrs = {}, ...children) {
  const el = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v);
  }
  for (const c of children.flat(Infinity)) if (c !== null && c !== undefined && c !== false) el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return el;
}
/** replaceChildren without the nulls and falses that conditional children leave behind. */
const fill = (el, ...kids) => el.replaceChildren(...kids.flat(Infinity).filter(k => k !== null && k !== undefined && k !== false));

const ICONS = {
  play: '<path class="solid" d="M8 5.5v13l10.5-6.5z"/>',
  pause: '<path d="M9 6v12M15 6v12"/>',
  step: '<path class="solid" d="M6 6.5v11l8-5.5z"/><path d="M17.5 6v12"/>',
  back: '<path class="solid" d="M18 6.5v11l-8-5.5z"/><path d="M6.5 6v12"/>',
  replay: '<path d="M4.5 12a7.5 7.5 0 1 0 2.2-5.3"/><path d="M4.5 4.5v4h4"/>',
  chevron: '<path d="M7 10l5 5 5-5"/>',
  arrow: '<path d="M5 12h14M13 6l6 6-6 6"/>',
  close: '<path d="M6.5 6.5l11 11M17.5 6.5l-11 11"/>',
  db: '<ellipse cx="12" cy="6" rx="7" ry="2.8"/><path d="M5 6v12c0 1.6 3.1 2.8 7 2.8s7-1.2 7-2.8V6"/><path d="M5 12c0 1.6 3.1 2.8 7 2.8s7-1.2 7-2.8"/>',
  trace: '<path d="M3.5 12h4l2.2-6 4.6 12 2.2-6h4"/>',
  sliders: '<path d="M4 7h9M17 7h3M4 17h3M11 17h9"/><circle cx="15" cy="7" r="2"/><circle cx="9" cy="17" r="2"/>',
  info: '<circle cx="12" cy="12" r="8.5"/><path d="M12 11v5M12 8h.01"/>',
  alert: '<path d="M12 4 2.8 19.5h18.4z"/><path d="M12 10v4.5M12 17h.01"/>',
  check: '<circle cx="12" cy="12" r="8.5"/><path d="m8.2 12.3 2.6 2.6 5-5.4"/>',
  bolt: '<path class="solid" d="M13.5 3 5 13.5h6L10 21l9-11h-6.2z"/>',
};
function icon(name, cls = '') {
  const el = document.createElementNS(SVG, 'svg');
  el.setAttribute('viewBox', '0 0 24 24');
  el.setAttribute('aria-hidden', 'true');
  el.setAttribute('class', `icon ${cls}`);
  el.innerHTML = ICONS[name];
  return el;
}

const ms = micros => micros == null ? '—' : micros >= 10_000 ? `${Math.round(micros / 1000).toLocaleString()} ms` : `${(micros / 1000).toFixed(1)} ms`;
const offset = micros => `${micros < 0 ? '−' : '+'}${Math.abs(micros / 1000).toFixed(Math.abs(micros) < 10_000 ? 1 : 0)} ms`;
const plural = (n, w) => `${n} ${w}${n === 1 ? '' : 's'}`;
const words = t => t.toLowerCase().replaceAll('_', ' ');
const TYPE = t => words(t).replace(/^./, c => c.toUpperCase());
const humanKey = k => k.replace(/([a-z])([A-Z])/g, '$1 $2').replaceAll('_', ' ').replace(/^./, c => c.toUpperCase());
const explore = (uid, query) => `${grafana}/explore?schemaVersion=1&panes=${encodeURIComponent(JSON.stringify({ z: {
  datasource: uid, range: { from: 'now-6h', to: 'now' },
  queries: [{ refId: 'A', datasource: { type: uid, uid }, ...(uid === 'tempo' ? { queryType: 'traceql', query } : { expr: query }) }] } }))}`;
const traceLink = (traceId, label = 'Trace') => traceId && grafana
  ? h('a', { class: 'ghost-button', href: explore('tempo', traceId), target: '_blank', rel: 'noopener', title: `Trace ${traceId} in Tempo` }, icon('trace'), label) : null;

async function api(path, options = {}) {
  const response = await fetch(API + path, { headers: { 'Content-Type': 'application/json' }, ...options });
  const body = response.status === 204 ? null : await response.json().catch(() => null);
  if (!response.ok) throw new Error(body?.detail || `HTTP ${response.status}`);
  return body;
}

// ---- state ---------------------------------------------------------------------------------------------
const state = {
  experiments: [],
  exp: null,            // ExperimentInfo
  cfg: null,            // the controls
  run: null,            // the run in the hero
  events: new Map(),    // seq -> RaceEvent of that run
  model: null,          // the hero run, laid out for drawing (once it has finished)
  revealed: false,      // the replay has reached the verdict (or the viewer skipped to it)
  history: [],
  busy: false,
  error: '',
  notice: '',
  confirmReset: false,
  advanced: false,
  menu: null,           // 'experiments' | 'about' | null
  stream: null,
  selected: null,       // { run, event } in the inspector
  techOpen: false,
  compare: null,        // { left, right, leftEvents, rightEvents, cmp } | { error }
  pick: { left: null, right: null },
};
const MODE_RETRIES = ['OPTIMISTIC', 'SERIALIZABLE'];
const TERMINAL = ['COMPLETED', 'FAILED'];
const done = run => run && TERMINAL.includes(run.status);
const modeOf = (mode, exp = state.exp) => exp?.modes.find(m => m.mode === mode);
const isolationLabel = i => words(i || '').replace(/^./, c => c.toUpperCase());

function defaults(exp, mode = exp.defaultMode) {
  const l = exp.limits;
  return {
    mode,
    isolation: modeOf(mode, exp).isolation,
    requests: l.defaultRequests,
    initialValue: l.defaultValue,
    delayMs: l.defaultDelayMs,
    interleaving: 'CONTROLLED',
    maxRetries: MODE_RETRIES.includes(mode) ? l.defaultRetries : 0,
  };
}

// ---- boot ----------------------------------------------------------------------------------------------
async function init() {
  if (grafana) { const g = $('grafana-link'); g.href = grafana; g.hidden = false; }
  try {
    const [catalog, lab] = await Promise.all([api('/experiments'), api('/state')]);
    state.experiments = catalog.data;
    connection(true);
    const params = new URLSearchParams(location.search);
    const wanted = state.experiments.find(e => e.id === params.get('experiment')) || state.experiments[0];
    await selectExperiment(wanted.id);
    const runId = Number(params.get('run')) || lab.runningRunId;
    if (runId) await openRun(runId);
  } catch (e) {
    connection(false, e.message);
  }
}

function connection(ok, why) {
  const c = $('connection');
  c.classList.toggle('disconnected', !ok);
  fill(c, h('i'), ok ? ' Connected' : ` ${why || 'Not connected'}`);
}

async function selectExperiment(id) {
  closeStream();
  heroPlayer.clear();
  comparePlayer.clear();
  Object.assign(state, {
    exp: state.experiments.find(e => e.id === id), run: null, events: new Map(), model: null, revealed: false,
    error: '', notice: '', menu: null, selected: null, compare: null, pick: { left: null, right: null }, techOpen: false,
  });
  state.cfg = defaults(state.exp);
  updateUrl();
  await loadHistory();
  renderAll();
}

async function loadHistory() {
  try { state.history = (await api(`/runs?experiment=${state.exp.id}&limit=20`)).data; } catch { state.history = []; }
}

function updateUrl() {
  const q = new URLSearchParams({ experiment: state.exp.id });
  if (state.run) q.set('run', state.run.id);
  history.replaceState(null, '', `?${q}`);
}

// ---- running -------------------------------------------------------------------------------------------
async function runRace(cfg = state.cfg, compareWith = null) {
  if (state.busy) return;
  Object.assign(state, { busy: true, error: '', notice: '', confirmReset: false, selected: null, revealed: false, model: null, techOpen: false });
  if (compareWith) state.compare = null;
  heroPlayer.clear();
  renderControls(); renderInspector();
  try {
    const created = await api('/runs', { method: 'POST', body: JSON.stringify({ experiment: state.exp.id, ...cfg }) });
    state.run = created;
    state.events = new Map();
    updateUrl();
    renderHero(); renderOutcome();
    $('hero').scrollIntoView({ behavior: reducedMotion ? 'auto' : 'smooth', block: 'start' });
    state.run = await api(`/runs/${created.id}/start`, { method: 'POST' });
    follow(created.id, compareWith);
  } catch (e) {
    state.busy = false; state.error = e.message;
    renderAll();
  }
}

function closeStream() {
  if (state.stream) { state.stream.close(); state.stream = null; }
}

function follow(id, compareWith) {
  closeStream();
  const es = new EventSource(`${API}/runs/${id}/stream`);
  state.stream = es;
  es.addEventListener('event', m => {
    const e = JSON.parse(m.data);
    if (state.run?.id !== e.runId) return;
    state.events.set(e.seq, e);
    scheduleCapture();
  });
  es.addEventListener('run', m => {
    const run = JSON.parse(m.data);
    if (state.run?.id !== run.id) return;
    state.run = run;
    if (done(run)) { es.close(); state.stream = null; finished(compareWith); }
  });
  es.onerror = () => {
    if (state.stream !== es) return;
    es.close(); state.stream = null;
    if (!done(state.run)) poll(id, compareWith);
  };
}

/** Fallback when the event stream drops: the same data, read back by polling. */
async function poll(id, compareWith) {
  while (state.run?.id === id && !done(state.run)) {
    try {
      const after = Math.max(0, ...state.events.keys());
      const [run, events] = await Promise.all([api(`/runs/${id}`), api(`/runs/${id}/events?after=${after}`)]);
      for (const e of events.data) state.events.set(e.seq, e);
      state.run = run;
      scheduleCapture();
    } catch { /* keep trying */ }
    await new Promise(r => setTimeout(r, 400));
  }
  if (state.run?.id === id) finished(compareWith);
}

let captureFrame = 0;
function scheduleCapture() {
  if (captureFrame) return;
  captureFrame = requestAnimationFrame(() => { captureFrame = 0; renderCapture(); });
}

async function finished(compareWith) {
  try {
    const [run, events] = await Promise.all([api(`/runs/${state.run.id}`), api(`/runs/${state.run.id}/events?limit=5000`)]);
    state.run = run;
    for (const e of events.data) state.events.set(e.seq, e);
  } catch { /* the streamed copy stands */ }
  state.busy = false;
  await loadHistory();
  showRun({ autoplay: true });
  if (compareWith && state.run.status === 'COMPLETED') {
    state.pick = { left: compareWith, right: state.run.id };
    await loadCompare({ quiet: true });
  }
}

async function openRun(id) {
  try {
    const [run, events] = await Promise.all([api(`/runs/${id}`), api(`/runs/${id}/events?limit=5000`)]);
    if (run.experiment !== state.exp?.id) await selectExperiment(run.experiment);
    state.run = run;
    state.events = new Map(events.data.map(e => [e.seq, e]));
    updateUrl();
    if (!done(run)) { state.busy = true; renderAll(); follow(id, null); return; }
    showRun({ autoplay: false });
  } catch (e) {
    state.error = e.message; renderControls();
  }
}

/** A finished run: lay it out, draw it, and replay it (or park the playhead at the end). */
function showRun({ autoplay }) {
  const run = state.run;
  state.model = run.status === 'COMPLETED' ? buildModel(run, [...state.events.values()]) : null;
  state.revealed = !autoplay || reducedMotion || !state.model;
  renderAll();
  if (state.model) heroPlayer.begin(autoplay && !reducedMotion);
}

// ---- 1. experiment header ------------------------------------------------------------------------------
function renderExperiment() {
  const e = state.exp;
  const menu = state.menu;
  fill($('experiment'),
    h('div', { class: 'xp-title' },
      h('span', { class: 'kicker' }, 'Race condition lab'),
      h('div', { class: 'xp-picker' },
        h('button', { type: 'button', class: 'xp-button', 'aria-haspopup': 'listbox', 'aria-expanded': String(menu === 'experiments'),
          onclick: ev => { ev.stopPropagation(); state.menu = menu === 'experiments' ? null : 'experiments'; renderExperiment(); } },
          h('b', {}, String(e.number).padStart(2, '0')), h('h1', {}, e.title), icon('chevron')),
        menu === 'experiments' ? h('div', { class: 'xp-menu', role: 'listbox', 'aria-label': 'Experiments' },
          state.experiments.map(x => h('button', { type: 'button', role: 'option', 'aria-selected': String(x.id === e.id),
            onclick: () => { state.menu = null; if (x.id !== e.id) selectExperiment(x.id); else renderExperiment(); } },
            h('b', {}, String(x.number).padStart(2, '0')), h('span', {}, h('strong', {}, x.title), h('small', {}, x.category))))) : null),
      h('p', { class: 'xp-question' }, e.question)),
    h('div', { class: 'xp-side' },
      h('div', { class: 'invariant-chip', title: 'Checked against the committed rows after every request finished' },
        h('span', {}, 'Invariant'), h('strong', {}, e.invariant)),
      h('div', { class: 'about' },
        h('button', { type: 'button', class: 'ghost-button', 'aria-expanded': String(menu === 'about'),
          onclick: ev => { ev.stopPropagation(); state.menu = menu === 'about' ? null : 'about'; renderExperiment(); } }, icon('info'), 'About'),
        menu === 'about' ? h('div', { class: 'about-pop', onclick: ev => ev.stopPropagation() },
          h('p', { class: 'kicker' }, e.category),
          h('p', {}, e.unsafeStory),
          h('p', { class: 'muted' }, e.learn),
          modeOf(state.cfg.mode) ? h('div', {}, h('p', { class: 'kicker' }, `${modeOf(state.cfg.mode).label} · the SQL`), h('pre', { class: 'code' }, modeOf(state.cfg.mode).sql)) : null) : null)));
}
document.addEventListener('click', () => { if (state.menu) { state.menu = null; renderExperiment(); } });

// ---- 2. controls ---------------------------------------------------------------------------------------
function renderControls() {
  const e = state.exp, l = e.limits, c = state.cfg, busy = state.busy;
  const set = patch => { Object.assign(state.cfg, patch); state.confirmReset = false; renderControls(); if (state.menu === 'about') renderExperiment(); };
  const number = (label, value, min, max, onchange, suffix) => h('label', { class: 'ctl' },
    h('span', {}, label),
    h('span', { class: 'num-input' },
      h('input', { type: 'number', value, min, max, disabled: busy, inputmode: 'numeric',
        onchange: ev => onchange(Math.max(min, Math.min(max, Math.round(Number(ev.target.value)) || min))) }),
      suffix ? h('i', {}, suffix) : null));
  const select = (label, value, options, onchange, disabled) => h('label', { class: 'ctl' },
    h('span', {}, label),
    h('span', { class: 'select' },
      h('select', { disabled: busy || disabled, onchange: ev => onchange(ev.target.value) },
        options.map(([v, text]) => h('option', { value: v, selected: v === value }, text))),
      icon('chevron')));
  const m = modeOf(c.mode);
  const armed = m && !m.fixes;
  const forced = c.mode === 'SERIALIZABLE';

  fill($('controls'),
    h('div', { class: 'ctl-row' },
      select('Strategy', c.mode, e.modes.map(x => [x.mode, `${x.label}${x.fixes ? '' : ' · breaks'}`]),
        mode => set({ mode, isolation: modeOf(mode).isolation, maxRetries: MODE_RETRIES.includes(mode) ? l.defaultRetries : 0 })),
      select('Isolation', c.isolation, ['READ_COMMITTED', 'REPEATABLE_READ', 'SERIALIZABLE'].map(i => [i, isolationLabel(i)]),
        isolation => set({ isolation }), forced),
      l.minValue !== l.maxValue ? number(e.valueLabel.replace(/^Initial /, '').replace(/^./, x => x.toUpperCase()), c.initialValue, l.minValue, l.maxValue, v => set({ initialValue: v })) : null,
      l.minRequests !== l.maxRequests
        ? number('Requests', c.requests, l.minRequests, l.maxRequests, v => set({ requests: v }))
        : h('div', { class: 'ctl fixed' }, h('span', {}, 'Requests'), h('strong', {}, e.roles.length ? e.roles.join(' · ') : l.minRequests)),
      number('Delay', c.delayMs, 0, 5000, v => set({ delayMs: v }), 'ms'),
      h('button', { type: 'button', class: 'ghost-button adv-toggle', 'aria-expanded': String(state.advanced), onclick: () => { state.advanced = !state.advanced; renderControls(); } },
        icon('sliders'), 'Advanced'),
      h('button', { type: 'submit', class: `primary-action run-button${armed ? ' armed' : ''}`, disabled: busy,
        title: armed ? `${m.label} can break the invariant: that is the point` : null },
        busy ? h('span', { class: 'spinner', 'aria-hidden': 'true' }) : icon('play'), busy ? 'Running…' : 'Run experiment')),
    state.advanced ? h('div', { class: 'ctl-advanced' },
      h('div', { class: 'ctl' }, h('span', {}, 'Interleaving'),
        h('div', { class: 'seg', role: 'group', 'aria-label': 'Interleaving' },
          [['CONTROLLED', 'Controlled'], ['NATURAL', 'Natural']].map(([v, t]) =>
            h('button', { type: 'button', 'aria-pressed': String(c.interleaving === v), disabled: busy, onclick: () => set({ interleaving: v }) }, t)))),
      number('Retries after an abort', c.maxRetries, 0, 10, v => set({ maxRetries: v })),
      h('p', { class: 'ctl-note' }, c.interleaving === 'CONTROLLED'
        ? 'Controlled: requests take turns at the read and meet before writing, so the dangerous schedule happens every run. SQL, locks, versions and aborts stay PostgreSQL\'s; the lab only picks when each request sends its next statement (hatched on the timeline).'
        : 'Natural: every request starts at once and the scheduler decides. With no delay the race window is tiny and the bug often hides.'),
      h('button', { type: 'button', class: state.confirmReset ? 'danger' : 'ghost-button', disabled: busy, title: 'Deletes the lab\'s runs and scenario rows (schema race_lab only)', onclick: resetLab },
        state.confirmReset ? 'Confirm: delete every run' : 'Reset lab data')) : null,
    state.error || state.notice ? h('p', { class: `ctl-message${state.error ? ' error' : ''}`, role: 'alert' }, state.error || state.notice) : null);
}
$('controls').addEventListener('submit', ev => { ev.preventDefault(); runRace(); });

async function resetLab() {
  if (!state.confirmReset) { state.confirmReset = true; renderControls(); return; }
  try {
    const r = await api('/reset', { method: 'POST' });
    const total = Object.values(r.deleted).reduce((a, b) => a + b, 0);
    await selectExperiment(state.exp.id);
    state.notice = `Deleted ${total} lab rows; run ids restart at 1.`;
    renderControls();
  } catch (e) { state.error = e.message; state.confirmReset = false; renderControls(); }
}

// ---- model: a finished run, ready to draw --------------------------------------------------------------
const verb = sql => (sql || '').trim().split(/\s+/)[0].toUpperCase() || 'WRITE';
const clip = (t, n = 18) => !t ? '' : t.length > n ? t.slice(0, n - 1) + '…' : t;

/** The pill an event draws on its lane: [verb, value, tone], or null when it draws something else. */
function pillOf(e, outcomes) {
  switch (e.type) {
    case 'TRANSACTION_STARTED': return [e.attempt > 1 ? `BEGIN #${e.attempt}` : 'BEGIN', '', 'neutral'];
    case 'READ_PERFORMED': return [e.data?.purpose === 'diagnose' ? 'RE-READ' : 'READ', clip(e.valueRead || e.target), 'read'];
    case 'DECISION_MADE': return ['CHECK', e.ok ? '✓' : '✗', e.ok ? 'check-ok' : 'check-no'];
    case 'LOCK_ACQUIRED': return [e.waitMicros > 0 ? 'GOT LOCK' : '', '', 'lock'];
    case 'WRITE_PERFORMED': {
      const v = verb(e.sql);
      if (e.rows === 0) return [v, '0 rows', 'reject'];
      return v === 'INSERT' ? ['INSERT', clip((e.target || '').split(' ')[0], 12), 'write'] : ['WRITE', clip(e.valueWritten || e.target), 'write'];
    }
    case 'VERSION_CONFLICT': return ['CONFLICT', `v${e.expectedVersion}≠v${e.actualVersion}`, 'bad'];
    case 'SERIALIZATION_FAILURE': return ['ABORT', '40001', 'bad'];
    case 'DEADLOCK_DETECTED': return ['DEADLOCK', '40P01', 'bad'];
    case 'TRANSACTION_COMMITTED': return ['COMMIT', '', 'good'];
    case 'TRANSACTION_ROLLED_BACK': {
      const final = outcomes[e.lane];
      return final?.outcome === 'REJECTED' && final.attempt === e.attempt ? ['REJECTED', '', 'reject'] : ['ROLLBACK', '', 'bad'];
    }
    case 'RETRY_SCHEDULED': return ['RETRY', '', 'wait'];
    default: return null;
  }
}

/** One sentence per step, for the caption above the lanes. Built from the event's own fields. */
function narrate(e) {
  const L = e.lane;
  switch (e.type) {
    case 'TRANSACTION_STARTED': return `${L} begins transaction ${e.txId ?? ''}${e.attempt > 1 ? ` (attempt ${e.attempt})` : ''}`;
    case 'READ_PERFORMED': return `${L} reads ${e.valueRead || e.target}`;
    case 'DECISION_MADE': return `${L} checks ${e.target}: ${e.ok ? '✓' : '✗'} ${e.message || ''}`;
    case 'LOCK_ACQUIRED': return e.waitMicros > 0 ? `${L} gets the ${e.lock} after waiting ${ms(e.waitMicros)}` : `${L} takes the ${e.lock}`;
    case 'TRANSACTION_BLOCKED': return `${L} is blocked: ${e.message}`;
    case 'WRITE_PERFORMED': return e.rows === 0 ? `${L}'s ${verb(e.sql)} matches 0 rows${e.message ? `: ${e.message}` : ''}`
      : `${L} writes ${e.valueWritten || e.target}, not committed yet${e.waitMicros ? ` (the statement waited ${ms(e.waitMicros)} for the lock)` : ''}`;
    case 'VERSION_CONFLICT': return `${L} is rejected: expected version ${e.expectedVersion}, actual ${e.actualVersion}`;
    case 'SERIALIZATION_FAILURE': return `PostgreSQL aborts ${L} to keep the schedule serializable (40001)`;
    case 'DEADLOCK_DETECTED': return `PostgreSQL finds a deadlock and aborts ${L} (40P01)`;
    case 'TRANSACTION_COMMITTED': return `${L} commits${e.message ? `: ${e.message}` : ''}`;
    case 'TRANSACTION_ROLLED_BACK': return `${L} rolls back${e.message ? `: ${e.message}` : ''}`;
    case 'RETRY_SCHEDULED': return `${L} starts over${e.message ? `: ${e.message}` : ''}`;
    case 'STATE_OBSERVED': return `Database, committed: ${e.valueWritten}`;
    case 'INVARIANT_CHECKED': return e.ok ? `Invariant preserved: ${e.valueWritten}` : `Invariant violated: ${e.valueWritten}`;
    default: return TYPE(e.type);
  }
}

const RACE_KINDS = { STALE_WRITE: 'RACE OCCURS HERE', CHANGED_UNDER_READER: 'RACE OCCURS HERE', DEADLOCK: 'DEADLOCK', VERSION_REJECTED: 'CONFLICT', SERIALIZATION_FAILURE: 'ABORTED' };

function buildModel(run, list) {
  const events = [...list].sort((a, b) => a.atMicros - b.atMicros || a.seq - b.seq);
  const lanes = (run.requests || []).map(r => r.lane);
  const laneSet = new Set(lanes);
  const t0 = (events.find(e => e.type === 'TRANSACTION_STARTED') || events[0])?.atMicros ?? 0;
  const T = micros => micros - t0;
  const bySeq = new Map(events.map(e => [e.seq, e]));
  const outcomes = {};
  for (const e of events) if (e.type === 'REQUEST_COMPLETED') outcomes[e.lane] = e;

  const pills = [], spans = [], txs = [], waits = [], steps = [], dbStates = [];
  const open = {}, blocked = {};
  const unblocked = new Set(events.filter(e => e.type === 'TRANSACTION_UNBLOCKED').map(e => `${e.lane}#${e.attempt}`));
  for (const e of events) {
    const t = T(e.atMicros);
    const end = e.durationMicros ? t + e.durationMicros : t;
    if (laneSet.has(e.lane)) {
      const key = `${e.lane}#${e.attempt}`;
      if (e.type === 'TRANSACTION_STARTED') open[key] = { lane: e.lane, attempt: e.attempt, from: t, begin: e };
      if ((e.type === 'TRANSACTION_COMMITTED' || e.type === 'TRANSACTION_ROLLED_BACK') && open[key]) {
        txs.push({ ...open[key], to: end, end: e.type === 'TRANSACTION_COMMITTED' ? 'committed' : 'rolled', last: e });
        delete open[key];
      }
      if ((e.type === 'DELAY' || e.type === 'SYNC_POINT') && e.durationMicros > 0)
        spans.push({ lane: e.lane, from: t, to: end, kind: e.type === 'DELAY' ? 'delay' : 'sync', e });
      if (e.type === 'TRANSACTION_BLOCKED' && !e.message?.startsWith('now')) {
        if (!unblocked.has(key)) blocked[key] = e;
        else { const w = waits.find(x => x.lane === e.lane && x.e.type === 'TRANSACTION_UNBLOCKED' && x.e.attempt === e.attempt); if (w) w.e = e; }
      }
      if (e.type === 'TRANSACTION_UNBLOCKED') {
        waits.push({ lane: e.lane, from: t, to: end, by: e.blockedBy || [], micros: e.waitMicros ?? e.durationMicros, e });
      }
      const p = pillOf(e, outcomes);
      if (p) pills.push({ e, lane: e.lane, t, verb: p[0], value: p[1], tone: p[2], minor: e.type === 'TRANSACTION_STARTED' || (e.type === 'LOCK_ACQUIRED' && !(e.waitMicros > 0)) });
      if (p || e.type === 'TRANSACTION_BLOCKED') steps.push({ t, e, text: narrate(e), tone: p?.[2] || 'wait' });
    }
    if (e.type === 'STATE_OBSERVED') { dbStates.push({ t, e }); steps.push({ t, e, text: narrate(e), tone: 'db' }); }
    if (e.type === 'INVARIANT_CHECKED') {
      pills.push({ e, lane: 'DB', t, verb: e.ok ? '✓ INVARIANT PRESERVED' : '✗ INVARIANT VIOLATED', value: '', tone: e.ok ? 'good' : 'bad', verdict: true });
      steps.push({ t, e, text: narrate(e), tone: e.ok ? 'good' : 'bad' });
    }
  }
  for (const w of Object.values(blocked)) waits.push({ lane: w.lane, from: T(w.atMicros), to: T(w.atMicros), by: w.blockedBy || [], micros: 0, e: w });
  // Committed states are pills on the database lane too.
  for (const d of dbStates) pills.push({ e: d.e, lane: 'DB', t: d.t, verb: '', value: clip(d.e.valueWritten, 40), tone: 'db' });
  pills.sort((a, b) => a.t - b.t || a.e.seq - b.e.seq);
  steps.sort((a, b) => a.t - b.t || a.e.seq - b.e.seq);

  // Key moments, from the backend's analysis. The race is the first bad one after the shared read.
  const highlights = (run.result?.highlights || []).map(m => ({ ...m, t: T(m.atMicros), until: m.untilMicros != null ? T(m.untilMicros) : null }));
  const shared = highlights.find(m => m.kind === 'SAME_VALUE_READ');
  const race = highlights.find(m => RACE_KINDS[m.kind] && m.tone === 'bad') || null;
  const violated = run.result && !run.result.invariant.holds;
  // The race window: from the shared read until the first of those readers commits.
  let window = null;
  if (shared && violated) {
    const readers = new Set(shared.lanes);
    const readEnd = Math.max(...(shared.seqs || []).map(q => bySeq.get(q)).filter(Boolean).map(e => T(e.atMicros)), shared.t);
    const commit = pills.find(p => readers.has(p.lane) && p.e.type === 'TRANSACTION_COMMITTED' && p.t > readEnd);
    if (commit) window = { from: shared.t, to: commit.t, lanes: shared.lanes };
  }

  const times = [...pills.map(p => p.t), ...spans.flatMap(x => [x.from, x.to]), ...waits.flatMap(w => [w.from, w.to]),
    ...txs.flatMap(x => [x.from, x.to]), ...Object.values(outcomes).map(o => T(o.atMicros))];
  return { run, events, bySeq, lanes, t0, T, pills, spans, txs, waits, steps, dbStates, outcomes, highlights, shared, race, window, violated, times };
}

// ---- time scale: one clock for every lane (and for both runs in a comparison) --------------------------
const measure = (() => {
  const ctx = document.createElement('canvas').getContext('2d');
  return (text, font) => { ctx.font = font; return ctx.measureText(text).width; };
})();
const SANS = '"Aptos", "Segoe UI Variable", "SF Pro Display", "Inter", sans-serif';
const MONO = '"SFMono-Regular", "Cascadia Code", "Roboto Mono", monospace';
/** What a pill says. Side by side, values shrink to what changed: READ 1, WRITE 0. */
function labelOf(p, compact) {
  if (!compact || p.lane === 'DB') return { verb: p.verb, value: p.value };
  const m = /^[a-z_]+=(.+)$/i.exec(p.value || '');
  return { verb: p.verb, value: p.e.type === 'WRITE_PERFORMED' && p.verb === 'INSERT' ? '' : m ? m[1] : p.value };
}
const pillsOf = (m, compact) => compact ? m.pills.filter(p => !p.minor) : m.pills;
function pillWidth(p, compact) {
  const size = compact ? 10.5 : 11;
  const { verb: vb, value } = labelOf(p, compact);
  if (!vb && !value && p.tone === 'lock') return 26;
  const v = vb ? measure(vb, `700 ${size}px ${SANS}`) + vb.length * .5 : 0;
  const val = value ? measure(value, `500 ${size}px ${MONO}`) : 0;
  return Math.ceil(v + val + (vb && value ? 5 : 0) + (compact ? 14 : 18) + (p.tone === 'lock' ? 13 : 0));
}

/**
 * Sequence spacing: every step gets room for its label, and gaps grow with the real time between them, so the
 * order and the overlap are exact while a 700 ms lab hold does not push everything off screen. The ruler keeps
 * the real elapsed time. Several models share the scale by time since their first BEGIN.
 */
function buildScale(models, avail, compact) {
  const PAD = compact ? 18 : 26;
  const keys = [...new Set(models.flatMap(m => m.times))].sort((a, b) => a - b);
  if (!keys.length) return { xOf: () => PAD, tOf: () => 0, width: avail, start: PAD, end: PAD };
  const xs = new Map();
  let x = PAD, prev = null;
  for (const t of keys) {
    if (prev !== null) { const dt = (t - prev) / 1000; x += dt <= 0.02 ? 1 : 4 + 18 * Math.log10(1 + dt); }
    xs.set(t, x); prev = t;
  }
  const shift = (from, by) => { for (const t of keys) if (t >= from) xs.set(t, xs.get(t) + by); };
  // Labels on one lane never overlap: each pill starts after the previous one ends.
  let tail = 0;
  for (const m of models) {
    const last = {};
    for (const p of pillsOf(m, compact)) {
      p.w = pillWidth(p, compact);
      const prevPill = last[p.lane];
      const need = prevPill ? xs.get(prevPill.t) + prevPill.w / 2 + p.w / 2 + (compact ? 5 : 7) : PAD + p.w / 2;
      const cur = xs.get(p.t);
      if (cur < need) shift(p.t, need - cur);
      last[p.lane] = p;
    }
    for (const p of Object.values(last)) tail = Math.max(tail, xs.get(p.t) + p.w / 2);
  }
  const first = xs.get(keys[0]);
  let lastX = Math.max(xs.get(keys[keys.length - 1]), tail);
  // Short runs stretch to the panel; long ones scroll.
  const room = avail - PAD - 12;
  if (lastX < room) {
    const k = Math.min(2.6, (room - first) / Math.max(1, lastX - first));
    for (const t of keys) xs.set(t, first + (xs.get(t) - first) * k);
    lastX = first + (lastX - first) * k;
  }
  const xOf = t => {
    if (xs.has(t)) return xs.get(t);
    if (t <= keys[0]) return xs.get(keys[0]);
    if (t >= keys[keys.length - 1]) return xs.get(keys[keys.length - 1]);
    let lo = 0, hi = keys.length - 1;
    while (hi - lo > 1) { const mid = (lo + hi) >> 1; keys[mid] <= t ? lo = mid : hi = mid; }
    return xs.get(keys[lo]) + (xs.get(keys[hi]) - xs.get(keys[lo])) * (t - keys[lo]) / (keys[hi] - keys[lo]);
  };
  const tOf = px => {
    if (px <= xs.get(keys[0])) return keys[0];
    for (let i = 1; i < keys.length; i++) {
      const a = xs.get(keys[i - 1]), b = xs.get(keys[i]);
      if (px <= b) return b === a ? keys[i] : keys[i - 1] + (keys[i] - keys[i - 1]) * (px - a) / (b - a);
    }
    return keys[keys.length - 1];
  };
  return { xOf, tOf, width: Math.max(avail, lastX + 20), start: first - 4, end: lastX };
}

// ---- swimlanes -----------------------------------------------------------------------------------------
const TOP = 72;           // annotations and the ruler
const LABEL_W = 168, LABEL_W_COMPACT = 104;
const TOP_COMPACT = 56;
const laneHeight = (n, compact) => compact ? (n <= 2 ? 62 : n <= 5 ? 50 : 38) : (n <= 2 ? 96 : n <= 4 ? 80 : n <= 8 ? 60 : 44);
const OUTCOME = { SUCCEEDED: ['succeeded', 'good'], REJECTED: ['declined', 'reject'], ABORTED: ['aborted', 'bad'], FAILED: ['failed', 'bad'] };

/**
 * Draws a model's lanes once; update(x) then reveals everything up to the playhead. Nothing is recomputed per
 * frame: pills and annotations toggle a class, bars and spans grow through one clip rectangle.
 */
function drawLanes(host, model, scale, { compact = false, onPick = null, id = 'hero' } = {}) {
  const n = model.lanes.length;
  const LANE = laneHeight(n, compact);
  const top = compact ? TOP_COMPACT : TOP;
  const DBH = compact ? 50 : 64;
  const width = scale.width;
  const height = top + n * LANE + DBH;
  const laneIdx = Object.fromEntries(model.lanes.map((l, i) => [l, i]));
  const cy = lane => lane === 'DB' ? top + n * LANE + DBH / 2 : top + laneIdx[lane] * LANE + LANE / 2;
  const X = t => scale.xOf(t);
  const PH = compact ? 20 : 24;
  const reveal = [];   // { x, el, pill? }
  const at = (x, el, extra = {}) => { reveal.push({ x, el, ...extra }); return el; };

  const bg = [], grow = [], over = [], marks = [], notes = [], ruler = [];

  // lanes
  model.lanes.forEach((l, i) => {
    bg.push(s('rect', { class: `lane-bg${i % 2 ? ' alt' : ''}`, x: 0, y: top + i * LANE, width, height: LANE }));
    bg.push(s('line', { class: 'lane-guide', x1: 0, x2: width, y1: cy(l), y2: cy(l) }));
  });
  bg.push(s('rect', { class: 'lane-bg db', x: 0, y: top + n * LANE, width, height: DBH }));
  bg.push(s('line', { class: 'lane-guide db', x1: 0, x2: width, y1: cy('DB'), y2: cy('DB') }));
  bg.push(s('line', { class: 'ruler-rule', x1: 0, x2: width, y1: top, y2: top }));

  // race window: behind everything, across the readers' lanes
  if (model.window) {
    const ys = model.window.lanes.filter(l => l in laneIdx).map(l => laneIdx[l]);
    const x1 = X(model.window.from), x2 = X(model.window.to);
    const y1 = top + Math.min(...ys) * LANE + 6, y2 = top + (Math.max(...ys) + 1) * LANE - 6;
    grow.push(s('rect', { class: 'race-window', x: x1, y: y1, width: Math.max(4, x2 - x1), height: y2 - y1, rx: 10 }));
    notes.push({ x: x1, text: 'RACE WINDOW', tone: 'bad', icon: true, y2: y1 });
  }

  // transactions: a track from BEGIN to COMMIT / ROLLBACK
  for (const t of model.txs) {
    const x1 = X(t.from), x2 = Math.max(X(t.to), x1 + 6), y = cy(t.lane);
    grow.push(s('rect', { class: `tx ${t.end}`, x: x1, y: y - 3, width: x2 - x1, height: 6, rx: 3 }));
  }
  // application work (dotted) and lab choreography (hatched), under the track
  for (const sp of model.spans) {
    const x1 = X(sp.from), x2 = X(sp.to), y = cy(sp.lane) + PH / 2 + 8;
    if (x2 - x1 < 3) continue;
    grow.push(s('g', { class: `span ${sp.kind}` },
      s('title', {}, sp.kind === 'delay' ? `Application work between read and write: ${ms(sp.e.durationMicros)}` : `Lab choreography: ${sp.e.message} (${ms(sp.e.durationMicros)})`),
      s('line', { x1: x1 + 2, x2: x2 - 2, y1: y, y2: y }),
      !compact && sp.kind === 'delay' && x2 - x1 > 90 ? s('text', { x: (x1 + x2) / 2, y: y + 13, 'text-anchor': 'middle' }, `app work ${ms(sp.e.durationMicros)}`) : null));
  }
  // lock waits: an amber band, and an arrow to the transaction holding the lock
  for (const w of model.waits) {
    const x1 = X(w.from), x2 = Math.max(X(w.to), x1 + 8), y = cy(w.lane);
    grow.push(s('rect', { class: 'wait-band', x: x1, y: y - PH / 2 - 7, width: x2 - x1, height: PH + 14, rx: 12 }));
    const label = `waits for ${w.by.join(', ') || 'a lock'}${w.micros ? ` · ${ms(w.micros)}` : ''}`;
    over.push(at(x1, s('text', { class: 'wait-label', x: x1 + 4, y: y - PH / 2 - 12 }, label)));
    for (const other of w.by) if (other in laneIdx) {
      const yo = cy(other), dir = yo > y ? 1 : -1;
      over.push(at(x1, s('path', { class: 'wait-arrow', d: `M ${x1} ${y + dir * (PH / 2 + 7)} C ${x1 - 26} ${y + dir * 30}, ${x1 - 26} ${yo - dir * 30}, ${x1 - 4} ${yo - dir * (PH / 2 + 2)}`, 'marker-end': `url(#arrow-${id})` },
        s('title', {}, `${w.lane} waits for ${other}'s lock`))));
    }
  }

  // shared read: both readers, bracketed
  if (model.shared) {
    const readPills = (model.shared.seqs || []).map(q => model.pills.find(p => p.e.seq === q)).filter(p => p?.w);
    if (readPills.length > 1) {
      const xl = Math.min(...readPills.map(p => X(p.t) - p.w / 2)) - 7, xr = Math.max(...readPills.map(p => X(p.t) + p.w / 2)) + 7;
      const yt = Math.min(...readPills.map(p => cy(p.lane))) - PH / 2 - 7, yb = Math.max(...readPills.map(p => cy(p.lane))) + PH / 2 + 7;
      const xLast = Math.max(...readPills.map(p => X(p.t)));
      over.push(at(xLast, s('rect', { class: `shared-read${model.shared.tone === 'bad' ? ' bad' : ''}`, x: xl, y: yt, width: xr - xl, height: yb - yt, rx: 14 }), { flash: true }));
      notes.push({ x: xl + 4, at: xLast, text: model.shared.title.toUpperCase(), tone: 'read', y2: yt });
    }
  }

  // the race itself: a line through the lanes where it happens
  if (model.race) {
    const x = X(model.race.t);
    const involved = (model.race.lanes || []).filter(l => l in laneIdx).map(l => laneIdx[l]);
    const y2 = involved.length ? top + (Math.max(...involved) + 1) * LANE : top + n * LANE;
    over.push(at(x, s('line', { class: 'race-line', x1: x, x2: x, y1: top, y2 }), { flash: true }));
    notes.push({ x, text: RACE_KINDS[model.race.kind], tone: 'bad', icon: true, y2: top });
  }

  // pills
  const tooltip = p => `${p.lane === 'DB' ? 'Database' : p.lane} · ${narrate(p.e)}\n${offset(p.t)} · click for details`;
  for (const p of pillsOf(model, compact)) {
    const x = X(p.t), y = cy(p.lane);
    const lb = labelOf(p, compact);
    const iconOnly = !lb.verb && !lb.value;
    const inRace = model.race && (model.race.seqs || []).includes(p.e.seq) && p.lane === model.race.lanes?.[0] && ['WRITE_PERFORMED', 'VERSION_CONFLICT', 'DEADLOCK_DETECTED', 'READ_PERFORMED'].includes(p.e.type) && model.race.kind !== 'SAME_VALUE_READ';
    const body = s('g', { class: 'pill-body' },
      s('rect', { x: -p.w / 2, y: -PH / 2, width: p.w, height: PH, rx: PH / 2 }),
      p.tone === 'lock' ? s('path', { class: 'lock-glyph', d: iconOnly ? 'M -4 -1 h 8 v 6 h -8 z M -2.5 -1 v -2.5 a 2.5 2.5 0 0 1 5 0 v 2.5' : `M ${-p.w / 2 + 9} ${-1} h 8 v 6 h -8 z M ${-p.w / 2 + 10.5} ${-1} v -2.5 a 2.5 2.5 0 0 1 5 0 v 2.5` }) : null,
      iconOnly ? null : s('text', { x: p.tone === 'lock' ? 6 : 0, y: compact ? 3.6 : 4, 'text-anchor': 'middle' },
        lb.verb ? s('tspan', { class: 'verb' }, lb.verb) : null,
        lb.value ? s('tspan', { class: `val${lb.value === '✓' ? ' ok' : lb.value === '✗' ? ' no' : ''}`, dx: lb.verb ? 5 : 0 }, lb.value) : null));
    const g = s('g', { class: `pill tone-${p.tone}${inRace ? ' race' : ''}${p.verdict ? ' verdict' : ''}`, transform: `translate(${x} ${y})`, tabindex: onPick ? 0 : -1, role: onPick ? 'button' : null,
      'data-seq': p.e.seq, 'aria-label': `${p.lane}: ${narrate(p.e)}`,
      onclick: onPick ? () => onPick(p.e) : null,
      onkeydown: onPick ? ev => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); ev.stopPropagation(); onPick(p.e); } } : null },
    s('title', {}, tooltip(p)), body);
    marks.push(at(x, g, { pill: p }));
  }

  // annotations above the ruler: two rows, never overlapping
  const rowEnd = [-Infinity, -Infinity];
  for (const note of notes.sort((a, b) => a.x - b.x)) {
    const size = compact ? 10 : 11;
    const w = measure(note.text, `750 ${size}px ${SANS}`) + note.text.length * .6 + (note.icon ? 30 : 18);
    const row = note.x - 4 > rowEnd[0] ? 0 : note.x - 4 > rowEnd[1] ? 1 : 0;
    rowEnd[row] = note.x + w;
    const y = (compact ? 6 : 8) + row * (compact ? 18 : 22);
    const hgt = compact ? 16 : 19;
    ruler.push(at(note.at ?? note.x, s('g', { class: `note tone-${note.tone}` },
      s('line', { class: 'note-lead', x1: note.x, x2: note.x, y1: y + hgt, y2: note.y2 }),
      s('rect', { x: note.x - 2, y, width: w, height: hgt, rx: 5 }),
      note.icon ? s('path', { class: 'note-bolt', d: `M ${note.x + 9.5} ${y + 3.5} l -4 6.5 h 3 l -0.8 5 l 4.6 -7 h -3.1 z` }) : null,
      s('text', { x: note.x + (note.icon ? 16 : 7), y: y + hgt / 2 + 3.8 }, note.text)), { flash: true }));
  }

  // ruler: real elapsed time since the first BEGIN
  let lastTick = -Infinity;
  for (const p of pillsOf(model, compact)) {
    if (p.lane === 'DB') continue;
    const x = X(p.t);
    if (x - lastTick < (compact ? 70 : 88)) continue;
    lastTick = x;
    ruler.push(s('line', { class: 'tick', x1: x, x2: x, y1: top - 5, y2: top }),
      s('text', { class: 'tick-label', x: x + 3, y: top - 7 }, offset(p.t)));
  }

  const playhead = s('g', { class: 'playhead' },
    s('line', { x1: 0, x2: 0, y1: top - 2, y2: height }),
    s('circle', { cx: 0, cy: top, r: 4 }));
  const clipRect = s('rect', { x: 0, y: 0, width: 0, height });
  const svg = s('svg', { class: 'lanes', width, height, viewBox: `0 0 ${width} ${height}`, role: 'group', 'aria-label': `Transactions of run ${model.run.id}` },
    s('defs', {},
      s('clipPath', { id: `clip-${id}` }, clipRect),
      s('marker', { id: `arrow-${id}`, viewBox: '0 0 10 10', refX: 8, refY: 5, markerWidth: 6, markerHeight: 6, orient: 'auto-start-reverse' }, s('path', { class: 'arrow-head', d: 'M 0 0 L 10 5 L 0 10 z' }))),
    ...bg, s('g', { 'clip-path': `url(#clip-${id})` }, ...grow), ...over, ...marks, ...ruler, playhead);
  svg.style.width = `${width}px`;
  svg.style.height = `${height}px`;

  // lane labels, outside the scroller so they stay put
  const outcomeChips = {};
  const labels = h('div', { class: 'lane-labels', style: `--top:${top}px` },
    h('div', { class: 'ruler-space' }),
    model.lanes.map(l => {
      const req = model.run.requests.find(r => r.lane === l);
      const o = model.outcomes[l];
      const [text, tone] = o ? OUTCOME[o.outcome] || [words(o.outcome), ''] : ['—', ''];
      const chip = h('span', { class: `outcome tone-${tone}` }, text);
      if (o) at(X(model.T(o.atMicros)), chip);
      outcomeChips[l] = chip;
      return h('div', { class: 'lane-label', style: `height:${LANE}px`, title: req ? `${req.requestId} · order ${req.orderId}` : null },
        h('b', { class: 'lane-badge' }, l),
        h('div', {}, h('strong', {}, req?.role || `Request ${l}`), chip));
    }),
    h('div', { class: 'lane-label db', style: `height:${DBH}px` }, h('b', { class: 'lane-badge db' }, icon('db')), h('div', {}, h('strong', {}, 'Database'), compact ? null : h('small', {}, 'committed state'))));

  const scroller = h('div', { class: 'lanes-scroll' }, svg);
  fill(host, labels, scroller);
  reveal.sort((a, b) => a.x - b.x);
  let shown = 0, current = null;
  const view = {
    model, scale, scroller, svg,
    update(x, playing) {
      clipRect.setAttribute('width', Math.max(0, x + 1));
      while (shown < reveal.length && reveal[shown].x <= x + .5) {
        const r = reveal[shown++];
        r.el.classList.add('on');
        if (playing && r.flash) { r.el.classList.remove('flash'); void r.el.getBBox?.(); r.el.classList.add('flash'); }
      }
      while (shown > 0 && reveal[shown - 1].x > x + .5) { const r = reveal[--shown]; r.el.classList.remove('on', 'flash'); }
      let latest = null;
      for (let i = shown - 1; i >= 0; i--) if (reveal[i].pill && reveal[i].pill.lane !== 'DB') { latest = reveal[i].el; break; }
      if (current !== latest) { current?.classList.remove('current'); latest?.classList.add('current'); current = latest; }
      playhead.setAttribute('transform', `translate(${x} 0)`);
      playhead.classList.toggle('parked', x >= scale.end - .5 || x <= scale.start + .5);
      if (playing) {
        const left = scroller.scrollLeft, w = scroller.clientWidth;
        if (x > left + w * .78 || x < left) scroller.scrollLeft = Math.max(0, x - w * .35);
      }
    },
    select(seq) {
      for (const el of svg.querySelectorAll('.pill.selected')) el.classList.remove('selected');
      if (seq != null) svg.querySelector(`.pill[data-seq="${seq}"]`)?.classList.add('selected');
    },
    reveal,
  };
  return view;
}

// ---- player: replays recorded events ------------------------------------------------------------------
const SPEED_PX = 150;     // playhead speed at 1×, px per second
const DWELL_MS = 1500;    // a key moment holds the playhead this long at 1×

class Player {
  constructor(onChange) { this.onChange = onChange; this.views = []; this.speed = 1; this.clear(); }
  clear() {
    cancelAnimationFrame(this.frame);
    Object.assign(this, { views: [], x: 0, start: 0, end: 0, playing: false, stops: [], dwells: [], passed: new Set(), hold: 0, frame: 0, last: 0, cursor: -1, ended: false });
  }
  /** views share one scale; stops are the x of every step, dwells the x of every key moment. */
  load(views, stops, dwells) {
    this.clear();
    this.views = views;
    const scale = views[0].scale;
    this.start = scale.start; this.end = scale.end;
    this.stops = [...new Set(stops.map(Math.round))].sort((a, b) => a - b);
    this.dwells = [...new Set(dwells.map(Math.round))].sort((a, b) => a - b);
  }
  begin(autoplay) { this.seek(autoplay ? this.start : this.end); if (autoplay) this.play(); }
  play() {
    if (!this.views.length) return;
    if (this.x >= this.end - .5) this.seek(this.start);
    this.playing = true; this.last = performance.now();
    cancelAnimationFrame(this.frame);
    this.frame = requestAnimationFrame(t => this.tick(t));
    this.emit();
  }
  pause() { this.playing = false; cancelAnimationFrame(this.frame); this.emit(); }
  toggle() { this.playing ? this.pause() : this.play(); }
  replay() { this.seek(this.start); this.play(); }
  step(dir = 1) {
    this.pause();
    const x = Math.round(this.x);
    const next = dir > 0 ? this.stops.find(s => s > x + .5) : [...this.stops].reverse().find(s => s < x - .5);
    this.seek(next ?? (dir > 0 ? this.end : this.start));
  }
  seek(x) {
    this.x = Math.max(this.start, Math.min(this.end, x));
    this.passed = new Set(this.dwells.filter(d => d < this.x - .5));
    this.hold = 0;
    this.draw();
  }
  setSpeed(v) { this.speed = v; this.emit(); }
  tick(now) {
    const dt = Math.min(64, now - this.last); this.last = now;
    if (this.hold > 0) this.hold -= dt * this.speed;
    else {
      let nx = this.x + SPEED_PX * this.speed * dt / 1000;
      const dwell = this.dwells.find(d => d > this.x - .5 && d <= nx && !this.passed.has(d));
      if (dwell !== undefined) { nx = dwell; this.passed.add(dwell); this.hold = DWELL_MS; }
      this.x = Math.min(this.end, nx);
    }
    this.draw(true);
    if (this.x >= this.end && this.hold <= 0) { this.playing = false; this.onEnd?.(); this.emit(); return; }
    if (this.playing) this.frame = requestAnimationFrame(t => this.tick(t));
  }
  draw(playing = false) {
    for (const v of this.views) v.update(this.x, playing);
    const cursor = this.stops.filter(s => s <= this.x + .5).length;
    if (cursor !== this.cursor || !playing) { this.cursor = cursor; this.emit(); }
    this.progress?.(this.x);
  }
  emit() { this.onChange?.(this); }
  get atEnd() { return this.x >= this.end - .5; }
  get holding() { return this.hold > 0; }
}

const heroPlayer = new Player(() => renderStage());
heroPlayer.onEnd = () => { if (!state.revealed) { state.revealed = true; renderOutcome(); renderCompare(); } };
const comparePlayer = new Player(() => renderCompareStage());

/** Playback controls, shared by the hero and the comparison. */
function transport(player, { label = '' } = {}) {
  const speeds = [.5, 1, 2];
  return h('div', { class: 'transport', role: 'group', 'aria-label': label || 'Playback' },
    h('button', { type: 'button', class: 'icon-button', title: 'Replay from the start', 'aria-label': 'Replay', onclick: () => player.replay() }, icon('replay')),
    h('button', { type: 'button', class: 'icon-button', title: 'Previous step (←)', 'aria-label': 'Previous step', onclick: () => player.step(-1) }, icon('back')),
    h('button', { type: 'button', class: 'icon-button play', title: player.playing ? 'Pause (space)' : 'Play (space)', 'aria-label': player.playing ? 'Pause' : 'Play', onclick: () => player.toggle() },
      icon(player.playing ? 'pause' : 'play')),
    h('button', { type: 'button', class: 'icon-button', title: 'Next step (→)', 'aria-label': 'Next step', onclick: () => player.step(1) }, icon('step')),
    h('div', { class: 'speed', role: 'group', 'aria-label': 'Speed' }, speeds.map(v =>
      h('button', { type: 'button', 'aria-pressed': String(player.speed === v), onclick: () => player.setSpeed(v) }, `${v}×`))));
}

function scrubber(player) {
  const bar = h('div', { class: 'scrub-fill' });
  const el = h('div', { class: 'scrubber', role: 'slider', tabindex: 0, 'aria-label': 'Replay position', 'aria-valuemin': 0, 'aria-valuemax': 100,
    onpointerdown: ev => {
      const rect = el.getBoundingClientRect();
      const go = e2 => player.seek(player.start + (player.end - player.start) * Math.max(0, Math.min(1, (e2.clientX - rect.left) / rect.width)));
      player.pause(); go(ev);
      const move = e2 => go(e2);
      const up = () => { removeEventListener('pointermove', move); removeEventListener('pointerup', up); };
      addEventListener('pointermove', move); addEventListener('pointerup', up);
    } }, bar);
  player.progress = x => {
    const pct = player.end > player.start ? (x - player.start) / (player.end - player.start) * 100 : 100;
    bar.style.width = `${pct}%`;
    el.setAttribute('aria-valuenow', Math.round(pct));
  };
  return el;
}

// ---- 3. hero -------------------------------------------------------------------------------------------
function renderHero() {
  const hero = $('hero');
  const run = state.run;
  if (!run) {
    const e = state.exp;
    fill(hero, h('div', { class: 'hero-empty' },
      h('div', { class: 'ghost-lanes', 'aria-hidden': 'true' },
        ['A', 'B'].map(l => h('div', { class: 'ghost-lane' }, h('b', { class: 'lane-badge' }, l), h('span'), h('span'), h('span'), h('span'))),
        h('div', { class: 'ghost-lane db' }, h('b', { class: 'lane-badge db' }, icon('db')), h('span'))),
      h('div', { class: 'hero-empty-copy' },
        h('h2', {}, 'Watch the race happen'),
        h('p', {}, e.unsafeStory),
        h('p', { class: 'muted' }, 'Each request is its own thread, connection and PostgreSQL transaction. Press ', h('strong', {}, 'Run experiment'), ' and the lab records every read, check, lock, write and commit, then replays them here on one clock.'))));
    return;
  }
  if (!done(run)) { renderCapture(); return; }
  if (run.status === 'FAILED' || !state.model) {
    fill(hero, heroHead(), h('div', { class: 'hero-failed' }, icon('alert'), h('div', {}, h('strong', {}, 'The run failed'), h('p', {}, run.error || 'No result was recorded.'))));
    return;
  }
  const body = h('div', { class: 'hero-body' });
  const lanesHost = h('div', { class: 'lanes-host' });
  const side = h('aside', { id: 'db-panel', class: 'db-panel', 'aria-live': 'polite' });
  body.append(lanesHost, side);
  fill(hero,
    heroHead(),
    h('div', { id: 'narration', class: 'narration', 'aria-live': 'polite' }),
    body,
    h('div', { class: 'hero-foot' },
      scrubber(heroPlayer),
      h('ul', { class: 'legend', 'aria-label': 'Legend' },
        h('li', { class: 'k-read' }, 'Read'), h('li', { class: 'k-write' }, 'Write'), h('li', { class: 'k-good' }, 'Commit'),
        h('li', { class: 'k-wait' }, 'Waiting · lock'), h('li', { class: 'k-bad' }, 'Violation · rollback · conflict'),
        h('li', { class: 'k-delay' }, 'App work'), h('li', { class: 'k-sync', title: 'The lab held this request so the race happens every run (controlled interleaving). Not a lock wait.' }, 'Lab choreography')),
      h('p', { class: 'foot-note' }, `Replay of ${state.events.size} recorded events. Spacing follows event order; the ruler shows real elapsed time.`)));
  const model = state.model;
  const scale = buildScale([model], Math.max(520, (lanesHost.clientWidth || hero.clientWidth - 280) - LABEL_W), false);
  const view = drawLanes(lanesHost, model, scale, { onPick: e => inspect(model.run, e), id: 'hero' });
  const stops = model.steps.map(st => scale.xOf(st.t));
  const dwells = model.highlights.filter(m => !m.kind.startsWith('INVARIANT')).map(m => m.kind === 'SAME_VALUE_READ'
    ? Math.max(...(m.seqs || []).map(q => model.bySeq.get(q)).filter(Boolean).map(e => scale.xOf(model.T(e.atMicros))), scale.xOf(m.t))
    : scale.xOf(m.t));
  heroPlayer.load([view], stops, dwells);
  if (state.selected?.run.id === run.id) view.select(state.selected.event.seq);
}

function heroHead() {
  const run = state.run, c = run.config, m = modeOf(c.mode);
  const inv = run.result?.invariant;
  const status = !done(run) ? h('span', { class: 'status-pill running' }, h('span', { class: 'live-dot' }), 'Capturing')
    : run.status === 'FAILED' ? h('span', { class: 'status-pill failed' }, 'Failed')
    : state.revealed ? h('span', { class: `status-pill ${inv.holds ? 'success' : 'failed'}` }, inv.holds ? 'Invariant preserved' : 'Invariant violated')
    : h('span', { class: 'status-pill' }, 'Replaying');
  return h('div', { class: 'hero-head' },
    h('div', { class: 'hero-meta' },
      h('span', { class: `mode-tag${m?.fixes ? ' safe' : ' unsafe'}` }, m?.label || c.mode),
      h('span', { class: 'meta' }, `Run #${run.id} · ${isolationLabel(c.isolation)} · ${plural(c.requests, 'request')} · ${c.delayMs} ms delay${c.interleaving === 'NATURAL' ? ' · natural' : ''}`),
      status),
    done(run) && state.model ? h('div', { class: 'hero-tools' }, transport(heroPlayer), traceLink(run.traceId)) : null);
}

/** While the run is in flight: the lanes fill with what PostgreSQL has reported so far. */
function renderCapture() {
  const run = state.run;
  if (!run || done(run)) return;
  const events = [...state.events.values()].sort((a, b) => a.seq - b.seq);
  const lanes = (run.requests || []).map(r => r.lane);
  const latest = {};
  for (const e of events) if (lanes.includes(e.lane) && pillOf(e, {})) latest[e.lane] = e;
  const db = events.filter(e => e.type === 'STATE_OBSERVED').pop();
  fill($('hero'), heroHead(), h('div', { class: 'capture' },
    h('div', { class: 'capture-lanes' }, lanes.map(l => {
      const e = latest[l];
      const p = e ? pillOf(e, {}) : null;
      return h('div', { class: 'capture-lane' }, h('b', { class: 'lane-badge' }, l),
        h('div', { class: 'capture-track' }, h('span', { class: 'capture-bar' })),
        p ? h('span', { class: `capture-pill tone-${p[2]}` }, h('b', {}, p[0]), p[1] ? ` ${p[1]}` : '') : h('span', { class: 'capture-pill' }, 'connecting'));
    }),
    h('div', { class: 'capture-lane db' }, h('b', { class: 'lane-badge db' }, icon('db')), h('div', { class: 'capture-track' }), h('span', { class: 'capture-pill tone-db mono' }, db?.valueWritten || '…'))),
    h('p', { class: 'capture-note' }, h('span', { class: 'spinner' }), `Recording from PostgreSQL · ${plural(events.length, 'event')}. The replay starts when every transaction has finished.`)));
}

/** Everything that follows the playhead: caption, database state, controls. */
function renderStage() {
  if (!state.model) return;
  const model = state.model;
  const player = heroPlayer;
  const view = player.views[0];
  if (!view) return;
  const x = player.x;
  const tools = $('hero').querySelector('.hero-tools .transport');
  if (tools) tools.replaceWith(transport(player));
  const status = $('hero').querySelector('.hero-meta .status-pill');
  if (status && done(state.run)) status.replaceWith(heroHead().querySelector('.status-pill'));
  renderNarration($('narration'), model, view.scale, x, player);
  renderDbPanel($('db-panel'), model, view.scale, x);
}

function renderNarration(box, model, scale, x, player) {
  if (!box) return;
  const items = [
    ...model.steps.map(st => ({ x: scale.xOf(st.t), text: st.text, tone: st.tone, lane: st.e.lane, order: 0 })),
    ...model.highlights.filter(m => !m.kind.startsWith('INVARIANT') && m.kind !== 'BLOCKED').map(m => ({
      x: m.kind === 'SAME_VALUE_READ' ? Math.max(scale.xOf(m.t), ...(m.seqs || []).map(q => model.bySeq.get(q)).filter(Boolean).map(e => scale.xOf(model.T(e.atMicros)))) : scale.xOf(m.t),
      text: m.title, detail: m.detail, tone: m.kind === 'SAME_VALUE_READ' ? 'read' : m.tone, moment: m, order: 1 })),
  ].sort((a, b) => a.x - b.x || a.order - b.order);
  let now = null;
  for (const it of items) if (it.x <= x + .5) now = it;
  const count = model.steps.filter(st => scale.xOf(st.t) <= x + .5).length;
  const key = `${now ? `${now.x}|${now.text}` : 'start'}|${state.revealed}|${player.atEnd && !player.playing}`;
  if (box.dataset.key === key) return;
  box.dataset.key = key;
  box.className = `narration tone-${now?.moment ? now.tone : 'plain'}${now?.moment ? ' moment' : ''}`;
  fill(box,
    h('span', { class: 'step-count num' }, `${String(count).padStart(2, '0')}/${String(model.steps.length).padStart(2, '0')}`),
    now?.moment ? h('span', { class: 'moment-mark' }, now.moment.kind === 'SAME_VALUE_READ' ? 'Key moment' : now.tone === 'bad' ? 'Race' : 'Moment') : now?.lane && now.lane !== 'LAB' ? h('b', { class: `lane-badge sm${now.lane === 'DB' ? ' db' : ''}` }, now.lane === 'DB' ? icon('db') : now.lane) : null,
    h('span', { class: 'narration-text' }, h('strong', {}, now ? now.text : 'Press play to watch the recorded transactions.'), now?.detail ? h('small', {}, now.detail) : null),
    player.atEnd && !player.playing ? h('button', { type: 'button', class: 'link-button', onclick: () => $('outcome').scrollIntoView({ behavior: reducedMotion ? 'auto' : 'smooth', block: 'start' }) }, 'See the result', icon('arrow')) : null,
    !state.revealed ? h('button', { type: 'button', class: 'link-button quiet', onclick: () => { player.pause(); player.seek(player.end); state.revealed = true; renderOutcome(); renderCompare(); } }, 'Skip to result') : null);
}

/** The committed state, as the engine read it, up to the playhead; and writes not committed yet. */
function renderDbPanel(box, model, scale, x) {
  if (!box) return;
  const seen = model.dbStates.filter(d => scale.xOf(d.t) <= x + .5);
  const inFlight = [];
  for (const p of model.pills) {
    if (p.e.type !== 'WRITE_PERFORMED' || p.e.rows === 0 || scale.xOf(p.t) > x + .5) continue;
    const tx = model.txs.find(t => t.lane === p.lane && t.attempt === p.e.attempt);
    if (!tx || scale.xOf(tx.to) > x + .5) inFlight.push(p);
  }
  const verdict = model.pills.find(p => p.verdict);
  const verdictShown = verdict && scale.xOf(verdict.t) <= x + .5;
  const key = `${seen.length}|${inFlight.map(p => p.e.seq).join(',')}|${verdictShown}`;
  if (box.dataset.key === key) return;
  const changedFrom = box.dataset.count ? Number(box.dataset.count) : seen.length;
  box.dataset.key = key; box.dataset.count = seen.length;
  const keys = [...new Set(model.dbStates.flatMap(d => Object.keys(d.e.data || {})))];
  const value = v => v == null ? '—' : typeof v === 'object' ? JSON.stringify(v) : String(v);
  const rows = keys.map(k => {
    const hist = [];
    for (const d of seen) { const v = value(d.e.data?.[k]); if (hist[hist.length - 1] !== v) hist.push(v); }
    const changed = seen.length > changedFrom && seen.length > 1 && value(seen[seen.length - 1].e.data?.[k]) !== value(seen[seen.length - 2].e.data?.[k]);
    return h('div', { class: `db-field${changed ? ' changed' : ''}` },
      h('span', { class: 'db-key' }, humanKey(k)),
      h('strong', { class: 'db-value num' }, hist.length ? hist[hist.length - 1] : '—'),
      hist.length > 1 ? h('span', { class: 'db-history num' }, hist.slice(0, -1).map(v => [h('span', {}, v), ' → ']), h('b', {}, hist[hist.length - 1])) : h('span', { class: 'db-history' }, seen.length ? 'initial' : ''));
  });
  fill(box,
    h('header', {}, icon('db'), h('div', {}, h('strong', {}, 'Database'), h('small', {}, 'committed, read after each transaction'))),
    h('div', { class: 'db-fields' }, rows.length ? rows : h('p', { class: 'muted' }, 'No committed state yet.')),
    h('div', { class: 'db-flight' },
      h('span', { class: 'db-sub' }, 'Uncommitted writes'),
      inFlight.length ? inFlight.map(p => h('div', { class: 'flight-row', title: narrate(p.e) }, h('b', { class: 'lane-badge sm' }, p.lane), h('span', { class: 'mono' }, clip(p.e.valueWritten || p.e.target, 26))))
        : h('span', { class: 'muted small' }, 'none')),
    verdictShown ? h('div', { class: `db-verdict ${verdict.e.ok ? 'good' : 'bad'}` }, icon(verdict.e.ok ? 'check' : 'alert'), h('span', {}, verdict.e.ok ? 'Invariant preserved' : 'Invariant violated')) : null);
}

// ---- 4–6. result, why, fix -----------------------------------------------------------------------------
function renderOutcome() {
  const box = $('outcome');
  const run = state.run;
  if (!run || run.status !== 'COMPLETED' || !run.result || !state.revealed) {
    fill(box, run && done(run) && run.status === 'COMPLETED' ? h('p', { class: 'outcome-wait' }, 'The verdict appears when the replay reaches the end.') : null);
    return;
  }
  const { invariant: inv, metrics: m, explanation: x, initialState, finalState } = run.result;
  const cfg = run.config;
  const facts = [
    [state.exp.valueLabel || 'Initial value', cfg.initialValue],
    ['Succeeded', m.succeeded],
    m.rejected ? ['Declined', m.rejected] : null,
    m.aborted ? ['Aborted', m.aborted] : null,
    ...Object.entries(finalState || {}).filter(([k]) => k !== 'version').map(([k, v]) => [`Final ${humanKey(k).toLowerCase()}`, typeof v === 'object' ? JSON.stringify(v) : v]),
  ].filter(Boolean);
  const result = h('article', { class: `verdict-card ${inv.holds ? 'good' : 'bad'}`, 'aria-labelledby': 'verdict-title' },
    h('div', { class: 'verdict-head' },
      h('span', { class: 'verdict-icon' }, icon(inv.holds ? 'check' : 'alert')),
      h('div', {}, h('h2', { id: 'verdict-title' }, inv.holds ? 'Invariant preserved' : 'Invariant violated'), h('p', {}, inv.statement))),
    h('dl', { class: 'facts' }, facts.flatMap(([k, v]) => [h('dt', {}, k), h('dd', { class: 'num' }, String(v))])),
    h('div', { class: 'expect' },
      h('div', {}, h('span', {}, 'Expected'), h('strong', {}, inv.expected)),
      h('div', { class: inv.holds ? 'ok' : 'no' }, h('span', {}, 'Actual'), h('strong', {}, inv.actual))),
    inv.detail ? h('p', { class: 'verdict-detail' }, inv.detail) : null,
    h('ul', { class: 'request-outcomes' }, run.result.requests.map(q => {
      const [text, tone] = OUTCOME[q.outcome] || [words(q.outcome), ''];
      return h('li', {}, h('b', { class: 'lane-badge sm' }, q.lane), h('span', { class: `outcome on tone-${tone}` }, text), h('span', { class: 'num muted' }, ms(q.latencyMicros)),
        q.lockWaitMicros ? h('span', { class: 'num wait-text' }, `waited ${ms(q.lockWaitMicros)}`) : null);
    })));

  fill(box, h('div', { class: 'outcome-grid' }, result, whyCard(run, x, inv)), fixCard(run, x));
}

/** The explanation's steps, condensed to the ones that carry the story; the full list stays one click away. */
function keySteps(run, x) {
  const model = state.model;
  const race = model?.race;
  const firstWrite = new Set();
  const kept = [];
  for (const st of x.steps) {
    const e = state.events.get(st.seq);
    if (!e) { kept.push({ ...st, lanes: [st.lane] }); continue; }
    const t = e.type;
    if (t === 'WRITE_PERFORMED') {
      const k = `${e.lane}#${e.attempt}`;
      if (e.rows !== 0 && firstWrite.has(k)) continue;
      firstWrite.add(k);
    } else if (!['READ_PERFORMED', 'DECISION_MADE', 'TRANSACTION_BLOCKED', 'VERSION_CONFLICT', 'SERIALIZATION_FAILURE', 'DEADLOCK_DETECTED', 'TRANSACTION_COMMITTED', 'TRANSACTION_ROLLED_BACK', 'RETRY_SCHEDULED'].includes(t)) continue;
    const stale = race && race.kind === 'STALE_WRITE' && t === 'WRITE_PERFORMED' && e.lane === race.lanes?.[0] && (race.seqs || []).includes(e.seq);
    const prev = kept[kept.length - 1];
    // Every request deciding the same way is one step.
    if (t === 'DECISION_MADE' && prev?.type === 'DECISION_MADE' && prev.ok === e.ok && prev.target === e.target) {
      prev.lanes.push(e.lane);
      prev.lanes.sort();
      continue;
    }
    kept.push({ ...st, type: t, ok: e.ok, target: e.target, lanes: [st.lane], stale, tone: stale ? 'bad' : st.tone });
  }
  const all = run.requests.length;
  return kept.map(k => k.type === 'DECISION_MADE' && k.lanes.length > 1
    ? { ...k, text: `${k.lanes.length === all ? (all === 2 ? 'Both' : 'All') : k.lanes.join(', ')} decide ${k.ok ? '✓' : '✗'} ${k.target}`, lane: null }
    : k);
}

function whyCard(run, x, inv) {
  const steps = keySteps(run, x);
  const m = modeOf(run.config.mode);
  const t0 = state.model?.t0 ?? 0;
  const events = [...state.events.values()].sort((a, b) => a.atMicros - b.atMicros || a.seq - b.seq);
  return h('article', { class: 'why-card', 'aria-labelledby': 'why-title' },
    h('div', { class: 'card-head' }, h('h2', { id: 'why-title' }, inv.holds ? 'Why it held' : 'Why did this happen?')),
    h('ol', { class: 'story' }, steps.map((st, i) => h('li', { class: `tone-${st.tone}${st.stale ? ' stale' : ''}` },
      h('span', { class: 'story-n num' }, i + 1),
      st.lanes?.length > 1 ? h('span', { class: 'story-lanes' }, st.lanes.map(l => h('b', { class: 'lane-badge sm' }, l))) : st.lane ? h('b', { class: 'lane-badge sm' }, st.lane) : h('span'),
      h('button', { type: 'button', class: 'story-text', disabled: !st.seq, onclick: () => st.seq && jumpTo(st.seq) },
        st.text, st.stale ? h('em', { class: 'stale-tag' }, 'stale decision') : null))),
      h('li', { class: `conclusion tone-${inv.holds ? 'good' : 'bad'}` }, h('span', { class: 'story-n' }, icon(inv.holds ? 'check' : 'arrow')), h('span'), h('strong', {}, inv.actual))),
    h('p', { class: 'mechanism' }, h('span', {}, m?.label || run.config.mode), x.mechanism),
    h('details', { class: 'technical', open: state.techOpen || null, ontoggle: ev => { state.techOpen = ev.target.open; } },
      h('summary', {}, `View technical execution · ${plural(events.length, 'event')}`),
      h('p', { class: 'muted small' }, x.conclusion),
      h('ol', { class: 'full-steps' }, x.steps.map(st => h('li', { class: `tone-${st.tone}` }, h('b', { class: 'lane-badge sm' }, st.lane || '·'),
        h('button', { type: 'button', class: 'story-text', disabled: !st.seq, onclick: () => st.seq && jumpTo(st.seq) }, st.text)))),
      h('div', { class: 'event-table-wrap' }, h('table', { class: 'event-table' },
        h('thead', {}, h('tr', {}, ['#', 'Time', 'Lane', 'Event', 'Value', 'Tx'].map(c => h('th', {}, c)))),
        h('tbody', {}, events.map(e => h('tr', { tabindex: 0, onclick: () => inspect(run, e), onkeydown: ev => { if (ev.key === 'Enter') inspect(run, e); } },
          h('td', { class: 'num' }, e.seq), h('td', { class: 'num mono' }, offset(e.atMicros - t0)), h('td', {}, e.lane), h('td', {}, TYPE(e.type)),
          h('td', { class: 'mono' }, clip(e.valueRead || e.valueWritten || e.lock || e.target || '', 34)), h('td', { class: 'mono' }, e.txId ?? '')))))),
      traceLink(run.traceId, 'Open the run\'s trace in Tempo')));
}

function fixCard(run, x) {
  const inv = run.result.invariant;
  const alternatives = state.exp.modes.filter(md => md.fixes && md.mode !== x.fix?.mode && md.mode !== run.config.mode);
  if (!inv.holds && x.fix) {
    const target = modeOf(x.fix.mode);
    return h('section', { class: 'fix-card', 'aria-labelledby': 'fix-title' },
      h('div', { class: 'fix-copy' },
        h('span', { class: 'kicker' }, 'Fix it'),
        h('h2', { id: 'fix-title' }, `Replay the same race with ${target?.label || x.fix.mode}`),
        h('p', {}, x.fix.why),
        target?.sql ? h('pre', { class: 'code fix-sql' }, target.sql) : null),
      h('div', { class: 'fix-actions' },
        h('button', { type: 'button', class: 'primary-action fix-button', disabled: state.busy, onclick: () => runFix(x.fix.mode, x.fix.isolation, run) },
          `Run with ${target?.label || x.fix.mode}`, icon('arrow')),
        h('p', { class: 'muted small' }, `Same requests, ${state.exp.valueLabel?.toLowerCase() || 'value'} and delay. Then both runs play side by side.`),
        alternatives.length ? h('div', { class: 'alt-fixes' }, h('span', {}, 'Or try'),
          alternatives.map(md => h('button', { type: 'button', class: 'chip-button', disabled: state.busy, title: md.how, onclick: () => runFix(md.mode, md.isolation, run) }, md.label))) : null));
  }
  const unsafe = state.history.find(r => r.id !== run.id && r.status === 'COMPLETED' && r.invariantHolds === false);
  const comparing = state.compare && !state.compare.error && [state.compare.left.id, state.compare.right.id].includes(run.id);
  if (inv.holds && unsafe && !comparing) {
    return h('section', { class: 'fix-card quiet' },
      h('div', { class: 'fix-copy' }, h('span', { class: 'kicker' }, 'Compare'), h('h2', {}, `Put this run next to run #${unsafe.id} (${modeOf(unsafe.mode)?.label || unsafe.mode})`)),
      h('div', { class: 'fix-actions' }, h('button', { type: 'button', class: 'primary-action', onclick: () => { state.pick = { left: unsafe.id, right: run.id }; loadCompare(); } }, 'Compare side by side', icon('arrow'))));
  }
  if (inv.holds && !alternatives.length) return null;
  return null;
}

function runFix(mode, isolation, previous) {
  const prev = previous.config;
  state.cfg = {
    mode, isolation: isolation || modeOf(mode)?.isolation,
    requests: prev.requests, initialValue: prev.initialValue, delayMs: prev.delayMs, interleaving: prev.interleaving,
    maxRetries: MODE_RETRIES.includes(mode) ? Math.max(prev.maxRetries, state.exp.limits.defaultRetries) : prev.maxRetries,
  };
  renderControls();
  runRace(state.cfg, previous.id);
}

/** From a step in the story to its event: park the playhead there and open it. */
function jumpTo(seq) {
  const e = state.events.get(seq);
  if (!e || !state.model) return;
  const view = heroPlayer.views[0];
  heroPlayer.pause();
  heroPlayer.seek(view.scale.xOf(state.model.T(e.atMicros)));
  $('hero').scrollIntoView({ behavior: reducedMotion ? 'auto' : 'smooth', block: 'start' });
  inspect(state.run, e);
}

// ---- 7. compare ----------------------------------------------------------------------------------------
async function loadCompare({ quiet = false } = {}) {
  const { left, right } = state.pick;
  if (!left || !right || left === right) return;
  try {
    const [cmp, l, r, le, re] = await Promise.all([
      api(`/compare?left=${left}&right=${right}`), api(`/runs/${left}`), api(`/runs/${right}`),
      api(`/runs/${left}/events?limit=5000`), api(`/runs/${right}/events?limit=5000`)]);
    state.compare = { cmp, left: l, right: r, leftEvents: le.data, rightEvents: re.data, autoplay: true };
  } catch (e) {
    state.compare = { error: e.message };
  }
  renderOutcome();
  renderCompare();
  if (!quiet) $('compare').scrollIntoView({ behavior: reducedMotion ? 'auto' : 'smooth', block: 'start' });
}

function renderCompare() {
  const box = $('compare');
  comparePlayer.clear();
  const finished = state.history.filter(r => r.status === 'COMPLETED');
  const c = state.compare;
  const ready = c && !c.error && (state.revealed || state.run?.id !== c.right.id);
  const picker = () => {
    const opts = which => [h('option', { value: '' }, which === 'left' ? 'Left run…' : 'Right run…'),
      ...finished.map(r => h('option', { value: r.id, selected: state.pick[which] === r.id }, `#${r.id} · ${modeOf(r.mode)?.label || r.mode} · ${r.invariantHolds ? 'held' : 'violated'}`))];
    return h('div', { class: 'compare-picker' },
      h('span', { class: 'select' }, h('select', { 'aria-label': 'Left run', onchange: ev => { state.pick.left = Number(ev.target.value) || null; } }, opts('left')), icon('chevron')),
      h('span', { class: 'vs' }, 'vs'),
      h('span', { class: 'select' }, h('select', { 'aria-label': 'Right run', onchange: ev => { state.pick.right = Number(ev.target.value) || null; } }, opts('right')), icon('chevron')),
      h('button', { type: 'button', class: 'ghost-button', onclick: () => loadCompare() }, 'Compare'));
  };
  if (!ready) {
    fill(box, finished.length >= 2 ? h('details', { class: 'compare-closed' },
      h('summary', {}, h('span', {}, 'Compare two runs'), h('small', {}, `${finished.length} finished runs of this experiment`)),
      c?.error ? h('p', { class: 'ctl-message error' }, c.error) : null, picker()) : null);
    return;
  }
  const left = buildModel(c.left, c.leftEvents);
  const right = buildModel(c.right, c.rightEvents);
  const label = run => modeOf(run.config.mode)?.label || run.config.mode;
  const head = run => {
    const held = run.result.invariant.holds;
    return h('div', { class: `side-head ${held ? 'good' : 'bad'}` },
      h('span', { class: `mode-tag ${modeOf(run.config.mode)?.fixes ? 'safe' : 'unsafe'}` }, label(run)),
      h('span', { class: 'meta' }, `Run #${run.id}`),
      h('span', { class: `status-pill ${held ? 'success' : 'failed'}` }, held ? 'Invariant preserved' : 'Invariant violated'));
  };
  const hostL = h('div', { class: 'lanes-host compact' }), hostR = h('div', { class: 'lanes-host compact' });
  const seqList = side => h('ol', { class: 'interleaving' }, (side.sequence || []).map(t => {
    const [lane, ...rest] = t.split(' ');
    const text = rest.join(' ');
    const tone = /COMMIT/.test(text) ? 'good' : /WAIT/.test(text) ? 'wait' : /READ/.test(text) ? 'read' : /ROLLBACK|ABORT|DEADLOCK|STALE|REJECT|CONFLICT|0 rows/i.test(text) ? 'bad' : '';
    return h('li', { class: `tone-${tone}` }, h('b', { class: 'lane-badge sm' }, lane), h('span', { class: 'mono' }, clip(text, 40)));
  }));
  const cmp = c.cmp;
  fill(box,
    h('div', { class: 'compare-head' },
      h('div', {}, h('span', { class: 'kicker' }, 'Unsafe vs safe'), h('h2', {}, `${label(c.left)} vs ${label(c.right)}`), h('p', { class: 'compare-verdict' }, cmp.verdict)),
      h('div', { class: 'compare-tools' }, h('div', { class: 'transport-slot' }), h('details', { class: 'compare-change' }, h('summary', {}, 'Change runs'), picker()))),
    h('div', { class: 'pair' },
      h('div', { class: 'side' }, head(c.left), hostL),
      h('div', { class: 'side' }, head(c.right), hostR)),
    h('div', { class: 'compare-scrub' }, scrubber(comparePlayer), h('p', { class: 'foot-note' }, 'Both runs on one clock: the same elapsed time sits at the same x on both sides.')),
    tradeoffs(c.left, c.right, label),
    h('details', { class: 'technical' }, h('summary', {}, 'Recorded interleavings'),
      h('div', { class: 'pair plain' }, seqList(cmp.left), seqList(cmp.right))));
  requestAnimationFrame(() => {
    const avail = Math.max(360, hostL.clientWidth - LABEL_W_COMPACT);
    const scale = buildScale([left, right], avail, true);
    const vl = drawLanes(hostL, left, scale, { compact: true, onPick: e => inspect(c.left, e), id: 'cmp-l' });
    const vr = drawLanes(hostR, right, scale, { compact: true, onPick: e => inspect(c.right, e), id: 'cmp-r' });
    // Horizontal scroll moves both sides together.
    let syncing = false;
    const sync = (a, b) => a.scroller.addEventListener('scroll', () => { if (syncing) return; syncing = true; b.scroller.scrollLeft = a.scroller.scrollLeft; syncing = false; });
    sync(vl, vr); sync(vr, vl);
    const stops = [...left.steps.map(st => scale.xOf(st.t)), ...right.steps.map(st => scale.xOf(st.t))];
    const dwells = [left, right].flatMap(m => m.highlights.filter(h2 => ['SAME_VALUE_READ', ...Object.keys(RACE_KINDS), 'BLOCKED'].includes(h2.kind)).map(h2 => scale.xOf(h2.t)));
    comparePlayer.load([vl, vr], stops, dwells);
    renderCompareStage();
    const autoplay = c.autoplay && !reducedMotion;
    c.autoplay = false;
    comparePlayer.begin(false);
    if (autoplay) {
      const io = new IntersectionObserver(entries => {
        if (entries.some(en => en.isIntersecting)) { io.disconnect(); comparePlayer.replay(); }
      }, { threshold: .45 });
      io.observe(box.querySelector('.pair'));
    }
  });
}

function renderCompareStage() {
  const slot = $('compare').querySelector('.transport-slot');
  if (slot) fill(slot, transport(comparePlayer, { label: 'Comparison playback' }));
}

/** Correctness vs contention vs latency, from each run's measured metrics. */
function tradeoffs(l, r, label) {
  const ml = l.result.metrics, mr = r.result.metrics;
  const bar = (a, b, fmt, lowerBetter = true) => {
    const max = Math.max(a, b, 1);
    const cell = (v, other) => h('div', { class: `bar-cell${v === other ? '' : (lowerBetter ? v < other : v > other) ? ' better' : ''}` },
      h('span', { class: 'bar' }, h('i', { style: `width:${Math.max(2, v / max * 100)}%` })), h('b', { class: 'num' }, fmt(v)));
    return [cell(a, b), cell(b, a)];
  };
  const rows = [
    ['Correctness', ...[l, r].map(run => {
      const held = run.result.invariant.holds;
      return h('div', { class: `bar-cell verdict-cell ${held ? 'good' : 'bad'}` }, icon(held ? 'check' : 'alert'), h('span', {}, run.result.invariant.actual));
    })],
    ['Lock waits', ...bar(ml.lockWaits, mr.lockWaits, v => plural(v, 'wait'))],
    ['Time waiting for locks', ...bar(ml.lockWaitMicros, mr.lockWaitMicros, ms)],
    ['Conflicts · retries', ...bar(ml.conflicts + ml.retries, mr.conflicts + mr.retries, v => v)],
    ['Latency p50', ...bar(ml.p50Micros, mr.p50Micros, ms)],
    ['Latency p95', ...bar(ml.p95Micros, mr.p95Micros, ms)],
  ];
  return h('div', { class: 'tradeoffs' },
    h('div', { class: 'trade-row head' }, h('span'), h('span', {}, label(l)), h('span', {}, label(r))),
    rows.map(([k, a, b]) => h('div', { class: 'trade-row' }, h('span', { class: 'trade-key' }, k), a, b)),
    h('p', { class: 'foot-note' }, `Latency includes the lab's choreography holds (${ms(ml.syncWaitMicros)} and ${ms(mr.syncWaitMicros)} in total), reported apart from lock waits.`));
}

// ---- 8. inspector --------------------------------------------------------------------------------------
function inspect(run, e) {
  state.selected = { run, event: e };
  for (const v of [...heroPlayer.views, ...comparePlayer.views]) v.select(v.model.run.id === run.id ? e.seq : null);
  renderInspector();
}

function closeInspector() {
  if (!state.selected) return;
  const seq = state.selected.event.seq;
  state.selected = null;
  for (const v of [...heroPlayer.views, ...comparePlayer.views]) v.select(null);
  renderInspector();
  document.querySelector(`.pill[data-seq="${seq}"]`)?.focus({ preventScroll: true });
}

function renderInspector() {
  const box = $('inspector');
  const sel = state.selected;
  box.classList.toggle('open', !!sel);
  box.setAttribute('aria-hidden', String(!sel));
  if (!sel) { fill(box); return; }
  const { run, event: e } = sel;
  const all = run.id === state.run?.id ? [...state.events.values()] : run.id === state.compare?.left?.id ? state.compare.leftEvents : state.compare?.rightEvents || [];
  const sorted = [...all].sort((a, b) => a.atMicros - b.atMicros || a.seq - b.seq);
  const t0 = sorted.find(x => x.type === 'TRANSACTION_STARTED')?.atMicros ?? 0;
  const same = x => x.lane === e.lane && x.attempt === e.attempt;
  const begin = sorted.find(x => same(x) && x.type === 'TRANSACTION_STARTED');
  const lockReq = sorted.find(x => same(x) && x.type === 'LOCK_REQUESTED' && (!e.target || x.target === e.target || e.type.startsWith('LOCK')));
  const lockGot = sorted.find(x => same(x) && x.type === 'LOCK_ACQUIRED' && (!lockReq || x.lock === lockReq.lock));
  const req = run.requests?.find(r => r.lane === e.lane);
  const kv = rows => {
    const list = rows.filter(([, v]) => v !== null && v !== undefined && v !== '');
    return list.length ? h('dl', { class: 'kv' }, list.flatMap(([k, v, cls]) => [h('dt', {}, k), h('dd', { class: cls || '' }, String(v))])) : null;
  };
  const section = (title, body) => body ? h('section', { class: 'insp-section' }, h('h3', {}, title), body) : null;
  const p = pillOf(e, {});
  const tone = p?.[2] || (e.type === 'TRANSACTION_BLOCKED' ? 'wait' : e.type === 'STATE_OBSERVED' ? 'db' : e.type === 'INVARIANT_CHECKED' ? (e.ok ? 'good' : 'bad') : 'neutral');
  fill(box,
    h('header', { class: 'insp-head' },
      h('div', {},
        h('span', { class: 'kicker' }, `Event #${e.seq} · run #${run.id}`),
        h('h2', {}, h('span', { class: `insp-dot tone-${tone}` }), TYPE(e.type)),
        h('p', {}, narrate(e))),
      h('button', { type: 'button', class: 'icon-button', 'aria-label': 'Close inspector', title: 'Close (Esc)', onclick: closeInspector }, icon('close'))),
    h('div', { class: 'insp-scroll' },
      section('Transaction', kv([
        ['Lane', e.lane === 'DB' ? 'Database (engine\'s own connection)' : e.lane === 'LAB' ? 'Experiment' : `Request ${e.lane}${req?.role ? ` · ${req.role}` : ''}`],
        ['Transaction ID', e.txId, 'mono'],
        ['Request ID', e.requestId || req?.requestId, 'mono'],
        ['Order', req?.orderId, 'mono'],
        ['Attempt', e.attempt],
        ['Thread', e.thread, 'mono'],
        ['Service instance', begin?.data?.instance || e.data?.instance, 'mono'],
        ['Backend pid', e.pid, 'mono'],
        ['Isolation level', e.isolation ? isolationLabel(e.isolation) : null],
      ])),
      section('Timing', kv([
        ['Timestamp', e.at ? new Date(e.at).toISOString().replace('T', ' ').replace('Z', ' UTC') : null, 'mono'],
        ['Since first BEGIN', offset(e.atMicros - t0), 'mono'],
        ['Duration', e.durationMicros != null ? ms(e.durationMicros) : null, 'mono'],
      ])),
      e.sql ? section('SQL', h('pre', { class: 'code' }, e.sql)) : null,
      section('Data', kv([
        ['Target', e.target],
        ['Value read', e.valueRead, 'mono'],
        ['Value written', e.valueWritten, 'mono'],
        ['Row version read', e.versionRead, 'mono'],
        ['Row version written', e.versionWritten, 'mono'],
        ['Expected version', e.expectedVersion, 'mono'],
        ['Actual version', e.actualVersion, 'mono'],
        ['Rows affected', e.rows, 'mono'],
        ['Decision', e.type === 'DECISION_MADE' ? `${e.ok ? '✓' : '✗'} ${e.target}` : null],
        ['Outcome', e.outcome],
        ['SQLSTATE', e.sqlState, 'mono'],
      ])),
      section('Locks', kv([
        ['Lock', e.lock || lockReq?.lock],
        ['Lock requested', lockReq ? offset(lockReq.atMicros - t0) : null, 'mono'],
        ['Lock acquired', lockGot ? `${offset(lockGot.atMicros - t0 + (lockGot.durationMicros || 0))}${lockGot.message ? ` · ${lockGot.message}` : ''}` : null, 'mono'],
        ['Lock wait', e.waitMicros != null ? ms(e.waitMicros) : lockGot?.waitMicros != null ? ms(lockGot.waitMicros) : null, 'mono'],
        ['Blocked by', e.blockedBy?.join(', ')],
      ])),
      e.data && Object.keys(e.data).some(k => k !== 'instance') ? section(e.type === 'DEADLOCK_DETECTED' ? 'PostgreSQL detail' : 'Recorded detail',
        kv(Object.entries(e.data).filter(([k]) => k !== 'instance').map(([k, v]) => [humanKey(k), typeof v === 'object' ? JSON.stringify(v) : v, 'mono']))) : null,
      section('Trace', h('div', { class: 'trace-row' }, h('code', {}, e.traceId || run.traceId || '—'), traceLink(e.traceId || run.traceId, 'Open in Tempo'))),
      e.lane !== 'LAB' ? section(e.lane === 'DB' ? 'Every committed state' : `Everything ${e.lane} did`,
        h('ol', { class: 'lane-events' }, sorted.filter(x => x.lane === e.lane).map(x => h('li', {},
          h('button', { type: 'button', 'aria-current': String(x.seq === e.seq), onclick: () => inspect(run, x) },
            h('span', { class: 'mono t' }, offset(x.atMicros - t0)), h('span', {}, TYPE(x.type)), h('span', { class: 'mono v' }, clip(x.valueRead || x.valueWritten || '', 18))))))) : null));
  box.querySelector('.insp-head .icon-button')?.focus({ preventScroll: true });
}

// ---- keyboard ------------------------------------------------------------------------------------------
document.addEventListener('keydown', ev => {
  if (ev.key === 'Escape') {
    if (state.menu) { state.menu = null; renderExperiment(); return; }
    if (state.selected) { closeInspector(); return; }
  }
  const tag = ev.target.tagName;
  if (['INPUT', 'SELECT', 'TEXTAREA'].includes(tag) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
  if (!heroPlayer.views.length) return;
  if (ev.key === ' ' && tag !== 'BUTTON' && !ev.target.closest?.('.pill')) { ev.preventDefault(); heroPlayer.toggle(); }
  if (ev.key === 'ArrowRight' && !ev.target.closest?.('.scrubber')) { ev.preventDefault(); heroPlayer.step(1); }
  if (ev.key === 'ArrowLeft' && !ev.target.closest?.('.scrubber')) { ev.preventDefault(); heroPlayer.step(-1); }
});

// ---- all -----------------------------------------------------------------------------------------------
function renderAll() {
  renderExperiment(); renderControls(); renderHero(); renderStage(); renderOutcome(); renderCompare(); renderInspector();
}

let resizeTimer = 0;
let lastWidth = innerWidth;
addEventListener('resize', () => {
  if (Math.abs(innerWidth - lastWidth) < 40) return;
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    lastWidth = innerWidth;
    if (!state.model) return;
    const x = heroPlayer.x, frac = (x - heroPlayer.start) / Math.max(1, heroPlayer.end - heroPlayer.start), was = heroPlayer.playing;
    renderHero();
    heroPlayer.seek(heroPlayer.start + frac * (heroPlayer.end - heroPlayer.start));
    renderStage();
    if (was) heroPlayer.play();
    if (state.compare && !state.compare.error) renderCompare();
  }, 200);
});
init();
