// Race condition lab (/race). Renders runs of real PostgreSQL transactions from the structured events the
// backend records (never from log text): one lane per request on one clock, the database's committed state
// beneath, and the moments that matter marked on the timeline. Live runs arrive over server-sent events.
'use strict';

const API = '/api/race-lab';
const grafana = document.body.dataset.grafana || '';
const $ = id => document.getElementById(id);

// ---- DOM helpers ---------------------------------------------------------------------------------------
function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else if (k === 'html') el.innerHTML = v;
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
const chip = (text, tone = '') => h('span', { class: `chip ${tone}` }, text);
const ms = micros => micros == null ? '—' : micros >= 10_000 ? `${Math.round(micros / 1000).toLocaleString()} ms` : `${(micros / 1000).toFixed(1)} ms`;
const plural = (n, w) => `${n} ${w}${n === 1 ? '' : 's'}`;
const explore = (uid, query) => `${grafana}/explore?schemaVersion=1&panes=${encodeURIComponent(JSON.stringify({ z: {
  datasource: uid, range: { from: 'now-6h', to: 'now' },
  queries: [{ refId: 'A', datasource: { type: uid, uid }, ...(uid === 'tempo' ? { queryType: 'traceql', query } : { expr: query }) }] } }))}`;
const traceLink = (traceId, label = 'Trace') => traceId && grafana
  ? h('a', { class: 'small-button', href: explore('tempo', traceId), target: '_blank', rel: 'noopener', title: `Trace ${traceId} in Tempo` }, label) : null;

async function api(path, options = {}) {
  const response = await fetch(API + path, { headers: { 'Content-Type': 'application/json' }, ...options });
  const body = response.status === 204 ? null : await response.json().catch(() => null);
  if (!response.ok) throw new Error(body?.detail || `HTTP ${response.status}`);
  return body;
}

// ---- state -------------------------------------------------------------------------------------------
const state = {
  experiments: [],
  exp: null,            // ExperimentInfo
  cfg: null,            // the form
  run: null,            // Run being shown
  events: new Map(),    // seq -> RaceEvent of the shown run
  selected: null,       // selected event seq
  moment: null,         // selected moment index
  history: [],
  pick: { left: null, right: null },
  comparison: null,
  scale: 'sequence',
  zoom: 1,
  fit: true,             // squeeze the whole run into the panel's width
  stream: null,
  busy: false,
  error: '',
  confirmReset: false,
  followTail: true,
  painted: 0,
  compareEvents: null,
  session: {},          // per experiment: { runs, violated, fixed, compared }
};
const MODE_RETRIES = ['OPTIMISTIC', 'SERIALIZABLE'];
const TERMINAL = ['COMPLETED', 'FAILED'];
const modeInfo = (mode = state.cfg?.mode) => state.exp?.modes.find(m => m.mode === mode);
const session = () => (state.session[state.exp?.id] ||= { runs: 0, violated: false, fixed: false, compared: false });
const sorted = () => [...state.events.values()].sort((a, b) => a.seq - b.seq);
const done = () => state.run && TERMINAL.includes(state.run.status);

function defaults(exp, mode = exp.defaultMode) {
  const l = exp.limits;
  const m = exp.modes.find(x => x.mode === mode);
  return {
    mode,
    isolation: m.isolation,
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
    await selectExperiment(wanted.id, false);
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

async function selectExperiment(id, keepRun = false) {
  state.exp = state.experiments.find(e => e.id === id);
  state.cfg = defaults(state.exp);
  state.error = '';
  state.comparison = null;
  state.compareRuns = null;
  state.compareEvents = null;
  state.pick = { left: null, right: null };
  if (!keepRun) { closeStream(); state.run = null; state.events = new Map(); state.selected = null; state.moment = null; }
  updateUrl();
  await loadHistory();
  renderAll();
}

async function loadHistory() {
  try {
    state.history = (await api(`/runs?experiment=${state.exp.id}&limit=15`)).data;
  } catch { state.history = []; }
}

function updateUrl() {
  const q = new URLSearchParams({ experiment: state.exp.id });
  if (state.run) q.set('run', state.run.id);
  history.replaceState(null, '', `?${q}`);
}

// ---- running -------------------------------------------------------------------------------------------
async function runRace(cfg = state.cfg, compareWith = null) {
  if (state.busy) return;
  state.busy = true; state.error = ''; state.confirmReset = false;
  renderConfigure();
  try {
    const created = await api('/runs', { method: 'POST', body: JSON.stringify({ experiment: state.exp.id, ...cfg }) });
    show(created, []);
    session().runs++;
    const started = await api(`/runs/${created.id}/start`, { method: 'POST' });
    state.run = started;
    follow(created.id, compareWith);
    $('timeline').scrollIntoView({ behavior: 'smooth', block: 'start' });
  } catch (e) {
    state.busy = false; state.error = e.message;
    renderAll();
  }
}

function show(run, events) {
  closeStream();
  state.run = run;
  state.events = new Map(events.map(e => [e.seq, e]));
  state.selected = null; state.moment = null; state.followTail = true; state.painted = 0; state.compareEvents = null;
  updateUrl();
  renderAll();
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
    schedule();
  });
  es.addEventListener('run', m => {
    const run = JSON.parse(m.data);
    if (state.run?.id !== run.id) return;
    state.run = run;
    if (TERMINAL.includes(run.status)) { es.close(); state.stream = null; finished(compareWith); }
    else schedule();
  });
  es.onerror = () => {
    if (state.stream !== es) return;
    es.close(); state.stream = null;
    if (!done()) poll(id, compareWith);
  };
}

/** Fallback when the event stream drops: the same data, read back by polling. */
async function poll(id, compareWith) {
  while (state.run?.id === id && !done()) {
    try {
      const after = Math.max(0, ...state.events.keys());
      const [run, events] = await Promise.all([api(`/runs/${id}`), api(`/runs/${id}/events?after=${after}`)]);
      for (const e of events.data) state.events.set(e.seq, e);
      state.run = run;
      schedule();
    } catch { /* keep trying */ }
    await new Promise(r => setTimeout(r, 400));
  }
  if (state.run?.id === id) finished(compareWith);
}

async function finished(compareWith) {
  state.busy = false;
  state.followTail = false;
  state.rewind = true;
  state.fit = true;
  try {
    const [run, events] = await Promise.all([api(`/runs/${state.run.id}`), api(`/runs/${state.run.id}/events?limit=5000`)]);
    state.run = run;
    for (const e of events.data) state.events.set(e.seq, e);
  } catch { /* the streamed copy stands */ }
  const lesson = session();
  if (state.run.result) {
    if (!state.run.result.invariant.holds) lesson.violated = true;
    else if (lesson.violated && modeInfo(state.run.config.mode)?.fixes) lesson.fixed = true;
  }
  await loadHistory();
  if (compareWith) {
    state.pick = { left: compareWith, right: state.run.id };
    await compare();
  }
  renderAll();
}

async function openRun(id) {
  try {
    const [run, events] = await Promise.all([api(`/runs/${id}`), api(`/runs/${id}/events?limit=5000`)]);
    if (run.experiment !== state.exp?.id) await selectExperiment(run.experiment, true);
    show(run, events.data);
    if (!TERMINAL.includes(run.status)) { state.busy = true; follow(id, null); }
  } catch (e) {
    state.error = e.message; renderConfigure();
  }
}

let frame = 0;
function schedule() {
  if (frame) return;
  frame = requestAnimationFrame(() => { frame = 0; renderTimeline(); renderMoments(); renderHead(); renderInspector(); renderResult(); renderWhy(); renderPath(); });
}

// ---- lesson path ---------------------------------------------------------------------------------------
function renderPath() {
  const lesson = session();
  const completed = done() && state.run?.status === 'COMPLETED';
  const violated = completed && state.run.result && !state.run.result.invariant.holds;
  const stops = [
    ['Configure', 'configure', lesson.runs > 0 ? 'done' : 'waiting'],
    ['Run', 'configure', state.busy ? 'waiting' : lesson.runs > 0 ? 'done' : ''],
    ['Live', 'timeline', state.busy ? 'waiting' : lesson.runs > 0 ? 'done' : ''],
    ['Explain', 'why', completed ? 'done' : ''],
    ['Result', 'result', completed ? 'done' : ''],
    ['Fix', 'fix', lesson.fixed ? 'done' : violated ? 'waiting' : ''],
    ['Compare', 'compare-section', lesson.compared ? 'done' : lesson.runs > 1 ? 'waiting' : ''],
  ];
  fill($('path'), ...stops.map(([label, target, st]) =>
    h('li', { class: st }, h('a', { href: `#${target}` }, h('i'), h('span', {}, label)))));
}

// ---- experiments ---------------------------------------------------------------------------------------
function renderExperiments() {
  fill($('experiments'), ...state.experiments.map(e =>
    h('button', { type: 'button', 'aria-pressed': String(e.id === state.exp?.id), onclick: () => e.id !== state.exp.id && selectExperiment(e.id) },
      h('b', {}, String(e.number).padStart(2, '0')), h('strong', {}, e.title))));
}

function renderLearn() {
  const e = state.exp;
  const m = modeInfo();
  fill($('learn'),
    h('p', { class: 'kicker' }, e.category),
    h('p', { class: 'q' }, e.question),
    h('p', { class: 'inv' }, h('span', {}, 'Invariant'), e.invariant),
    h('p', { class: 'story' }, e.unsafeStory),
    h('details', {},
      h('summary', {}, m ? `${m.label} · how it runs` : 'How it runs'),
      h('p', {}, e.learn),
      m ? h('p', {}, m.how) : null,
      m ? h('pre', { class: 'code' }, m.sql) : null));
}

// ---- configure -----------------------------------------------------------------------------------------
function renderConfigure() {
  const e = state.exp, l = e.limits, c = state.cfg;
  const set = patch => { Object.assign(state.cfg, patch); state.confirmReset = false; renderLearn(); renderConfigure(); };
  const seg = (label, options, value, onpick, extra = {}) => h('fieldset', {},
    h('legend', {}, label),
    h('div', { class: 'seg', role: 'group', 'aria-label': label }, options.map(([v, text, cls, disabled, title]) =>
      h('button', { type: 'button', class: cls || '', 'aria-pressed': String(v === value), disabled: disabled || state.busy, title, onclick: () => onpick(v) }, text))),
    extra.hint ? h('p', { class: 'hint' }, extra.hint) : null);

  const requestOptions = [2, 5, 10].filter(n => n >= l.minRequests && n <= l.maxRequests);
  const custom = !requestOptions.includes(c.requests);
  const numberInput = (value, min, max, onchange, label) => h('input', { type: 'number', value, min, max, 'aria-label': label, disabled: state.busy,
    onchange: ev => { const v = Math.max(min, Math.min(max, Number(ev.target.value) || min)); onchange(v); } });
  const forcedIsolation = c.mode === 'SERIALIZABLE';
  const armed = modeInfo() && !modeInfo().fixes;

  fill($('configure'), 
    h('h2', { id: 'configure-title' }, 'Configure'),
    h('p', { class: 'hint' }, 'New request, order and customer ids every run'),
    seg('Mode', e.modes.map(m => [m.mode, m.label, m.fixes ? 'fixes' : 'breaks', false, m.how]), c.mode,
      mode => set({ mode, isolation: e.modes.find(x => x.mode === mode).isolation, maxRetries: MODE_RETRIES.includes(mode) ? e.limits.defaultRetries : 0 })),
    l.minRequests === l.maxRequests
      ? h('div', { class: 'field' }, h('span', {}, 'Concurrent requests'), h('strong', {}, `${l.minRequests} (${e.roles.join(' · ') || 'fixed'})`))
      : h('fieldset', {}, h('legend', {}, 'Concurrent requests'),
        h('div', { class: 'inline' },
          h('div', { class: 'seg', role: 'group', 'aria-label': 'Concurrent requests' }, requestOptions.map(n =>
            h('button', { type: 'button', 'aria-pressed': String(n === c.requests), disabled: state.busy, onclick: () => set({ requests: n }) }, n))),
          numberInput(c.requests, l.minRequests, l.maxRequests, v => set({ requests: v }), 'Custom number of requests'),
          h('span', { class: 'hint' }, custom ? 'custom' : `up to ${l.maxRequests}`))),
    h('div', { class: 'row2' },
      l.minValue !== l.maxValue ? h('label', { class: 'field' }, h('span', {}, e.valueLabel), numberInput(c.initialValue, l.minValue, l.maxValue, v => set({ initialValue: v }), e.valueLabel)) : h('div'),
      h('label', { class: 'field' }, h('span', {}, 'Retries after an abort'), numberInput(c.maxRetries, 0, 10, v => set({ maxRetries: v }), 'Retries'))),
    h('label', { class: 'field' }, h('span', {}, `Delay between read and write: ${c.delayMs} ms`),
      h('div', { class: 'inline' },
        h('input', { type: 'range', min: 0, max: 1000, step: 10, value: Math.min(c.delayMs, 1000), disabled: state.busy, 'aria-label': 'Delay in milliseconds',
          oninput: ev => { state.cfg.delayMs = Number(ev.target.value); ev.target.closest('label').querySelector('span').textContent = `Delay between read and write: ${state.cfg.delayMs} ms`; },
          onchange: () => renderConfigure() }),
        numberInput(c.delayMs, 0, 5000, v => set({ delayMs: v }), 'Delay in milliseconds')),
      h('span', { class: 'hint' }, 'Application work between reading and writing: the race window.')),
    seg('Transaction isolation', [
      ['READ_COMMITTED', 'Read committed', '', forcedIsolation],
      ['REPEATABLE_READ', 'Repeatable read', '', forcedIsolation],
      ['SERIALIZABLE', 'Serializable', '', false]], c.isolation,
      isolation => set({ isolation }),
      { hint: forcedIsolation ? 'Serializable mode always runs at SERIALIZABLE.' : `Default for ${modeInfo()?.label}: ${modeInfo()?.isolation.replace('_', ' ').toLowerCase()}.` }),
    seg('Interleaving', [['CONTROLLED', 'Controlled'], ['NATURAL', 'Natural']], c.interleaving, interleaving => set({ interleaving }),
      { hint: c.interleaving === 'CONTROLLED'
        ? 'Requests take turns (A reads, then B) and meet before writing, so the dangerous schedule happens every run. Locks, versions and aborts stay PostgreSQL\'s; the lab only chooses when each request sends its next statement.'
        : 'All requests start together; the scheduler and the database decide. With no delay the race window is tiny, and the bug hides.' }),
    h('div', { class: 'run-row' },
      h('button', { type: 'submit', class: `primary-action${armed ? ' armed' : ''}`, disabled: state.busy, title: armed ? 'This strategy can break the invariant' : null },
        state.busy ? 'Running…' : 'Run race'),
      h('button', { type: 'button', class: state.confirmReset ? 'danger' : 'quiet', disabled: state.busy, title: 'Deletes the lab\'s runs and scenario rows (schema race_lab only)',
        onclick: resetLab }, state.confirmReset ? 'Confirm: delete all runs' : 'Reset lab data')),
    h('p', { class: 'form-error', role: 'alert' }, state.error));
}
$('configure').addEventListener('submit', ev => { ev.preventDefault(); runRace(); });

async function resetLab() {
  if (!state.confirmReset) { state.confirmReset = true; renderConfigure(); return; }
  state.confirmReset = false;
  try {
    const r = await api('/reset', { method: 'POST' });
    const total = Object.values(r.deleted).reduce((a, b) => a + b, 0);
    state.error = '';
    state.session = {};
    await selectExperiment(state.exp.id);
    $('configure').querySelector('.form-error').textContent = `Deleted ${total} lab rows; ids restart at 1.`;
  } catch (e) { state.error = e.message; renderConfigure(); }
}

// ---- timeline head -------------------------------------------------------------------------------------
function renderHead() {
  const r = state.run;
  if (!r) {
    fill($('timeline-head'), h('div', { class: 'title' }, h('h2', { id: 'observe-title' }, 'Observe'), h('span', { class: 'hint' }, 'Run the race to draw its interleaving.')));
    return;
  }
  const c = r.config;
  const m = state.exp.modes.find(x => x.mode === c.mode);
  const inv = r.result?.invariant;
  const status = r.status === 'COMPLETED'
    ? h('span', { class: `pill ${inv.holds ? 'good' : 'bad'}` }, h('i'), inv.holds ? 'Invariant preserved' : 'Invariant violated')
    : r.status === 'FAILED' ? h('span', { class: 'pill bad' }, h('i'), 'Run failed')
    : h('span', { class: 'pill running' }, h('i'), `Running · ${state.events.size} events`);
  fill($('timeline-head'), 
    h('div', { class: 'title' },
      h('h2', { id: 'observe-title' }, `Run #${r.id} · ${state.exp.title}`),
      h('div', { class: 'chips' }, chip(m?.label || c.mode, m?.fixes ? 'good' : 'bad'), chip(c.isolation.replace('_', ' ').toLowerCase()),
        chip(plural(c.requests, 'request')), chip(`${c.delayMs} ms delay`), chip(c.interleaving.toLowerCase()), c.maxRetries ? chip(`${c.maxRetries} retries`) : null)),
    h('div', { class: 'tools' },
      status,
      h('div', { class: 'seg', role: 'group', 'aria-label': 'Time scale' },
        [['sequence', 'Sequence'], ['real', 'Real time']].map(([v, t]) => h('button', { type: 'button', 'aria-pressed': String(state.scale === v), title: v === 'sequence' ? 'Every step gets room; gaps grow with elapsed time' : 'Linear time: widths are durations',
          onclick: () => { state.scale = v; renderHead(); renderTimeline(); } }, t))),
      h('button', { type: 'button', class: 'small-button', 'aria-pressed': String(state.fit), title: 'Fit the whole run in the panel', onclick: () => { state.fit = !state.fit; renderHead(); renderTimeline(); } }, state.fit ? 'Fitted' : 'Fit'),
      h('button', { type: 'button', class: 'small-button', title: 'Zoom out', 'aria-label': 'Zoom out', onclick: () => { state.fit = false; state.zoom = Math.max(.5, state.zoom / 1.3); renderHead(); renderTimeline(); } }, '−'),
      h('button', { type: 'button', class: 'small-button', title: 'Zoom in (room for every label)', 'aria-label': 'Zoom in', onclick: () => { state.fit = false; state.zoom = Math.min(4, state.zoom * 1.3); renderHead(); renderTimeline(); } }, '+'),
      traceLink(r.traceId)));
}

// ---- moments -------------------------------------------------------------------------------------------
/** Server highlights once the run is done; while it runs, the same kinds derived from events as they come. */
function moments() {
  if (state.run?.result?.highlights) return state.run.result.highlights;
  const out = [];
  for (const e of sorted()) {
    if (e.type === 'TRANSACTION_BLOCKED' && !e.message?.startsWith('now'))
      out.push({ kind: 'BLOCKED', tone: 'wait', atMicros: e.atMicros, lanes: [e.lane, ...(e.blockedBy || [])], seqs: [e.seq], title: `Transaction ${e.lane} is waiting for ${e.lock ? 'the ' + e.lock : 'a lock'} held by ${(e.blockedBy || []).join(', ')}`, detail: 'PostgreSQL reports the backend waiting (pg_blocking_pids).' });
    if (e.type === 'VERSION_CONFLICT')
      out.push({ kind: 'VERSION_REJECTED', tone: 'retry', atMicros: e.atMicros, lanes: [e.lane], seqs: [e.seq], title: `Optimistic lock rejected ${e.lane} because expected version=${e.expectedVersion}, actual version=${e.actualVersion}`, detail: e.message });
    if (e.type === 'SERIALIZATION_FAILURE')
      out.push({ kind: 'SERIALIZATION_FAILURE', tone: 'retry', atMicros: e.atMicros, lanes: [e.lane], seqs: [e.seq], title: `Serialization failure: transaction ${e.lane} must retry`, detail: e.message });
    if (e.type === 'DEADLOCK_DETECTED')
      out.push({ kind: 'DEADLOCK', tone: 'bad', atMicros: e.atMicros, lanes: [e.lane, ...(e.blockedBy || [])], seqs: [e.seq], title: `Deadlock: PostgreSQL aborted ${e.lane}`, detail: e.data?.postgresDetail || e.message });
  }
  return out;
}

function renderMoments() {
  const list = moments();
  const callout = $('race-callout');
  const lead = list.find(m => m.tone === 'bad') || list.find(m => m.tone === 'retry' || m.tone === 'wait') || null;
  if (!lead) callout.hidden = true;
  else {
    callout.hidden = false;
    callout.className = `race-callout ${lead.tone || ''}`;
    const index = list.indexOf(lead);
    fill(callout,
      h('span', { class: 'mark' }, lead.tone === 'bad' ? 'Race' : 'Moment'),
      h('div', {}, h('strong', {}, lead.title), lead.detail ? h('p', {}, lead.detail) : null),
      h('button', { type: 'button', class: 'small-button', onclick: () => selectMoment(index) }, 'Show on timeline'));
  }
  fill($('moments-strip'), ...list.map((m, i) =>
    h('button', { type: 'button', class: `moment ${m.tone}`, 'aria-pressed': String(state.moment === i), title: `${m.title}\n${m.detail || ''}`,
      onclick: () => selectMoment(i) }, h('b', {}, String(i + 1).padStart(2, '0')), h('span', {}, m.title))));
}

function selectMoment(i) {
  state.moment = state.moment === i ? null : i;
  state.selected = null;
  renderMoments(); renderTimeline(); renderInspector();
  if (state.moment !== null) scrollToMicros(moments()[i].atMicros);
}

// ---- timeline ------------------------------------------------------------------------------------------
const LANE = 92, DBH = 72, RULER = 48, PAD = 36;
let xOf = () => 0;

const MARKER = {
  TRANSACTION_STARTED: e => [e.attempt > 1 ? `BEGIN #${e.attempt}` : 'BEGIN', 'op'],
  READ_PERFORMED: e => [e.data?.purpose === 'diagnose' ? `RE-READ ${e.valueRead || ''}` : `READ ${e.valueRead || '—'}`, 'op'],
  DECISION_MADE: e => [e.ok ? 'CHECK ✓' : 'CHECK ✗', e.ok ? 'check-ok' : 'check-no'],
  LOCK_ACQUIRED: e => e.waitMicros > 0 ? ['GETS LOCK', 'op'] : null,
  TRANSACTION_BLOCKED: e => e.message?.startsWith('now') ? null : [`WAITS for ${(e.blockedBy || []).join(',')}`, 'wait'],
  WRITE_PERFORMED: e => e.rows > 0
    ? [verb(e.sql) === 'INSERT' ? `INSERT ${e.target}` : `${verb(e.sql)} ${e.valueWritten || ''}`.trim(), 'op']
    : [`${verb(e.sql)} → 0 rows`, 'check-no'],
  VERSION_CONFLICT: e => [`STALE v${e.expectedVersion}≠v${e.actualVersion}`, 'retry'],
  SERIALIZATION_FAILURE: () => ['ABORT 40001', 'retry'],
  DEADLOCK_DETECTED: () => ['DEADLOCK 40P01', 'bad'],
  TRANSACTION_COMMITTED: () => ['COMMIT', 'good'],
  TRANSACTION_ROLLED_BACK: e => ['ROLLBACK', /aborted|stale|error/.test(e.message || '') ? 'bad' : 'op'],
  RETRY_SCHEDULED: () => ['RETRY', 'retry'],
};
const verb = sql => (sql || '').split(' ')[0].toUpperCase() || 'WRITE';

function spans(events) {
  const times = [];
  for (const e of events) {
    times.push(e.atMicros);
    if (e.durationMicros && ['TRANSACTION_UNBLOCKED', 'DELAY', 'SYNC_POINT', 'TRANSACTION_COMMITTED', 'TRANSACTION_ROLLED_BACK'].includes(e.type)) times.push(e.atMicros + e.durationMicros);
  }
  return times;
}

function buildScale(events, lanesWidth) {
  const times = [...new Set(spans(events))].sort((a, b) => a - b);
  if (!times.length) return { xOf: () => PAD, width: lanesWidth };
  if (state.scale === 'real') {
    const t0 = times[0], t1 = Math.max(times[times.length - 1], t0 + 1);
    const per = Math.max(lanesWidth - 2 * PAD, 200) * (state.fit ? 1 : state.zoom) / (t1 - t0);
    return { xOf: t => PAD + (t - t0) * per, width: PAD * 2 + (t1 - t0) * per };
  }
  const xs = new Map();
  let x = PAD, prev = null;
  for (const t of times) {
    if (prev !== null) { const dt = (t - prev) / 1000; x += dt <= 0.02 ? 1 : state.zoom * (12 + 30 * Math.log10(1 + dt)); }
    xs.set(t, x); prev = t;
  }
  // Room for every marker's label: consecutive markers of one lane stay at least MIN apart.
  const MIN = 78 * Math.max(.7, state.zoom);
  const last = {};
  for (const e of events) {
    if (!markerOf(e)) continue;
    const cur = xs.get(e.atMicros);
    const need = (last[e.lane] ?? -Infinity) + MIN;
    if (cur < need) { const shift = need - cur; for (const t of times) if (t >= e.atMicros) xs.set(t, xs.get(t) + shift); }
    last[e.lane] = xs.get(e.atMicros);
  }
  // Fit: the whole run in the panel's width; labels that no longer fit are dropped, not overlapped.
  const lastX = xs.get(times[times.length - 1]);
  if (state.fit && lastX + 2 * PAD > lanesWidth) {
    const k = (lanesWidth - 3 * PAD) / Math.max(1, lastX - PAD);
    for (const t of times) xs.set(t, PAD + (xs.get(t) - PAD) * k);
  }
  const keys = times;
  const f = t => {
    if (xs.has(t)) return xs.get(t);
    let lo = 0, hi = keys.length - 1;
    if (t <= keys[0]) return xs.get(keys[0]);
    if (t >= keys[hi]) return xs.get(keys[hi]);
    while (hi - lo > 1) { const mid = (lo + hi) >> 1; keys[mid] <= t ? lo = mid : hi = mid; }
    const a = keys[lo], b = keys[hi];
    return xs.get(a) + (xs.get(b) - xs.get(a)) * (t - a) / (b - a);
  };
  return { xOf: f, width: xs.get(keys[keys.length - 1]) + PAD * 2 };
}
const LABEL_MAX = 18;
const markerOf = e => {
  const m = MARKER[e.type]?.(e);
  return m ? [m[0].length > LABEL_MAX ? m[0].slice(0, LABEL_MAX - 1) + '…' : m[0], m[1]] : null;
};

function renderTimeline(host = $('timeline'), run = state.run, eventList = null) {
  const live = host === $('timeline');
  if (!live && !run) return;
  if (!run) {
    fill(host, h('div', { class: 'empty' },
      h('strong', {}, 'No run yet'),
      h('span', {}, 'Each request gets its own lane and its own PostgreSQL connection. Configure the race and press Run race.')));
    return;
  }
  const scroller = host.querySelector('.lanes-scroll');
  const keepLeft = scroller?.scrollLeft ?? 0;
  const events = eventList || sorted();
  const lanes = run.requests || [];
  const laneIndex = Object.fromEntries(lanes.map((r, i) => [r.lane, i]));
  const width0 = Math.max(600, host.clientWidth - 152);
  const scale = buildScale(events, width0);
  xOf = scale.xOf;
  const now = events.length ? events[events.length - 1].atMicros : 0;
  const width = Math.max(scale.width, width0);
  const height = RULER + lanes.length * LANE + DBH;
  const cy = lane => RULER + laneIndex[lane] * LANE + LANE / 2;
  const selected = live ? state.selected : null;
  const marks = live ? moments() : (run.result?.highlights || []);
  const moment = live && state.moment !== null ? marks[state.moment] : null;
  const lead = marks.find(m => m.tone === 'bad') || marks.find(m => m.tone === 'retry' || m.tone === 'wait') || null;
  const inMoment = e => moment && (moment.seqs || []).includes(e.seq);

  const layers = { bg: [], moments: [], bars: [], spans: [], arrows: [], marks: [], ruler: [] };
  // backgrounds
  lanes.forEach((r, i) => layers.bg.push(
    s('rect', { class: `lane-bg${i % 2 ? ' alt' : ''}`, x: 0, y: RULER + i * LANE, width, height: LANE }),
    s('line', { class: 'lane-rule', x1: 0, x2: width, y1: RULER + (i + 1) * LANE, y2: RULER + (i + 1) * LANE })));
  layers.bg.push(s('rect', { class: 'lane-bg db', x: 0, y: RULER + lanes.length * LANE, width, height: DBH }));
  layers.bg.push(s('line', { class: 'ruler-rule', x1: 0, x2: width, y1: RULER, y2: RULER }));

  // ruler ticks: elapsed time since the first transaction began
  const t0 = events.find(e => e.type === 'TRANSACTION_STARTED')?.atMicros ?? 0;
  let lastTick = -Infinity;
  for (const e of events) {
    if (!markerOf(e) || e.atMicros < t0) continue;
    const x = xOf(e.atMicros);
    if (x - lastTick < 96) continue;
    lastTick = x;
    layers.ruler.push(s('line', { class: 'tick', x1: x, x2: x, y1: RULER - 6, y2: RULER }),
      s('text', { class: 'tick-label', x: x + 3, y: RULER - 8 }, `+${((e.atMicros - t0) / 1000).toFixed(e.atMicros - t0 < 10_000 ? 1 : 0)} ms`));
  }

  // transactions: one bar per attempt, from BEGIN to COMMIT/ROLLBACK
  const open = {};
  for (const e of events) {
    if (!(e.lane in laneIndex)) continue;
    const key = `${e.lane}#${e.attempt}`;
    if (e.type === 'TRANSACTION_STARTED') open[key] = e;
    if ((e.type === 'TRANSACTION_COMMITTED' || e.type === 'TRANSACTION_ROLLED_BACK') && open[key]) {
      bar(open[key], e.atMicros + (e.durationMicros || 0), e.type === 'TRANSACTION_COMMITTED' ? 'committed' : 'rolled');
      delete open[key];
    }
  }
  for (const b of Object.values(open)) bar(b, now, 'open');
  function bar(begin, end, cls) {
    const x1 = xOf(begin.atMicros), x2 = Math.max(xOf(end), x1 + 8), y = cy(begin.lane);
    layers.bars.push(s('rect', { class: `tx ${cls}`, x: x1, y: y - 9, width: x2 - x1, height: 18, rx: 9 }));
  }

  // spans: application work, lab choreography, lock waits (closed and still open)
  const waiting = {};
  for (const e of events) {
    if (!(e.lane in laneIndex)) continue;
    const y = cy(e.lane);
    if (e.type === 'DELAY' || e.type === 'SYNC_POINT') {
      const x1 = xOf(e.atMicros), x2 = xOf(e.atMicros + e.durationMicros);
      if (x2 - x1 < 2) continue;
      layers.spans.push(s('rect', { class: e.type === 'DELAY' ? 'delay' : 'sync', x: x1, y: y - 5, width: x2 - x1, height: 10, rx: 3, fill: e.type === 'DELAY' ? `url(#dots-${host.id})` : `url(#hatch-${host.id})` },
        s('title', {}, e.type === 'DELAY' ? `Application work ${ms(e.durationMicros)}` : `Lab choreography ${ms(e.durationMicros)}: ${e.message}`)));
    }
    if (e.type === 'TRANSACTION_BLOCKED' && !e.message?.startsWith('now')) {
      waiting[`${e.lane}#${e.attempt}`] = e;
      for (const other of e.blockedBy || []) if (other in laneIndex) {
        const x = xOf(e.atMicros), yo = cy(other), dir = yo > y ? 1 : -1;
        layers.arrows.push(s('path', { class: 'wait-arrow', d: `M ${x} ${y + dir * 10} C ${x + 34} ${y + dir * 26}, ${x + 34} ${yo - dir * 26}, ${x + 8} ${yo - dir * 10}`, 'marker-end': `url(#arrow-${host.id})` },
          s('title', {}, `${e.lane} waits for ${other}`)));
      }
    }
    if (e.type === 'TRANSACTION_UNBLOCKED') {
      delete waiting[`${e.lane}#${e.attempt}`];
      waitSpan(e.lane, e.atMicros, e.atMicros + e.durationMicros, e.blockedBy, e.waitMicros);
    }
  }
  for (const b of Object.values(waiting)) waitSpan(b.lane, b.atMicros, now, b.blockedBy, now - b.atMicros);
  function waitSpan(lane, from, to, by, micros) {
    const x1 = xOf(from), x2 = Math.max(xOf(to), x1 + 6), y = cy(lane);
    layers.spans.push(s('rect', { class: 'wait', x: x1, y: y - 13, width: x2 - x1, height: 26, rx: 7 },
      s('title', {}, `${lane} waiting for ${(by || []).join(', ')}: ${ms(micros)}`)));
    if (x2 - x1 > 72) layers.spans.push(s('text', { class: 'sub wait-label', x: (x1 + x2) / 2 + 6, y: y + 4, 'text-anchor': 'middle' }, `waits ${ms(micros)}`));
  }

  // markers
  const row = {};
  const lastX = {};
  const labelEnd = {}; // lane#row -> right edge of the last label drawn there
  for (const e of events) {
    if (!(e.lane in laneIndex)) continue;
    const m = markerOf(e);
    if (!m) continue;
    const [label, cls] = m;
    const x = xOf(e.atMicros), y = cy(e.lane);
    const close = x - (lastX[e.lane] ?? -Infinity) < 120;
    row[e.lane] = close ? 1 - (row[e.lane] ?? 1) : 0;
    lastX[e.lane] = x;
    const above = row[e.lane] === 0;
    const half = label.length * 3.4 + 4;
    const slot = `${e.lane}#${row[e.lane]}`;
    const showLabel = x - half > (labelEnd[slot] ?? -Infinity);
    if (showLabel) labelEnd[slot] = x + half;
    const textCls = cls === 'good' ? 'good' : cls === 'bad' || cls === 'check-no' ? 'bad' : cls === 'retry' ? 'retry' : cls === 'wait' ? 'wait' : '';
    const fresh = live && !done() && e.seq > state.painted;
    layers.marks.push(s('g', { class: `ev${fresh ? ' arrive' : ''}${e.seq === selected || inMoment(e) ? ' selected' : ''}`, tabindex: 0, role: 'button', 'data-seq': e.seq,
      'aria-label': `${e.lane}: ${label}`, onclick: live ? () => select(e.seq) : null, onkeydown: ev => { if (live && (ev.key === 'Enter' || ev.key === ' ')) { ev.preventDefault(); select(e.seq); } } },
      s('title', {}, `${e.lane} · ${label}${e.message ? `\n${e.message}` : ''}\n+${((e.atMicros - t0) / 1000).toFixed(2)} ms · click for details`),
      s('circle', { class: `dot ${cls}`, cx: x, cy: y, r: 6 }),
      showLabel ? s('text', { class: `${textCls}${/READ|UPDATE|INSERT|STALE/.test(label) ? ' mono' : ''}`, x, y: above ? y - 16 : y + 28, 'text-anchor': 'middle' }, label) : null));
  }

  // database lane: committed state as the engine read it after each transaction
  const dbY = RULER + lanes.length * LANE, dbC = dbY + DBH / 2;
  const states = events.filter(e => e.type === 'STATE_OBSERVED');
  const invariant = events.find(e => e.type === 'INVARIANT_CHECKED');
  const endX = Math.max(xOf(now), ...states.map(e => xOf(e.atMicros))) + 20;
  if (states.length) layers.bars.push(s('line', { class: 'db-line', x1: xOf(states[0].atMicros), x2: endX, y1: dbC, y2: dbC }));
  let pillEnd = -Infinity;
  states.forEach((st, i) => {
    const x = xOf(st.atMicros);
    const final = i === states.length - 1;
    const next = final ? Infinity : xOf(states[i + 1].atMicros);
    const text = st.valueWritten || '';
    const w = Math.min(Math.max(text.length * 6.4 + 16, 40), final ? Infinity : Math.max(next - x - 8, 40));
    const last = final && invariant;
    // The final state keeps its full text: right-aligned at the edge if it would run past it.
    const px = final ? Math.max(pillEnd + 4, Math.min(x + 4, width - 6 - w)) : x + 4;
    pillEnd = px + w;
    layers.marks.push(s('g', { class: `ev${st.seq === selected ? ' selected' : ''}`, tabindex: 0, role: 'button', 'aria-label': `Committed state: ${text}`, onclick: () => select(st.seq) },
      s('title', {}, `${st.message}\n${text}`),
      s('rect', { class: `db-pill${last ? (invariant.ok ? ' good' : ' bad') : ''}`, x: px, y: dbC - 12, width: w, height: 24, rx: 12 }),
      s('text', { class: 'db-value', x: px + 8, y: dbC + 4 }, fit(text, w - 14)),
      s('circle', { class: 'db-change', cx: x, cy: dbC, r: 4 })));
  });

  // key moments: a numbered badge on the ruler, a guide through the lanes involved, a band for durations
  marks.forEach((m, i) => {
    const involved = (m.lanes || []).filter(l => l in laneIndex);
    const ys = involved.length ? involved.map(l => RULER + laneIndex[l] * LANE) : [RULER];
    const top = Math.min(...ys), bottom = involved.length ? Math.max(...ys) + LANE : height;
    const x = xOf(m.atMicros);
    const dim = moment && moment !== m;
    const g = s('g', { class: `moment-g${dim ? ' dim' : ''}` });
    if (m.untilMicros && xOf(m.untilMicros) - x > 4) g.append(s('rect', { class: `moment-band tone-${m.tone}`, x, y: top, width: xOf(m.untilMicros) - x, height: bottom - top }));
    g.append(s('line', { class: `moment-line tone-${m.tone}${m === lead ? ' race' : ''}`, x1: x, x2: x, y1: RULER - 12, y2: bottom }));
    g.append(s('g', { class: 'moment-badge', style: live ? 'cursor:pointer' : '', onclick: live ? () => selectMoment(i) : null },
      s('title', {}, m.title), s('circle', { class: `tone-${m.tone}`, cx: x, cy: RULER - 22, r: 10 }), s('text', { x, y: RULER - 18.5, 'text-anchor': 'middle' }, i + 1)));
    layers.moments.push(g);
  });
  if (live && !done() && events.length) layers.ruler.push(s('line', { class: 'now-line', x1: xOf(now), x2: xOf(now), y1: RULER, y2: height }));
  if (live) state.painted = events.reduce((n, e) => Math.max(n, e.seq), state.painted);

  const svg = s('svg', { class: 'lanes', width, height, viewBox: `0 0 ${width} ${height}`, role: 'img', 'aria-label': `Timeline of run ${run.id}` },
    s('defs', {},
      s('marker', { id: `arrow-${host.id}`, viewBox: '0 0 10 10', refX: 8, refY: 5, markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse' }, s('path', { d: 'M 0 0 L 10 5 L 0 10 z', fill: '#356ae6' })),
      s('pattern', { id: `dots-${host.id}`, width: 5, height: 5, patternUnits: 'userSpaceOnUse' }, s('circle', { cx: 2.5, cy: 2.5, r: 1.1, fill: '#8994a7' })),
      s('pattern', { id: `hatch-${host.id}`, width: 6, height: 6, patternUnits: 'userSpaceOnUse', patternTransform: 'rotate(45)' }, s('rect', { width: 2, height: 6, fill: '#c9d2df' }))),
    ...layers.bg, ...layers.moments, ...layers.bars, ...layers.spans, ...layers.arrows, ...layers.marks, ...layers.ruler);

  const settled = TERMINAL.includes(run.status);
  const outcome = lane => events.find(e => e.type === 'REQUEST_COMPLETED' && e.lane === lane);
  const OUT = { SUCCEEDED: ['succeeded', 'good'], REJECTED: ['declined', ''], ABORTED: ['aborted', 'bad'], FAILED: ['failed', 'bad'] };
  const labels = h('div', { class: 'lane-labels' },
    h('div', { class: 'ruler-space' }, state.scale === 'sequence' ? 'Sequence scale' : 'Real time'),
    lanes.map(r => {
      const o = outcome(r.lane);
      const [text, tone] = o ? OUT[o.outcome] || [o.outcome, ''] : [settled ? '—' : 'running', settled ? '' : 'wait'];
      return h('div', { class: 'lane-label', title: `${r.requestId} · ${r.orderId} · ${r.customerId}` },
        h('b', {}, r.lane), h('strong', {}, r.role ? `${r.lane} · ${r.role}` : `Request ${r.lane}`),
        h('span', { class: 'outcome' }, chip(text, tone)));
    }),
    h('div', { class: 'lane-label db' }, h('b', {}, 'DB'), h('strong', {}, 'Committed state'), h('small', {}, 'read after each commit')));
  const scroll = h('div', { class: 'lanes-scroll' }, svg);
  if (live) scroll.addEventListener('scroll', () => { state.followTail = scroll.scrollLeft + scroll.clientWidth >= scroll.scrollWidth - 30; });
  fill(host, labels, scroll);
  if (!live) scroll.scrollLeft = 0;
  else if (!done() && state.followTail) scroll.scrollLeft = scroll.scrollWidth;
  else if (state.rewind) { scroll.scrollLeft = 0; state.rewind = false; }
  else scroll.scrollLeft = keepLeft;
}

function fit(text, px) {
  const max = Math.max(3, Math.floor(px / 6.4));
  return text.length > max ? text.slice(0, max - 1) + '…' : text;
}

function scrollToMicros(micros) {
  const scroll = $('timeline').querySelector('.lanes-scroll');
  if (!scroll) return;
  scroll.scrollTo({ left: Math.max(0, xOf(micros) - scroll.clientWidth / 3), behavior: 'smooth' });
}

function select(seq) {
  state.selected = seq; state.moment = null;
  renderTimeline(); renderMoments(); renderInspector();
  const e = state.events.get(seq);
  if (e) scrollToMicros(e.atMicros);
}

$('timeline').addEventListener('keydown', ev => {
  if (!['ArrowLeft', 'ArrowRight'].includes(ev.key)) return;
  const list = sorted().filter(markerOf);
  if (!list.length) return;
  let i = list.findIndex(e => e.seq === state.selected);
  i = ev.key === 'ArrowRight' ? Math.min(list.length - 1, i + 1) : Math.max(0, i < 0 ? 0 : i - 1);
  ev.preventDefault();
  select(list[i].seq);
  $('timeline').querySelector(`[data-seq="${list[i].seq}"]`)?.focus();
});

// ---- inspector -----------------------------------------------------------------------------------------
const TYPE = t => t.toLowerCase().replaceAll('_', ' ').replace(/^./, c => c.toUpperCase());

function renderInspector() {
  const box = $('inspector');
  const e = state.selected !== null ? state.events.get(state.selected) : null;
  const moment = !e && state.moment !== null ? moments()[state.moment] : null;
  box.hidden = !e && !moment;
  if (box.hidden) return;
  const close = h('button', { type: 'button', class: 'quiet close', onclick: () => { state.selected = null; state.moment = null; renderTimeline(); renderMoments(); renderInspector(); } }, 'Close');
  if (moment) return fill(box, close, ...momentView(moment));
  const t0 = sorted().find(x => x.type === 'TRANSACTION_STARTED')?.atMicros ?? 0;
  const begin = sorted().find(x => x.lane === e.lane && x.type === 'TRANSACTION_STARTED' && x.attempt === e.attempt);
  const req = state.run.requests.find(r => r.lane === e.lane);
  const rows = [
    ['Transaction', req ? `${e.lane} · ${req.requestId}` : e.lane],
    ['Order · customer', req ? `${req.orderId} · ${req.customerId}` : null, 'mono'],
    ['Attempt', e.attempt],
    ['Transaction id', e.txId, 'mono'],
    ['Backend pid', e.pid, 'mono'],
    ['Thread', e.thread, 'mono'],
    ['Instance', begin?.data?.instance, 'mono'],
    ['Isolation', e.isolation],
    ['Time', e.at ? new Date(e.at).toISOString().replace('T', ' ').replace('Z', ' UTC') : null, 'mono'],
    ['Offset', `+${((e.atMicros - t0) / 1000).toFixed(3)} ms`, 'mono'],
    ['Duration', e.durationMicros != null ? ms(e.durationMicros) : null],
    ['Target', e.target],
    ['Value read', e.valueRead, 'mono'],
    ['Value written', e.valueWritten, 'mono'],
    ['Version read', e.versionRead, 'mono'],
    ['Version written', e.versionWritten, 'mono'],
    ['Expected version', e.expectedVersion, 'mono'],
    ['Actual version', e.actualVersion, 'mono'],
    ['Lock', e.lock],
    ['Blocked by', e.blockedBy?.join(', ')],
    ['Lock wait', e.waitMicros != null ? ms(e.waitMicros) : null],
    ['Rows', e.rows],
    ['Decision', e.ok != null && e.type === 'DECISION_MADE' ? `${e.ok ? '✓' : '✗'} ${e.target}` : null],
    ['Outcome', e.outcome],
    ['SQLSTATE', e.sqlState, 'mono'],
    ['Trace', e.traceId, 'mono'],
  ].filter(([, v]) => v !== null && v !== undefined && v !== '');
  const laneEvents = sorted().filter(x => x.lane === e.lane);
  const data = e.data ? Object.entries(e.data).filter(([k]) => !['instance'].includes(k)) : [];
  fill(box, close,
    h('header', {},
      h('span', { class: 'kicker' }, `Event #${e.seq} · ${e.lane === 'DB' ? 'database' : e.lane === 'LAB' ? 'experiment' : 'transaction ' + e.lane}`),
      h('h2', {}, TYPE(e.type)),
      e.message ? h('p', { class: 'help' }, e.message) : null),
    h('dl', { class: 'kv' }, rows.flatMap(([k, v, cls]) => [h('dt', {}, k), h('dd', { class: cls || '' }, String(v))])),
    e.sql ? h('div', { class: 'insp-section' }, h('h3', {}, 'SQL'), h('pre', { class: 'code' }, e.sql)) : null,
    data.length ? h('div', { class: 'insp-section' }, h('h3', {}, e.type === 'DEADLOCK_DETECTED' ? 'PostgreSQL detail' : 'Details'),
      h('dl', { class: 'kv' }, data.flatMap(([k, v]) => [h('dt', {}, k), h('dd', { class: 'mono' }, typeof v === 'object' ? JSON.stringify(v) : String(v))]))) : null,
    e.traceId && grafana ? h('div', { class: 'inline' }, traceLink(e.traceId, 'Open this transaction\'s trace')) : null,
    e.lane !== 'LAB' ? h('div', { class: 'insp-section' },
      h('h3', {}, e.lane === 'DB' ? 'Committed states' : `Everything ${e.lane} did`),
      h('ul', { class: 'lane-events' }, laneEvents.map(x => h('li', {},
        h('button', { type: 'button', 'aria-current': String(x.seq === e.seq), onclick: () => select(x.seq) },
          h('span', { class: 't' }, `+${((x.atMicros - t0) / 1000).toFixed(1)}`), h('span', {}, `${TYPE(x.type)}${x.valueRead ? ' · ' + x.valueRead : x.valueWritten ? ' · ' + x.valueWritten : ''}`)))))) : null);
}

function momentView(m) {
  const events = (m.seqs || []).map(seq => state.events.get(seq)).filter(Boolean);
  return [
    h('header', {}, h('span', { class: 'kicker' }, 'Key moment'), h('h2', {}, m.title), h('div', { class: 'inline' }, chip(m.kind.toLowerCase().replaceAll('_', ' '), m.tone === 'info' ? '' : m.tone))),
    m.detail ? h('p', { class: 'help' }, m.detail) : null,
    events.length ? h('div', { class: 'insp-section' }, h('h3', {}, 'Made of these events'),
      h('ul', { class: 'lane-events' }, events.map(x => h('li', {}, h('button', { type: 'button', onclick: () => select(x.seq) },
        h('span', { class: 't' }, x.lane), h('span', {}, `${TYPE(x.type)}${x.valueRead ? ' · ' + x.valueRead : x.valueWritten ? ' · ' + x.valueWritten : ''}`)))))) : null,
  ];
}

function helpView() {
  return [
    h('header', {}, h('span', { class: 'kicker' }, 'Inspector'), h('h2', {}, 'Reading the timeline')),
    h('div', { class: 'help' },
      h('p', {}, 'Each lane is one request: its own thread, its own connection, its own transaction. Time runs left to right on one clock for all of them; the bottom lane is what the database had committed.'),
      h('ul', {},
        h('li', {}, 'A rounded bar is a transaction, from BEGIN to COMMIT or ROLLBACK.'),
        h('li', {}, 'A dashed blue box is a lock wait reported by PostgreSQL; its arrow points at the transaction holding the lock.'),
        h('li', {}, 'Dotted: the configured application work between read and write. Hatched: the lab holding a request so the race happens every time.'),
        h('li', {}, 'Numbered badges are key moments; the strip above lists them.')),
      h('p', {}, 'Click any mark for its transaction id, backend pid, SQL, values, versions, locks and trace. Arrow keys step through the events.')),
  ];
}

// ---- result ----------------------------------------------------------------------------------------------
function renderResult() {
  const r = state.run;
  const box = $('result');
  const metrics = $('metrics');
  if (!r || r.status !== 'COMPLETED' || !r.result) {
    fill(metrics);
    fill(box, r?.status === 'FAILED' ? h('p', { class: 'verdict bad' }, `The run failed: ${r.error}`) : null);
    return;
  }
  const { metrics: m, invariant: inv, requests } = r.result;
  const finalState = [...state.events.values()].filter(e => e.type === 'STATE_OBSERVED').sort((a, b) => a.seq - b.seq).pop()?.valueWritten;
  const metric = (label, value, title) => h('div', { class: 'metric', title }, h('span', {}, label), h('strong', {}, value));
  fill(metrics,
    metric('Conflicts', m.conflicts, `${m.versionConflicts} version, ${m.serializationFailures} serialization, ${m.deadlocks} deadlock`),
    metric('Retries', m.retries),
    metric('Lock wait', ms(m.lockWaitMicros), `${m.lockWaits} waits reported by PostgreSQL`),
    metric('p50', ms(m.p50Micros)),
    metric('p95', ms(m.p95Micros), `n = ${m.requests}`),
    metric('Duration', ms(m.durationMicros)));
  fill(box,
    h('h2', { id: 'result-title' }, 'Result'),
    h('p', { class: `verdict ${inv.holds ? 'good' : 'bad'}` }, inv.holds ? 'Invariant preserved' : 'Invariant violated', h('small', {}, inv.statement)),
    h('dl', { class: 'result-table' }, ...[
      ['Expected', inv.expected, ''],
      ['Actual', inv.actual, inv.holds ? 'good' : 'bad'],
      ['Committed state', finalState || '—', ''],
    ].flatMap(([k, v, cls]) => [h('dt', {}, k), h('dd', { class: cls }, String(v))])),
    h('ul', { class: 'requests' }, requests.map(q => h('li', {},
      h('b', {}, q.lane),
      chip(q.outcome.toLowerCase(), { SUCCEEDED: 'good', ABORTED: 'bad', FAILED: 'bad' }[q.outcome] || ''),
      h('span', {}, `${q.attempts} ${q.attempts === 1 ? 'try' : 'tries'}`),
      h('span', { class: 'num' }, ms(q.latencyMicros)),
      q.lockWaitMicros ? h('span', {}, `lock ${ms(q.lockWaitMicros)}`) : null,
      q.detail ? h('span', { class: 'hint' }, q.detail) : null))));
}

function renderWhy() {
  const r = state.run;
  const box = $('why');
  if (!r || r.status !== 'COMPLETED' || !r.result) {
    fill(box); fill($('fix'));
    return;
  }
  const x = r.result.explanation;
  const inv = r.result.invariant;
  const m = state.exp.modes.find(z => z.mode === r.config.mode);
  const unsafeRun = state.history.find(z => z.id !== r.id && z.invariantHolds === false);
  fill(box,
    h('h2', { id: 'why-title' }, 'Why this happened'),
    h('p', { class: `conclusion${inv.holds ? '' : ' bad'}` }, h('strong', {}, x.headline), ' ', x.conclusion),
    h('ol', { class: 'why-steps' }, x.steps.map(st => h('li', { class: st.tone },
      st.lane ? h('span', { class: 'lane-dot' }, st.lane) : h('span'),
      h('button', { type: 'button', disabled: !st.seq, onclick: () => st.seq && select(st.seq) }, st.text)))),
    h('div', { class: 'mechanism' }, h('h3', {}, `What ${m?.label || r.config.mode} does`), h('p', {}, x.mechanism)));
  fill($('fix'),
    x.fix ? h('div', { class: 'fix-row' },
      h('div', {}, h('strong', {}, x.fix.label), h('p', {}, x.fix.why)),
      h('button', { type: 'button', class: 'primary-action', disabled: state.busy, onclick: () => runFix(x.fix, r.id) }, 'Run the fix'))
    : h('div', { class: 'fix-row' },
      h('button', { type: 'button', disabled: state.busy, onclick: () => runRace({ ...r.config }, r.id) }, 'Run again'),
      unsafeRun ? h('button', { type: 'button', onclick: async () => { state.pick = { left: unsafeRun.id, right: r.id }; await compare(); } }, `Compare with run #${unsafeRun.id}`) : null));
}

function runFix(fix, previousId) {
  const prev = state.run.config;
  const cfg = { ...prev, mode: fix.mode, isolation: fix.isolation || state.exp.modes.find(m => m.mode === fix.mode)?.isolation,
    maxRetries: MODE_RETRIES.includes(fix.mode) ? Math.max(prev.maxRetries, state.exp.limits.defaultRetries) : prev.maxRetries };
  state.cfg = { mode: cfg.mode, isolation: cfg.isolation, requests: cfg.requests, initialValue: cfg.initialValue, delayMs: cfg.delayMs, interleaving: cfg.interleaving, maxRetries: cfg.maxRetries };
  renderLearn(); renderConfigure();
  runRace(state.cfg, previousId);
}

// ---- compare ---------------------------------------------------------------------------------------------
async function compare() {
  const { left, right } = state.pick;
  if (!left || !right || left === right) return;
  try {
    const [cmp, leftRun, rightRun, leftEv, rightEv] = await Promise.all([
      api(`/compare?left=${left}&right=${right}`),
      api(`/runs/${left}`),
      api(`/runs/${right}`),
      api(`/runs/${left}/events?limit=5000`),
      api(`/runs/${right}/events?limit=5000`),
    ]);
    state.comparison = cmp;
    state.compareRuns = { left: leftRun, right: rightRun };
    state.compareEvents = { left: leftEv.data, right: rightEv.data };
    session().compared = true;
  } catch (e) {
    state.comparison = { error: e.message };
    state.compareRuns = null;
    state.compareEvents = null;
  }
  renderCompare(); renderPath();
  $('compare-section').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function renderCompare() {
  const box = $('compare-section');
  const label = r => state.exp.modes.find(m => m.mode === r.mode)?.label || r.mode;
  const doneRuns = state.history.filter(r => r.status === 'COMPLETED');
  const pick = r => {
    if (state.pick.left === r.id) state.pick.left = null;
    else if (state.pick.right === r.id) state.pick.right = null;
    else if (!state.pick.left) state.pick.left = r.id;
    else state.pick.right = r.id;
    renderCompare();
  };
  const c = state.comparison;
  const fmt = (d, v) => d.unit === 'ms' ? `${Math.round(v)} ms` : d.metric === 'Invariant held' ? (v ? 'held' : 'violated') : d.unit === 'ok/s' ? `${v}/s` : v;
  const interesting = new Set(['Invariant held', 'Conflicts', 'Retries', 'Lock waits', 'Time waiting for locks', 'Latency p95']);
  const sideHead = (which, id) => {
    const run = state.compareRuns?.[which];
    const summary = doneRuns.find(r => r.id === id);
    const name = run ? (state.exp.modes.find(m => m.mode === run.config.mode)?.label || run.config.mode) : label(summary || { mode: '' });
    const held = run?.result?.invariant?.holds ?? summary?.invariantHolds;
    return h('h3', {}, `#${id} · ${name}`, held == null ? null : chip(held ? 'held' : 'violated', held ? 'good' : 'bad'));
  };
  fill(box,
    h('div', { class: 'compare-head' },
      h('div', {},
        h('h2', { id: 'compare-title' }, 'Compare'),
        h('p', {}, 'Pick two finished runs. The first click is the left timeline, the second is the right. Both are the events those runs recorded, on the same scale.')),
      h('button', { type: 'button', class: 'primary-action', disabled: !state.pick.left || !state.pick.right || state.pick.left === state.pick.right, onclick: compare }, 'Compare')),
    doneRuns.length ? h('div', { class: 'picker' }, doneRuns.map(r =>
      h('button', { type: 'button', class: 'small-button', 'aria-pressed': String(state.pick.left === r.id || state.pick.right === r.id), onclick: () => pick(r) },
        `${state.pick.left === r.id ? 'Left · ' : state.pick.right === r.id ? 'Right · ' : ''}#${r.id} ${label(r)}`))) : h('p', { class: 'hint' }, 'Finish two runs of this experiment to compare them.'),
    c?.error ? h('p', { class: 'form-error' }, c.error) : null,
    c && !c.error ? h('p', { class: 'verdict-line' }, c.verdict) : null,
    c && !c.error ? h('ul', { class: 'diff-line' }, c.differences.filter(d => interesting.has(d.metric)).map(d =>
      h('li', {}, d.metric, ' ', h('b', { class: d.better === 'left' ? 'better' : '' }, fmt(d, d.left)), ' · ', h('b', { class: d.better === 'right' ? 'better' : '' }, fmt(d, d.right))))) : null,
    state.compareRuns ? h('div', { class: 'pair' },
      h('div', {}, sideHead('left', state.pick.left), h('div', { id: 'cmp-left', class: 'timeline', tabindex: '-1' })),
      h('div', {}, sideHead('right', state.pick.right), h('div', { id: 'cmp-right', class: 'timeline', tabindex: '-1' }))) : null);
  if (state.compareRuns && state.compareEvents) {
    requestAnimationFrame(() => {
      const left = $('cmp-left'), right = $('cmp-right');
      if (left) renderTimeline(left, state.compareRuns.left, state.compareEvents.left);
      if (right) renderTimeline(right, state.compareRuns.right, state.compareEvents.right);
    });
  }
}

document.addEventListener('keydown', ev => {
  if (ev.key === 'Escape' && !$('inspector').hidden) {
    state.selected = null; state.moment = null;
    renderTimeline(); renderMoments(); renderInspector();
  }
});

// ---- all -------------------------------------------------------------------------------------------------
function renderAll() {
  renderPath(); renderExperiments(); renderLearn(); renderConfigure(); renderHead(); renderMoments();
  renderTimeline(); renderInspector(); renderResult(); renderWhy(); renderCompare();
}
window.addEventListener('resize', () => schedule());
init();
