// Advanced failure lab. Every value is what a stage or the inspector read back from PostgreSQL,
// Kafka or a coordinator process; the browser only lays it out and asks for the next stage.
'use strict';

const $ = selector => document.querySelector(selector);
const grafana = document.body.dataset.grafana || '';
const view = { labs: [], lab: null, running: false, state: null };

function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value === false || value == null) continue;
    if (key === 'class') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat()) if (child != null && child !== false) node.append(child.nodeType ? child : String(child));
  return node;
}

const chip = (text, tone = '') => h('span', { class: `chip ${tone}` }, text);
// Instants from Java may carry nanoseconds, which Date cannot parse: read the clock time directly.
const time = iso => {
  if (!iso) return '—';
  const m = /T(\d\d:\d\d:\d\d)(\.\d{1,3})?/.exec(String(iso));
  if (m) return m[1] + (m[2] || '.000').padEnd(4, '0');
  const d = new Date(iso);
  return isNaN(d) ? String(iso) : d.toISOString().slice(11, 23);
};
const explore = query => `${grafana}/explore?schemaVersion=1&panes=${encodeURIComponent(JSON.stringify({ z: {
  datasource: 'tempo', range: { from: 'now-6h', to: 'now' },
  queries: [{ refId: 'A', datasource: { type: 'tempo', uid: 'tempo' }, queryType: 'traceql', query }] } }))}`;
const traceLink = traceId => traceId && grafana ? h('a', { class: 'small-link', href: explore(traceId), target: '_blank', rel: 'noopener', title: `Trace ${traceId} in Tempo` }, 'Trace') : null;

async function api(method, path, pending) {
  try {
    if (pending) toast(pending);
    const response = await fetch(path, { method, headers: { Accept: 'application/json' } });
    const text = await response.text();
    const data = text ? JSON.parse(text) : null;
    if (!response.ok) throw Object.assign(new Error(data && (data.detail || data.title) || `HTTP ${response.status}`), { code: data && data.code });
    if (pending) toast(null);
    connection(true);
    return data;
  } catch (e) {
    if (e instanceof TypeError) connection(false);
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
  if (error) toastTimer = setTimeout(() => { t.hidden = true; }, 9000);
}

function connection(ok) {
  const c = $('#connection');
  c.classList.toggle('disconnected', !ok);
  c.lastChild.textContent = ok ? ' Live' : ' Control plane unreachable';
}

// ---- Infrastructure --------------------------------------------------------------------------------

async function loadStatus() {
  const s = await api('GET', '/api/failures/status');
  const item = (label, st, extra) => h('span', { class: `chip ${st.up ? 'good' : 'bad'}`, title: st.detail || '' }, `${label}: ${st.up ? extra(st) : 'down'}`);
  $('#infra').replaceChildren(
    item('failure-postgres', s.failurePostgres, st => `max_prepared_transactions=${st.max_prepared_transactions}`),
    item('kafka', s.kafka, st => st.authentication),
    item('kafka-secure', s.kafkaSecure, st => st.authentication));
  $('#infra-hint').hidden = s.failurePostgres.up && s.kafkaSecure.up;
}

// ---- Labs ---------------------------------------------------------------------------------------------

async function loadLabs() {
  view.labs = await api('GET', '/api/failures/labs');
  if (!view.lab) view.lab = view.labs.some(l => l.id === location.hash.slice(1)) ? location.hash.slice(1) : view.labs[0].id;
  $('#lab-tabs').replaceChildren(...view.labs.map(l => h('button', {
    type: 'button', role: 'tab', 'aria-selected': l.id === view.lab ? 'true' : 'false',
    onclick: () => { view.lab = l.id; history.replaceState(null, '', `#${l.id}`); view.state = null; loadLabs(); loadInspector(); }
  }, l.title, l.run && l.run.stages.length === 5 ? h('span', { class: 'tab-done', 'aria-label': 'all five stages held' }, ' ✓') : null)));
  renderLab();
}

function comparison(lab) {
  const rows = new Map();
  for (const st of lab.run ? lab.run.stages : [])
    for (const row of (st.result && st.result.compare) || []) {
      const r = rows.get(row.aspect) || { aspect: row.aspect, naive: null, correct: null };
      if (row.naive != null) r.naive = row.naive;
      if (row.correct != null) r.correct = row.correct;
      rows.set(row.aspect, r);
    }
  return [...rows.values()];
}

function renderLab() {
  const lab = view.labs.find(l => l.id === view.lab);
  const done = lab.run ? lab.run.stages : [];
  const next = done.length;
  const failure = lab.run && lab.run.lastFailure;
  const rows = comparison(lab);
  $('#lab').replaceChildren(...[
    h('div', { class: 'lab-top' },
      h('div', {}, h('h3', {}, lab.title), h('p', {}, lab.summary),
        h('div', { class: 'requires' }, h('span', { class: 'label' }, 'Runs on'), ...lab.requires.map(r => chip(r)))),
      h('div', { class: 'buttons' }, h('button', { type: 'button', disabled: view.running || lab.busy, onclick: () => resetLab(lab.id) }, 'Reset experiment'))),
    h('div', { class: 'designs' },
      h('article', { class: 'design naive' }, h('h4', {}, 'Naive design'), h('p', {}, lab.naive)),
      h('article', { class: 'design correct' }, h('h4', {}, 'Correct design'), h('p', {}, lab.correct))),
    rows.length ? h('table', { class: 'compare' },
      h('thead', {}, h('tr', {}, h('th', {}, 'Measured'), h('th', { class: 'naive' }, 'Naive design'), h('th', { class: 'correct' }, 'Correct design'))),
      h('tbody', {}, ...rows.map(r => h('tr', {}, h('th', { scope: 'row' }, r.aspect), h('td', { class: 'naive' }, r.naive || '—'), h('td', { class: 'correct' }, r.correct || (next < 3 ? 'after stage 4' : '—')))))) : null,
    h('ol', { class: 'rail', 'aria-label': 'Stages' }, ...lab.stages.map((name, i) => h('li', {
      class: i < next ? 'done' : failure && i === next ? 'failed' : i === next ? 'next' : '' }, h('span', {}, String(i + 1)), name))),
    h('ol', { class: 'stages' }, ...lab.stages.map((name, i) => {
      const result = done[i] || (failure && i === next ? failure : null);
      const isNext = i === next;
      return h('li', { class: done[i] ? 'done' : result ? 'failed' : isNext ? 'next' : '' }, h('div', {},
        h('div', { class: 'stage-head' },
          h('strong', {}, name),
          result ? h('small', {}, `${time(result.at)} · ${result.millis.toLocaleString()} ms`) : null,
          result ? traceLink(result.traceId) : null,
          h('span', { class: 'grow' }),
          isNext ? h('button', { type: 'button', class: 'primary-action', disabled: view.running || lab.busy, onclick: () => runStage(lab.id, i) },
            view.running ? 'Running…' : result ? `Run ${name.toLowerCase()} again` : i === 0 ? 'Start' : `Run ${name.toLowerCase()}`) : null,
          i === 0 && next > 0 ? h('button', { type: 'button', disabled: view.running || lab.busy, onclick: () => runStage(lab.id, 0) }, 'Start over') : null),
        h('p', { class: 'stage-plan' }, lab.plan[i]),
        result && result.error ? h('p', { class: 'stage-error' }, result.error) : null,
        result && result.result ? stageResult(result.result, !done[i + 1]) : null));
    }))].filter(Boolean));
}

async function runStage(id, n) {
  view.running = true;
  renderLab();
  try {
    await api('POST', `/api/failures/labs/${id}/stages/${n}`, n === 0 ? 'Setting up and running the first stage…' : 'Running on the real infrastructure… (lock timeouts and replays take a few seconds)');
    toast(null);
  } catch (e) {
    // A stage whose claim did not hold stays visible with what it measured.
  } finally {
    view.running = false;
    await loadLabs();
    loadInspector();
    refreshRuns();
  }
}

async function resetLab(id) {
  view.running = true;
  renderLab();
  try {
    const result = await api('POST', `/api/failures/labs/${id}/reset`, 'Resetting the experiment’s own databases and topics…');
    toast(Object.values(result).join(' · '));
    setTimeout(() => toast(null), 5000);
  } finally {
    view.running = false;
    await loadLabs();
    loadInspector();
  }
}

// The claims stay visible; the measurements behind them fold away once a later stage has run.
function stageResult(result, open) {
  const { checks: claims, ...rest } = result;
  return h('div', { class: 'stage-result' },
    claims ? h('div', {}, h('h4', {}, 'Claims checked'), checks(claims)) : null,
    h('details', { class: 'measurements', open: open || null }, h('summary', {}, 'What the stage measured'), render(rest)));
}

// ---- Rendering a stage's result ------------------------------------------------------------------------

const SKIP = new Set(['compare']);
const CODE = new Set(['value', 'naiveSql', 'vacuumVerbose']);

function render(value, key) {
  if (value == null) return h('span', { class: 'muted' }, '—');
  if (key === 'checks') return checks(value);
  if (key && /^otherWork/.test(key)) return probes(value);
  if (key === 'attempts' || key === 'decisions') return decisions(value);
  if (key === 'coordinatorEvents' || key === 'sagaRecovery') return events(value);
  if (CODE.has(key) && typeof value === 'object') return h('pre', {}, Array.isArray(value) ? value.join('\n') : JSON.stringify(value, null, 2));
  if (Array.isArray(value)) {
    if (!value.length) return chip('none');
    if (value.every(v => v && typeof v === 'object' && !Array.isArray(v))) return table(value);
    return h('span', { class: 'mono' }, value.map(v => typeof v === 'object' ? JSON.stringify(v) : v).join(', '));
  }
  // Per-partition offsets ({"0": 8, "1": 5}) on one line.
  if (typeof value === 'object' && Object.keys(value).length && Object.entries(value).every(([k, v]) => /^\d+$/.test(k) && typeof v !== 'object'))
    return h('span', { class: 'partitions' }, ...Object.entries(value).map(([k, v]) => h('span', {}, h('small', {}, `p${k}`), ` ${cell(v)}`)));
  if (typeof value === 'object') {
    const simple = [], nested = [];
    for (const [k, v] of Object.entries(value)) {
      if (SKIP.has(k)) continue;
      (v !== null && typeof v === 'object') ? nested.push([k, v]) : simple.push([k, v]);
    }
    // Checks first: they are the stage's verdict.
    nested.sort((a, b) => (b[0] === 'checks') - (a[0] === 'checks'));
    return h('div', { class: 'result' },
      ...nested.filter(([k]) => k === 'checks').map(([k, v]) => h('div', {}, h('h4', {}, 'Claims checked'), render(v, k))),
      simple.length ? h('dl', { class: 'kv' }, ...simple.map(([k, v]) => h('div', {}, h('dt', {}, label(k)), h('dd', { class: typeof v === 'boolean' ? (v ? 'yes' : 'no') : '' }, cell(v))))) : null,
      ...nested.filter(([k]) => k !== 'checks').map(([k, v]) => h('div', {}, h('h4', {}, label(k)), render(v, k))));
  }
  return h('span', {}, cell(value));
}

const label = k => k.replace(/([a-z0-9])([A-Z])/g, '$1 $2').replace(/^./, c => c.toUpperCase());
const cell = v => v == null ? '—' : typeof v === 'boolean' ? (v ? 'yes' : 'no') : typeof v === 'number' ? v.toLocaleString() : String(v);

function table(rows) {
  const columns = [...new Set(rows.flatMap(r => Object.keys(r)))].slice(0, 18);
  return h('div', { class: 'table-wrap' }, h('table', {}, h('thead', {}, h('tr', {}, ...columns.map(c => h('th', {}, c)))),
    h('tbody', {}, ...rows.slice(0, 200).map(r => h('tr', {}, ...columns.map(c => {
      const v = r[c];
      if (v !== null && typeof v === 'object') return h('td', { class: 'mono' }, JSON.stringify(v));
      if (typeof v === 'boolean') return h('td', { class: v ? 'yes' : 'no' }, v ? 'yes' : 'no');
      if (c === 'value' && typeof v === 'string') return h('td', { class: 'mono' }, v);
      return h('td', { class: typeof v === 'number' ? 'num' : '' }, cell(v));
    }))))));
}

function checks(rows) {
  return h('table', { class: 'checks' }, h('thead', {}, h('tr', {}, h('th', {}, ''), h('th', {}, 'Claim'), h('th', {}, 'Expected'), h('th', {}, 'Observed'))),
    h('tbody', {}, ...rows.map(c => h('tr', { class: c.ok ? 'held' : 'broken' },
      h('td', { class: c.ok ? 'yes' : 'no', 'aria-label': c.ok ? 'holds' : 'does not hold' }, c.ok ? '✓' : '✗'),
      h('td', {}, c.claim), h('td', { class: 'mono' }, c.expected), h('td', { class: 'mono' }, String(c.observed).replace(/\bnull\b/g, 'none'))))));
}

// Other work against the same rows: how long each waited and who blocked it (0 = prepared transaction).
function probes(rows) {
  const max = Math.max(4000, ...rows.map(p => p.elapsedMs));
  return h('div', { class: 'probes' }, ...rows.map(p => h('div', { class: `probe ${p.outcome === 'DONE' ? 'ok' : 'blocked'}` },
    h('div', { class: 'probe-head' }, h('strong', {}, p.name), chip(p.outcome === 'DONE' ? `done in ${p.elapsedMs} ms` : `${p.outcome} after ${p.elapsedMs.toLocaleString()} ms`, p.outcome === 'DONE' ? 'good' : 'bad'),
      p.waited ? chip(`waited on ${p.waitEvent}, blocked by pid ${p.blockedBy.join(', ')}${p.blockedBy.includes(0) ? ' (a prepared transaction: no session)' : ''}`, 'warn') : chip('never waited')),
    h('div', { class: 'probe-bar', role: 'img', 'aria-label': `${p.elapsedMs} ms of ${max}` }, h('i', { style: `width:${Math.max(0.6, p.elapsedMs / max * 100).toFixed(2)}%` })),
    h('code', {}, `${p.database} · ${p.sql} · backend ${p.backendPid}${p.sqlstate ? ` · SQLSTATE ${p.sqlstate}` : ''} · rolled back`))));
}

function decisions(rows) {
  const columns = ['principal', 'operation', 'resource', 'decision', 'stage', 'error', 'offset', 'kafka_offset', 'broker'].filter(c => rows.some(r => r[c] != null));
  return h('div', { class: 'table-wrap' }, h('table', {}, h('thead', {}, h('tr', {}, ...columns.map(c => h('th', {}, c.replace('kafka_', ''))))),
    h('tbody', {}, ...rows.map(r => h('tr', { title: r.detail || '' }, ...columns.map(c => h('td', { class: c === 'decision' ? (r[c] === 'ALLOWED' ? (String(r.broker).startsWith('open') && String(r.principal).includes('attacker') ? 'no' : 'yes') : 'no') : c === 'error' ? 'mono' : '' }, cell(r[c]))))))));
}

function events(rows) {
  return h('ol', { class: 'events' }, ...rows.map(e => h('li', { class: /KILL|ERROR/.test(e.event || '') ? 'bad' : '' },
    h('span', { class: 'mono' }, time(e.at)), h('strong', {}, e.event || e.action),
    h('span', {}, Object.entries(e).filter(([k]) => !['event', 'at', 'action'].includes(k)).map(([k, v]) => `${k}=${v}`).join(' · ')))));
}

// ---- Inspector -------------------------------------------------------------------------------------------

async function loadInspector() {
  if (view.running) return;
  const id = view.lab;
  let state;
  try {
    state = await fetch(`/api/failures/labs/${id}/state`, { headers: { Accept: 'application/json' } }).then(r => r.json().then(b => r.ok ? b : Promise.reject(b)));
  } catch (e) {
    $('#inspector').replaceChildren(h('p', { class: 'empty' }, (e && e.detail) || 'The inspector could not read the lab’s infrastructure.'));
    return;
  }
  if (id !== view.lab) return;
  view.state = state;
  $('#inspector-at').textContent = `read ${new Date().toISOString().slice(11, 19)}`;
  const renderer = { 'two-phase': inspectTwoPhase, isolation: inspectIsolation, 'disaster-recovery': inspectRecovery, trust: inspectTrust }[id];
  $('#inspector').replaceChildren(...renderer(state));
}

const pane = (title, note, ...body) => h('article', { class: 'pane' }, h('header', {}, h('h3', {}, title), note), ...body);

function inspectTwoPhase(s) {
  const prepared = s.preparedTransactions || [];
  return [
    pane('Prepared transactions (pg_prepared_xacts)', chip(prepared.length ? `${prepared.length} in doubt` : 'none in doubt', prepared.length ? 'warn' : 'good'),
      prepared.length ? h('div', { class: 'table-wrap' }, h('table', {}, h('thead', {}, h('tr', {}, ...['gid', 'database', 'xid', 'in doubt for', 'logged decision', 'resolve by hand'].map(c => h('th', {}, c)))),
        h('tbody', {}, ...prepared.map(p => h('tr', {}, h('td', { class: 'mono' }, p.gid), h('td', {}, p.database), h('td', { class: 'num mono' }, p.xid),
          h('td', { class: 'num' }, `${(p.in_doubt_ms / 1000).toFixed(1)} s`), h('td', {}, p.loggedDecision === 'null' ? 'none (never decided)' : p.loggedDecision),
          h('td', {}, p.gid.startsWith('zs2pc-') ? h('div', { class: 'buttons' },
            h('button', { type: 'button', onclick: () => resolve(p.gid, 'commit') }, 'COMMIT PREPARED'),
            h('button', { type: 'button', onclick: () => resolve(p.gid, 'rollback') }, 'ROLLBACK PREPARED')) : '—')))))) : h('p', { class: 'empty' }, 'Nothing is prepared. Leave a transaction in doubt to practise the recovery:'),
      h('div', { class: 'buttons' },
        h('button', { type: 'button', onclick: () => inDoubt('after-prepare') }, 'Kill coordinator after PREPARE'),
        h('button', { type: 'button', onclick: () => inDoubt('after-decision') }, 'Kill coordinator after logging COMMIT'),
        h('button', { type: 'button', class: 'primary-action', disabled: !prepared.length, onclick: recover }, 'Recover from the coordinator log'))),
    pane('Locks held by prepared transactions (pg_locks)', chip('pid null: no session owns them'), render(s.preparedLocks || [])),
    pane('Sessions waiting for a lock', chip('pg_blocking_pids: 0 = a prepared transaction'), render(s.waiting || [])),
    s.ready ? pane('Coordinator log (fl_coordinator)', null, render(s.coordinatorLog || []), h('h4', {}, 'Latest events'), render(s.coordinatorEvents || [])) : pane('Not set up', null, h('p', { class: 'empty' }, s.note || '')),
    s.rows ? pane('Participants', chip('fl_payments · fl_inventory'), h('div', { class: 'two' }, h('div', {}, h('h4', {}, 'Accounts'), render(s.rows.accounts), h('h4', {}, 'Payments'), render(s.rows.payments)),
      h('div', {}, h('h4', {}, 'Stock'), render(s.rows.stock), h('h4', {}, 'Reservations'), render(s.rows.reservations)))) : null,
  ].filter(Boolean);
}

async function inDoubt(crashAt) {
  const r = await api('POST', `/api/failures/two-phase/in-doubt?crashAt=${crashAt}`, 'Starting a 2PC checkout and killing its coordinator…');
  toast(`Coordinator pid ${r.coordinatorPid} killed (exit ${r.exitCode}) ${crashAt}; logged decision: ${r.decision || 'none'}.`);
  loadInspector();
}

async function resolve(gid, action) {
  await api('POST', `/api/failures/two-phase/prepared/${encodeURIComponent(gid)}/${action}`, `${action.toUpperCase()} PREPARED '${gid}'…`);
  toast(`${action.toUpperCase()} PREPARED '${gid}' done. Resolving only one branch of a transaction breaks its atomicity: check the other.`);
  loadInspector();
}

async function recover() {
  const actions = await api('POST', '/api/failures/two-phase/recover', 'Reading the coordinator log and resolving each branch…');
  toast(actions.map(a => `${a.gid}: ${a.action} (${a.why})`).join(' · ') || 'Nothing to recover.');
  loadInspector();
}

function inspectIsolation(s) {
  return [pane('Recent race-lab runs of these scenarios', h('a', { class: 'small-link', href: '/race' }, 'Open the timelines in the race lab'),
    s.runningRaceRun ? chip(`race run ${s.runningRaceRun} in progress`, 'warn') : null, render(s.recentRuns || []))];
}

function inspectRecovery(s) {
  const tiles = h('div', { class: 'tiles' },
    tile('RPO window', s.rpoSeconds >= 0 ? `${s.rpoSeconds} s` : '—', 'backup → disaster'),
    tile('Events at risk', s.rpoEvents >= 0 ? s.rpoEvents : '—', 'missing after restore'),
    tile('RTO, safe design', s.rtoMillisSafe >= 0 ? `${(s.rtoMillisSafe / 1000).toFixed(1)} s` : '—', 'drop → verified'),
    tile('RTO, naive + repair', s.rtoMillisNaive >= 0 ? `${(s.rtoMillisNaive / 1000).toFixed(1)} s` : '—', 'drop → verified'));
  return [
    pane('Recovery objectives (measured)', null, tiles),
    pane('Positions per partition', chip('Kafka vs database'), positions(s.positions || {}), s.kafka ? h('p', { class: 'empty' }, s.kafka) : null),
    pane('Ledgers against the topic', null, render(s.ledgers || [])),
    pane('Databases and backups', null, render(s.databases || [])),
  ];
}

const tile = (name, value, note) => h('div', { class: 'tile' }, h('span', {}, name), h('strong', {}, String(value)), h('small', {}, note));

function positions(p) {
  const sources = Object.entries(p);
  const parts = [...new Set(sources.flatMap(([, m]) => Object.keys(m || {})))].sort();
  if (!parts.length) return h('p', { class: 'empty' }, 'Nothing consumed yet.');
  return h('div', { class: 'table-wrap' }, h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'position'), ...parts.map(n => h('th', {}, `partition ${n}`)))),
    h('tbody', {}, ...sources.map(([name, m]) => h('tr', {}, h('th', { scope: 'row' }, label(name)), ...parts.map(n => h('td', { class: 'num mono' }, m && m[n] != null ? m[n] : '—')))))));
}

function inspectTrust(s) {
  return [
    pane('Security decisions', chip('what each broker answered'), render(s.decisions || [], 'decisions')),
    pane('Shipments reconciled against payments', null, render(s.reconciliation || [])),
    pane('ACLs on kafka-secure', chip('StandardAuthorizer, deny by default'), typeof s.secureAcls === 'string' ? h('p', { class: 'empty' }, s.secureAcls) : render(s.secureAcls || [])),
    pane('Shipments and payments', null, h('div', { class: 'two' }, h('div', {}, h('h4', {}, 'Shipments'), render(s.shipments || [])), h('div', {}, h('h4', {}, 'Payments'), render(s.payments || [])))),
  ];
}

// ---- Runs and wiring ---------------------------------------------------------------------------------------

async function refreshRuns() {
  const runs = await api('GET', '/api/failures/runs?limit=10');
  $('#runs').replaceChildren(...(runs.length ? runs.map(r => h('details', {}, h('summary', {}, chip(r.mode, 'good'), h('strong', {}, r.summary), h('span', { class: 'mono' }, String(r.at).replace('T', ' ').slice(0, 19))),
    h('div', { class: 'lab' }, render(r.result.stages.map(st => ({ stage: st.name, at: st.at, ms: st.millis, trace: st.traceId })))))) : [h('p', { class: 'empty' }, 'No experiment has completed all five stages yet.')]));
}

async function init() {
  $('#inspector-refresh').addEventListener('click', loadInspector);
  loadStatus().catch(() => {});
  await loadLabs();
  loadInspector();
  refreshRuns().catch(() => {});
  setInterval(() => { if (!document.hidden) loadStatus().catch(() => {}); }, 10000);
  setInterval(() => { if (!document.hidden) loadInspector(); }, 4000);
}

document.addEventListener('DOMContentLoaded', init);
