// Events over time. Every state shown is folded by order-service from its event store; every
// offset is Kafka's own. The browser lays them out and asks for more.
'use strict';

const $ = selector => document.querySelector(selector);
const SVG = 'http://www.w3.org/2000/svg';
const view = { orders: [], order: null, moments: [], version: 0, at: null, selected: null, labs: [], lab: null };

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

function s(tag, attrs = {}, text) {
  const node = document.createElementNS(SVG, tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  if (text != null) node.textContent = text;
  return node;
}

async function api(method, path, pending) {
  try {
    if (pending) toast(pending);
    const response = await fetch(path, { method });
    const text = await response.text();
    const data = text ? JSON.parse(text) : null;
    if (!response.ok) throw new Error(data && (data.detail || data.title) || `HTTP ${response.status}`);
    if (pending) toast(null);
    connection(true);
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
  if (error) toastTimer = setTimeout(() => { t.hidden = true; }, 7000);
}

function connection(ok) {
  const c = $('#connection');
  c.classList.toggle('disconnected', !ok);
  c.lastChild.textContent = ok ? ' Live' : ' Control plane unreachable';
}

const short = type => (type || '').replace(/^Order/, '');
const time = iso => iso ? iso.replace('T', ' ').replace('Z', '') : '—';

// ---- Orders -------------------------------------------------------------------------------------

async function loadOrders(customer) {
  const list = await api('GET', `/api/history/orders?limit=40${customer ? `&customer=${encodeURIComponent(customer)}` : ''}`);
  view.orders = Array.isArray(list) ? list : [];
  $('#order-list').replaceChildren(...view.orders.map(o => h('li', {}, h('button', {
    type: 'button', 'aria-current': view.order === o.order.id ? 'true' : 'false', onclick: () => open(o.order.id),
  }, h('strong', {}, `${o.order.customerId}`), h('small', { class: 'mono' }, o.order.id.slice(0, 8)), h('small', {}, `${o.order.status} · saga ${o.saga ? o.saga.state : '—'} · v${o.order.version}`)))));
}

async function open(id) {
  view.order = id;
  view.moments = await api('GET', `/api/history/orders/${id}`);
  view.version = view.moments.length;
  view.at = null;
  view.selected = view.moments.length - 1;
  const slider = $('#version');
  slider.max = view.moments.length;
  slider.value = view.version;
  $('#at').value = '';
  const last = view.moments[view.moments.length - 1].stateAfter;
  $('#order-head').replaceChildren(h('h3', {}, `Order of ${last.customerId}`), h('span', { class: 'mono' }, id), h('span', { class: 'chip' }, `${view.moments.length} events`),
    view.moments.some(m => m.event.storedVersion < m.event.currentVersion) ? h('span', { class: 'chip upcast' }, 'has events stored at an older schema') : null);
  for (const b of document.querySelectorAll('.order-list button')) b.setAttribute('aria-current', b.textContent.includes(id.slice(0, 8)) ? 'true' : 'false');
  await rebuildAtVersion(view.version);
  inspect(view.selected);
  $('#kafka').replaceChildren();
  $('#kafka-note').textContent = 'not read yet';
}

// ---- Timeline, state, inspector ----------------------------------------------------------------------

function renderTimeline() {
  const box = $('#timeline');
  const moments = view.moments;
  if (!moments.length) { box.replaceChildren(); return; }
  const times = moments.map(m => Date.parse(m.event.recordedAt));
  const t0 = times[0], t1 = Math.max(times[times.length - 1], t0 + 1000);
  const x = t => 30 + (t - t0) / (t1 - t0) * 940;
  const svg = s('svg', { viewBox: '0 0 1000 120', role: 'img', 'aria-label': 'Order events on a time axis' });
  svg.append(s('line', { class: 'axis', x1: 20, x2: 980, y1: 60, y2: 60 }));
  svg.append(s('text', { class: 'tick', x: 20, y: 112 }, time(moments[0].event.recordedAt)));
  svg.append(s('text', { class: 'tick', x: 980, y: 112, 'text-anchor': 'end' }, `+${((t1 - t0) / 1000).toFixed(1)} s`));
  let lastX = -100, row = 0;
  moments.forEach((m, i) => {
    const e = m.event;
    const applied = view.at ? Date.parse(e.recordedAt) <= Date.parse(view.at) : i < view.version;
    const cx = x(times[i]);
    row = cx - lastX < 90 ? 1 - row : 0;
    lastX = cx;
    const g = s('g', { class: `ev ${applied ? 'applied' : 'later'}${i === view.selected ? ' selected' : ''}${e.storedVersion < e.currentVersion ? ' upcast' : ''}`, tabindex: 0 });
    g.append(s('circle', { cx, cy: 60, r: 7 }));
    g.append(s('text', { x: cx, y: row ? 94 : 36, 'text-anchor': 'middle' }, `${e.version} ${short(e.type)}`));
    g.append(s('text', { class: 'v', x: cx, y: row ? 106 : 24, 'text-anchor': 'middle' }, e.storedVersion < e.currentVersion ? `stored v${e.storedVersion} → read v${e.currentVersion}` : `schema v${e.storedVersion}`));
    g.addEventListener('click', () => inspect(i));
    g.addEventListener('keydown', ev => { if (ev.key === 'Enter') inspect(i); });
    svg.append(g);
  });
  if (view.at) {
    const cx = Math.max(20, Math.min(980, x(Date.parse(view.at))));
    svg.append(s('line', { class: 'now', x1: cx, x2: cx, y1: 8, y2: 84 }));
    svg.append(s('text', { class: 'now-label', x: cx + 4, y: 14 }, 'as of'));
  }
  box.replaceChildren(svg);
}

const FIELDS = ['status', 'customerId', 'lines', 'total', 'currency', 'paymentId', 'reservationId', 'trackingNumber', 'cancelReason', 'version'];

function renderState(state, previous, note) {
  $('#state-note').textContent = note;
  $('#state').replaceChildren(...FIELDS.flatMap(f => {
    const value = state[f];
    const before = previous ? previous[f] : undefined;
    const changed = previous && JSON.stringify(value) !== JSON.stringify(before);
    const shown = f === 'lines' ? (value || []).map(l => `${l.quantity} × ${l.sku} @ ${l.unitPrice}`).join(', ') || '—' : value == null ? '—' : String(value);
    return [h('dt', {}, f), h('dd', { class: changed ? 'changed' : '' }, shown)];
  }));
}

async function rebuildAtVersion(k) {
  view.version = k;
  view.at = null;
  $('#version-label').textContent = `${k} of ${view.moments.length}`;
  const rebuilt = await api('GET', `/api/history/orders/${view.order}/rebuild?version=${k}`);
  const previous = k > 1 ? view.moments[k - 2].stateAfter : k === 1 ? {} : null;
  renderState(rebuilt.state, previous, `folded ${rebuilt.applied.length} of ${view.moments.length} events`);
  if (k > 0) {
    view.selected = k - 1;
    // Just before the chosen event reached Kafka: the replay starts with it.
    $('#at').value = view.moments[k - 1].event.recordedAt;
  }
  renderTimeline();
  if (k > 0) inspect(k - 1);
}

async function rebuildAtInstant() {
  const raw = $('#at').value.trim();
  if (!raw) { toast('Enter an instant like 2026-09-26T10:42:07.123Z', true); return; }
  const at = new Date(raw).toISOString();
  const rebuilt = await api('GET', `/api/history/orders/${view.order}/rebuild?at=${encodeURIComponent(at)}`);
  view.at = at;
  view.version = rebuilt.applied.length;
  $('#version').value = view.version;
  $('#version-label').textContent = `${view.version} of ${view.moments.length} (as of ${time(at)})`;
  const previous = view.version > 1 ? view.moments[view.version - 2].stateAfter : view.version === 1 ? {} : null;
  renderState(rebuilt.state, previous, `as of ${time(at)}: ${rebuilt.applied.length} recorded by then, ${rebuilt.later.length} after`);
  renderTimeline();
}

function inspect(i) {
  view.selected = i;
  const e = view.moments[i].event;
  const stored = e.stored.payload || {};
  const added = Object.keys(e.decoded).filter(k => !(k in stored));
  $('#inspect-note').textContent = `#${e.version} ${e.type} · stored v${e.storedVersion} · read as v${e.currentVersion}`;
  $('#inspect-note').className = e.storedVersion < e.currentVersion ? 'chip upcast' : 'chip';
  const decoded = h('pre', {});
  const lines = JSON.stringify(e.decoded, null, 2).split('\n');
  for (const line of lines) {
    const key = (line.match(/^\s*"([^"]+)":/) || [])[1];
    decoded.append(added.includes(key) ? h('span', { class: 'added' }, `${line}  ← added by upcaster\n`) : `${line}\n`);
  }
  $('#inspector').replaceChildren(
    h('div', {}, h('h3', {}, `As stored (schema v${e.storedVersion}), position ${e.position}, ${time(e.recordedAt)}`), h('pre', {}, JSON.stringify(stored, null, 2))),
    h('div', {}, h('h3', {}, `As read today (v${e.currentVersion})`), decoded));
  renderTimeline();
}

async function replayKafka() {
  const raw = $('#at').value.trim();
  const at = raw ? new Date(new Date(raw).getTime() - 1000).toISOString() : new Date(Date.parse(view.moments[0].event.recordedAt) - 1000).toISOString();
  const result = await api('GET', `/api/history/orders/${view.order}/kafka?at=${encodeURIComponent(at)}`, 'Looking up offsets in Kafka’s time index…');
  const ids = view.moments.map(m => m.event.eventId);
  $('#kafka-note').textContent = `from ${time(at)} · ${result.recordsSince} records on the topic since`;
  const max = Math.max(1, ...result.spans.map(sp => sp.end));
  $('#kafka').replaceChildren(
    h('div', { class: 'spans' }, ...result.spans.map(sp => h('div', { class: 'span-row' },
      h('span', { class: sp.partition === result.partition ? 'chip info' : 'chip' }, `partition ${sp.partition}`),
      h('span', { class: 'span-bar', title: `${sp.from} → ${sp.end}` }, h('i', { style: `left:${(sp.from / max * 100).toFixed(2)}%` })),
      h('span', { class: 'mono' }, `offset ${sp.from} → end ${sp.end} (${sp.end - sp.from})`)))),
    h('p', { class: 'step-plan' }, `This order's key hashes to partition ${result.partition}; reading it from offset ${(result.spans.find(sp => sp.partition === result.partition) || {}).from} finds its records in order:`),
    h('table', {}, h('thead', {}, h('tr', {}, ...['offset', 'Kafka timestamp', 'type', 'schema', 'same event as #'].map(c => h('th', {}, c)))),
      h('tbody', {}, ...result.records.map(r => h('tr', {}, h('td', { class: 'num mono' }, r.offset), h('td', { class: 'mono' }, time(r.timestamp)), h('td', {}, r.type),
        h('td', {}, `v${r.schemaVersion}`), h('td', { class: ids.includes(r.eventId) ? 'yes' : 'no' }, ids.includes(r.eventId) ? String(ids.indexOf(r.eventId) + 1) : 'not in the event store'))))));
}

// ---- Labs ---------------------------------------------------------------------------------------------

async function loadLabs() {
  view.labs = await api('GET', '/api/history/labs');
  // /history#schema opens that lab's tab.
  if (!view.lab) view.lab = view.labs.some(l => l.id === location.hash.slice(1)) ? location.hash.slice(1) : view.labs[0].id;
  $('#lab-tabs').replaceChildren(...view.labs.map(l => h('button', { type: 'button', role: 'tab', 'aria-selected': l.id === view.lab ? 'true' : 'false', onclick: () => { view.lab = l.id; history.replaceState(null, '', `#${l.id}`); loadLabs(); } }, l.title)));
  renderLab();
}

function renderLab() {
  const lab = view.labs.find(l => l.id === view.lab);
  const done = lab.run ? lab.run.steps : [];
  const next = done.length;
  $('#lab').replaceChildren(
    h('div', { class: 'lab-top' }, h('h3', {}, lab.title), h('p', {}, lab.summary)),
    h('ol', { class: 'steps' }, ...lab.steps.map((name, i) => {
      const result = done[i];
      return h('li', { class: result ? 'done' : i === next ? 'next' : '' }, h('div', {},
        h('div', { class: 'step-head' }, h('strong', {}, name), result ? h('small', {}, `${time(result.at)} · ${result.millis} ms`) : null,
          i === next ? h('button', { type: 'button', class: 'primary-action', onclick: () => runStep(lab.id, i) }, i === 0 ? 'Start' : `Run ${name.toLowerCase()}`) : null,
          i === 0 && next > 0 ? h('button', { type: 'button', onclick: () => runStep(lab.id, 0) }, 'Start over') : null),
        h('p', { class: 'step-plan' }, lab.plan[i]),
        result ? h('div', { class: 'step-result' }, render(result.result)) : null));
    })));
}

async function runStep(id, n) {
  const orderId = id === 'time-travel' && n === 0 && view.order ? `?orderId=${view.order}` : '';
  await api('POST', `/api/history/labs/${id}/steps/${n}${orderId}`, n === 0 ? 'Running the first step…' : 'Running… (some steps wait for sagas or the log cleaner, up to two minutes)');
  toast(null);
  await loadLabs();
  refreshRuns();
}

// A generic, readable rendering of a step's JSON result: tables for lists of records, key/value
// grids for objects, code blocks for payloads, prose for the "text" of Understand.
const CODE = new Set(['stored', 'decoded', 'written', 'record', 'state', 'config', 'readModel', 'decision', 'value']);

function render(value, key) {
  if (value == null) return h('span', {}, '—');
  if (key === 'text') return h('p', { class: 'understand' }, value);
  if (key === 'matrix') return matrix(value);
  if (CODE.has(key) && typeof value === 'object') return h('pre', {}, JSON.stringify(value, null, 2));
  if (Array.isArray(value)) {
    if (!value.length) return h('span', { class: 'chip' }, 'none');
    if (value.every(v => v && typeof v === 'object' && !Array.isArray(v))) return table(value);
    return h('span', { class: 'mono' }, value.map(v => typeof v === 'object' ? JSON.stringify(v) : v).join(', '));
  }
  if (typeof value === 'object') {
    const simple = [], nested = [];
    for (const [k, v] of Object.entries(value)) (v !== null && typeof v === 'object' || k === 'text') ? nested.push([k, v]) : simple.push([k, v]);
    return h('div', { class: 'step-result' },
      simple.length ? h('dl', { class: 'kv' }, ...simple.map(([k, v]) => h('div', {}, h('dt', {}, k), h('dd', { class: typeof v === 'boolean' ? (v ? 'yes' : 'no') : '' }, typeof v === 'boolean' ? (v ? 'yes' : 'no') : String(v))))) : null,
      ...nested.map(([k, v]) => h('div', {}, k === 'text' ? null : h('h4', {}, k), render(v, k))));
  }
  return h('span', {}, String(value));
}

function table(rows) {
  const columns = [...new Set(rows.flatMap(r => Object.keys(r)))].slice(0, 9);
  return h('table', {}, h('thead', {}, h('tr', {}, ...columns.map(c => h('th', {}, c)))),
    h('tbody', {}, ...rows.slice(0, 200).map(r => h('tr', {}, ...columns.map(c => {
      const v = r[c];
      if (v !== null && typeof v === 'object') return h('td', { class: 'mono' }, JSON.stringify(v));
      if (typeof v === 'boolean') return h('td', { class: v ? 'yes' : 'no' }, v ? 'yes' : 'no');
      return h('td', { class: typeof v === 'number' ? 'num' : '' }, v == null ? '—' : String(v));
    })))));
}

function matrix(rows) {
  const readers = rows.find(r => r.readings) ? rows.find(r => r.readings).readings.map(r => r.reader) : [];
  return h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'event written by'), ...readers.map(r => h('th', {}, r)))),
    h('tbody', {}, ...rows.map(row => h('tr', {}, h('td', {}, row.written),
      ...(row.readings || []).map(r => h('td', { class: r.read ? 'yes' : 'no', title: r.read ? JSON.stringify(r.understood) : r.outcome }, r.read ? '✓ reads it' : `✗ ${r.outcome}`))))));
}

async function refreshRuns() {
  const runs = await api('GET', '/api/history/runs?limit=10');
  $('#runs').replaceChildren(...(runs.length ? runs.map(r => h('details', {}, h('summary', {}, h('span', { class: 'chip good' }, r.mode), h('strong', {}, r.summary), h('span', { class: 'mono' }, time(r.at))),
    h('div', { class: 'lab' }, render(r.result.steps.map(st => ({ step: st.name, at: st.at, ms: st.millis })))))) : [h('p', { class: 'empty' }, 'No experiment has been completed yet.')]));
}

// ---- Wiring ---------------------------------------------------------------------------------------------

async function init() {
  $('#version').addEventListener('change', e => rebuildAtVersion(Number(e.target.value)));
  $('#version').addEventListener('input', e => { $('#version-label').textContent = `${e.target.value} of ${view.moments.length}`; });
  $('#rebuild-at').addEventListener('click', rebuildAtInstant);
  $('#kafka-replay').addEventListener('click', replayKafka);
  $('#order-search').addEventListener('submit', e => {
    e.preventDefault();
    const q = $('#order-id').value.trim();
    if (/^[0-9a-f-]{36}$/i.test(q)) open(q); else loadOrders(q || null);
  });
  await loadOrders(null);
  const finished = view.orders.find(o => o.saga && o.saga.state === 'COMPLETED') || view.orders[0];
  if (finished) await open(finished.order.id);
  await loadLabs();
  refreshRuns();
}

document.addEventListener('DOMContentLoaded', init);
