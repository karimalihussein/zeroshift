// Event-driven lab control plane. Renders only what the backend reports (polled once a second);
// every control calls a real lever (a service admin API, Kafka Connect or a Kafka topic) through
// /api/events/actions. Nothing here advances on a timer of its own.
const el = id => document.getElementById(id);
const number = value => new Intl.NumberFormat().format(value ?? 0);
const clock = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3, hour12: false });
const time = value => value ? clock.format(new Date(value)) : '—';
const seconds = value => time(value).slice(0, 8);
const duration = ms => ms >= 86_400_000 ? `${Math.round(ms / 86_400_000)} d` : ms >= 3_600_000 ? `${Math.round(ms / 3_600_000)} h` : ms >= 60_000 ? `${Math.round(ms / 60_000)} min` : `${Math.round(ms / 1000)} s`;
const since = (from, to) => from && to ? Math.max(0, new Date(to) - new Date(from)) : null;
const short = id => id ? String(id).slice(0, 8) : '—';
/** A message type that may wrap, but only between its words: PaymentAuthorized → Payment·Authorized. */
const camel = text => String(text).split(/(?=[A-Z][a-z])/).flatMap((part, i) => i ? [document.createElement('wbr'), part] : [part]);
const ok = node => node && !node.error;
const isUuid = value => /^[0-9a-f]{8}-[0-9a-f-]{27}$/.test(value || '');
const grafana = document.body.dataset.grafana;
// Grafana Explore for one query; datasource uids are fixed by infra/observability provisioning.
const explore = (uid, query) => `${grafana}/explore?schemaVersion=1&panes=${encodeURIComponent(JSON.stringify({ z: {
  datasource: uid, range: { from: 'now-6h', to: 'now' },
  queries: [{ refId: 'A', datasource: { type: uid, uid }, ...(uid === 'tempo' ? { queryType: 'traceql', query } : { expr: query }) }] } }))}`;

let state = null;
let selected = null; // followed order id
let journey = null;
let fold = null;
let focus = null; // { kind: 'message' | 'stage', id }
let busy = false;
let activeTab = readPref('events.tab', 'orders');
let labs = null; // /api/events/labs: recorded experiment runs and the tracking lab
let activeLab = readPref('events.lab', 'idempotency');
let messageTarget = 'message';
let seen = new Set();
const openGroups = new Set();
const lastRender = new Map();

function readPref(key, fallback) { try { return localStorage.getItem(key) || fallback; } catch { return fallback; } }
function writePref(key, value) { try { localStorage.setItem(key, value); } catch { /* private mode */ } }

/** Safe element builder: strings become text nodes, never HTML. */
function h(tag, attrs = {}, ...children) {
  const svgTag = ['svg', 'path', 'circle', 'rect', 'line', 'polyline'].includes(tag);
  const node = svgTag ? document.createElementNS('http://www.w3.org/2000/svg', tag) : document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === 'class') node.setAttribute('class', value);
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat()) if (child !== null && child !== undefined && child !== false) node.append(child instanceof Node ? child : String(child));
  return node;
}
/** Renders only when the data behind a region changed, so hover, focus and scroll survive polling. */
function renderIf(key, data, render) {
  const signature = JSON.stringify(data);
  if (lastRender.get(key) === signature) return;
  lastRender.set(key, signature);
  render();
}
const chip = (text, tone = '') => h('span', { class: `chip ${tone}` }, text);
const button = (label, onclick, attrs = {}) => h('button', { type: 'button', class: `small-button ${attrs.class || ''}`, onclick, disabled: busy || attrs.disabled, title: attrs.title }, label);
const empty = text => h('p', { class: 'empty' }, text);

// ---- Icons: one stroke family, 24-unit grid -----------------------------------------------------
const ICONS = {
  send: 'M4 12h13M12 5l7 7-7 7',
  api: 'M8 6l-5 6 5 6M16 6l5 6-5 6',
  db: 'M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3',
  outbox: 'M4 13h4l2 3h4l2-3h4M5 13l2-8h10l2 8v6H5z',
  cdc: 'M4 7h11M11 3l4 4-4 4M20 17H9M13 13l-4 4 4 4',
  kafka: 'M4 6h16M4 12h16M4 18h16M8 4v4M14 10v4M10 16v4',
  consumers: 'M9 11a3 3 0 100-6 3 3 0 000 6zM3 20c0-3.3 2.7-5 6-5s6 1.7 6 5M17 11a3 3 0 100-6M21 20c0-2.6-1.5-4.2-3.6-4.8',
  saga: 'M5 5h5v5H5zM14 14h5v5h-5zM10 7.5h4a3 3 0 013 3V14M7.5 10v4a3 3 0 003 3H14',
  services: 'M3 7h18v10H3zM7 11h2M11 11h2',
  projection: 'M4 5h16v14H4zM4 10h16M10 10v9',
  check: 'M5 12.5l4.5 4.5L19 7.5',
  wait: 'M12 7v5l3 2M12 3a9 9 0 100 18 9 9 0 000-18z',
  retry: 'M20 11a8 8 0 00-14.3-4.9L4 8M4 4v4h4M4 13a8 8 0 0014.3 4.9L20 16M20 20v-4h-4',
  undo: 'M9 14L4 9l5-5M4 9h10a6 6 0 010 12h-3',
  fail: 'M7 7l10 10M17 7L7 17',
  dice: 'M5 4h14a1 1 0 011 1v14a1 1 0 01-1 1H5a1 1 0 01-1-1V5a1 1 0 011-1zM9 9h.01M15 15h.01M15 9h.01M9 15h.01M12 12h.01',
  copy: 'M9 9h10v10H9zM5 15V5h10',
  close: 'M6 6l12 12M18 6L6 18',
  trace: 'M3 12h4l3-7 4 14 3-7h4',
  plus: 'M12 5v14M5 12h14',
  minus: 'M5 12h14'
};
const icon = (name, cls = 'ico') => h('svg', { class: cls, viewBox: '0 0 24 24', 'aria-hidden': 'true' }, h('path', { d: ICONS[name] }));
const STATE_ICON = { done: 'check', waiting: 'wait', retrying: 'retry', compensated: 'undo', failed: 'fail', idle: 'wait' };
const STATE_LABEL = { done: 'Done', waiting: 'Waiting', retrying: 'Retrying', compensated: 'Compensated', failed: 'Failed', idle: 'Not reached' };

// ---- Message vocabulary --------------------------------------------------------------------------
const LANES = [
  { id: 'order', label: 'Order service · saga', icon: 'api' },
  { id: 'payment', label: 'Payment', icon: 'services' },
  { id: 'inventory', label: 'Inventory', icon: 'services' },
  { id: 'shipping', label: 'Shipping', icon: 'services' },
  { id: 'projection', label: 'Read model', icon: 'projection' }
];
const COMPENSATION = new Set(['RefundPayment', 'ReleaseStock', 'PaymentRefunded', 'StockReleased', 'OrderCancelled']);
const NEGATIVE = new Set(['PaymentDeclined', 'StockRejected', 'ShipmentFailed']);
const decisionTone = d => ({ PROCESSED: 'good', DUPLICATE_SKIPPED: 'info', IGNORED: '', RETRY_SCHEDULED: 'warn', DEAD_LETTERED: 'bad' })[d] || '';
const decisionLabel = d => ({ PROCESSED: 'processed', DUPLICATE_SKIPPED: 'duplicate skipped', IGNORED: 'ignored', RETRY_SCHEDULED: 'retry', DEAD_LETTERED: 'dead-lettered' })[d] || d.toLowerCase();
const sagaState = s => ({ COMPLETED: 'done', CANCELLED: 'failed', COMPENSATING: 'compensated' })[s] || 'waiting';
const laneOf = m => {
  const producer = m.producer || '';
  if (producer.startsWith('payment')) return 1;
  if (producer.startsWith('inventory')) return 2;
  if (producer.startsWith('shipping')) return 3;
  if (producer) return 0;
  const topic = m.topic || m.kafka?.[0]?.topic || '';
  return topic.startsWith('payment.events') ? 1 : topic.startsWith('inventory.events') ? 2 : topic.startsWith('shipping.events') ? 3 : 0;
};
const kindOf = m => (m.topic || m.kafka?.[0]?.topic || '').endsWith('.commands') ? 'command' : m.type?.startsWith('Order') ? 'event' : 'reply';
const deliveriesOf = (m, projection) => (m.deliveries || []).filter(d => (d.consumer === 'order-projection') === !!projection);

/** Where one message stands, from its real outbox row, Kafka records and consumer decisions. */
function messageState(m, projection = false) {
  // order.events has one consumer, the projection: its decision is the event's own delivery.
  const onlyProjected = !projection && (m.topic || m.kafka?.[0]?.topic) === 'order.events';
  const deliveries = onlyProjected ? m.deliveries || [] : deliveriesOf(m, projection);
  if (deliveries.some(d => d.decision === 'DEAD_LETTERED')) return 'failed';
  if (!projection && !m.outboxAt && m.kafka?.length) return 'failed'; // on Kafka without a transaction: a ghost
  if (!m.kafka?.length) return 'waiting';
  const settled = deliveries.some(d => ['PROCESSED', 'DUPLICATE_SKIPPED', 'IGNORED'].includes(d.decision));
  if (!settled) return deliveries.some(d => d.decision === 'RETRY_SCHEDULED') ? 'retrying' : 'waiting';
  if (projection) return deliveries.some(d => d.decision === 'IGNORED') && !deliveries.some(d => d.decision === 'PROCESSED') ? 'failed' : 'done';
  if (NEGATIVE.has(m.type)) return 'failed';
  if (COMPENSATION.has(m.type)) return 'compensated';
  return 'done';
}

// ---- Actions -------------------------------------------------------------------------------------
/** Every lever: a real action. {@code where} is the message line its outcome is shown in. */
async function act(action, body = {}, pending, where = 'message') {
  if (busy) return;
  busy = true;
  messageTarget = where;
  showMessage(pending || `${action}…`);
  lastRender.clear();
  try {
    const response = await fetch(`/api/events/actions/${action}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(result.detail || `HTTP ${response.status}`);
    return result;
  } catch (error) {
    showMessage(error.message, true);
    throw error;
  } finally {
    busy = false;
    lastRender.clear();
    refresh();
  }
}
function showMessage(text, error = false) {
  const message = el(messageTarget) || el('message');
  message.textContent = text;
  message.classList.toggle('error', error);
}
const copy = text => h('button', { type: 'button', class: 'copy', title: 'Copy', 'aria-label': 'Copy', onclick: () => navigator.clipboard?.writeText(text) }, icon('copy'));

// ---- Composer: random realistic orders, editable as fields or JSON --------------------------------
const FIRST = ['Maya', 'Tomás', 'Aisha', 'Kenji', 'Lena', 'Omar', 'Priya', 'Jonas', 'Chloé', 'Mateo', 'Yara', 'Felix', 'Amara', 'Ravi', 'Ines', 'Noah', 'Zanele', 'Hugo', 'Mei', 'Karim', 'Sofia', 'Emeka', 'Astrid', 'Diego', 'Leila', 'Arjun', 'Nora', 'Tariq', 'Elif', 'Samuel'];
const LAST = ['Okafor', 'Lindqvist', 'Haddad', 'Tanaka', 'Moreau', 'Castillo', 'Nair', 'Becker', 'Mensah', 'Rossi', 'Kowalski', 'Ahmadi', 'Dubois', 'Silva', 'Novak', 'Osei', 'Fischer', 'Yamamoto', 'Petrov', 'Herrera', 'Adeyemi', 'Bergström', 'Farouk', 'Chen', 'Walsh', 'Ibrahim', 'Laurent', 'Svensson', 'Aziz', 'Kaur'];
const randomInt = (min, max) => { const r = new Uint32Array(1); crypto.getRandomValues(r); return min + (r[0] % (max - min + 1)); };
const pick = list => list[randomInt(0, list.length - 1)];
let draft = { customerId: '', items: [] };
let draftGenerated = true;

function randomDraft() {
  const catalog = Array.isArray(state?.catalog) ? state.catalog : [];
  const stock = new Map((Array.isArray(state?.stock) ? state.stock : []).map(i => [i.sku, i.available]));
  const inStock = catalog.filter(p => (stock.get(p.sku) ?? 1) > 0);
  const pool = [...(inStock.length ? inStock : catalog)];
  const items = [];
  for (let n = randomInt(1, Math.min(3, pool.length)); n > 0 && pool.length; n--) {
    const product = pool.splice(randomInt(0, pool.length - 1), 1)[0];
    const expensive = product.price >= 200;
    const cap = Math.max(1, Math.min(expensive ? 1 : 4, stock.get(product.sku) ?? 4));
    items.push({ sku: product.sku, quantity: randomInt(1, cap) });
  }
  return { customerId: `${pick(FIRST)} ${pick(LAST)}`, items };
}
function regenerate() {
  draft = randomDraft();
  draftGenerated = true;
  renderComposer(true);
}

const SCENARIOS = [
  { id: 'happy', label: 'Happy path', note: 'Nothing armed: payment, stock and shipping all succeed; the saga completes.' },
  { id: 'decline', label: 'Payment declines', note: 'Arms payment-decline: cancelled with nothing to compensate.', arm: ['payment-service', 'payment-decline', 'decline', 1] },
  { id: 'reject', label: 'Inventory rejects', note: 'Arms inventory-reject after payment: the saga refunds the charge.', arm: ['inventory-service', 'inventory-reject', 'reject', 1] },
  { id: 'ship-fail', label: 'Shipping fails', note: 'Arms shipping-fail at the pivot: stock is released and the payment refunded.', arm: ['shipping-service', 'shipping-fail', 'fail', 1] },
  { id: 'error-3', label: 'Consumer error ×3', note: 'payment-service throws three times: retried with backoff, then processed.', arm: ['payment-service', 'transient-error', 'error', 3] },
  { id: 'error-6', label: 'Consumer error ×6 → DLQ', note: 'Retries exhausted: the command is dead-lettered and the saga times out into compensation.', arm: ['payment-service', 'transient-error', 'error', 6] },
  { id: 'crash', label: 'Crash after commit', note: 'payment-service commits, dies before the offset commit, restarts and skips the redelivery as a duplicate.', arm: ['payment-service', 'crash-after-commit', 'crash', 1] },
  { id: 'gateway-down', label: 'Payment gateway down', note: 'Gateway 503s: in-process retries, breaker opens, Kafka retries, DLQ, saga timeout. Restore it under Services.', gateway: 'down' },
  { id: 'race', label: 'Two orders race for stock', note: 'Sends two orders for the first item at once with inventory slowed: optimistic locking makes one retry.', race: true },
  { id: 'dual-lost', label: 'Dual write: lost event', note: 'Anti-pattern: commits the order, then kills order-service before it publishes. The event never exists.', dual: 'COMMIT_THEN_CRASH' },
  { id: 'dual-ghost', label: 'Dual write: ghost event', note: 'Anti-pattern: publishes to Kafka, then rolls the database back. Consumers see an order that does not exist.', dual: 'PUBLISH_THEN_ROLLBACK' }
];
const scenario = () => SCENARIOS.find(s => s.id === el('scenario').value) || SCENARIOS[0];

function renderComposer(force = false) {
  const catalog = Array.isArray(state?.catalog) ? state.catalog : null;
  const stock = new Map((Array.isArray(state?.stock) ? state.stock : []).map(i => [i.sku, i]));
  const inside = document.activeElement?.closest?.('#composer');
  if (!force && inside) return;
  const customer = el('customer');
  if (document.activeElement !== customer) customer.value = draft.customerId;
  if (!catalog) { el('lines').replaceChildren(empty('order-service is unreachable: the catalog cannot be read.')); syncJson(); return; }
  el('lines').replaceChildren(...draft.items.map((item, index) => {
    const product = catalog.find(p => p.sku === item.sku);
    const level = stock.get(item.sku);
    const setQuantity = q => { item.quantity = Math.max(1, Math.min(99, q || 1)); draftGenerated = false; renderComposer(true); };
    return h('div', { class: 'line' },
      h('select', { 'aria-label': `Item ${index + 1} product`, onchange: e => { item.sku = e.target.value; draftGenerated = false; renderComposer(true); } },
        ...catalog.map(p => h('option', { value: p.sku, selected: p.sku === item.sku || null }, `${p.name} · $${p.price}`))),
      h('div', { class: 'qty' },
        h('button', { type: 'button', 'aria-label': 'One fewer', onclick: () => setQuantity(item.quantity - 1) }, icon('minus')),
        h('input', { type: 'number', min: 1, max: 99, value: item.quantity, 'aria-label': `${product?.name || item.sku} quantity`, onchange: e => setQuantity(Number(e.target.value)) }),
        h('button', { type: 'button', 'aria-label': 'One more', onclick: () => setQuantity(item.quantity + 1) }, icon('plus'))),
      h('button', { type: 'button', class: 'remove-line', 'aria-label': 'Remove item', disabled: draft.items.length === 1 || null, onclick: () => { draft.items.splice(index, 1); draftGenerated = false; renderComposer(true); } }, icon('close')),
      h('small', {}, level ? `${product ? `$${(product.price * item.quantity).toFixed(2)} · ` : ''}${level.available} available · ${level.reserved} reserved in inventory-service` : 'stock unknown'));
  }));
  el('add-line').disabled = draft.items.length >= catalog.length;
  syncJson();
}
function syncJson() {
  const json = el('json');
  if (document.activeElement === json) return;
  json.value = JSON.stringify(draft, null, 2);
  json.classList.remove('invalid');
  el('json-error').textContent = '';
}
function readJson() {
  const parsed = JSON.parse(el('json').value);
  if (typeof parsed?.customerId !== 'string' || !Array.isArray(parsed.items)) throw new Error('Expected {"customerId": "…", "items": [{"sku": "…", "quantity": 1}]}');
  return parsed;
}
function renderScenarioControls() {
  const current = scenario();
  el('scenario-note').textContent = current.note;
  const send = el('send');
  send.replaceChildren(icon('send'), current.arm || current.gateway ? 'Arm fault & send' : current.dual ? 'Run anti-pattern' : current.race ? 'Send two orders' : 'Send order');
  send.classList.toggle('arms', !!(current.arm || current.gateway || current.dual));
  send.disabled = busy;
  el('randomize').disabled = busy;
}

async function send() {
  let body = draft;
  if (el('json-editor').open) {
    try { body = readJson(); } catch (e) { el('json').classList.add('invalid'); el('json-error').textContent = e.message; return; }
  }
  if (!body.customerId?.trim()) { showMessage('Add a customer before sending.', true); el('customer').focus(); return; }
  if (!body.items?.length) { showMessage('Add at least one item.', true); return; }
  const plan = scenario();
  try {
    if (plan.arm) { const [service, name, mode, times] = plan.arm; await act('fault-arm', { service, name, mode, times }, `Arming ${name} on ${service}…`); }
    if (plan.gateway) await act('gateway', { mode: plan.gateway }, 'Taking the payment gateway down…');
    if (plan.dual) {
      const result = await act('dual-write', { mode: plan.dual, order: body }, plan.dual === 'COMMIT_THEN_CRASH' ? 'Dual write: committing, then crashing…' : 'Dual write: publishing, then rolling back…')
        .catch(() => null);
      if (plan.dual === 'COMMIT_THEN_CRASH') showMessage('order-service died between the database commit and the Kafka publish: the order exists, its event never will. The outbox makes this impossible.');
      else if (result) { follow(result.orderId); showMessage(result.outcome); }
    } else if (plan.race) {
      await act('fault-arm', { service: 'inventory-service', name: 'inventory-slow', mode: '700', times: 2 }, 'Widening the race window…');
      const sku = body.items[0].sku;
      const orders = [{ customerId: body.customerId, items: [{ sku, quantity: 1 }] }, { customerId: randomDraft().customerId, items: [{ sku, quantity: 1 }] }];
      const results = await Promise.all(orders.map(order => fetch('/api/events/actions/place-order', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ order }) }).then(r => r.json())));
      if (results[0]?.orderId) follow(results[0].orderId);
      showMessage(`Two orders reserve ${sku} at once. On different partitions both read version n and one retries (StockChanged in Retries & DLQ); on the same partition they run in turn.`);
    } else {
      const result = await act('place-order', { order: body }, 'Sending…');
      follow(result.orderId);
      showMessage(`Order ${short(result.orderId)} accepted: one transaction wrote OrderPlaced, the saga and AuthorizePayment. Watch it cross the lanes.`);
    }
    if (draftGenerated || !plan.dual) regenerate();
  } catch { /* message already shown */ } finally { refresh(); }
}

// ---- Flow rail: ten stops, from the followed order or, without one, from system health ------------
const STAGES = [
  ['request', 'Request', 'send'], ['api', 'Order API', 'api'], ['tx', 'Postgres tx', 'db'], ['outbox', 'Outbox', 'outbox'], ['debezium', 'Debezium', 'cdc'],
  ['kafka', 'Kafka', 'kafka'], ['consumers', 'Consumers', 'consumers'], ['saga', 'Saga', 'saga'], ['participants', 'Pay · Stock · Ship', 'services'], ['projection', 'Projection', 'projection']
];
const PARTICIPANTS = {
  payment: { name: 'Payment', commands: ['AuthorizePayment', 'RefundPayment'], replies: { PaymentAuthorized: 'done', PaymentDeclined: 'failed', PaymentRefunded: 'compensated' } },
  inventory: { name: 'Stock', commands: ['ReserveStock', 'ReleaseStock'], replies: { StockReserved: 'done', StockRejected: 'failed', StockReleased: 'compensated' } },
  shipping: { name: 'Shipping', commands: ['ScheduleShipment'], replies: { ShipmentScheduled: 'done', ShipmentFailed: 'failed' } }
};
const worst = states => ['failed', 'retrying', 'waiting', 'compensated', 'done'].find(s => states.includes(s)) || 'idle';

function participantState(messages, p) {
  const replies = messages.filter(m => p.replies[m.type]);
  const commands = messages.filter(m => p.commands.includes(m.type));
  if (!commands.length) return { state: 'idle', text: 'not reached' };
  const stuck = commands.map(c => messageState(c)).find(s => s === 'failed' || s === 'retrying');
  if (stuck) return { state: stuck, text: stuck === 'failed' ? 'command dead-lettered' : 'retrying' };
  if (replies.length < commands.length) return { state: 'waiting', text: `waiting for ${commands.at(-1).type} reply` };
  const last = replies.at(-1);
  return { state: p.replies[last.type], text: last.type };
}

function orderStages(s, j) {
  const messages = j.messages || [];
  const o = j.order;
  const writeOk = ok(o);
  const placed = messages.find(m => m.type === 'OrderPlaced');
  const inOutbox = messages.filter(m => m.outboxAt);
  const onKafka = messages.filter(m => m.kafka?.length);
  const pending = inOutbox.filter(m => !m.kafka?.length);
  const latencies = inOutbox.filter(m => m.kafka?.length).map(m => since(m.outboxAt, m.kafka[0].timestamp)).sort((a, b) => a - b);
  const deliveries = messages.flatMap(m => m.deliveries || []);
  const count = d => deliveries.filter(x => x.decision === d).length;
  const unconsumed = onKafka.filter(m => !(m.deliveries || []).length);
  const read = j.readModel;
  const saga = o?.saga;
  const parts = Object.entries(PARTICIPANTS).map(([id, p]) => ({ id, ...participantState(messages, p), name: p.name }));
  const ghost = !writeOk && onKafka.length;
  const records = new Set(onKafka.flatMap(m => m.kafka.map(k => k.topic)));
  return {
    request: { state: writeOk || messages.length ? 'done' : 'failed', detail: writeOk ? `POST /orders · ${o.order.customerId}` : 'no such order' },
    api: { state: writeOk ? 'done' : ghost ? 'failed' : 'waiting', detail: writeOk ? `v${o.order.version} · ${o.order.status}` : ghost ? 'not in the event store' : 'not recorded' },
    tx: { state: placed?.outboxAt ? 'done' : ghost ? 'failed' : 'waiting', detail: placed?.outboxAt ? `committed ${seconds(placed.outboxAt)}` : ghost ? 'rolled back' : '—' },
    outbox: { state: !inOutbox.length ? (ghost ? 'failed' : 'waiting') : pending.length ? 'waiting' : 'done', detail: `${inOutbox.length} rows${pending.length ? ` · ${pending.length} waiting` : ''}` },
    debezium: { state: !inOutbox.length ? (ghost ? 'failed' : 'idle') : pending.length ? 'waiting' : 'done', detail: latencies.length ? `WAL → Kafka ${latencies[Math.floor(latencies.length / 2)]} ms median` : pending.length ? 'WAL not read yet' : '—' },
    kafka: { state: onKafka.length ? 'done' : 'waiting', detail: `${onKafka.length} records · ${records.size} topics` },
    consumers: { state: count('DEAD_LETTERED') ? 'failed' : deliveries.some(d => d.decision === 'RETRY_SCHEDULED') && unconsumed.length + messages.filter(m => messageState(m) === 'retrying').length ? 'retrying' : unconsumed.length ? 'waiting' : onKafka.length ? 'done' : 'idle',
      detail: `${count('PROCESSED')} processed${count('DUPLICATE_SKIPPED') ? ` · ${count('DUPLICATE_SKIPPED')} dup` : ''}${count('RETRY_SCHEDULED') ? ` · ${count('RETRY_SCHEDULED')} retries` : ''}${count('DEAD_LETTERED') ? ' · DLQ' : ''}` },
    saga: { state: !saga ? 'idle' : saga.state === 'CANCELLED' ? (saga.compensations.length ? 'compensated' : 'failed') : sagaState(saga.state), detail: saga ? saga.state.replaceAll('_', ' ').toLowerCase() : '—' },
    participants: { state: worst(parts.map(p => p.state)), detail: parts.filter(p => p.state !== 'idle').map(p => `${p.name.toLowerCase()} ${p.text.replace(/^(Payment|Stock|Shipment)/, '').toLowerCase() || p.state}`).join(' · ') || 'not reached', sub: parts },
    projection: { state: ok(read) ? (writeOk && read.status === o.order.status ? 'done' : 'waiting') : onKafka.some(m => m.topic === 'order.events') ? 'waiting' : 'idle', detail: ok(read) ? `${read.status.toLowerCase()} · ${read.eventsApplied} events` : 'not projected yet' }
  };
}

function systemStages(s) {
  const services = s.services || {};
  const tone = t => ({ ok: 'done', degraded: 'retrying', down: 'failed' })[t];
  const outboxes = Object.values(services).filter(ok).map(v => v.outbox).filter(o => o && o.slot);
  const connectors = ok(s.connectors) ? Object.values(s.connectors) : null;
  const running = connectors ? connectors.filter(c => c.status.connector.state === 'RUNNING' && c.status.tasks.every(t => t.state === 'RUNNING')).length : 0;
  const topics = Array.isArray(s.topics) ? s.topics : null;
  const groups = Array.isArray(s.groups) ? s.groups.filter(g => g.groupId !== 'control-plane-tap') : null;
  const lag = groups ? groups.reduce((sum, g) => sum + g.totalLag, 0) : 0;
  const orders = Array.isArray(s.orders) ? s.orders : [];
  const inFlight = orders.filter(o => !['COMPLETED', 'CANCELLED'].includes(o.saga.state)).length;
  const projectionLag = groups?.find(g => g.groupId === 'order-projection')?.totalLag ?? 0;
  const unpublished = Object.values(s.unpublished || {}).reduce((sum, u) => sum + u.count, 0);
  const orderUp = ok(services['order-service']) || ok(services['order-service-b']);
  const parts = ['payment-service', 'inventory-service', 'shipping-service'].map(n => ({ id: n, name: n, state: ok(services[n]) ? 'done' : 'failed' }));
  return {
    request: { state: orderUp ? 'done' : 'failed', detail: orderUp ? 'POST /orders ready' : 'order-service down' },
    api: { state: tone(orderUp ? 'ok' : 'down'), detail: `${number(orders.length)} recent orders` },
    tx: { state: outboxes.length ? 'done' : 'failed', detail: `${number(outboxes.reduce((sum, o) => sum + o.rows, 0))} outbox rows` },
    outbox: { state: !outboxes.length ? 'failed' : unpublished ? 'waiting' : 'done', detail: unpublished ? `${number(unpublished)} not on Kafka yet` : 'all published' },
    debezium: { state: !connectors ? 'failed' : running === connectors.length ? 'done' : 'retrying', detail: connectors ? `${running}/${connectors.length} connectors running` : 'Connect unreachable' },
    kafka: { state: topics ? 'done' : 'failed', detail: topics ? `${topics.length} topics · ${topics.reduce((n, t) => n + t.partitions.length, 0)} partitions` : 'unreachable' },
    consumers: { state: !groups ? 'failed' : lag || groups.some(g => g.state !== 'STABLE') ? 'retrying' : 'done', detail: groups ? `lag ${number(lag)} · ${groups.filter(g => g.state === 'STABLE').length}/${groups.length} stable` : 'unreachable' },
    saga: { state: inFlight ? 'waiting' : 'done', detail: `${inFlight} in flight` },
    participants: { state: worst(parts.map(p => p.state)), detail: `${parts.filter(p => p.state === 'done').length}/3 services up`, sub: parts },
    projection: { state: !ok(s.projection) ? 'failed' : projectionLag ? 'waiting' : 'done', detail: ok(s.projection) ? `lag ${number(projectionLag)} · ${number(s.projection.orders)} orders` : 'unreachable' }
  };
}

function renderRail() {
  const followed = journey && selected;
  const stages = followed ? orderStages(state, journey) : systemStages(state);
  renderIf('rail', [stages, focus, !!followed], () => {
    el('rail').replaceChildren(...STAGES.map(([id, name, glyph]) => {
      const stage = stages[id];
      return h('li', { class: `s-${stage.state} ${focus?.kind === 'stage' && focus.id === id ? 'is-selected' : ''}` },
        h('button', { type: 'button', onclick: () => inspect({ kind: 'stage', id }), 'aria-label': `${name}: ${STATE_LABEL[stage.state]}. ${stage.detail}` },
          h('span', { class: 'stop' }, icon(stage.state === 'idle' ? glyph : STATE_ICON[stage.state] === 'check' ? glyph : STATE_ICON[stage.state])),
          h('span', { class: 'stop-name' }, name),
          stage.sub ? h('span', { class: 'stop-sub', 'aria-hidden': 'true' }, ...stage.sub.map(p => h('i', { class: `s-${p.state}`, title: `${p.name}: ${p.text || p.state}` }))) : null,
          h('span', { class: 'stop-detail' }, stage.detail)));
    }));
    el('flow-note').textContent = followed
      ? `Order ${short(selected)}: each stop shows how far this order has really got. Click a stop for its details.`
      : 'No order followed: each stop shows the health of that part of the system. Send an order to follow it.';
  });
}

// ---- Causation graph -------------------------------------------------------------------------------
function buildGraph(j) {
  const messages = [...(j.messages || [])].sort((a, b) => (a.outboxAt || a.kafka?.[0]?.timestamp || '~').localeCompare(b.outboxAt || b.kafka?.[0]?.timestamp || '~'));
  const byId = new Map(messages.map(m => [m.eventId, m]));
  const nodes = [];
  const edges = [];
  messages.forEach((m, row) => {
    nodes.push({ id: m.eventId, message: m, lane: laneOf(m), row, state: messageState(m) });
    if (m.causationId && byId.has(m.causationId)) edges.push({ from: m.causationId, to: m.eventId, compensation: COMPENSATION.has(m.type) });
    if ((m.topic || m.kafka?.[0]?.topic) === 'order.events') {
      nodes.push({ id: `proj:${m.eventId}`, message: m, lane: 4, row, projection: true, state: m.kafka?.length ? messageState(m, true) : 'idle' });
      edges.push({ from: m.eventId, to: `proj:${m.eventId}`, projection: true });
    }
  });
  return { nodes, edges, rows: messages.length };
}

function nodeTags(m, projection) {
  const tags = [];
  const deliveries = deliveriesOf(m, projection);
  const dups = deliveries.filter(d => d.decision === 'DUPLICATE_SKIPPED').length;
  const retries = deliveries.filter(d => d.decision === 'RETRY_SCHEDULED').length;
  if (retries) tags.push(chip(`${retries} ${retries === 1 ? 'retry' : 'retries'}`, 'warn'));
  if (dups) tags.push(chip(`${dups} duplicate${dups > 1 ? 's' : ''} skipped`, 'info'));
  if (deliveries.some(d => d.decision === 'DEAD_LETTERED')) tags.push(chip('dead-lettered', 'bad'));
  if (deliveries.some(d => d.decision === 'IGNORED')) tags.push(chip('ignored', ''));
  if (!projection && !m.outboxAt && m.kafka?.length) tags.push(chip('in no outbox: ghost', 'bad'));
  if (!projection && m.sagaTransition) tags.push(chip(`saga → ${m.sagaTransition.toState.replaceAll('_', ' ').toLowerCase()}`, sagaState(m.sagaTransition.toState) === 'done' ? 'good' : m.sagaTransition.toState === 'COMPENSATING' ? 's-compensated' : m.sagaTransition.toState === 'CANCELLED' ? 'bad' : 'info'));
  if (!projection && !m.causationId && m.type !== 'OrderPlaced' && m.outboxAt) tags.push(chip('from timeout scanner', 'warn'));
  return tags;
}

function renderGraph() {
  const graph = el('graph');
  if (!selected) {
    renderIf('graph', ['empty', recentOrders().map(o => [o.saga.orderId, o.saga.state])], () => {
      const recent = recentOrders().slice(0, 6);
      graph.replaceChildren(h('div', { class: 'graph-empty' },
        h('h3', {}, 'No order followed yet'),
        h('p', {}, 'Send a test order above and it is followed here automatically: every message it causes appears in the lane of the service that produced it, linked to the message that caused it. Or pick a recent order.'),
        recent.length ? h('div', { class: 'recent-list' }, ...recent.map(({ saga, order }) => h('button', { type: 'button', onclick: () => follow(saga.orderId) },
          h('strong', {}, `${order.customerId}`), h('span', {}, `${short(saga.orderId)} · $${order.total} · `, chip(saga.state.toLowerCase(), `s-${sagaState(saga.state)}`))))) : null));
      drawEdges();
    });
    return;
  }
  if (!journey) { renderIf('graph', ['loading', selected], () => graph.replaceChildren(empty('Reading the order’s journey…'))); return; }
  const g = buildGraph(journey);
  renderIf('graph', [selected, journey.messages, focus], () => {
    const fresh = new Set();
    const nodes = g.nodes.map(n => {
      const m = n.message;
      if (!seen.has(n.id)) { fresh.add(n.id); seen.add(n.id); }
      const k = m.kafka?.[0];
      const deliveries = !n.projection && (m.topic || k?.topic) === 'order.events' ? m.deliveries || [] : deliveriesOf(m, n.projection);
      const settled = deliveries.find(d => ['PROCESSED', 'DUPLICATE_SKIPPED', 'IGNORED', 'DEAD_LETTERED'].includes(d.decision));
      const pips = n.projection ? [] : [
        m.outboxAt ? 'on' : m.kafka?.length ? 'bad' : '',
        m.kafka?.length ? 'on' : m.outboxAt ? 'wait' : '',
        settled ? (settled.decision === 'DEAD_LETTERED' ? 'bad' : 'on') : deliveries.length ? 'retry' : m.kafka?.length ? 'wait' : ''
      ];
      const selectedNode = focus?.kind === 'message' && focus.id === n.id;
      const cause = n.projection ? m.type : (journey.messages || []).find(x => x.eventId === m.causationId)?.type;
      const title = n.projection ? (m.type || 'event').replace(/^Order/, '') || 'event' : m.type || 'unknown';
      return h('button', {
        type: 'button', class: `node s-${n.state} ${n.projection ? 'proj-node' : ''} ${selectedNode ? 'is-selected' : ''} ${fresh.has(n.id) && seen.size > fresh.size ? 'is-new' : ''}`,
        style: `grid-column:${n.lane + 1};grid-row:${n.row + 2}`, 'data-node': n.id, 'data-lane': LANES[n.lane].label,
        'aria-label': `${n.projection ? 'Projection of ' : ''}${m.type}: ${STATE_LABEL[n.state]}`, onclick: () => inspect({ kind: 'message', id: n.id })
      },
        h('span', { class: 'node-top' }, h('span', { class: 'node-dot' }, icon(STATE_ICON[n.state])), h('span', { class: 'node-type' }, n.projection ? ['order_view ← ', ...camel(title)] : camel(title))),
        h('span', { class: 'node-where' }, n.projection ? (settled ? `${decisionLabel(settled.decision)} · ${seconds(settled.at)}` : 'not consumed yet') : `${kindOf(m)} · ${k ? `${k.topic} p${k.partition}@${k.offset}` : m.outboxAt ? `${m.topic} · in outbox` : m.topic || ''}`),
        pips.length ? h('span', { class: 'steps' }, ...['outbox', 'kafka', 'consumer'].map((label, i) => h('span', { class: `step ${pips[i]}` }, h('i'), label))) : null,
        cause ? h('span', { class: 'node-cause' }, `caused by ${cause}`) : null,
        (tags => tags.length ? h('span', { class: 'node-tags' }, ...tags) : null)(nodeTags(m, n.projection)));
    });
    const lanes = h('div', { class: 'lanes', style: `grid-template-rows:auto repeat(${Math.max(1, g.rows)}, auto)` },
      ...LANES.map((lane, i) => h('div', { class: 'lane-bg', style: `grid-column:${i + 1}` })),
      ...LANES.map((lane, i) => h('div', { class: 'lane-head', style: `grid-column:${i + 1};grid-row:1` }, icon(lane.icon), lane.label)),
      h('svg', { class: 'edges', 'aria-hidden': 'true' }),
      ...(nodes.length ? nodes : [h('p', { class: 'empty', style: 'grid-column:1 / -1;grid-row:2' }, journey.order?.error ? `No such order: ${journey.order.error}` : 'No messages yet: the order’s transaction has not committed.')]));
    graph.replaceChildren(h('p', { class: 'graph-legend' }, 'Each message is a node in the lane of the service that produced it, linked to the message that caused it. Its three steps: committed to the ', h('b', {}, 'outbox'), ', read from the WAL by Debezium onto ', h('b', {}, 'Kafka'), ', then handled by a ', h('b', {}, 'consumer'), '. Dashed violet links are compensation.'), lanes);
    graph.dataset.edges = JSON.stringify(g.edges);
    requestAnimationFrame(drawEdges);
  });
}

function drawEdges() {
  const lanes = el('graph').querySelector('.lanes');
  const svg = lanes?.querySelector('.edges');
  if (!svg) return;
  const edges = JSON.parse(el('graph').dataset.edges || '[]');
  const box = lanes.getBoundingClientRect();
  const find = id => lanes.querySelector(`[data-node="${CSS.escape(id)}"]`);
  const hot = focus?.kind === 'message' ? focus.id : null;
  svg.setAttribute('viewBox', `0 0 ${box.width} ${box.height}`);
  svg.replaceChildren(...edges.map(e => {
    const a = find(e.from)?.getBoundingClientRect();
    const b = find(e.to)?.getBoundingClientRect();
    if (!a || !b) return null;
    const ax = a.left - box.left, ay = a.top - box.top, bx = b.left - box.left, by = b.top - box.top;
    let d;
    if (Math.abs(a.left - b.left) < 4) { // same lane: down the left edge
      const x = ax + 10;
      d = `M${x} ${ay + a.height} C ${x} ${ay + a.height + 12}, ${x} ${by - 12}, ${x} ${by}`;
    } else {
      const right = b.left > a.left;
      const x1 = right ? ax + a.width : ax, y1 = ay + a.height / 2;
      const x2 = right ? bx : bx + b.width, y2 = by + Math.min(18, b.height / 2);
      const bend = Math.max(24, Math.abs(x2 - x1) / 2);
      d = `M${x1} ${y1} C ${x1 + (right ? bend : -bend)} ${y1}, ${x2 - (right ? bend : -bend)} ${y2}, ${x2} ${y2}`;
    }
    return h('path', { d, class: `${e.compensation ? 'compensation' : ''} ${e.projection ? 'to-projection' : ''} ${hot && (hot === e.from || hot === e.to) ? 'hot' : ''}` });
  }).filter(Boolean));
}
new ResizeObserver(() => requestAnimationFrame(drawEdges)).observe(el('graph'));

// ---- Order header ------------------------------------------------------------------------------------
function recentOrders() { return Array.isArray(state?.orders) ? state.orders : []; }

function renderOrderHead() {
  const orders = recentOrders();
  const o = journey?.order;
  const writeOk = ok(o);
  const own = writeOk ? `${o.order.customerId} · ${(o.saga?.state || o.order.status).replaceAll('_', ' ').toLowerCase()}` : null;
  renderIf('order-select', [orders.map(x => [x.saga.orderId, x.saga.state, x.order.customerId]), selected, own], () => {
    const options = orders.map(({ saga, order }) => h('option', { value: saga.orderId, selected: saga.orderId === selected || null }, `${short(saga.orderId)} · ${order.customerId} · ${saga.state.replaceAll('_', ' ').toLowerCase()}`));
    if (selected && !orders.some(x => x.saga.orderId === selected)) options.unshift(h('option', { value: selected, selected: true }, `${short(selected)} · ${own || 'older order'}`));
    el('order-select').replaceChildren(h('option', { value: '', selected: !selected || null }, selected ? 'Stop following' : 'Follow an order…'), ...options);
  });
  const read = journey?.readModel;
  const traceId = journey?.messages?.find(m => m.traceparent)?.traceparent.split('-')[1];
  renderIf('order-head', [selected, writeOk && o.order, writeOk && o.saga?.state, ok(read) && read.status, traceId, !!fold, busy, writeOk && o.snapshotVersion], () => {
    if (!selected || !journey) { el('order-facts').replaceChildren(); el('order-tools').replaceChildren(); return; }
    const saga = o?.saga;
    el('order-facts').replaceChildren(
      saga ? h('span', { class: `saga-pill chip s-${saga.state === 'CANCELLED' && saga.compensations.length ? 'compensated' : sagaState(saga.state)}` }, `Saga ${saga.state.replaceAll('_', ' ').toLowerCase()}`) : chip(writeOk ? 'no saga' : 'not in the event store', 'bad'),
      writeOk ? h('span', {}, h('strong', {}, `$${o.order.total}`), ` · ${o.order.lines.reduce((n, l) => n + l.quantity, 0)} items · ${o.order.customerId}`) : null,
      writeOk ? h('span', {}, 'Read model ', ok(read) ? chip(read.status === o.order.status ? 'in sync' : `behind (${read.status.toLowerCase()})`, read.status === o.order.status ? 'good' : 'warn') : chip('not projected', 'warn')) : null);
    el('order-tools').replaceChildren(
      traceId && grafana ? h('a', { class: 'small-button button-link', href: explore('tempo', traceId), target: '_blank', rel: 'noopener' }, icon('trace'), 'Trace') : null,
      button('Rebuild from events', loadFold, { disabled: !writeOk, title: 'Fold the event store one event at a time, no snapshot' }),
      button('Discard snapshot', () => act('discard-snapshot', { orderId: selected }).then(() => showMessage('Snapshot discarded: the next load folds every event from version 1.'), () => {}), { disabled: !writeOk || !o.snapshotVersion }),
      button('Stop following', () => follow(null)));
  });
}

async function loadFold() {
  const response = await fetch(`/api/events/orders/${selected}/fold`);
  fold = response.ok ? await response.json() : null;
  inspect(null);
}

// ---- Inspector ----------------------------------------------------------------------------------------
function inspect(target) {
  focus = focus && target && focus.kind === target.kind && focus.id === target.id ? null : target;
  renderAll();
  if (target && window.matchMedia('(max-width: 1080px)').matches) el('inspector').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function kv(pairs) { return h('dl', { class: 'kv' }, ...pairs.filter(Boolean).flatMap(([k, v]) => [h('dt', {}, k), h('dd', {}, v)])); }
function section(title, ...content) { return h('section', { class: 'insp-section' }, h('h3', {}, title), ...content); }
function idRow(label, value) { return value ? h('div', { class: 'id-row' }, h('span', {}, label), h('code', { title: value }, value), copy(value)) : null; }
function timeline(items) { return h('ol', { class: 'timeline' }, ...items.filter(Boolean).map(([st, title, text, at]) => h('li', { class: `s-${st}` }, h('i'), h('div', {}, h('b', {}, title), text ? h('span', {}, text) : null), h('time', {}, at ? time(at) : '')))); }
const plus = (from, to, label) => { const ms = since(from, to); return ms === null ? '' : `+${number(ms)} ms ${label}`; };

function renderInspector() {
  const data = [focus, selected, journey, focus?.kind === 'stage' ? stageSource(focus.id) : null, fold];
  renderIf('inspector', data, () => {
    const node = el('inspector');
    if (focus?.kind === 'message' && journey) {
      const projection = focus.id.startsWith('proj:');
      const m = (journey.messages || []).find(x => x.eventId === focus.id.replace('proj:', ''));
      if (m) { node.replaceChildren(...messageInspector(m, projection)); return; }
    }
    if (focus?.kind === 'stage') { node.replaceChildren(...stageInspector(focus.id)); return; }
    if (selected && journey) { node.replaceChildren(...orderInspector()); return; }
    node.replaceChildren(h('div', { class: 'insp-head' }, h('h2', {}, 'Inspector')), h('div', { class: 'insp-body' },
      h('p', { class: 'callout' }, 'Follow an order to see its write model, saga, read model and trace here. Then click any node for its payload, headers, offsets and consumer decisions, or any stop on the rail for that stage.')));
  });
}

function messageInspector(m, projection) {
  const k = m.kafka || [];
  const deliveries = deliveriesOf(m, projection).sort((a, b) => a.at.localeCompare(b.at));
  const st = messageState(m, projection);
  const commit = m.outboxAt;
  const steps = projection ? [] : [
    [commit ? 'done' : m.kafka?.length ? 'failed' : 'waiting', commit ? `Committed to the ${m.producer} outbox` : 'In no outbox', commit ? `same transaction as the state change${m.eventStorePosition ? ` · event store #${m.eventStorePosition} v${m.eventStoreVersion}` : ''}` : 'published outside a database transaction', commit],
    ...k.map(r => ['done', `Debezium → ${r.topic} p${r.partition}@${r.offset}`, `read from the WAL ${plus(commit, r.timestamp, 'after commit')}`.trim(), r.timestamp]),
    k.length ? null : ['waiting', 'Not on Kafka yet', 'connector paused, or the WAL not read yet', null]
  ];
  const consumed = deliveries.map(d => [d.decision === 'PROCESSED' ? 'done' : d.decision === 'DEAD_LETTERED' ? 'failed' : d.decision === 'RETRY_SCHEDULED' ? 'retrying' : d.decision === 'DUPLICATE_SKIPPED' ? 'waiting' : 'idle',
    `${d.consumer}${d.instance && d.instance !== d.consumer ? ` @ ${d.instance}` : ''}: ${decisionLabel(d.decision)}${d.attempt > 1 ? ` · attempt ${d.attempt}` : ''}`,
    `${d.topic}-${d.partition}@${d.offset} · ${d.detail}${commit ? ` · ${plus(commit, d.at, 'after commit')}` : ''}`, d.at]);
  const first = k[0];
  return [
    h('div', { class: 'insp-head' },
      h('h2', {}, projection ? ['order_view ← ', ...camel(m.type)] : camel(m.type || 'unknown')),
      h('p', { class: 'insp-meta' }, projection ? 'Projection into order_view, the CQRS read side' : `${kindOf(m)} · produced by ${m.producer || 'no producer (ghost)'}`),
      h('div', { class: 'chips' }, chip(STATE_LABEL[st], `s-${st}`), m.schemaVersion ? chip(`schema v${m.schemaVersion}`) : null, first ? chip(`${first.topic} · p${first.partition}@${first.offset}`) : null)),
    h('div', { class: 'insp-body' },
      st === 'failed' && !projection && !m.outboxAt ? h('p', { class: 'callout bad' }, 'This record reached Kafka without a committed outbox row: the dual-write anti-pattern. Consumers acted on an order that does not exist.') : null,
      deliveries.some(d => d.decision === 'DUPLICATE_SKIPPED') ? h('p', { class: 'callout' }, 'The same event id arrived again (redelivery, replay or a duplicate publish). The idempotency record in processed_message made the consumer skip it: no second effect.') : null,
      deliveries.some(d => d.decision === 'DEAD_LETTERED') ? h('p', { class: 'callout bad' }, 'Retries were exhausted and the record was parked on the dead-letter topic. Redrive it from Retries & DLQ once the cause is fixed.') : null,
      steps.length ? section('Journey', timeline(steps)) : null,
      section(projection ? 'Projection' : 'Consumers', deliveries.length ? timeline(consumed) : empty(k.length ? 'On Kafka, not consumed yet.' : 'Nothing to consume yet.')),
      !projection && m.sagaTransition ? section('Saga effect', kv([['Transition', `${m.sagaTransition.fromState || '∅'} → ${m.sagaTransition.toState}`], ['Detail', m.sagaTransition.detail]])) : null,
      section('Identifiers', idRow('event id', m.eventId), idRow('caused by', m.causationId), idRow('correlation', m.correlationId), idRow('traceparent', m.traceparent)),
      first?.headers ? section('Kafka headers', kv(Object.entries(first.headers).map(([key, value]) => [key, String(value)]))) : null,
      section('Envelope', h('pre', { class: 'code' }, JSON.stringify(m.envelope ?? null, null, 2))),
      first ? h('div', { class: 'insp-actions' },
        button('Publish duplicate', () => act('duplicate', { topic: first.topic, partition: first.partition, offset: first.offset }, 'Publishing the same record again…').then(() => showMessage('Same event id, new offset: watch the consumer record a duplicate skip.'), () => {}), { title: 'Publish this exact record again: same key, value and headers' }),
        m.traceparent && grafana ? h('a', { class: 'small-button button-link', href: explore('tempo', m.traceparent.split('-')[1]), target: '_blank', rel: 'noopener' }, icon('trace'), 'Open trace') : null) : null)
  ];
}

function orderInspector() {
  const o = journey.order;
  const read = journey.readModel;
  const writeOk = ok(o);
  const saga = o?.saga;
  const traceId = journey.messages?.find(m => m.traceparent)?.traceparent.split('-')[1];
  const calls = Array.isArray(journey.gatewayCalls) ? journey.gatewayCalls : [];
  return [
    h('div', { class: 'insp-head' }, h('h2', {}, writeOk ? `${o.order.customerId} · $${o.order.total}` : `Order ${short(selected)}`), h('p', { class: 'insp-meta' }, 'The order you are following'), idRow('order id', selected)),
    h('div', { class: 'insp-body' },
      !writeOk ? h('p', { class: 'callout bad' }, 'Not in the event store: a lost or ghost order. Its messages, if any, are shown in the graph.') : null,
      saga?.failureReason ? h('p', { class: `callout ${saga.compensations.length ? 'warn' : 'bad'}` }, `${saga.failureReason}${saga.compensations.length ? `. Compensated: ${saga.compensations.join(', ')}.` : '.'}`) : null,
      writeOk ? section('Write model (event-sourced)', kv([['Status', o.order.status], ['Version', `v${o.order.version} · rebuilt from ${o.rebuiltFrom}`], ['Items', o.order.lines.map(l => `${l.quantity}× ${l.sku}`).join(', ')], ['Total', `$${o.order.total} ${o.order.currency}`]])) : null,
      section('Read model (CQRS)', ok(read) ? kv([['Status', read.status], ['Consistency', writeOk && read.status === o.order.status ? 'in sync with the write model' : 'behind: catching up through Kafka'], ['Events applied', read.eventsApplied], ['Last', `${read.lastEventType} at ${read.lastOffset}`]]) : empty('Not projected yet: waiting for order.events, or never published.')),
      saga ? section('Saga', timeline((o.transitions || []).map(t => [sagaState(t.toState) === 'done' ? 'done' : t.toState === 'CANCELLED' ? (saga.compensations.length ? 'compensated' : 'failed') : t.toState === 'COMPENSATING' ? 'compensated' : 'waiting', `${t.fromState ? `${t.fromState} → ` : ''}${t.toState}`, `${t.triggerType}: ${t.detail}`, t.at])),
        saga.deadline ? h('p', { class: 'callout' }, `Step deadline ${time(saga.deadline)}: the lease-holding replica compensates if no reply arrives by then.`) : null) : null,
      calls.length ? section('Payment gateway attempts', timeline(calls.slice().reverse().map(c => [c.outcome === 'CHARGED' ? 'done' : c.outcome === 'DECLINED' ? 'failed' : 'retrying', c.outcome, `${c.latencyMs} ms · breaker ${c.breakerState} · ${c.detail}`, c.at]))) : null,
      fold ? section('Aggregate rebuilt event by event', timeline(fold.map(step => ['done', `v${step.version} ${step.type}`, `status after: ${step.stateAfter.status}`, null]))) : null,
      journey.unmatched?.length ? section('Records without a readable envelope', ...journey.unmatched.map(u => h('pre', { class: 'code' }, JSON.stringify(u, null, 2)))) : null,
      section('Trace', idRow('correlation', journey.messages?.[0]?.correlationId), idRow('trace id', traceId),
        traceId && grafana ? h('div', { class: 'insp-actions' }, h('a', { class: 'small-button button-link', href: explore('tempo', traceId), target: '_blank', rel: 'noopener' }, icon('trace'), 'Trace in Tempo'), h('a', { class: 'small-button button-link', href: explore('loki', `{service_name=~".+"} | trace_id="${traceId}"`), target: '_blank', rel: 'noopener' }, 'Logs in Loki'))
          : empty(traceId ? 'One trace spans every service and Kafka hop.' : 'No traceparent: the OpenTelemetry agent was not attached.')))
  ];
}

function stageSource(id) {
  const s = state || {};
  return { order: journey && selected ? orderStages(s, journey)[id] : null, system: systemStages(s)[id], connectors: id === 'debezium' ? s.connectors : null, services: ['tx', 'outbox', 'participants'].includes(id) ? s.services : null, groups: id === 'consumers' ? s.groups : null, projection: id === 'projection' ? s.projection : null };
}
const STAGE_TEXT = {
  request: 'The operator (or a client) calls POST /orders on either order-service replica.',
  api: 'order-service validates and prices the order, then appends OrderPlaced to its event store.',
  tx: 'One PostgreSQL transaction writes the event, the saga row and the outbox rows for OrderPlaced and AuthorizePayment. All or nothing.',
  outbox: 'Outbox rows wait in the WAL until Debezium reads them. A paused connector makes them pile up here, never lost.',
  debezium: 'Debezium reads committed outbox inserts from the WAL and routes each to its topic, keyed by order id.',
  kafka: 'Keyed by order id: every message about one order lands on the same partition, in commit order.',
  consumers: 'Each consumer claims the event id in processed_message in the same transaction as its effect: duplicates are skipped.',
  saga: 'order-service orchestrates: each reply advances the saga; failures send compensating commands.',
  participants: 'Payment, inventory and shipping answer commands from their own databases and outboxes.',
  projection: 'order-query-service folds order.events into order_view: eventually consistent with the write model.'
};
function stageInspector(id) {
  const [, name] = STAGES.find(s => s[0] === id);
  const src = stageSource(id);
  const current = src.order || src.system;
  const j = journey;
  const messages = j?.messages || [];
  const body = [h('p', { class: 'callout' }, STAGE_TEXT[id])];
  if (src.order) {
    if (id === 'kafka' || id === 'debezium' || id === 'outbox') body.push(section('This order’s records', timeline(messages.map(m => [m.kafka?.length ? 'done' : m.outboxAt ? 'waiting' : 'failed', m.type, m.kafka?.[0] ? `${m.kafka[0].topic} p${m.kafka[0].partition}@${m.kafka[0].offset}${m.outboxAt ? ` · ${plus(m.outboxAt, m.kafka[0].timestamp, 'after commit')}` : ''}` : m.outboxAt ? 'in the outbox, not on Kafka yet' : 'no outbox row', m.kafka?.[0]?.timestamp || m.outboxAt]))));
    if (id === 'consumers' || id === 'projection') body.push(section(id === 'projection' ? 'Projection deliveries' : 'Deliveries', timeline(messages.flatMap(m => (m.deliveries || []).filter(d => id !== 'projection' || d.consumer === 'order-projection').map(d => [d.decision === 'PROCESSED' ? 'done' : d.decision === 'DEAD_LETTERED' ? 'failed' : d.decision === 'RETRY_SCHEDULED' ? 'retrying' : 'waiting', `${m.type} → ${d.consumer}`, `${decisionLabel(d.decision)}${d.attempt > 1 ? ` · attempt ${d.attempt}` : ''} · ${d.detail}`, d.at])).sort((a, b) => (a[3] || '').localeCompare(b[3] || '')))));
    if (id === 'saga' && j.order?.transitions) body.push(section('Transitions', timeline(j.order.transitions.map(t => [sagaState(t.toState), `${t.fromState || '∅'} → ${t.toState}`, t.detail, t.at]))));
    if (id === 'participants') body.push(section('Services', timeline(src.order.sub.map(p => [p.state, p.name, p.text, null]))));
    if (id === 'tx' || id === 'api') { const placed = messages.find(m => m.type === 'OrderPlaced'); if (placed) body.push(section('Committed together', kv([['Event store', `#${placed.eventStorePosition} v${placed.eventStoreVersion}`], ['Outbox', messages.filter(m => m.outboxAt === placed.outboxAt || m.causationId === placed.eventId && m.producer === 'order-service').map(m => m.type).join(', ')], ['At', time(placed.outboxAt)]]))); }
  }
  if (id === 'debezium' && ok(src.connectors)) body.push(section('Connectors', timeline(Object.entries(src.connectors).map(([n, c]) => [c.status.connector.state === 'RUNNING' ? 'done' : c.status.connector.state === 'PAUSED' ? 'waiting' : 'failed', n, `tasks: ${c.status.tasks.map(t => t.state).join(', ') || 'none'}`, null]))));
  if (id === 'consumers' && Array.isArray(src.groups)) body.push(section('Consumer groups', timeline(src.groups.map(g => [g.totalLag ? 'waiting' : g.state === 'STABLE' ? 'done' : 'retrying', g.groupId, `${g.state.toLowerCase()} · ${g.members.length} members · lag ${g.totalLag}`, null]))));
  body.push(section('System now', kv([['State', STATE_LABEL[src.system.state]], ['Detail', src.system.detail]])));
  return [h('div', { class: 'insp-head' }, h('h2', {}, name), h('p', { class: 'insp-meta' }, src.order ? `This stage for order ${short(selected)}` : 'This stage across the whole system'), h('div', { class: 'chips' }, chip(STATE_LABEL[current.state], `s-${current.state}`), chip(current.detail))), h('div', { class: 'insp-body' }, ...body)];
}

// ---- System tabs ----------------------------------------------------------------------------------------
const TABS = [
  { id: 'orders', label: 'Orders', badge: s => { const n = recentOrders().filter(o => !['COMPLETED', 'CANCELLED'].includes(o.saga.state)).length; return n ? [`${n} in flight`, 'warn'] : [recentOrders().length, '']; }, render: renderOrdersTab },
  { id: 'topics', label: 'Kafka topics', badge: s => [Array.isArray(s.topics) ? s.topics.length : '!', Array.isArray(s.topics) ? '' : 'bad'], render: renderTopicsTab },
  { id: 'groups', label: 'Consumer groups', badge: s => { const lag = Array.isArray(s.groups) ? s.groups.reduce((n, g) => n + g.totalLag, 0) : null; return lag === null ? ['!', 'bad'] : [`lag ${number(lag)}`, lag ? 'warn' : '']; }, render: renderGroupsTab },
  { id: 'dlq', label: 'Retries & DLQ', badge: s => { const n = Array.isArray(s.deadLetters) ? s.deadLetters.length : 0; return [n, n ? 'bad' : '']; }, render: renderDlqTab },
  { id: 'services', label: 'Services & faults', badge: s => { const services = Object.values(s.services || {}); const down = services.filter(x => !ok(x)).length; const armed = services.filter(ok).reduce((n, x) => n + x.faults.length, 0); return down ? [`${down} down`, 'bad'] : armed ? [`${armed} armed`, 'warn'] : [`${services.length} up`, '']; }, render: renderServicesTab },
  { id: 'read', label: 'Read model', badge: s => { const lag = Array.isArray(s.groups) ? s.groups.find(g => g.groupId === 'order-projection')?.totalLag ?? 0 : 0; return [lag ? `lag ${lag}` : ok(s.projection) ? number(s.projection.orders) : '!', lag ? 'warn' : ok(s.projection) ? '' : 'bad']; }, render: renderReadTab },
  { id: 'activity', label: 'Activity', badge: s => [Array.isArray(s.actions) ? s.actions.length : 0, ''], render: renderActivityTab }
];

function renderTabs() {
  const s = state;
  renderIf('tabs', [TABS.map(t => t.badge(s)), activeTab, busy], () => {
    el('tabs').replaceChildren(...TABS.map(t => {
      const [count, tone] = t.badge(s);
      return h('button', { type: 'button', role: 'tab', id: `tab-${t.id}`, 'aria-selected': String(t.id === activeTab), 'aria-controls': 'tab-panel', tabindex: t.id === activeTab ? 0 : -1,
        onclick: () => { activeTab = t.id; writePref('events.tab', t.id); lastRender.delete('tab-panel'); renderAll(); },
        onkeydown: e => { if (!['ArrowRight', 'ArrowLeft'].includes(e.key)) return; const i = TABS.findIndex(x => x.id === activeTab); const next = TABS[(i + (e.key === 'ArrowRight' ? 1 : TABS.length - 1)) % TABS.length]; activeTab = next.id; writePref('events.tab', next.id); lastRender.delete('tab-panel'); renderAll(); el(`tab-${next.id}`).focus(); } },
        t.label, h('span', { class: `count ${tone}` }, count));
    }));
  });
  const tab = TABS.find(t => t.id === activeTab) || TABS[0];
  el('tab-panel').setAttribute('aria-labelledby', `tab-${tab.id}`);
  if (document.activeElement?.closest?.('#tab-panel details, #tab-panel input')) return;
  renderIf('tab-panel', [tab.id, tabData(tab.id), selected, busy], () => el('tab-panel').replaceChildren(...[tab.render(s)].flat()));
}
function tabData(id) {
  const s = state;
  return ({ orders: [s.orders, s.readModel], topics: s.topics, groups: [s.groups, s.services && Object.fromEntries(Object.entries(s.services).map(([k, v]) => [k, ok(v) ? v.consumers : null]))], dlq: [s.decisions, s.deadLetters], services: [s.services, s.connectors, s.gateway, s.lease, s.unpublished], read: [s.projection, s.groups?.find?.(g => g.groupId === 'order-projection')?.totalLag], activity: [s.actions, s.tap] })[id];
}
const intro = (text, ...extra) => h('div', { class: 'tab-intro' }, h('p', {}, text), ...extra);

function renderOrdersTab(s) {
  if (!Array.isArray(s.orders)) return empty('order-service is unreachable.');
  if (!s.orders.length) return empty('No orders yet. Send one from the composer above.');
  const read = new Map((Array.isArray(s.readModel) ? s.readModel : []).map(r => [r.orderId, r]));
  return [intro('Sagas, newest first. The write model is the event-sourced order; the read model is its CQRS projection, caught up through Kafka. Click a row to follow it.'),
    h('div', { class: 'table-wrap' }, h('table', { class: 'data-table' }, h('thead', {}, h('tr', {}, ...['Order', 'Customer', 'Items', 'Total', 'Saga', 'Write model', 'Read model'].map(t => h('th', { class: `${t === 'Total' ? 'num' : ''} ${['Order', 'Items', 'Write model'].includes(t) ? 'hide-sm' : ''}` }, t)))),
      h('tbody', {}, ...s.orders.map(({ saga, order }) => {
        const projected = read.get(saga.orderId);
        const inSync = projected && projected.status === order.status;
        return h('tr', { class: `selectable ${selected === saga.orderId ? 'selected' : ''}`, onclick: () => { follow(saga.orderId); window.scrollTo({ top: 0, behavior: 'smooth' }); } },
          h('td', { class: 'mono hide-sm' }, short(saga.orderId)), h('td', {}, order.customerId), h('td', { class: 'hide-sm' }, order.lines.map(l => `${l.quantity}× ${l.sku.replace('SKU-', '').toLowerCase()}`).join(', ')), h('td', { class: 'num' }, `$${order.total}`),
          h('td', {}, chip(saga.state.replaceAll('_', ' ').toLowerCase(), `s-${saga.state === 'CANCELLED' && saga.compensations.length ? 'compensated' : sagaState(saga.state)}`)), h('td', { class: 'hide-sm' }, order.status.toLowerCase()),
          h('td', {}, projected ? chip(inSync ? 'in sync' : `behind · ${projected.status.toLowerCase()}`, inSync ? 'good' : 'warn') : chip('not projected yet', 'warn')));
      }))))];
}

function renderTopicsTab(s) {
  const topics = s.topics;
  if (!Array.isArray(topics)) return empty(`Kafka unreachable: ${topics?.error || ''}`);
  return [intro('Every topic and partition, with its earliest and latest offsets, read from the broker through the Kafka AdminClient.', button('Produce 20 records to lab.retention-demo', () => act('retention-demo', {}, 'Producing…').then(() => showMessage('20 records on lab.retention-demo. Closed segments older than a minute are deleted: watch its earliest offset move.'), () => {}), { title: '1-minute retention' })),
    ...topics.map(t => {
      const max = Math.max(1, ...t.partitions.map(p => p.latest));
      const retention = t.retentionMs === '-1' ? 'kept forever' : `retention ${duration(Number(t.retentionMs))}`;
      return h('div', { class: `topic ${t.name.endsWith('.dlt') ? 'dlt' : ''}` }, h('div', {}, h('strong', {}, t.name), h('small', {}, `${t.partitions.length} partition${t.partitions.length > 1 ? 's' : ''} · ${retention}`)),
        h('div', { class: 'partitions' }, ...t.partitions.map(p => h('div', { class: 'partition' }, h('span', {}, `p${p.partition}`),
          h('span', { class: 'bar', title: `offsets ${p.earliest}–${p.latest}` }, h('i', { style: `left:${p.earliest / max * 100}%;right:${100 - p.latest / max * 100}%` })),
          h('span', {}, `${number(p.earliest)} → ${number(p.latest)}`)))));
    })];
}

function renderGroupsTab(s) {
  const groups = s.groups;
  if (!Array.isArray(groups)) return empty(`Kafka unreachable: ${groups?.error || ''}`);
  const owners = new Map();
  for (const [name, svc] of Object.entries(s.services || {})) if (ok(svc)) for (const c of svc.consumers) owners.set(c.groupId, [...(owners.get(c.groupId) || []), { service: name, consumer: c }]);
  return [intro('Members, partition assignments, committed offsets and lag per consumer group. A group may run on several replicas: pause, resume and replay act on every live one.'),
    h('div', { class: 'cards' }, ...groups.map(g => {
      const list = owners.get(g.groupId) || [];
      const o = list[0];
      const paused = list.some(x => x.consumer.pauseRequested);
      const all = action => list.reduce((chain, x) => chain.then(() => act(action, { service: x.service, consumer: x.consumer.id })), Promise.resolve()).catch(() => {});
      return h('div', { class: 'card' },
        h('header', {}, h('strong', {}, g.groupId), h('div', { class: 'chips' }, chip(g.state.toLowerCase(), g.state === 'STABLE' ? 'good' : g.state === 'EMPTY' ? 'bad' : 'warn'), chip(`lag ${number(g.totalLag)}`, g.totalLag ? 'warn' : 'good'), paused ? chip('paused', 'warn') : null)),
        h('div', { class: 'members' }, ...(g.members.length ? g.members.map(m => h('div', { class: 'member' }, h('span', { class: 'mono' }, m.clientId), ...(m.assignments.length > 6 ? [chip(`${m.assignments.length} partitions`)] : m.assignments.map(a => chip(a))))) : [empty('No members: nothing is consuming. Offsets stay committed in Kafka.')])),
        h('details', { class: 'more', open: g.totalLag > 0 || openGroups.has(g.groupId) || null, ontoggle: e => e.target.open ? openGroups.add(g.groupId) : openGroups.delete(g.groupId) },
          h('summary', {}, `Committed offsets on ${g.partitions.length} partitions${g.totalLag ? ` · ${g.partitions.filter(p => p.lag).length} lagging` : ''}`),
          h('table', { class: 'lag-table' }, h('tbody', {}, ...g.partitions.map(p => h('tr', { class: p.lag ? 'lagging' : '' }, h('td', {}, `${p.topic}-${p.partition}`), h('td', {}, `committed ${p.committed ?? '—'}`), h('td', {}, `end ${p.latest}`), h('td', {}, `lag ${p.lag ?? '—'}`)))))),
        o ? h('div', { class: 'buttons' },
          button(paused ? 'Resume' : 'Pause', () => all(paused ? 'consumer-resume' : 'consumer-pause'), { title: `On ${list.map(x => x.service).join(' and ')}` }),
          button('Replay from 0', () => {
            const partitions = g.partitions.filter(p => p.committed !== null).map(p => ({ topic: p.topic, partition: p.partition }));
            if (!confirm(`Stop ${g.groupId} on ${list.length} replica(s), move its committed offsets back to 0 on ${partitions.length} partition(s), start again? Everything is redelivered; the inbox should skip it all as duplicates.`)) return;
            act('consumer-seek', { group: g.groupId, consumer: o.consumer.id, owners: list.map(x => x.service), partitions, offset: 0 }, 'Stopping consumers, moving offsets…')
              .then(() => showMessage(`${g.groupId} rewound: watch its lag jump, then duplicate skips drain it.`), () => {});
          }, { title: 'Stop the consumer on every replica, reset its committed offsets, start again' })) : null);
    }))];
}

function renderDlqTab(s) {
  const decisions = s.decisions || [];
  const records = Array.isArray(s.deadLetters) ? s.deadLetters : [];
  const openOrder = id => isUuid(id) ? button('Follow order', () => { follow(id); window.scrollTo({ top: 0, behavior: 'smooth' }); }) : null;
  return [intro('Every delivery outcome, from every consumer: processed, duplicate skipped, ignored, retried with backoff, or dead-lettered. Unreadable records skip the retries and go straight to the dead-letter topic.',
    h('div', { class: 'buttons' }, ...['payment.commands', 'inventory.commands', 'order.events'].map(topic => button(`Poison ${topic}`, () => act('poison', { topic }, 'Publishing an unreadable record…').then(() => showMessage(`Unreadable record on ${topic}: not retried, dead-lettered on the first failure.`), () => {}), { class: 'danger-soft' })))),
    h('div', { class: 'split' },
      h('div', {}, h('h3', { class: 'sub' }, `Consumer decisions · newest ${decisions.length}`), h('div', { class: 'feed' }, ...(decisions.length ? decisions.map(d => h('div', { class: 'feed-row' }, h('time', {}, seconds(d.at)), chip(decisionLabel(d.decision), decisionTone(d.decision)),
        h('p', {}, `${d.consumer}${d.instance && d.instance !== d.consumer ? ` @ ${d.instance}` : ''} · ${d.type || 'unreadable'} · ${d.topic}-${d.partition}@${d.offset}${d.attempt > 1 ? ` · attempt ${d.attempt}` : ''}`, h('small', {}, d.detail)), openOrder(d.orderId))) : [empty('No deliveries yet.')]))),
      h('div', {}, h('h3', { class: 'sub' }, `Dead-letter topics · ${records.length} parked`), h('div', { class: 'feed' }, ...(records.length ? records.map(r => {
        const headers = JSON.parse(r.headers || '{}');
        return h('div', { class: 'feed-row' }, h('time', {}, seconds(r.kafka_timestamp)), chip(r.topic, 'bad'),
          h('p', {}, `${r.type || 'unreadable'} · from ${headers['kafka_dlt-original-topic'] || '?'}-${headers['kafka_dlt-original-partition'] || '?'}@${headers['kafka_dlt-original-offset'] || '?'}`, h('small', {}, headers['kafka_dlt-exception-message'] || headers['kafka_dlt-exception-fqcn'] || '')),
          h('div', { class: 'buttons' }, button('Redrive', () => act('redrive', { topic: r.topic, partition: r.kafka_partition, offset: r.kafka_offset }, 'Sending it back to its topic…').then(() => showMessage('Redriven: delivered again. If the cause is fixed it succeeds; a poison record lands here again.'), () => {})), openOrder(r.record_key)));
      }) : [empty('Nothing dead-lettered.')]))))];
}

const SERVICE_FAULTS = {
  'order-service': [['scanner-stall', '8000', 'Stall timeout scanner 8 s (fencing)']],
  'payment-service': [['payment-decline', 'decline', 'Decline next payment']],
  'inventory-service': [['inventory-reject', 'reject', 'Reject next reservation'], ['inventory-slow', '700', 'Slow next 2 reservations (race)', 2]],
  'shipping-service': [['shipping-fail', 'fail', 'Fail next shipment']]
};
function renderServicesTab(s) {
  const services = s.services || {};
  const lease = ok(s.lease) ? s.lease : null;
  const cards = Object.keys(services).map(name => {
    const svc = services[name];
    if (!ok(svc)) return h('div', { class: 'card down' }, h('header', {}, h('strong', {}, name), chip('down', 'bad')), empty(svc?.error || 'No answer. If you crashed it, Docker is restarting it.'));
    const armed = new Map(svc.faults.map(f => [f.name, f]));
    const fault = (label, faultName, mode, times, cls) => armed.has(faultName)
      ? button(`Clear ${faultName}`, () => act('fault-clear', { service: name, name: faultName }), { class: 'danger-soft' })
      : button(label, () => act('fault-arm', { service: name, name: faultName, mode, times }), { class: cls });
    const holder = lease && name.startsWith('order-service') ? lease : null;
    return h('div', { class: 'card' },
      h('header', {}, h('strong', {}, name), h('div', { class: 'chips' }, holder?.owner === name && holder.held ? chip(`scanner lease · token ${holder.token}`, 'info') : null, chip('up', 'good'))),
      kv([
        ['Consumers', h('div', { class: 'chips' }, ...svc.consumers.map(c => chip(`${c.pauseRequested ? 'paused' : c.running ? 'running' : 'stopped'} · ${c.assignedPartitions.length} partitions`, c.pauseRequested ? 'warn' : 'good')))],
        ['Outbox', svc.outbox?.slot ? `${number(svc.outbox.rows)} rows · ${number(s.unpublished?.[name]?.count ?? 0)} not on Kafka · slot ${svc.outbox.slotActive ? 'active' : 'idle'}` : 'none (read side)'],
        holder ? ['Lease', holder.held ? `held by ${holder.owner}, token ${holder.token}, until ${time(holder.expiresAt)}` : 'free'] : null,
        ['Faults', svc.faults.length ? h('div', { class: 'chips' }, ...svc.faults.map(f => chip(`${f.name}=${f.mode}${f.remaining !== null ? ` ×${f.remaining}` : ''}`, 'bad'))) : 'none armed']
      ]),
      h('div', { class: 'buttons' }, fault('Error ×3', 'transient-error', 'error', 3, ''), fault('Crash after commit', 'crash-after-commit', 'crash', 1, 'danger-soft'),
        ...(SERVICE_FAULTS[name] || []).map(([f, mode, label, times]) => fault(label, f, mode, times ?? 1, '')),
        button('Crash now', () => confirm(`Kill ${name}'s JVM now? Docker restarts it.`) && act('crash', { service: name }), { class: 'danger-soft' })));
  });
  const connectors = s.connectors;
  const g = s.gateway;
  return [intro('Crash, pause and break things on purpose. Faults live in each service’s own database, so they survive the crashes they cause.'),
    h('div', { class: 'cards' }, ...cards),
    h('div', { class: 'split', style: 'margin-top:18px' },
      h('div', {}, h('h3', { class: 'sub' }, 'Debezium connectors'), !ok(connectors) ? empty(`Kafka Connect unreachable: ${connectors?.error || ''}`) : !Object.keys(connectors).length ? empty('No connectors registered yet: connect-init registers them once the services are healthy.') :
        h('div', { class: 'feed' }, ...Object.entries(connectors).map(([name, c]) => { const st = c.status.connector.state; return h('div', { class: 'feed-row' }, h('strong', { class: 'mono' }, name), chip(st.toLowerCase(), st === 'RUNNING' ? 'good' : st === 'PAUSED' ? 'warn' : 'bad'), h('p', {}, h('small', {}, `tasks: ${c.status.tasks.map(t => t.state).join(', ') || 'none'}`)),
          button(st === 'PAUSED' ? 'Resume' : 'Pause', () => act(st === 'PAUSED' ? 'connector-resume' : 'connector-pause', { connector: name }).then(() => showMessage(st === 'PAUSED' ? 'Resumed: the WAL backlog drains to Kafka.' : 'Paused: commits still succeed; outbox rows wait in the WAL.'), () => {}))); }))),
      h('div', {}, h('h3', { class: 'sub' }, 'Payment gateway & circuit breaker'), !ok(g) ? empty(`payment-service unreachable: ${g?.error || ''}`) : [
        h('div', { class: 'gateway-modes' }, ...['healthy', 'slow', 'down', 'declining'].map(mode => h('button', { type: 'button', class: 'small-button', 'aria-pressed': String(g.mode === mode), disabled: busy, onclick: () => act('gateway', { mode }) }, mode)), button('Reset breaker', () => act('breaker-reset'))),
        h('div', { class: 'stats' }, h('div', {}, 'Breaker', h('strong', { style: `color:var(--${g.breakerState === 'CLOSED' ? 'green' : g.breakerState === 'OPEN' ? 'red' : 'amber'})` }, g.breakerState.toLowerCase())), h('div', {}, 'Failure rate', h('strong', {}, g.failureRate < 0 ? '—' : `${Math.round(g.failureRate)}%`)), h('div', {}, 'Buffered calls', h('strong', {}, g.bufferedCalls)), h('div', {}, 'Refused while open', h('strong', {}, g.notPermittedCalls))),
        h('div', { class: 'feed' }, ...(g.recentCalls.length ? g.recentCalls.slice(0, 8).map(c => h('div', { class: 'feed-row' }, h('time', {}, seconds(c.at)), chip(c.outcome, c.outcome === 'CHARGED' ? 'good' : c.outcome === 'DECLINED' ? 'warn' : 'bad'), h('p', {}, `${c.latencyMs} ms · breaker ${c.breakerState.toLowerCase()}`, h('small', {}, `order ${short(c.orderId)} · ${c.detail}`)))) : [empty('No gateway calls yet.')]))]))];
}

function renderReadTab(s) {
  const p = s.projection;
  if (!ok(p)) return empty(`order-query-service unreachable: ${p?.error || ''}`);
  const lag = Array.isArray(s.groups) ? s.groups.find(g => g.groupId === 'order-projection')?.totalLag ?? 0 : 0;
  return [intro('The CQRS query side: order-query-service folds order.events into its own database. Rebuild truncates both read models, forgets the projection’s idempotency records and replays the topic from offset 0.',
    button('Rebuild projection', () => confirm('Truncate the read models and replay order.events from the beginning?') && act('rebuild-projection', {}, 'Rebuilding…').then(() => showMessage('Rebuilding: watch order-projection lag spike, then drain to 0.'), () => {}))),
    h('div', { class: 'stats' }, h('div', {}, 'Orders projected', h('strong', {}, number(p.orders))), h('div', {}, 'Customers', h('strong', {}, number(p.customers))), h('div', {}, 'Projection lag', h('strong', {}, number(lag))), h('div', {}, 'Last projected', h('strong', {}, p.lastProjectedAt ? seconds(p.lastProjectedAt) : '—')))];
}

function renderActivityTab(s) {
  const tap = s.tap || {};
  return [intro(`Operator actions, durable and newest first. The control plane also taps every topic with its own consumer group: ${tap.status || '—'} · ${number(tap.records || 0)} records stored · last poll ${tap.lastPoll ? seconds(tap.lastPoll) : '—'}.`),
    h('div', { class: 'feed' }, ...(Array.isArray(s.actions) && s.actions.length ? s.actions.map(a => h('div', { class: 'feed-row' }, h('time', {}, seconds(a.at)), chip(a.action, a.ok ? '' : 'bad'), h('p', {}, a.target, h('small', {}, a.outcome)))) : [empty('No actions yet.')]))];
}


// ---- Experiments: learning labs on the real system ------------------------------------------------
/** A lab lever: the same real actions as everywhere else, with messages shown inside the lab. */
function labAct(action, body, pending) {
  return act(action, body, pending, 'lab-message');
}
/** Runs a scenario as a visible sequence of real actions; stops at the first that fails. */
async function scenarioSteps(steps, done) {
  try {
    for (const [action, body, pending] of steps) await labAct(action, body, pending);
    if (done) showMessage(done);
  } catch { /* the failing step's message is shown */ }
}
const labOrder = () => randomDraft();
const steps = list => h('ol', { class: 'lab-steps' }, ...list.map(([name, text, actions]) =>
  h('li', { class: 'lab-step' }, h('span', { class: 'step-name' }, name), h('div', { class: 'step-body' }, h('p', {}, text), actions?.length ? h('div', { class: 'buttons' }, ...actions) : null))));
const labButton = (label, onclick, tone = '', title) => h('button', { type: 'button', class: `small-button ${tone}`, disabled: busy, title, onclick }, label);
const evidence = (title, ...content) => h('section', { class: 'evidence' }, h('h3', {}, title), ...content);

const LABS = [
  { id: 'idempotency', label: 'Client retries', status: idempotencyStatus, render: renderIdempotencyLab },
  { id: 'ordering', label: 'Ordering & partitions', status: orderingStatus, render: renderOrderingLab },
  { id: 'ryw', label: 'Read your writes', status: rywStatus, render: renderRywLab }
];

function renderLabs() {
  renderIf('lab-tabs', [LABS.map(l => l.status()), activeLab], () => el('lab-tabs').replaceChildren(...LABS.map(l => {
    const [text, tone] = l.status();
    return h('button', { type: 'button', role: 'tab', id: `lab-tab-${l.id}`, 'aria-selected': String(l.id === activeLab), 'aria-controls': 'lab-panel',
      onclick: () => { activeLab = l.id; writePref('events.lab', l.id); renderAll(); } }, l.label, h('span', { class: `count ${tone}` }, text));
  })));
  const lab = LABS.find(l => l.id === activeLab) || LABS[0];
  el('lab-panel').setAttribute('aria-labelledby', `lab-tab-${lab.id}`);
  renderIf('lab-panel', [lab.id, labs, busy, lab.id === 'ryw' ? projectionState() : null, lab.id === 'ordering' ? state?.groups?.find?.(g => g.groupId === 'carrier-tracking') : null], () => {
    const message = el('lab-message')?.textContent || '';
    const messageError = el('lab-message')?.classList.contains('error');
    el('lab-panel').replaceChildren(...[lab.render()].flat(), h('p', { id: 'lab-message', class: `composer-message ${messageError ? 'error' : ''}`, role: 'status', 'aria-live': 'polite' }, message));
  });
}

// Lab 1: an order request whose answer is lost, retried by a client with or without an Idempotency-Key.
function idempotencyStatus() {
  const run = labs?.idempotency?.[0];
  const orders = labs?.idempotencyOrders?.length ?? 0;
  if (!run) return ['not run', ''];
  return orders > 1 ? [`${orders} orders from 1 request`, 'bad'] : orders === 1 ? ['1 request, 1 order', 'good'] : ['running…', 'warn'];
}
function renderIdempotencyLab() {
  const run = labs?.idempotency?.[0];
  const r = run?.result;
  const orders = labs?.idempotencyOrders || [];
  const retry = (withKey, slowAnswers) => labAct('exp-idempotency', { withKey, slowAnswers, order: labOrder() },
    `Client sends the order${withKey ? ' with an Idempotency-Key' : ''}; order-service will hold ${slowAnswers > 1 ? 'every' : 'its first'} answer 2.5 s…`)
    .then(res => showMessage(res.clientSawOrder ? `The client saw one order (${short(res.clientSawOrder)}). Now look at what really exists →` : 'Every attempt timed out: the client believes the order failed. Now look at what really exists →'), () => {});
  const known = r?.clientSawOrder;
  const keep = known || orders.at(-1)?.orderId; // the order the client knows, or the first one created
  return h('div', { class: 'lab-grid' },
    steps([
      ['Learn', 'A client that times out cannot tell a lost request from a lost answer. Retrying is the right reflex, but without idempotency the server treats the retry as a new order.'],
      ['Trigger', 'order-service commits the order, then holds its answer 2.5 s. The client gives up after 1 s and retries.', [labButton('Send with a retrying client', () => retry(false, 1), 'danger-soft')]],
      ['Observe', 'Left: what the client saw, attempt by attempt. Right: what exists for that one request: orders, sagas and card charges.'],
      ['Break', 'Make every answer slow: the client gives up believing the order failed, while each attempt created one.', [labButton('Every answer slow', () => retry(false, 3), 'danger-soft')]],
      ['Understand', 'The first attempt did succeed; only its answer was late. Each retry was a new request with a new order id, so payment-service, idempotent per order, charged each one.'],
      ['Fix', 'Send an Idempotency-Key. order-service claims it in the same transaction as the order; a retry finds it and gets the first order back, even when every answer is slow.',
        [labButton('Retry with an Idempotency-Key', () => retry(true, 1)), labButton('Key, every answer slow', () => retry(true, 3))]],
      ['Recover', 'Duplicates that already happened are refunded with a real RefundPayment command. Once shipped, the saga cannot undo the order: prevention beats cure.']
    ]),
    h('div', { class: 'lab-evidence' },
      !r ? evidence('Evidence', empty('Run the trigger to see the client’s attempts and what they created.')) : [
        evidence(`What the client saw · ${run.mode}`,
          h('p', { class: 'evidence-note' }, `Client timeout ${r.clientTimeoutMs} ms · server answer delayed ${r.serverDelayMs} ms${r.idempotencyKey ? ` · key ${r.idempotencyKey.slice(0, 14)}…` : ' · no key'}`),
          timeline(r.attempts.map(a => [a.orderId ? 'done' : 'failed', `Attempt ${a.attempt}: ${a.outcome}`, `${a.ms} ms${a.orderId ? ` · order ${short(a.orderId)}` : ''}`, a.at]))),
        evidence('What really exists for that request',
          h('div', { class: 'big-facts' }, h('div', {}, h('strong', {}, '1'), 'request'), h('div', { class: orders.length > 1 ? 'bad' : '' }, h('strong', {}, orders.length), orders.length === 1 ? 'order' : 'orders'),
            h('div', { class: orders.reduce((n, o) => n + o.charges, 0) > 1 ? 'bad' : '' }, h('strong', {}, orders.reduce((n, o) => n + o.charges, 0)), 'charges'), h('div', {}, h('strong', {}, orders.filter(o => o.refunded).length), 'refunded')),
          orders.length ? h('table', { class: 'data-table' }, h('tbody', {}, ...orders.map(o => h('tr', {},
            h('td', { class: 'mono' }, short(o.orderId)), h('td', {}, `$${o.total}`), h('td', {}, chip(o.saga.replaceAll('_', ' ').toLowerCase(), `s-${sagaState(o.saga)}`)),
            h('td', {}, o.orderId === known ? chip('the client knows this one', 'info') : o.orderId === keep ? chip('first order', '') : chip('duplicate', 'bad')),
            h('td', {}, `${o.charges} charge${o.charges === 1 ? '' : 's'}`, o.refunded ? chip('refunded', 's-compensated') : null),
            h('td', { class: 'buttons' }, button('Follow', () => { follow(o.orderId); window.scrollTo({ top: 0, behavior: 'smooth' }); }),
              o.orderId !== keep && !o.refunded ? button('Refund duplicate', () => labAct('refund', { orderId: o.orderId, reason: 'duplicate created by a client retry' }, 'Sending RefundPayment…').then(() => showMessage('RefundPayment is on its way through the outbox and Kafka; payment-service refunds it once. Follow the order to watch it.'), () => {}), { class: 'danger-soft' }) : null)))))
            : empty('Looking up the orders…')),
        labs.idempotency.length > 1 ? evidence('Earlier runs', h('div', { class: 'feed' }, ...labs.idempotency.slice(1).map(x => h('div', { class: 'feed-row' }, h('time', {}, seconds(x.at)), chip(x.mode), h('p', {}, x.summary))))) : null]));
}

// Lab 2: carrier scans keyed right or wrong, a partition count that changes, and a guard that holds.
function orderingStatus() {
  const t = labs?.tracking;
  if (!ok(t)) return ['shipping down', 'bad'];
  const regressions = t.parcels.reduce((n, p) => n + p.regressions, 0);
  const wrong = t.parcels.filter(p => p.lastSeq < 4 && t.scans.some(sc => sc.trackingNumber === p.trackingNumber && sc.seq === 4)).length;
  if (!t.parcels.length) return ['not run', ''];
  const lag = state?.groups?.find?.(g => g.groupId === 'carrier-tracking')?.totalLag ?? 0;
  if (lag) return [`catching up · lag ${lag}`, 'warn'];
  return wrong ? [`${wrong} wrong status`, 'bad'] : regressions ? [`${regressions} regressions`, 'warn'] : ['in order', 'good'];
}
function renderOrderingLab() {
  const t = labs?.tracking;
  if (!ok(t)) return empty(`shipping-service is unreachable: ${t?.error || ''}`);
  const group = state?.groups?.find?.(g => g.groupId === 'carrier-tracking');
  const partitions = t.partitions;
  const old = [...Array(partitions).keys()].join(',');
  const slowOn = p => labAct('fault-arm', { service: 'shipping-service', name: 'carrier-slow', mode: `${p}:700` }, `Slowing partition ${p} (700 ms per scan)…`);
  const wrongKey = () => scenarioSteps([
    ['tracking-guard', { on: false }, 'Turning the sequence guard off…'], ['fault-arm', { service: 'shipping-service', name: 'carrier-slow', mode: '0:700' }, 'Slowing partition 0…'],
    ['carrier-scans', { keying: 'scan', parcels: 4 }, 'The carrier scans 4 parcels, keying each scan by its own id…']], 'Scans of one parcel went to different partitions. Partition 0 is slow, so later scans overtake earlier ones: watch the parcels’ statuses go backwards.');
  const repartition = () => scenarioSteps([
    ['tracking-guard', { on: false }, 'Turning the sequence guard off…'], ['consumer-pause', { service: 'shipping-service', consumer: 'carrier-tracking' }, 'Pausing the tracking consumer: scans stay in flight…'],
    ['carrier-scans', { keying: 'tracking', parcels: 6, fromSeq: 1, toSeq: 2 }, 'Scans 1–2, correctly keyed by tracking number…'],
    ['carrier-partitions', { count: partitions + 3 }, `Adding partitions: ${partitions} → ${partitions + 3}…`],
    ['carrier-scans', { keying: 'tracking', parcels: 6, fromSeq: 3, toSeq: 4 }, 'Scans 3–4, same keys, more partitions…'],
    ['fault-arm', { service: 'shipping-service', name: 'carrier-slow', mode: `${old}:600` }, 'The old partitions are behind (600 ms per scan)…'],
    ['consumer-resume', { service: 'shipping-service', consumer: 'carrier-tracking' }, 'Resuming the consumer…']], 'Same key, different partition: parcels whose key now hashes to a new partition get scans 3–4 before 1–2.');
  const hotKey = () => scenarioSteps([['carrier-scans', { keying: 'hub', parcels: 6 }, 'Every scan keyed by its hub…']], 'All scans on one partition: order is safe, but one consumer does all the work while the others idle.');
  const byPartition = new Map();
  for (const sc of t.scans) byPartition.set(sc.partition, (byPartition.get(sc.partition) || 0) + 1);
  const most = Math.max(1, ...byPartition.values());
  return h('div', { class: 'lab-grid' },
    steps([
      ['Learn', 'Kafka keeps order only within a partition, and a key always maps to the same partition only while the partition count stays the same. A parcel’s scans must share a key to arrive in order.'],
      ['Trigger', 'The carrier keys each scan by its own id (a real bug class), and partition 0 is slow.', [labButton('Scans keyed by scan id', wrongKey, 'danger-soft')]],
      ['Observe', 'Each scan’s partition and offset, the order it was handled in, and what it did to the parcel’s status.'],
      ['Break', 'Correct keys still break when partitions are added in flight: the key hashes somewhere new. Or pick one hot key: ordered, but no parallelism.', [labButton(`Add partitions in flight (${partitions} → ${partitions + 3})`, repartition, 'danger-soft'), labButton('One hot key', hotKey)]],
      ['Understand', 'Two consumer threads worked two partitions; nothing in Kafka orders records across partitions. Last-write-wins then lets an older scan overwrite a newer status.'],
      ['Fix', 'Key by tracking number, and make the projection refuse scans older than the one it applied: correct whatever order they arrive in.',
        [labButton('Scans keyed by tracking number', () => slowOn(0).then(() => labAct('carrier-scans', { keying: 'tracking', parcels: 4 }, 'Keying by tracking number…')).catch(() => {})), labButton(t.guard ? 'Turn sequence guard off' : 'Turn sequence guard on', () => labAct('tracking-guard', { on: !t.guard }))]],
      ['Recover', 'Replay the topic into an empty projection (with the guard on it comes out right), then recreate the topic with 3 partitions: partitions can never be removed.',
        [labButton('Replay tracking', () => labAct('tracking-replay', {}, 'Rewinding carrier-tracking to offset 0…')), labButton('Clear slow partition', () => labAct('fault-clear', { service: 'shipping-service', name: 'carrier-slow' })), labButton('Recreate topic (3 partitions)', () => confirm('Delete shipping.carrier-scans and recreate it with 3 partitions? Its scans and the tracking projection are lost.') && labAct('carrier-reset', {}, 'Recreating the topic…'), 'danger-soft')]]
    ]),
    h('div', { class: 'lab-evidence' },
      evidence('Now', h('div', { class: 'chips' }, chip(`${partitions} partitions`), chip(t.guard ? 'sequence guard on' : 'sequence guard off', t.guard ? 'good' : 'warn'), t.slow ? chip(`slow partition ${t.slow.replace(':', ' · ')} ms`, 'warn') : chip('no slow partition'),
        group ? chip(`carrier-tracking lag ${number(group.totalLag)}`, group.totalLag ? 'warn' : 'good') : null)),
      evidence('Parcels (tracking projection)', t.parcels.length ? h('table', { class: 'data-table' }, h('thead', {}, h('tr', {}, ...['Parcel', 'Status', 'Last scan', 'Regressions', 'Refused'].map(x => h('th', {}, x)))),
        h('tbody', {}, ...t.parcels.map(p => { const delivered = t.scans.some(sc => sc.trackingNumber === p.trackingNumber && sc.seq === 4);
          return h('tr', {}, h('td', { class: 'mono' }, p.trackingNumber), h('td', {}, chip(p.status.replaceAll('_', ' ').toLowerCase(), p.status === 'DELIVERED' ? 'good' : delivered ? 'bad' : 'info'), delivered && p.status !== 'DELIVERED' ? h('small', { class: 'muted' }, ' but it was delivered') : null),
            h('td', {}, p.lastSeq), h('td', {}, p.regressions ? chip(p.regressions, 'bad') : '0'), h('td', {}, p.staleSkipped ? chip(p.staleSkipped, 'good') : '0')); })))
        : empty('No parcels tracked yet. Shipped orders are the parcels; run the trigger.')),
      evidence('Scans per partition', h('div', { class: 'partition-bars' }, ...[...Array(partitions).keys()].map(p => h('div', { class: 'pbar' }, h('span', {}, `p${p}`), h('i', { style: `width:${(byPartition.get(p) || 0) / most * 100}%` }), h('b', {}, byPartition.get(p) || 0))))),
      evidence('Scans in the order they were handled', t.scans.length ? h('div', { class: 'feed scan-feed' }, ...t.scans.slice(0, 24).map(sc => h('div', { class: 'feed-row' }, h('time', {}, seconds(sc.at)),
        chip(sc.outcome.replace('_', ' ').toLowerCase(), sc.outcome === 'APPLIED' ? 'good' : sc.outcome === 'REGRESSED' ? 'bad' : 's-compensated'),
        h('p', {}, `${sc.trackingNumber} · scan ${sc.seq} ${sc.status.replaceAll('_', ' ').toLowerCase()}`, h('small', {}, `p${sc.partition}@${sc.offset} · key ${String(sc.recordKey).slice(0, 16)}`))))) : empty('No scans handled yet.'))));
}

// Lab 3: write, then read at once: stale, refused, or waited for.
function projectionState() {
  const svc = state?.services?.['order-query-service'];
  const consumer = ok(svc) ? svc.consumers.find(c => c.groupId === 'order-projection') : null;
  return { paused: !!consumer?.pauseRequested, consumer: consumer?.id, lag: state?.groups?.find?.(g => g.groupId === 'order-projection')?.totalLag ?? null };
}
function rywStatus() {
  const run = labs?.['read-your-writes']?.[0];
  if (!run) return ['not run', ''];
  const status = run.result.read.status;
  return status === 404 ? ['stale read', 'bad'] : status === 409 ? ['refused stale', 'warn'] : ['read your write', 'good'];
}
function renderRywLab() {
  const runs = labs?.['read-your-writes'] || [];
  const p = projectionState();
  const read = mode => labAct('exp-ryw', { mode, order: labOrder() }, mode === 'token' ? 'Writing, then reading with the version as a consistency token…' : mode === 'write-model' ? 'Writing, then reading from the write model…' : 'Writing, then reading the read model at once…')
    .then(res => showMessage(res.verdict), () => {});
  const pause = p.consumer ? h('button', { type: 'button', class: 'small-button danger-soft', disabled: busy || p.paused,
    onclick: () => labAct('consumer-pause', { service: 'order-query-service', consumer: p.consumer }, 'Pausing the projection: the read model stops following order.events…') }, 'Pause projection') : null;
  const resume = p.consumer ? h('button', { type: 'button', class: 'small-button', disabled: busy || !p.paused,
    onclick: () => labAct('consumer-resume', { service: 'order-query-service', consumer: p.consumer }, 'Resuming the projection: its backlog drains…') }, 'Resume projection') : null;
  return h('div', { class: 'lab-grid' },
    steps([
      ['Learn', 'CQRS reads come from a projection that follows the writes through the outbox, Debezium and Kafka. Right after a write, the read side may not have it yet.'],
      ['Trigger', 'Place an order and immediately read it back from the read model, as a UI would after “Order placed”.', [labButton('Write, then read at once', () => read('naive'))]],
      ['Observe', 'The write’s version (the order’s event count), how long the read took, and what it returned.'],
      ['Break', 'Pause the projection: now the read model cannot catch up, and every fresh order is “not found” to the user who just placed it.', [pause]],
      ['Understand', 'The read model is eventually consistent. Without knowing which write to wait for, the read side can only answer with what it has, even when that is nothing.'],
      ['Fix', 'Send the write’s version back as a consistency token: the read side waits until it has applied that version, or says honestly that it has not (409), never serving stale data as current. Or read from the write model.',
        [labButton('Read with the consistency token', () => read('token')), labButton('Read from the write model', () => read('write-model'))]],
      ['Recover', 'Resume the projection: its backlog drains, and token reads succeed after a short wait.', [resume]]
    ]),
    h('div', { class: 'lab-evidence' },
      evidence('Now', h('div', { class: 'chips' }, chip(p.paused ? 'projection paused' : 'projection running', p.paused ? 'bad' : 'good'), p.lag !== null ? chip(`order-projection lag ${number(p.lag)}`, p.lag ? 'warn' : 'good') : null)),
      evidence('Write, then read', runs.length ? h('table', { class: 'data-table' }, h('thead', {}, h('tr', {}, ...['At', 'Read', 'Write', 'Read result', ''].map(x => h('th', {}, x)))),
        h('tbody', {}, ...runs.map(run => { const r = run.result;
          return h('tr', {}, h('td', { class: 'mono' }, seconds(run.at)), h('td', {}, ({ naive: 'read model', token: 'token', 'write-model': 'write model' })[r.mode] || r.mode), h('td', {}, `v${r.write.version} · ${r.write.ms} ms`),
            h('td', {}, chip(`${r.read.status}`, r.read.status === 200 ? 'good' : r.read.status === 409 ? 'warn' : 'bad'), ` ${r.read.ms} ms${r.read.waitedMs ? ` (waited ${r.read.waitedMs})` : ''}`), h('td', {}, h('small', {}, r.verdict))); })))
        : empty('No runs yet. Start with the trigger.'))));
}

// ---- Following an order -------------------------------------------------------------------------------
function follow(orderId) {
  selected = orderId || null;
  history.replaceState(null, '', selected ? `?order=${selected}` : location.pathname);
  journey = null;
  fold = null;
  focus = null;
  seen = new Set();
  lastRender.clear();
  renderAll();
  if (selected) refreshJourney().then(() => {
    // Nodes present when an order is opened are history, not arrivals: only later ones animate in.
    if (journey) buildGraph(journey).nodes.forEach(n => seen.add(n.id));
    lastRender.delete('graph');
    renderAll();
    el('graph').closest('.workbench').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  });
}
async function refreshJourney() {
  if (!selected) return;
  const id = selected;
  try {
    const response = await fetch(`/api/events/orders/${id}`);
    const next = await response.json();
    if (id === selected) journey = next;
  } catch { /* the next poll retries */ }
}

// ---- Loop ---------------------------------------------------------------------------------------------
function renderAll() {
  if (!state) return;
  renderRail();
  renderScenarioControls();
  renderOrderHead();
  renderGraph();
  renderInspector();
  renderLabs();
  renderTabs();
}
async function refresh() {
  try {
    const response = await fetch('/api/events/state');
    if (!response.ok) throw new Error();
    state = await response.json();
    el('connection').replaceChildren(h('i'), ' Connected');
    el('connection').classList.remove('disconnected');
    if (!draft.items.length && Array.isArray(state.catalog)) regenerate();
    else renderComposer();
    await Promise.all([refreshJourney(), fetch('/api/events/labs').then(r => r.ok ? r.json() : null).then(v => { labs = v; }).catch(() => {})]);
    renderAll();
  } catch {
    el('connection').replaceChildren(h('i'), ' Disconnected');
    el('connection').classList.add('disconnected');
  }
}

el('scenario').replaceChildren(...SCENARIOS.map(s => h('option', { value: s.id }, s.label)));
el('scenario').addEventListener('change', () => { lastRender.clear(); renderScenarioControls(); });
el('randomize').replaceChildren(icon('dice'), 'Randomize');
el('randomize').addEventListener('click', regenerate);
el('add-line').addEventListener('click', () => {
  const used = new Set(draft.items.map(i => i.sku));
  const next = (state?.catalog || []).find(p => !used.has(p.sku));
  if (next) { draft.items.push({ sku: next.sku, quantity: 1 }); draftGenerated = false; renderComposer(true); }
});
el('customer').addEventListener('input', e => { draft.customerId = e.target.value; draftGenerated = false; syncJson(); });
el('json').addEventListener('input', () => {
  try { const parsed = readJson(); draft = parsed; draftGenerated = false; el('json').classList.remove('invalid'); el('json-error').textContent = ''; el('customer').value = draft.customerId; }
  catch (e) { el('json').classList.add('invalid'); el('json-error').textContent = e.message; }
});
el('json').addEventListener('blur', () => renderComposer(true));
el('composer').addEventListener('submit', event => { event.preventDefault(); send(); });
el('order-select').addEventListener('change', e => follow(e.target.value));
if (grafana) { const link = el('grafana-link'); link.href = grafana; link.hidden = false; }
renderScenarioControls();

async function poll() { await refresh(); setTimeout(poll, 1000); }
const linked = new URLSearchParams(location.search).get('order');
if (linked) follow(linked);
poll();
