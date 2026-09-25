// Event-driven lab control plane. Renders only what the backend reports; every button calls a real
// lever (a service admin API, Kafka Connect or a Kafka topic) through /api/events/actions.
const el = id => document.getElementById(id);
const number = value => new Intl.NumberFormat().format(value);
const clock = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3, hour12: false });
const time = value => value ? clock.format(new Date(value)) : '—';
const duration = ms => ms >= 86_400_000 ? `${Math.round(ms / 86_400_000)} d` : ms >= 3_600_000 ? `${Math.round(ms / 3_600_000)} h` : ms >= 60_000 ? `${Math.round(ms / 60_000)} min` : `${Math.round(ms / 1000)} s`;
const short = id => id ? String(id).slice(0, 8) : '—';
const SERVICE_FAULTS = {
  'order-service': [['scanner-stall', '8000', 'Stall timeout scanner 8 s (fencing)']],
  'payment-service': [['payment-decline', 'decline', 'Decline next payment']],
  'inventory-service': [['inventory-reject', 'reject', 'Reject next reservation'], ['inventory-slow', '700', 'Slow next 2 reservations (race)', 2]],
  'shipping-service': [['shipping-fail', 'fail', 'Fail next shipment']]
};
let state = null;
const grafana = document.body.dataset.grafana;
// Grafana Explore for one query; datasource uids are fixed by infra/observability provisioning.
const explore = (uid, query) => `${grafana}/explore?schemaVersion=1&panes=${encodeURIComponent(JSON.stringify({ z: {
  datasource: uid, range: { from: 'now-6h', to: 'now' },
  queries: [{ refId: 'A', datasource: { type: uid, uid }, ...(uid === 'tempo' ? { queryType: 'traceql', query } : { expr: query }) }] } }))}`;
let selected = null;
let journey = null;
let fold = null;
let busy = false;
const expanded = new Set();
const openGroups = new Set();
const quantities = new Map([['SKU-MOUSE', 1], ['SKU-CABLE', 1]]);

/** Safe element builder: strings become text nodes, never HTML. */
function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === 'class') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat()) if (child !== null && child !== undefined && child !== false) node.append(child instanceof Node ? child : String(child));
  return node;
}
const chip = (text, tone = '') => h('span', { class: `chip ${tone}` }, text);
const button = (label, onclick, attrs = {}) => h('button', { type: 'button', class: `small-button ${attrs.class || ''}`, onclick, disabled: busy || attrs.disabled, title: attrs.title }, label);
const empty = text => h('p', { class: 'empty' }, text);
const ok = node => node && !node.error;
const decisionTone = d => ({ PROCESSED: 'good', DUPLICATE_SKIPPED: 'info', IGNORED: '', RETRY_SCHEDULED: 'warn', DEAD_LETTERED: 'bad' })[d] || '';
const sagaTone = s => ({ COMPLETED: 'good', CANCELLED: 'bad', COMPENSATING: 'warn' })[s] || 'info';

async function act(action, body = {}, pending) {
  if (busy) return;
  busy = true;
  showMessage(pending || `${action}…`);
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
    refresh();
  }
}

function showMessage(text, error = false) {
  const message = el('message');
  message.textContent = text;
  message.classList.toggle('error', error);
}

// ---- Pipeline ----------------------------------------------------------------------------------
function renderPipeline(s) {
  const services = s.services || {};
  const outboxes = Object.values(services).filter(ok).map(v => v.outbox).filter(o => o && o.slot);
  const connectors = ok(s.connectors) ? Object.values(s.connectors) : null;
  const running = connectors ? connectors.filter(c => c.status.connector.state === 'RUNNING' && c.status.tasks.every(t => t.state === 'RUNNING')).length : 0;
  const topics = Array.isArray(s.topics) ? s.topics : null;
  const records = topics ? topics.filter(t => !t.name.endsWith('.dlt')).flatMap(t => t.partitions).reduce((sum, p) => sum + p.latest, 0) : 0;
  const groups = Array.isArray(s.groups) ? s.groups.filter(g => g.groupId !== 'control-plane-tap') : null;
  const lag = groups ? groups.reduce((sum, g) => sum + g.totalLag, 0) : 0;
  const orders = Array.isArray(s.orders) ? s.orders : [];
  const inFlight = orders.filter(o => !['COMPLETED', 'CANCELLED'].includes(o.saga.state)).length;
  const projection = ok(s.projection) ? s.projection : null;
  const projectionLag = groups?.find(g => g.groupId === 'order-projection')?.totalLag ?? 0;
  const cdcLag = outboxes.reduce((sum, o) => sum + (o.lagBytes || 0), 0);
  const unpublished = Object.values(s.unpublished || {}).reduce((sum, u) => sum + u.count, 0);
  const allUp = Object.keys(services).every(n => ok(services[n]));
  const j = journey && !journey.order?.error ? journey : null;
  const messages = j?.messages || [];
  const stages = [
    ['API', 'order-service', ok(services['order-service']) ? 'POST /orders' : 'unreachable', ok(services['order-service']) ? number(orders.length) : 'down', ok(services['order-service']) ? 'ok' : 'down', !!j],
    ['DB transaction', 'Event store + saga + outbox', 'one commit per step', number(outboxes.reduce((sum, o) => sum + o.rows, 0)), outboxes.length ? 'ok' : 'down', messages.some(m => m.outboxAt)],
    ['Outbox → WAL', 'Replication slots', `${outboxes.every(o => o.slotActive) ? 'all slots streaming' : `${outboxes.filter(o => !o.slotActive).length} slot(s) idle`} · ${number(cdcLag)} B WAL unconfirmed`, `${number(unpublished)} unpublished`, !outboxes.length ? 'down' : outboxes.every(o => o.slotActive) && !unpublished ? 'ok' : 'degraded', messages.some(m => m.outboxAt)],
    ['Debezium', 'Kafka Connect', connectors ? `${running}/${connectors.length} connectors running` : 'unreachable', connectors ? `${running}/${connectors.length}` : 'down', !connectors ? 'down' : running === connectors.length ? 'ok' : 'degraded', messages.some(m => m.kafka?.length)],
    ['Kafka', 'Topics & partitions', topics ? `${topics.length} topics · ${number(topics.reduce((n, t) => n + t.partitions.length, 0))} partitions` : 'unreachable', topics ? number(records) : 'down', topics ? 'ok' : 'down', messages.some(m => m.kafka?.length)],
    ['Consumers', 'Consumer groups', groups ? `${groups.filter(g => g.state === 'STABLE').length}/${groups.length} groups stable` : 'unreachable', groups ? `lag ${number(lag)}` : 'down', !groups || !allUp ? 'down' : lag > 0 || groups.some(g => g.state !== 'STABLE') ? 'degraded' : 'ok', messages.some(m => m.deliveries?.length)],
    ['Saga', 'Orchestrator', `${orders.filter(o => o.saga.state === 'COMPLETED').length} completed · ${orders.filter(o => o.saga.state === 'CANCELLED').length} cancelled`, `${inFlight} in flight`, inFlight ? 'degraded' : 'ok', !!j?.order?.saga],
    ['Read model', 'order-query-service', projection ? `projection lag ${number(projectionLag)}` : 'unreachable', projection ? number(projection.orders) : 'down', !projection ? 'down' : projectionLag ? 'degraded' : 'ok', ok(j?.readModel)]
  ];
  el('pipeline').replaceChildren(...stages.map(([title, what, detail, metric, tone, reached]) =>
    h('li', { class: `${tone} ${selected && reached ? 'reached' : ''}` }, h('small', {}, title), h('strong', {}, what), h('em', {}, metric), h('span', {}, detail))));
  el('pipeline-note').textContent = selected
    ? `Blue marks the stages order ${short(selected)} has reached. Every figure is read from the running system.`
    : 'Every stage below is read from the running system: databases, Debezium, Kafka and the services themselves.';
}

// ---- Order form & scenarios --------------------------------------------------------------------
function renderOrderForm(s) {
  if (!Array.isArray(s.catalog)) { el('order-lines').replaceChildren(empty('order-service is unreachable.')); return; }
  const stock = new Map((Array.isArray(s.stock) ? s.stock : []).map(i => [i.sku, i]));
  const focused = document.activeElement?.dataset?.sku;
  el('order-lines').replaceChildren(...s.catalog.map(product => {
    const input = h('input', { type: 'number', min: 0, max: 99, value: quantities.get(product.sku) || 0, 'data-sku': product.sku, 'aria-label': `${product.name} quantity`,
      oninput: e => quantities.set(product.sku, Math.max(0, Number(e.target.value) || 0)) });
    const item = stock.get(product.sku);
    return h('label', { class: 'order-line' }, h('span', {}, `${product.name} · $${product.price}`, h('small', {}, item ? `${item.available} available · ${item.reserved} reserved (inventory-service)` : 'stock unknown')), input);
  }));
  if (focused) el('order-lines').querySelector(`[data-sku="${focused}"]`)?.focus();
}

function currentOrder() {
  return { customerId: el('customer').value.trim() || 'alice', items: [...quantities].filter(([, q]) => q > 0).map(([sku, quantity]) => ({ sku, quantity })) };
}

async function place(body = currentOrder()) {
  const result = await act('place-order', { order: body }, 'Placing order…');
  select(result.orderId);
  showMessage(`Order ${short(result.orderId)} accepted: one transaction wrote OrderPlaced, the saga and AuthorizePayment.`);
}

async function scenario(setup, note) {
  try {
    await setup();
    await place();
    showMessage(note);
  } catch { /* message already shown */ }
}

function renderScenarios() {
  const arm = (service, name, mode, times = 1) => act('fault-arm', { service, name, mode, times }, `Arming ${name}…`);
  const list = [
    ['Happy path', () => Promise.resolve(), 'Watch it reach COMPLETED.', ''],
    ['Payment declines', () => arm('payment-service', 'payment-decline', 'decline'), 'Declined: cancelled, nothing to compensate.', ''],
    ['Inventory rejects', () => arm('inventory-service', 'inventory-reject', 'reject'), 'Rejected after payment: the saga refunds it.', ''],
    ['Shipping fails', () => arm('shipping-service', 'shipping-fail', 'fail'), 'Failed at the pivot: stock released and payment refunded.', ''],
    ['Consumer error ×3', () => arm('payment-service', 'transient-error', 'error', 3), 'Three failures, retried with backoff, then success.', ''],
    ['Consumer error ×6 → DLQ', () => arm('payment-service', 'transient-error', 'error', 6), 'Retries exhausted: dead-lettered; the saga times out and compensates.', 'danger'],
    ['Crash after commit', () => arm('payment-service', 'crash-after-commit', 'crash'), 'payment-service commits, dies before the offset commit, restarts, and skips the redelivery as a duplicate.', 'danger'],
    ['Gateway down', () => act('gateway', { mode: 'down' }), 'Gateway 503s: in-process retries, breaker opens, Kafka retries, DLQ, saga timeout.', 'danger']
  ];
  const race = async () => {
    try {
      await act('fault-arm', { service: 'inventory-service', name: 'inventory-slow', mode: '700', times: 2 }, 'Widening the race window…');
      const order = { customerId: 'race', items: [{ sku: 'SKU-KEYBOARD', quantity: 1 }] };
      await Promise.all([fetch('/api/events/actions/place-order', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ order }) }),
        fetch('/api/events/actions/place-order', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ order: { ...order, customerId: 'race-2' } }) })]);
      showMessage('Two orders reserve SKU-KEYBOARD at once. If they land on different partitions, both read version n; one write wins and the other retries (StockChanged in the decisions feed). On the same partition they run one after the other: no conflict.');
    } catch { /* shown */ } finally { refresh(); }
  };
  el('scenarios').replaceChildren(...list.map(([label, setup, note, tone]) => h('button', { type: 'button', class: tone, disabled: busy, onclick: () => scenario(setup, note) }, label)),
    h('button', { type: 'button', disabled: busy, onclick: race, title: 'Optimistic locking on the stock row' }, 'Concurrent reservations'),
    h('button', { type: 'button', class: 'danger', disabled: busy, title: 'Commits the order, then kills order-service before it publishes to Kafka',
      onclick: () => act('dual-write', { mode: 'COMMIT_THEN_CRASH', order: currentOrder() }, 'Dual write: committing, then crashing…').then(() => {}, () => showMessage('order-service died between the database commit and the Kafka publish: the order exists, its event never will. The outbox makes this impossible.')) }, 'Dual write: lost event'),
    h('button', { type: 'button', class: 'danger', disabled: busy, title: 'Publishes to Kafka, then rolls the database back',
      onclick: () => act('dual-write', { mode: 'PUBLISH_THEN_ROLLBACK', order: currentOrder() }, 'Dual write: publishing, then rolling back…').then(r => { select(r.orderId); showMessage(r.outcome); }, () => {}) }, 'Dual write: ghost event'));
}

// ---- Orders ------------------------------------------------------------------------------------
function renderOrders(s) {
  if (!Array.isArray(s.orders)) { el('orders').replaceChildren(h('tr', {}, h('td', { colspan: 6 }, empty('order-service is unreachable.')))); return; }
  const read = new Map((Array.isArray(s.readModel) ? s.readModel : []).map(r => [r.order_id, r]));
  if (!s.orders.length) { el('orders').replaceChildren(h('tr', {}, h('td', { colspan: 6 }, empty('No orders yet. Place one.')))); return; }
  el('orders').replaceChildren(...s.orders.map(({ saga, order }) => {
    const projected = read.get(saga.orderId);
    const inSync = projected && projected.status === order.status;
    return h('tr', { class: `selectable ${selected === saga.orderId ? 'selected' : ''}`, onclick: () => select(saga.orderId) },
      h('td', { class: 'mono' }, short(saga.orderId)), h('td', {}, order.customerId), h('td', { class: 'num' }, `$${order.total}`),
      h('td', {}, chip(saga.state, sagaTone(saga.state))), h('td', {}, order.status),
      h('td', {}, projected ? chip(inSync ? `${projected.status} · in sync` : `${projected.status} · behind`, inSync ? 'good' : 'warn') : chip('not projected yet', 'warn')));
  }));
}

function select(orderId) {
  if (!orderId) return;
  selected = orderId;
  history.replaceState(null, '', `?order=${orderId}`);
  fold = null;
  expanded.clear();
  el('journey-panel').hidden = false;
  refreshJourney().then(() => el('journey-panel').scrollIntoView({ behavior: 'smooth', block: 'start' }));
}

// ---- Journey -----------------------------------------------------------------------------------
async function refreshJourney() {
  if (!selected) return;
  try {
    const response = await fetch(`/api/events/orders/${selected}`);
    journey = await response.json();
    renderJourney();
  } catch { /* next poll retries */ }
}

function renderJourney() {
  if (!journey) return;
  const o = journey.order;
  const read = journey.readModel;
  el('journey-title').textContent = `Order ${short(selected)}`;
  const writeOk = ok(o);
  const summary = [
    ['Write model (event-sourced)', writeOk ? o.order.status : 'not in the event store', writeOk ? `v${o.order.version} · rebuilt from ${o.rebuiltFrom}` : 'No events: a lost or ghost order'],
    ['Read model (CQRS)', ok(read) ? read.status : 'not projected', ok(read) ? `${read.events_applied} events · last ${read.last_event_type} at ${read.last_offset}` : 'Waiting for order.events, or never published'],
    ['Consistency', writeOk && ok(read) ? (read.status === o.order.status ? 'in sync' : 'eventually…') : '—', 'Read side follows via Kafka; lag is real'],
    ['Saga', o?.saga ? o.saga.state : '—', o?.saga ? (o.saga.failureReason || (o.saga.compensations.length ? `undone: ${o.saga.compensations.join(', ')}` : o.saga.deadline ? `step deadline ${time(o.saga.deadline)}` : 'no deadline')) : '—'],
    ['Customer · total', writeOk ? o.order.customerId : '—', writeOk ? `$${o.order.total} ${o.order.currency}` : '—'],
    ['Correlation id', short(journey.messages?.[0]?.correlationId), 'Shared by every message below']
  ];
  const traceId = journey.messages?.find(m => m.traceparent)?.traceparent.split('-')[1];
  el('journey-summary').replaceChildren(...summary.map(([label, value, note]) => h('div', {}, h('span', { class: 'label' }, label), h('strong', {}, value), h('span', {}, note))),
    h('div', {}, h('span', { class: 'label' }, 'Trace'), h('strong', { class: 'mono' }, short(traceId)),
      traceId && grafana ? h('span', {}, h('a', { href: explore('tempo', traceId), target: '_blank', rel: 'noopener' }, 'Trace in Tempo'), ' · ', h('a', { href: explore('loki', `{service_name=~".+"} | trace_id="${traceId}"`), target: '_blank', rel: 'noopener' }, 'Logs in Loki'))
        : h('span', {}, traceId ? 'One trace across every service and Kafka hop' : 'No traceparent: the agent was not attached')));
  const rows = [];
  for (const m of journey.messages || []) {
    const kind = m.topic?.endsWith('.commands') ? 'command' : m.type?.startsWith('Order') ? 'event' : 'reply';
    const outbox = m.outboxAt
      ? h('div', { class: 'stage-cell' }, h('b', {}, time(m.outboxAt)), h('span', {}, `${m.producer} outbox`), m.eventStorePosition ? h('span', {}, `event store #${m.eventStorePosition} · v${m.eventStoreVersion}`) : null)
      : h('div', { class: 'stage-cell' }, h('span', { class: 'missing' }, 'In no outbox'), h('span', {}, 'Published outside a transaction'));
    const kafka = m.kafka?.length
      ? h('div', { class: 'stage-cell' }, ...m.kafka.map(k => h('div', {}, h('b', {}, `${k.topic} p${k.partition} @${k.offset}`), h('div', { class: 'muted' }, m.outboxAt ? `WAL read +${Math.max(0, new Date(k.timestamp) - new Date(m.outboxAt))} ms (Debezium timestamp)` : `Debezium timestamp ${time(k.timestamp)}`),
        h('div', { class: 'muted' }, `seen on Kafka ${time(k.tappedAt)}${m.outboxAt ? ` · +${Math.max(0, new Date(k.tappedAt) - new Date(m.outboxAt))} ms after commit` : ''}`))))
      : h('div', { class: 'stage-cell' }, h('span', { class: 'missing' }, 'Not on Kafka yet'), h('span', {}, 'Connector paused, or WAL not read yet'));
    const deliveries = m.deliveries?.length
      ? h('div', {}, ...m.deliveries.sort((a, b) => a.at.localeCompare(b.at)).map(d => h('div', { class: 'delivery' }, h('div', { class: 'chips' }, chip(d.instance && (d.instance !== d.service || state?.services?.[`${d.service}-b`]) ? `${d.consumer} @ ${d.instance}` : d.consumer), chip(d.decision, decisionTone(d.decision)), d.attempt > 1 ? chip(`attempt ${d.attempt}`, 'warn') : null), h('span', {}, `${time(d.at)} · ${d.detail}`))))
      : m.kafka?.length ? h('span', { class: 'missing' }, 'Not consumed yet') : h('span', { class: 'muted' }, '—');
    const saga = m.sagaTransition ? h('div', { class: 'stage-cell' }, h('b', {}, `${m.sagaTransition.from_state || '∅'} → ${m.sagaTransition.to_state}`), h('span', {}, m.sagaTransition.detail)) : h('span', { class: 'muted' }, '—');
    const first = m.kafka?.[0];
    rows.push(h('tr', {},
      h('td', {}, h('div', { class: 'msg-type' }, h('strong', {}, m.type || 'unknown'), h('small', {}, `${kind} · ${m.topic || first?.topic || '?'} · v${m.schemaVersion || '?'}`), h('small', { class: 'mono' }, `event ${short(m.eventId)}${m.causationId ? ` ← ${short(m.causationId)}` : ''}`))),
      h('td', {}, outbox), h('td', {}, kafka), h('td', {}, deliveries), h('td', {}, saga),
      h('td', {}, h('div', { class: 'chips' },
        button(expanded.has(m.eventId) ? 'Hide' : 'Payload', () => { expanded.has(m.eventId) ? expanded.delete(m.eventId) : expanded.add(m.eventId); renderJourney(); }),
        first ? button('Duplicate', () => act('duplicate', { topic: first.topic, partition: first.partition, offset: first.offset }, 'Publishing the same record again…').then(() => showMessage('Same event id, new offset: watch the consumer record DUPLICATE_SKIPPED.'), () => {}), { title: 'Publish this exact record again (same key, value, headers)' }) : null))));
    if (expanded.has(m.eventId))
      rows.push(h('tr', { class: 'payload-row' }, h('td', { colspan: 6 }, h('pre', { class: 'payload' }, JSON.stringify({ envelope: m.envelope, headers: first?.headers, traceparent: m.traceparent }, null, 2)))));
  }
  el('journey-messages').replaceChildren(...(rows.length ? rows : [h('tr', {}, h('td', { colspan: 6 }, empty('No messages found for this order.')))]));

  const extra = [];
  if (fold) extra.push(h('div', {}, h('h3', { class: 'sub' }, 'Aggregate rebuilt event by event'), h('ol', { class: 'fold' }, ...fold.map(step => h('li', {}, h('b', {}, `v${step.version}`), h('span', {}, step.type), chip(step.stateAfter.status, 'info'))))));
  if (o?.transitions?.length) extra.push(h('div', {}, h('h3', { class: 'sub' }, 'Saga transitions (durable)'), h('ol', { class: 'fold' }, ...o.transitions.map((t, i) => h('li', {}, h('b', {}, i + 1), h('span', {}, `${t.trigger_type}: ${t.detail}`), chip(t.to_state, sagaTone(t.to_state)))))));
  const calls = Array.isArray(journey.gatewayCalls) ? journey.gatewayCalls : [];
  if (calls.length) extra.push(h('div', {}, h('h3', { class: 'sub' }, 'Payment gateway attempts'), h('ol', { class: 'fold' }, ...calls.slice().reverse().map((c, i) => h('li', {}, h('b', {}, i + 1), h('span', {}, `${time(c.at)} · ${c.latency_ms} ms · breaker ${c.breaker_state} · ${c.detail}`), chip(c.outcome, c.outcome === 'CHARGED' ? 'good' : c.outcome === 'DECLINED' ? 'warn' : 'bad'))))));
  if (journey.unmatched?.length) extra.push(h('div', {}, h('h3', { class: 'sub' }, 'Records without a readable envelope'), ...journey.unmatched.map(u => h('p', { class: 'mono' }, JSON.stringify(u)))));
  el('journey-extra').replaceChildren(...extra);
  el('journey-snapshot').disabled = busy || !writeOk || !o.snapshotVersion;
  el('journey-fold').disabled = busy || !writeOk;
}

// ---- Kafka -------------------------------------------------------------------------------------
function renderTopics(topics) {
  if (!Array.isArray(topics)) { el('topics').replaceChildren(empty(`Kafka unreachable: ${topics?.error || ''}`)); return; }
  el('topics').replaceChildren(...topics.map(t => {
    const max = Math.max(1, ...t.partitions.map(p => p.latest));
    const retention = t.retentionMs === '-1' ? 'kept forever' : `retention ${duration(Number(t.retentionMs))}`;
    return h('div', { class: 'topic' }, h('div', {}, h('strong', {}, t.name), h('small', {}, `${t.partitions.length} partition${t.partitions.length > 1 ? 's' : ''} · ${retention}`)),
      h('div', { class: 'partitions' }, ...t.partitions.map(p => h('div', { class: 'partition' }, h('span', {}, `p${p.partition}`),
        h('span', { class: 'bar', title: `offsets ${p.earliest}–${p.latest}` }, h('i', { style: `left:${p.earliest / max * 100}%;right:${100 - p.latest / max * 100}%` })),
        h('span', {}, `${number(p.earliest)} → ${number(p.latest)}`)))));
  }));
}

function renderGroups(groups, services) {
  if (!Array.isArray(groups)) { el('groups').replaceChildren(empty(`Kafka unreachable: ${groups?.error || ''}`)); return; }
  // A group may run on several replicas: levers act on every live one.
  const owners = new Map();
  for (const [name, s] of Object.entries(services || {})) if (ok(s)) for (const c of s.consumers) owners.set(c.groupId, [...(owners.get(c.groupId) || []), { service: name, consumer: c }]);
  el('groups').replaceChildren(...groups.map(g => {
    const list = owners.get(g.groupId) || [];
    const o = list[0];
    const paused = list.some(x => x.consumer.pauseRequested);
    const all = action => list.reduce((chain, x) => chain.then(() => act(action, { service: x.service, consumer: x.consumer.id })), Promise.resolve()).catch(() => {});
    return h('div', { class: 'group' },
      h('header', {}, h('div', { class: 'chips' }, h('strong', {}, g.groupId), chip(g.state, g.state === 'STABLE' ? 'good' : g.state === 'EMPTY' ? 'bad' : 'warn'), chip(`lag ${number(g.totalLag)}`, g.totalLag ? 'warn' : 'good'), paused ? chip('paused', 'warn') : null),
        o ? h('div', { class: 'chips' },
          button(paused ? 'Resume' : 'Pause', () => all(paused ? 'consumer-resume' : 'consumer-pause'), { title: `On ${list.map(x => x.service).join(' and ')}` }),
          button('Replay from 0', () => {
            const partitions = g.partitions.filter(p => p.committed !== null).map(p => ({ topic: p.topic, partition: p.partition }));
            if (!confirm(`Stop ${g.groupId} on ${list.length} replica(s), move its committed offsets back to 0 on ${partitions.length} partition(s), start again? Everything is redelivered; the inbox should skip it all as duplicates.`)) return;
            act('consumer-seek', { group: g.groupId, consumer: o.consumer.id, owners: list.map(x => x.service), partitions, offset: 0 }, 'Stopping consumers, moving offsets…')
              .then(() => showMessage(`${g.groupId} rewound: watch its lag jump, then DUPLICATE_SKIPPED decisions drain it.`), () => {});
          }, { title: 'Stop the consumer on every replica, reset its committed offsets, start again' })) : null),
      h('div', { class: 'members' }, g.members.length ? g.members.map(m => h('div', { class: 'member' }, h('span', { class: 'mono' }, m.clientId), ...(m.assignments.length > 6 ? [chip(`${m.assignments.length} partitions`)] : m.assignments.map(a => chip(a))))) : h('span', { class: 'muted' }, 'No members: nothing is consuming. Offsets stay committed in Kafka.')),
      h('details', { open: g.totalLag > 0 || openGroups.has(g.groupId) || null, ontoggle: e => e.target.open ? openGroups.add(g.groupId) : openGroups.delete(g.groupId) },
        h('summary', {}, `Committed offsets on ${g.partitions.length} partitions${g.totalLag ? ` · ${g.partitions.filter(p => p.lag).length} lagging` : ''}`),
        h('table', { class: 'lag-table' }, h('tbody', {}, ...g.partitions.map(p => h('tr', { class: p.lag ? 'lagging' : '' }, h('td', {}, `${p.topic}-${p.partition}`), h('td', {}, `committed ${p.committed ?? '—'}`), h('td', {}, `end ${p.latest}`), h('td', {}, `lag ${p.lag ?? '—'}`)))))));
  }));
}

// ---- Services, connectors, gateway ------------------------------------------------------------
function renderServices(services) {
  el('services').replaceChildren(...Object.keys(services || {}).map(name => {
    const s = services?.[name];
    const lease = ok(state?.lease) && name.startsWith('order-service') ? state.lease : null;
    if (!ok(s)) return h('div', { class: 'service down' }, h('header', {}, h('strong', {}, name), chip('down', 'bad')), h('p', { class: 'muted' }, s?.error || 'No answer. If you crashed it, Docker is restarting it.'));
    const armed = new Map(s.faults.map(f => [f.name, f]));
    const fault = (label, faultName, mode, times, tone) => armed.has(faultName)
      ? button(`Clear ${faultName}`, () => act('fault-clear', { service: name, name: faultName }), { class: 'danger' })
      : button(label, () => act('fault-arm', { service: name, name: faultName, mode, times }), { class: tone });
    const specific = (SERVICE_FAULTS[name] || []).map(([f, mode, label, times]) => fault(label, f, mode, times ?? 1));
    return h('div', { class: 'service' },
      h('header', {}, h('strong', {}, name), h('div', { class: 'chips' }, lease?.owner === name && lease.held ? chip(`scanner lease · token ${lease.token}`, 'info') : null, chip('up', 'good'))),
      h('dl', {},
        h('dt', {}, 'Consumers'), h('dd', {}, ...s.consumers.map(c => chip(`${c.id}: ${c.pauseRequested ? 'paused' : c.running ? 'running' : 'stopped'} · ${c.assignedPartitions.length} partitions`, c.pauseRequested ? 'warn' : 'good'))),
        s.outbox?.slot ? [h('dt', {}, 'Outbox'), h('dd', {}, `${number(s.outbox.rows)} rows · ${number(state?.unpublished?.[name]?.count ?? 0)} not yet on Kafka · slot ${s.outbox.slotActive ? 'active' : 'idle'}`)] : [h('dt', {}, 'Outbox'), h('dd', {}, 'none (read side)')],
        lease ? [h('dt', {}, 'Lease'), h('dd', {}, lease.held ? `saga-timeout-scanner held by ${lease.owner} (token ${lease.token}) until ${time(lease.expires_at)}` : 'saga-timeout-scanner free')] : null,
        h('dt', {}, 'Faults'), h('dd', {}, s.faults.length ? h('div', { class: 'chips' }, ...s.faults.map(f => chip(`${f.name}=${f.mode}${f.remaining !== null ? ` ×${f.remaining}` : ''}`, 'bad'))) : 'none armed')),
      h('div', { class: 'buttons' },
        fault('Error ×3', 'transient-error', 'error', 3, ''), fault('Crash after commit', 'crash-after-commit', 'crash', 1, 'danger'), ...specific,
        button('Crash now', () => confirm(`Kill ${name}'s JVM now? Docker restarts it.`) && act('crash', { service: name }), { class: 'danger' })));
  }));
}

function renderConnectors(connectors) {
  if (!ok(connectors)) { el('connectors').replaceChildren(empty(`Kafka Connect unreachable: ${connectors?.error || ''}`)); return; }
  const list = Object.entries(connectors);
  if (!list.length) { el('connectors').replaceChildren(empty('No connectors registered yet (connect-init registers them once the services are healthy).')); return; }
  el('connectors').replaceChildren(...list.map(([name, c]) => {
    const st = c.status.connector.state;
    const tasks = c.status.tasks.map(t => t.state).join(', ') || 'no tasks';
    return h('div', { class: 'connector' }, h('div', {}, h('strong', {}, name), h('div', { class: 'muted' }, `tasks: ${tasks}`)), h('div', { class: 'chips' }, chip(st, st === 'RUNNING' ? 'good' : st === 'PAUSED' ? 'warn' : 'bad'),
      button(st === 'PAUSED' ? 'Resume' : 'Pause', () => act(st === 'PAUSED' ? 'connector-resume' : 'connector-pause', { connector: name }).then(() => showMessage(st === 'PAUSED' ? 'Resumed: the WAL backlog drains to Kafka.' : 'Paused: commits still succeed; outbox rows wait in the WAL (watch the slot lag grow).'), () => {}))));
  }));
}

function renderGateway(g) {
  if (!ok(g)) { el('gateway').replaceChildren(empty(`payment-service unreachable: ${g?.error || ''}`)); return; }
  el('gateway').replaceChildren(
    h('div', { class: 'gateway-modes' }, ...['healthy', 'slow', 'down', 'declining'].map(mode => h('button', { type: 'button', class: 'small-button', 'aria-pressed': String(g.mode === mode), disabled: busy, onclick: () => act('gateway', { mode }) }, mode)), button('Reset breaker', () => act('breaker-reset'))),
    h('div', { class: 'breaker' }, h('div', { class: g.breakerState }, 'Breaker', h('strong', {}, g.breakerState)), h('div', {}, 'Failure rate', h('strong', {}, g.failureRate < 0 ? '—' : `${Math.round(g.failureRate)}%`)), h('div', {}, 'Buffered calls', h('strong', {}, g.bufferedCalls)), h('div', {}, 'Refused (open)', h('strong', {}, g.notPermittedCalls))),
    h('div', { class: 'feed' }, ...(g.recentCalls.length ? g.recentCalls.slice(0, 8).map(c => h('div', { class: 'feed-row' }, h('time', {}, time(c.at).slice(0, 8)), chip(c.outcome, c.outcome === 'CHARGED' ? 'good' : c.outcome === 'DECLINED' ? 'warn' : 'bad'), h('p', {}, `${c.latency_ms} ms · breaker ${c.breaker_state}`, h('small', {}, `order ${short(c.order_id)} · ${c.detail}`)))) : [empty('No gateway calls yet.')])));
}

// ---- Feeds -------------------------------------------------------------------------------------
function renderDecisions(decisions) {
  el('decisions').replaceChildren(...(decisions?.length ? decisions.map(d => h('div', { class: 'feed-row' }, h('time', {}, time(d.at).slice(0, 8)), chip(d.decision, decisionTone(d.decision)),
    h('p', {}, `${d.consumer} · ${d.type || 'unreadable'} · ${d.topic}-${d.kafka_partition}@${d.kafka_offset}${d.attempt > 1 ? ` · attempt ${d.attempt}` : ''}`, h('small', {}, d.detail), d.order_id && /^[0-9a-f-]{36}$/.test(d.order_id) ? h('div', { class: 'row-actions' }, button('Open order', () => select(d.order_id))) : null))) : [empty('No deliveries yet.')]));
}

function renderDeadLetters(records) {
  el('poison').replaceChildren(...['payment.commands', 'inventory.commands', 'order.events'].map(topic => button(`Poison ${topic}`, () => act('poison', { topic }, 'Publishing an unreadable record…').then(() => showMessage(`Unreadable record on ${topic}: not retried, dead-lettered on the first failure.`), () => {}), { class: 'danger' })));
  el('dead-letters').replaceChildren(...(Array.isArray(records) && records.length ? records.map(r => {
    const headers = JSON.parse(r.headers || '{}');
    return h('div', { class: 'feed-row' }, h('time', {}, time(r.kafka_timestamp).slice(0, 8)), chip(r.topic, 'bad'),
      h('p', {}, `${r.type || 'unreadable'} · from ${headers['kafka_dlt-original-topic'] || '?'}-${headers['kafka_dlt-original-partition'] || '?'}@${headers['kafka_dlt-original-offset'] || '?'}`,
        h('small', {}, headers['kafka_dlt-exception-message'] || headers['kafka_dlt-exception-fqcn'] || ''),
        h('div', { class: 'row-actions' }, button('Redrive', () => act('redrive', { topic: r.topic, partition: r.kafka_partition, offset: r.kafka_offset }, 'Sending it back to its topic…').then(() => showMessage('Redriven: it is delivered again. If the cause is fixed it succeeds; a poison record lands here again.'), () => {})),
          r.record_key && /^[0-9a-f-]{36}$/.test(r.record_key) ? button('Open order', () => select(r.record_key)) : null)));
  }) : [empty('Nothing dead-lettered.')]));
}

function renderReadModel(s) {
  const p = s.projection;
  if (!ok(p)) { el('read-model').replaceChildren(empty(`order-query-service unreachable: ${p?.error || ''}`)); return; }
  const lag = Array.isArray(s.groups) ? s.groups.find(g => g.groupId === 'order-projection')?.totalLag ?? 0 : 0;
  el('read-model').replaceChildren(
    h('div', { class: 'read-stats' }, h('div', {}, 'Orders projected', h('strong', {}, number(p.orders))), h('div', {}, 'Customers', h('strong', {}, number(p.customers))), h('div', {}, 'Projection lag', h('strong', {}, number(lag)))),
    h('p', { class: 'panel-note' }, `Last projected ${p.last_projected_at ? time(p.last_projected_at) : '—'}. Rebuild truncates both tables, forgets the projection's idempotency records and replays order.events from offset 0.`),
    button('Rebuild projection', () => confirm('Truncate the read models and replay order.events from the beginning?') && act('rebuild-projection', {}, 'Rebuilding…').then(() => showMessage('Rebuilding: watch order-projection lag spike, then drain to 0.'), () => {})));
}

function renderActions(s) {
  el('actions').replaceChildren(...(Array.isArray(s.actions) && s.actions.length ? s.actions.map(a => h('div', { class: 'feed-row' }, h('time', {}, time(a.at).slice(0, 8)), chip(a.action, a.ok ? '' : 'bad'), h('p', {}, a.target, h('small', {}, a.outcome)))) : [empty('No actions yet.')]));
  const tap = s.tap || {};
  el('tap').textContent = `Tap (consumer group control-plane-tap): ${tap.status || '—'} · ${number(tap.records || 0)} records stored · last poll ${tap.lastPoll ? time(tap.lastPoll) : '—'}`;
}

// ---- Loop --------------------------------------------------------------------------------------
function render(s) {
  state = s;
  const connection = el('connection');
  connection.innerHTML = '<i></i> Connected';
  connection.classList.remove('disconnected');
  renderPipeline(s);
  if (!document.activeElement?.closest?.('#order-form')) renderOrderForm(s);
  renderScenarios();
  renderOrders(s);
  renderTopics(s.topics);
  renderGroups(s.groups, s.services);
  renderServices(s.services);
  renderConnectors(s.connectors);
  renderGateway(s.gateway);
  renderDecisions(s.decisions);
  renderDeadLetters(s.deadLetters);
  renderReadModel(s);
  renderActions(s);
  el('retention-demo').disabled = busy;
}

async function refresh() {
  try {
    const response = await fetch('/api/events/state');
    if (!response.ok) throw new Error();
    render(await response.json());
    await refreshJourney();
  } catch {
    const connection = el('connection');
    connection.innerHTML = '<i></i> Disconnected';
    connection.classList.add('disconnected');
  }
}

el('order-form').addEventListener('submit', event => { event.preventDefault(); place().catch(() => {}); });
el('journey-close').addEventListener('click', () => { selected = null; history.replaceState(null, '', location.pathname); journey = null; el('journey-panel').hidden = true; if (state) render(state); });
el('journey-snapshot').addEventListener('click', () => act('discard-snapshot', { orderId: selected }).then(() => showMessage('Snapshot discarded: the next load folds every event from version 1.'), () => {}));
el('journey-fold').addEventListener('click', async () => {
  const response = await fetch(`/api/events/orders/${selected}/fold`);
  fold = response.ok ? await response.json() : null;
  renderJourney();
});
el('retention-demo').addEventListener('click', () => act('retention-demo', {}, 'Producing…').then(() => showMessage('20 records on lab.retention-demo. Closed segments older than a minute are deleted: the earliest offset moves forward.'), () => {}));
if (grafana) el('footer-links').append(' · ', h('a', { href: grafana, target: '_blank', rel: 'noopener' }, 'Grafana: metrics, traces, logs'));

async function poll() { await refresh(); setTimeout(poll, 1000); }
const linked = new URLSearchParams(location.search).get('order');
if (linked) { selected = linked; el('journey-panel').hidden = false; }
poll();
