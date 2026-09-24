const el = id => document.getElementById(id);
const number = value => new Intl.NumberFormat().format(value);
let busy = false;
let latest;
let lastStage;
function render(data) {
  latest = data;
  const s = data.migration;
  if (s.stage !== lastStage && !busy) {
    const guidance = {
      IDLE: data.source.customers ? 'Start traffic, then start the migration.' : 'Generate demo data, start traffic, then start the migration.',
      SNAPSHOT: 'Copying bounded batches. You can pause or simulate a crash to explore recovery.',
      CATCH_UP: 'Applying committed source changes to PostgreSQL.',
      PREPARE: 'Preparing indexes, constraints, sequences and statistics.',
      READY: 'Validate the databases, then choose Cutover when you are ready.',
      FREEZE: 'Source writes are fenced while final validation and cutover finish.',
      COMPLETE: 'Migration complete. Writes go to PostgreSQL. Reset to run the demo again.'
    };
    showMessage(guidance[s.stage]);
    lastStage = s.stage;
  }
  el('connection').textContent = 'Connected · live backend state';
  for (const side of ['source', 'target']) {
    const counts = data[side];
    el(`${side}-count`).textContent = number(counts.customers + counts.orders);
    el(`${side}-detail`).textContent = `${number(counts.customers)} customers · ${number(counts.orders)} orders`;
  }
  el('primary').textContent = s.primary === 'SQL_SERVER' ? 'SQL Server' : 'PostgreSQL';
  el('stage').textContent = s.stage.replaceAll('_', ' ').toLowerCase().replace(/^./, c => c.toUpperCase());
  el('run-status').textContent = s.status.toLowerCase();
  el('progress').value = data.progress;
  el('percent').textContent = `${Math.round(data.progress)}%`;
  el('table').textContent = s.stage === 'IDLE' ? '—' : s.table.toLowerCase();
  el('batch').textContent = number(s.batches);
  el('rate').textContent = number(Math.round(s.rowsPerSecond));
  el('cdc').textContent = `${data.pending === null ? '—' : number(data.pending)} / ${number(s.applied)}`;
  el('checkpoint').textContent = s.checkpoint ? `${new Date(s.checkpoint).toLocaleTimeString()} · key ${s.lastId} · v${s.version}` : '—';
  el('validation').textContent = s.validation;
  el('logs').textContent = data.logs.join('\n');
  el('traffic').textContent = s.traffic ? (s.stage === 'FREEZE' ? 'Traffic waiting for cutover' : 'Traffic running · INSERT / UPDATE / DELETE') : 'Traffic stopped';
  const trafficButton = el('traffic-button');
  trafficButton.dataset.action = s.traffic ? 'traffic-stop' : 'traffic-start';
  trafficButton.textContent = s.traffic ? 'Stop Traffic' : 'Start Traffic';
  const running = s.status === 'RUNNING';
  const allowed = {
    seed: s.stage === 'IDLE' && !s.traffic && data.source.customers === 0,
    start: s.stage === 'IDLE', pause: running,
    resume: !running && !['IDLE', 'COMPLETE'].includes(s.stage),
    crash: running, validate: s.stage === 'READY' && !s.cdcPaused, cutover: s.stage === 'READY' && running && !s.cdcPaused,
    reset: true, 'traffic-start': s.stage !== 'FREEZE', 'traffic-stop': true
  };
  document.querySelectorAll('[data-action]').forEach(button => { button.disabled = busy || !allowed[button.dataset.action]; });
  el('seed-rows').disabled = busy || !allowed.seed;
  if (!el('seed-rows').value) el('seed-rows').placeholder = number(data.seedRows);
  document.dispatchEvent(new CustomEvent('migration-state', { detail: data }));
  if (s.error || data.captureError) {
    showMessage(s.error || data.captureError, true);
    el('message').dataset.backendError = 'true';
  } else if (el('message').dataset.backendError === 'true') {
    showMessage('Backend recovered. The displayed checkpoint is current.');
    el('message').dataset.backendError = 'false';
  }
}
function showMessage(message, error = false) {
  el('message').textContent = message;
  el('message').classList.toggle('error', error);
}
async function refresh() {
  try {
    const response = await fetch('/api/status');
    if (!response.ok) throw new Error('Backend unavailable');
    render(await response.json());
  } catch (error) {
    el('connection').textContent = 'Disconnected · displayed values may be stale';
    document.dispatchEvent(new CustomEvent('migration-disconnected'));
    document.querySelectorAll('[data-action]').forEach(button => { button.disabled = true; });
    el('seed-rows').disabled = true;
  }
}
document.querySelector('.controls').addEventListener('click', async event => {
  const button = event.target.closest('[data-action]');
  if (!button || busy) return;
  const action = button.dataset.action;
  if (action === 'reset' && !confirm('Delete demo data in both databases and reset migration progress?')) return;
  busy = true;
  if (latest) render(latest);
  showMessage('Working…');
  try {
    const rows = el('seed-rows').value;
    if (action === 'seed' && rows) showMessage(`Generating ${number(rows)} customers and ${number(rows)} orders…`);
    const query = action === 'seed' && rows ? `?rows=${encodeURIComponent(rows)}` : '';
    const response = await fetch(`/api/actions/${action}${query}`, { method: 'POST' });
    const body = await response.json();
    if (!response.ok) throw new Error(body.detail || 'Action failed');
    showMessage(body.message);
    if (action === 'reset') document.dispatchEvent(new CustomEvent('migration-reset'));
  } catch (error) { showMessage(error.message, true); }
  finally { busy = false; await refresh(); }
});
async function poll() { await refresh(); setTimeout(poll, 1000); }
poll();
