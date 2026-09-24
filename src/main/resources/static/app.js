const el = id => document.getElementById(id);
const number = value => new Intl.NumberFormat().format(value);
const CUTOVER_STAGES = ['FREEZE', 'VALIDATION', 'CUTOVER'];
const ROLLBACK_STAGES = ['ROLLBACK_PREPARE', 'REVERSE_CATCH_UP', 'ROLLBACK_VALIDATION', 'ROLLBACK_FREEZE', 'FINAL_SYNC', 'SWITCH_PRIMARY'];
const WRITE_HELD_STAGES = [...CUTOVER_STAGES, 'ROLLBACK_FREEZE', 'FINAL_SYNC', 'SWITCH_PRIMARY'];
let busy = false;
let latest;
let lastStage;
let logPaused = false;
let backendLogs = [];
let visibleLogs = [];
// Events up to this backend log id are hidden by "Clear"; the backend keeps them.
let clearedThrough = 0;
let clearedAt = null;

function render(data) {
  latest = data;
  const s = data.migration;
  if (s.stage !== lastStage && !busy) {
    const guidance = {
      IDLE: data.source.customers ? 'Source data is ready. Start traffic or begin the migration.' : 'Generate demo data to prepare SQL Server.',
      SNAPSHOT: 'Copying a bounded snapshot into PostgreSQL.',
      CATCH_UP: 'Replaying committed SQL Server changes through CDC.',
      PREPARE: 'Building indexes and validating target constraints.',
      READY: 'Ready for Cutover — 95%. Review validation, then cut over when ready.',
      FREEZE: 'SQL Server writes are fenced while final CDC changes drain.',
      VALIDATION: 'Comparing source and target behind the write fence.',
      CUTOVER: 'Validation passed. Switching application writes to PostgreSQL.',
      COMPLETED: 'Migration completed successfully. PostgreSQL is now Primary.',
      ROLLBACK_PREPARE: 'Preparing a safe reverse synchronization to SQL Server.',
      REVERSE_CATCH_UP: 'Replaying PostgreSQL changes back into SQL Server.',
      ROLLBACK_VALIDATION: 'Validating both databases before the rollback fence.',
      ROLLBACK_FREEZE: 'PostgreSQL writes are fenced while the final reverse changes drain.',
      FINAL_SYNC: 'Applying the final reverse change set behind the write fence.',
      SWITCH_PRIMARY: 'Validation passed. Switching application writes back to SQL Server.',
      ROLLED_BACK: 'Rollback completed successfully. SQL Server is Primary again.'
    };
    showMessage(guidance[s.stage]);
    lastStage = s.stage;
  }

  const connection = el('connection');
  connection.innerHTML = '<i></i> Connected';
  connection.classList.remove('disconnected');
  for (const side of ['source', 'target']) {
    const counts = data[side];
    el(`${side}-count`).textContent = number(counts.customers + counts.orders);
    el(`${side}-detail`).textContent = `${number(counts.customers)} customers · ${number(counts.orders)} orders`;
  }

  renderMigration(data);
  renderCompletion(data.completion);
  renderRollback(data.rollback, s);
  renderTraffic(data.traffic, s.stage);
  receiveLogs(data.events || []);

  const running = s.status === 'RUNNING';
  const allowed = {
    seed: s.stage === 'IDLE' && !s.traffic && data.source.customers === 0,
    start: data.migrationStartAllowed,
    pause: running && ['SNAPSHOT', 'CATCH_UP', 'PREPARE', 'READY', 'ROLLBACK_PREPARE', 'REVERSE_CATCH_UP', 'ROLLBACK_VALIDATION'].includes(s.stage),
    resume: !running && !['IDLE', 'COMPLETED', 'ROLLED_BACK'].includes(s.stage),
    crash: running,
    validate: s.stage === 'READY' && !s.cdcPaused,
    cutover: s.stage === 'READY' && running && !s.cdcPaused,
    reset: true,
    rollback: data.rollback.available,
    'rollback-abort': data.rollback.abortable,
    'traffic-start': !WRITE_HELD_STAGES.includes(s.stage),
    'traffic-stop': data.traffic.running
  };
  document.querySelectorAll('[data-action]').forEach(button => { button.disabled = busy || !allowed[button.dataset.action]; });
  el('seed-rows').disabled = busy || !allowed.seed;
  if (!el('seed-rows').value) el('seed-rows').placeholder = number(data.seedRows);
  document.dispatchEvent(new CustomEvent('migration-state', { detail: data }));

  if (s.error || data.captureError) {
    showMessage(s.error || data.captureError, true);
    el('message').dataset.backendError = 'true';
  } else if (el('message').dataset.backendError === 'true') {
    showMessage('Backend recovered. The durable checkpoint is current.');
    el('message').dataset.backendError = 'false';
  }
}

function renderMigration(data) {
  const s = data.migration;
  const stageLabels = {
    IDLE: 'Idle', SNAPSHOT: 'Snapshot', CATCH_UP: 'CDC catch-up', PREPARE: 'Indexes & constraints',
    READY: 'Ready for Cutover', FREEZE: 'Write freeze', VALIDATION: 'Final validation',
    CUTOVER: 'Cutover', COMPLETED: 'Completed', ROLLBACK_PREPARE: 'Preparing rollback',
    REVERSE_CATCH_UP: 'Reverse CDC catch-up', ROLLBACK_VALIDATION: 'Rollback validation',
    ROLLBACK_FREEZE: 'Rollback write freeze', FINAL_SYNC: 'Final reverse sync',
    SWITCH_PRIMARY: 'Switching primary', ROLLED_BACK: 'Rolled back'
  };
  const trackStates = {
    IDLE: 'Waiting to start', SNAPSHOT: 'Copying source rows', CATCH_UP: 'Applying change stream',
    PREPARE: 'Preparing target', READY: 'Operator confirmation required', FREEZE: 'Source writes fenced',
    VALIDATION: 'Checksums and counts', CUTOVER: 'Switching primary', COMPLETED: 'Successful',
    ROLLBACK_PREPARE: 'Preparing reverse capture', REVERSE_CATCH_UP: 'Applying PostgreSQL changes',
    ROLLBACK_VALIDATION: 'Comparing both databases', ROLLBACK_FREEZE: 'PostgreSQL writes fenced',
    FINAL_SYNC: 'Draining final changes', SWITCH_PRIMARY: 'Restoring SQL Server', ROLLED_BACK: 'Rollback successful'
  };
  el('stage').textContent = stageLabels[s.stage];
  el('track-state').textContent = trackStates[s.stage];
  el('run-status').textContent = s.stage === 'READY' ? 'Waiting for cutover' : s.stage === 'ROLLED_BACK' ? 'Rollback complete' : s.status.toLowerCase();
  el('run-status').className = `status-pill ${statusTone(s)}`;
  el('progress').value = data.progress;
  el('percent').textContent = `${Math.round(data.progress)}%`;
  el('primary').textContent = databaseName(s.primary);
  el('source-role').textContent = s.primary === 'SQL_SERVER' ? 'Primary' : 'Source';
  el('target-role').textContent = s.primary === 'POSTGRESQL' ? 'Primary' : s.stage === 'ROLLED_BACK' ? 'Secondary' : 'Target';
  document.querySelector('.source-node').classList.toggle('is-primary', s.primary === 'SQL_SERVER');
  document.querySelector('.target-node').classList.toggle('is-primary', s.primary === 'POSTGRESQL');
  const topology = el('migration-topology');
  topology.classList.toggle('is-moving', s.status === 'RUNNING' && !ROLLBACK_STAGES.includes(s.stage) && !['READY', 'COMPLETED'].includes(s.stage));
  topology.classList.toggle('is-reversing', s.status === 'RUNNING' && ROLLBACK_STAGES.includes(s.stage));

  const rank = { IDLE: -1, SNAPSHOT: 0, CATCH_UP: 1, PREPARE: 2, READY: 3, FREEZE: 3, VALIDATION: 3, CUTOVER: 4, COMPLETED: 5,
    ROLLBACK_PREPARE: 5, REVERSE_CATCH_UP: 5, ROLLBACK_VALIDATION: 5, ROLLBACK_FREEZE: 5, FINAL_SYNC: 5, SWITCH_PRIMARY: 5, ROLLED_BACK: 5 }[s.stage];
  document.querySelectorAll('.stage-node').forEach((node, index) => {
    node.classList.toggle('done', rank > index);
    node.classList.toggle('active', rank === index && s.stage !== 'COMPLETED');
  });
  el('track-fill').style.transform = `scaleX(${Math.max(0, Math.min(100, rank * 25)) / 100})`;

  const migrationFinished = s.stage === 'COMPLETED' || s.stage === 'ROLLED_BACK' || ROLLBACK_STAGES.includes(s.stage);
  const migrated = migrationFinished ? data.target.customers + data.target.orders : s.copied;
  const total = migrationFinished ? data.target.customers + data.target.orders : s.expected;
  el('rows-summary').textContent = `${number(migrated)} / ${number(total)} rows`;
  el('table').textContent = ['IDLE', 'COMPLETED', 'ROLLED_BACK'].includes(s.stage) ? '—' : s.table.toLowerCase();
  el('batch').textContent = number(s.batches);
  el('rate').textContent = number(Math.round(s.rowsPerSecond));
  el('cdc').textContent = `${data.pending === null ? '—' : number(data.pending)} / ${number(s.applied)}`;
  el('checkpoint').textContent = s.checkpoint ? `${new Date(s.checkpoint).toLocaleTimeString()} · key ${s.lastId} · v${s.version}` : '—';
  el('validation').textContent = s.validation;
  el('rollback-state').textContent = rollbackState(s, data.rollback);
  el('cutover-note').textContent = cutoverNote(s);
}

function statusTone(state) {
  if (state.status === 'SUCCESS') return 'success';
  if (['FAILED', 'CRASHED'].includes(state.status)) return 'failed';
  if (state.stage === 'READY' || state.status === 'PAUSED') return 'waiting';
  return state.status === 'RUNNING' ? 'running' : '';
}

function rollbackState(state, rollback) {
  if (state.stage === 'IDLE') return 'No migration active';
  if (rollback.completed) return 'Completed · SQL Server restored';
  if (rollback.inProgress) return `${Math.round(rollback.progress)}% · ${number(rollback.pending)} pending`;
  if (rollback.available) return 'Available · reverse capture active';
  if (CUTOVER_STAGES.includes(state.stage)) return state.status === 'FAILED' ? 'Cutover halted · source fenced' : 'SQL Server writes fenced';
  if (state.stage === 'COMPLETED') return 'Source retained · writes fenced';
  return 'SQL Server remains authoritative';
}

function cutoverNote(state) {
  if (state.stage === 'READY') return 'Final CDC drain and validation run behind the write fence.';
  if (CUTOVER_STAGES.includes(state.stage)) return 'Cutover is in progress. Source writes remain fenced.';
  if (state.stage === 'COMPLETED') return 'Cutover completed and recorded by the backend.';
  if (ROLLBACK_STAGES.includes(state.stage)) return 'Rollback is in progress. Primary switches only after final validation.';
  if (state.stage === 'ROLLED_BACK') return 'Rollback completed and SQL Server is Primary again.';
  return 'Available after CDC catch-up and target preparation.';
}

function renderRollback(rollback, state) {
  const panel = el('rollback-panel');
  const controls = el('rollback-controls');
  const visible = rollback.available || rollback.inProgress || rollback.completed;
  panel.hidden = !visible;
  controls.hidden = !(rollback.available || rollback.inProgress);
  if (!visible) return;

  const failed = rollback.inProgress && ['FAILED', 'CRASHED'].includes(state.status);
  el('rollback-title').textContent = rollback.completed ? 'Rollback completed successfully'
    : failed ? 'Rollback requires attention'
      : rollback.inProgress ? 'Rollback in progress' : 'Rollback available';
  el('rollback-message').textContent = rollback.completed
    ? `${number(rollback.verifiedRows)} rows verified in ${formatDuration(rollback.durationMillis)} · ${number(rollback.dataLost)} lost.`
    : failed ? 'The backend stopped safely. Review the event log, then resume or abort while it is still safe.'
      : rollback.inProgress ? `${stageName(state.stage)} · primary remains ${databaseName(state.primary)} until the switch is committed.`
        : 'Post-cutover PostgreSQL writes are being captured for a lossless reverse synchronization.';
  el('rollback-progress').value = rollback.progress;
  el('rollback-progress-label').textContent = `${Math.round(rollback.progress)}%`;
  el('rollback-validation').textContent = rollback.validationPassed ? 'Validation passed' : rollback.validation;
  el('rollback-pending').textContent = number(rollback.pending);
  el('rollback-applied').textContent = number(rollback.applied);
  el('rollback-conflicts').textContent = number(rollback.conflicts);
  renderRollbackSteps(rollback, state, failed);
  // "0 data lost" is shown only when the backend reports it: after final validation matched with nothing pending.
  el('rollback-facts').hidden = !(rollback.completed && rollback.dataLost !== null);
  if (rollback.completed && rollback.dataLost !== null) el('rollback-data-lost').textContent = number(rollback.dataLost);
}

const ROLLBACK_STEPS = [...ROLLBACK_STAGES, 'ROLLED_BACK'];
function renderRollbackSteps(rollback, state, failed) {
  const steps = el('rollback-steps');
  steps.hidden = !(rollback.inProgress || rollback.completed);
  if (steps.hidden) return;
  const current = rollback.completed ? ROLLBACK_STEPS.length : ROLLBACK_STEPS.indexOf(state.stage);
  steps.querySelectorAll('li').forEach((step, index) => {
    step.classList.toggle('done', index < current || (rollback.completed && index === ROLLBACK_STEPS.length - 1));
    step.classList.toggle('active', index === current && !failed);
    step.classList.toggle('failed', index === current && failed);
    if (index === current) step.setAttribute('aria-current', 'step'); else step.removeAttribute('aria-current');
  });
}

function stageName(stage) {
  return ({ ROLLBACK_PREPARE: 'Preparing reverse sync', REVERSE_CATCH_UP: 'Reverse CDC catch-up',
    ROLLBACK_VALIDATION: 'Validating both databases', ROLLBACK_FREEZE: 'PostgreSQL write freeze',
    FINAL_SYNC: 'Final reverse sync', SWITCH_PRIMARY: 'Switching primary' })[stage] || stage;
}

function renderCompletion(completion) {
  const panel = el('completion');
  panel.hidden = !completion.successful;
  if (!completion.successful) return;
  // Every figure comes from durable backend timestamps and counters; nothing is timed in the browser.
  const recorded = (value, format) => value === null || value === undefined ? 'Not recorded' : format(value);
  el('completion-rows').textContent = number(completion.totalMigratedRows);
  el('completion-duration').textContent = recorded(completion.durationMillis, formatDuration);
  el('completion-throughput').textContent = recorded(completion.rowsPerSecond, rate => number(Math.round(rate)));
  el('completion-freeze').textContent = recorded(completion.writeFreezeMillis, formatDuration);
  el('completion-cdc-pending').textContent = recorded(completion.cdcPending, number);
  el('completion-cdc-applied').textContent = number(completion.cdcApplied);
  el('completion-primary').textContent = `${databaseName(completion.primary)} is now Primary`;
  el('completion-validation').textContent = completion.validationPassed ? completion.validation : 'Validation not confirmed';
  // A checked fact only when the backend recorded a drained stream; the tile shows any other value.
  el('completion-cdc').hidden = completion.cdcPending !== 0;
  const at = value => clock.format(new Date(value));
  el('completion-summary').textContent = completion.startedAt && completion.completedAt
    ? `Started ${at(completion.startedAt)} · PostgreSQL primary since ${at(completion.completedAt)}`
    : 'Start or completion time was not recorded.';
}

const databaseName = primary => primary === 'SQL_SERVER' ? 'SQL Server' : 'PostgreSQL';
function formatDuration(milliseconds) {
  if (milliseconds < 1000) return `${milliseconds} ms`;
  const seconds = milliseconds / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)} s`;
  const whole = Math.round(seconds);
  return `${Math.floor(whole / 60)}m ${whole % 60}s`;
}

function renderTraffic(t, stage) {
  const waiting = t.running && WRITE_HELD_STAGES.includes(stage);
  el('traffic').textContent = t.running ? (waiting ? 'Waiting at cutover fence' : `Routing to ${databaseName(t.target)}`) : 'Traffic stopped';
  const trafficButton = el('traffic-button');
  trafficButton.dataset.action = t.running ? 'traffic-stop' : 'traffic-start';
  setButtonLabel(trafficButton, 'activity', t.running ? 'Stop live traffic' : 'Start live traffic');
  el('traffic-target').textContent = t.running ? (waiting ? `${databaseName(t.target)} · fenced` : databaseName(t.target)) : 'Not running';
  el('traffic-rate').textContent = t.operationsPerSecond.toFixed(1);
  for (const key of ['total', 'inserts', 'updates', 'deletes', 'reads', 'errors']) el(`traffic-${key}`).textContent = number(t[key]);
  el('traffic-routing').textContent = `Committed operations · SQL Server ${number(t.sqlServerOperations)} · PostgreSQL ${number(t.postgresOperations)}`;
}

function setButtonLabel(button, icon, label) {
  button.replaceChildren();
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  const use = document.createElementNS('http://www.w3.org/2000/svg', 'use');
  use.setAttribute('href', `#icon-${icon}`);
  svg.append(use);
  button.append(svg, document.createTextNode(label));
}

function receiveLogs(logs) {
  backendLogs = logs;
  if (logPaused) return;
  visibleLogs = logs;
  renderLogs();
}

const clock = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
function parseLog(event) {
  const at = new Date(event.at);
  const time = clock.format(at);
  const message = event.message;
  const raw = `${time} ${message}`;
  const operation = message.match(/^(READ|INSERT|UPDATE|DELETE|COPY)\b/)?.[1] || '';
  const level = /FAILED|error|crash|blocked|expired|conflict/i.test(message) ? 'ERROR'
    : /paused|requested|frozen|stopped|waiting/i.test(message) ? 'WARNING'
      : /completed successfully|successful|passed|complete|generated|started|resumed|applied/i.test(message) ? 'SUCCESS' : 'INFO';
  const category = /\b(?:READ|INSERT|UPDATE|DELETE)\b|Traffic/i.test(message) ? 'TRAFFIC'
    : /rollback|reverse sync|reverse catch|reverse change|reverse capture|conflict|PostgreSQL writes fenced|write fence lifted/i.test(message) ? 'ROLLBACK'
      : /CDC|Change Tracking|net changes|capture/i.test(message) ? 'CDC'
      : /cutover|validation|freeze|fenced|primary|sequence/i.test(message) ? 'CUTOVER' : 'MIGRATION';
  return { id: event.id, at, raw, time, message, operation, level, category };
}

// Elapsed time since the backend recorded the event, e.g. "just now", "12s ago", "3 min ago".
function formatAge(at) {
  const seconds = Math.max(0, Math.floor((Date.now() - at.getTime()) / 1000));
  if (seconds < 1) return 'just now';
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} h ago`;
  return `${Math.floor(hours / 24)} d ago`;
}

function refreshAges() {
  document.querySelectorAll('#logs .log-age').forEach(age => { age.textContent = formatAge(new Date(Number(age.dataset.at))); });
}

function renderLogs() {
  const query = el('log-search').value.trim().toLowerCase();
  const level = el('log-level').value;
  const category = el('log-category').value;
  const events = visibleLogs.filter(event => event.id > clearedThrough).map(parseLog).filter(event =>
    (level === 'ALL' || event.level === level) &&
    (category === 'ALL' || event.category === category) &&
    (!query || event.raw.toLowerCase().includes(query)));
  const viewer = el('logs');
  viewer.replaceChildren();
  if (!events.length) {
    const empty = document.createElement('p');
    empty.className = 'log-empty';
    empty.textContent = clearedAt && !visibleLogs.some(event => event.id > clearedThrough)
      ? `Cleared at ${clock.format(clearedAt)}. New events will appear here.`
      : visibleLogs.length ? 'No events match these filters.' : 'Waiting for backend events…';
    viewer.append(empty);
  } else {
    for (const event of events) {
      const row = document.createElement('div');
      row.className = 'log-row';
      const time = document.createElement('time'); time.className = 'log-time'; time.dateTime = event.at.toISOString(); time.textContent = event.time;
      const age = document.createElement('span'); age.className = 'log-age'; age.dataset.at = String(event.at.getTime()); age.textContent = formatAge(event.at);
      const badge = document.createElement('span'); badge.className = `log-level ${event.level.toLowerCase()}`; badge.textContent = event.level;
      const category = document.createElement('span'); category.className = 'log-category'; category.textContent = event.category;
      const message = document.createElement('span'); message.className = 'log-message';
      if (event.operation) { const operation = document.createElement('b'); operation.className = 'log-operation'; operation.textContent = event.operation; message.append(operation, document.createTextNode(event.message.slice(event.operation.length).trim())); }
      else message.textContent = event.message;
      row.append(time, age, badge, category, message);
      viewer.append(row);
    }
    viewer.scrollTop = 0;
  }
  el('log-count').textContent = `${number(events.length)} ${events.length === 1 ? 'event' : 'events'}${logPaused ? ' · stream paused' : ''}${clearedAt ? ` · cleared at ${clock.format(clearedAt)}` : ''}`;
  el('log-clear').disabled = !visibleLogs.some(event => event.id > clearedThrough);
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
    const connection = el('connection');
    connection.innerHTML = '<i></i> Disconnected';
    connection.classList.add('disconnected');
    document.dispatchEvent(new CustomEvent('migration-disconnected'));
    document.querySelectorAll('[data-action]').forEach(button => { button.disabled = true; });
    el('seed-rows').disabled = true;
  }
}

document.querySelector('.controls').addEventListener('click', async event => {
  const button = event.target.closest('[data-action]');
  if (!button || busy) return;
  const action = button.dataset.action;
  if (action === 'reset' && !confirm('Delete demo data in both databases and reset all migration progress?')) return;
  if (action === 'rollback' && !confirm('Roll back to SQL Server?\n\nPostgreSQL stays Primary while its post-cutover writes are replayed into SQL Server and validated. Application writes pause briefly for the final sync. If a conflict or validation failure is found, nothing is switched and PostgreSQL remains Primary.')) return;
  if (action === 'rollback-abort' && !confirm('Abort this rollback attempt? PostgreSQL will remain Primary.')) return;
  busy = true;
  if (latest) render(latest);
  showMessage('Working…');
  try {
    const rows = el('seed-rows').value;
    if (action === 'seed' && rows) showMessage(`Generating ${number(rows)} customers and orders…`);
    const query = action === 'seed' && rows ? `?rows=${encodeURIComponent(rows)}` : '';
    const response = await fetch(`/api/actions/${action}${query}`, { method: 'POST' });
    const body = await response.json();
    if (!response.ok) throw new Error(body.detail || 'Action failed');
    showMessage(body.message);
    if (action === 'reset') document.dispatchEvent(new CustomEvent('migration-reset'));
  } catch (error) { showMessage(error.message, true); }
  finally { busy = false; await refresh(); }
});

for (const id of ['log-search', 'log-level', 'log-category']) el(id).addEventListener(id === 'log-search' ? 'input' : 'change', renderLogs);
el('log-pause').addEventListener('click', () => {
  logPaused = !logPaused;
  const button = el('log-pause');
  button.classList.toggle('is-paused', logPaused);
  setButtonLabel(button, logPaused ? 'play' : 'pause', logPaused ? 'Resume stream' : 'Pause stream');
  if (!logPaused) visibleLogs = backendLogs;
  renderLogs();
});

el('log-clear').addEventListener('click', () => {
  clearedThrough = Math.max(clearedThrough, ...visibleLogs.map(event => event.id));
  clearedAt = new Date();
  renderLogs();
});
// Ages keep counting while the stream is paused.
setInterval(refreshAges, 1000);

async function poll() { await refresh(); setTimeout(poll, 1000); }
poll();
