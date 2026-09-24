// Renders backend observations and submits operator commands. Every lamp, counter and moving
// packet below is driven by a value /api/status reported; motion only replays a difference between
// two real polls, it never predicts one.
const el = id => document.getElementById(id);
const number = value => new Intl.NumberFormat().format(value);
const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)');
const narrow = matchMedia('(max-width: 820px)');
const STAGES = ['SNAPSHOT', 'CATCH_UP', 'PREPARE', 'READY', 'FREEZE', 'VALIDATION', 'CUTOVER'];
const FENCED = ['FREEZE', 'VALIDATION', 'CUTOVER'];
const STAGE_LABELS = {
  IDLE: 'Idle', SNAPSHOT: 'Snapshot copy', CATCH_UP: 'CDC catch-up', PREPARE: 'Indexes & constraints',
  READY: 'Ready for Cutover', FREEZE: 'Write freeze', VALIDATION: 'Final validation', CUTOVER: 'Cutover',
  COMPLETED: 'Migration completed successfully'
};
let busy = false;
let latest;
let previous;
let lastStage;

function render(data) {
  latest = data;
  const s = data.migration;
  if (s.stage !== lastStage && !busy) {
    const guidance = {
      IDLE: data.source.customers ? 'Start live traffic, then start the migration.' : 'Generate demo data, start live traffic, then start the migration.',
      SNAPSHOT: 'Copying bounded batches. You can pause or simulate a crash to explore recovery.',
      CATCH_UP: 'Applying committed source changes to PostgreSQL.',
      PREPARE: 'Preparing indexes, constraints, sequences and statistics.',
      READY: 'Ready for Cutover — 95%. Validate if desired, then choose Cutover.',
      FREEZE: 'Source writes are fenced; draining final CDC changes.',
      VALIDATION: 'Source and target are being validated behind the write fence.',
      CUTOVER: 'Validation passed; switching the primary database to PostgreSQL.',
      COMPLETED: 'PostgreSQL is now Primary and takes all application writes. SQL Server stays fenced. Reset to run the lab again.'
    };
    showMessage(guidance[s.stage]);
    lastStage = s.stage;
  }
  connection(true);
  renderHeader(data);
  renderTermini(data);
  renderTrack(data);
  renderFeeder(data);
  renderInstruments(data);
  renderCompletion(data.completion);
  renderTraffic(data.traffic, s.stage);
  logbook.ingest(data.logs);
  if (previous) animateDeltas(previous, data);
  previous = data;

  const running = s.status === 'RUNNING';
  const allowed = {
    seed: s.stage === 'IDLE' && !s.traffic && data.source.customers === 0,
    start: data.migrationStartAllowed, pause: running && ['SNAPSHOT', 'CATCH_UP', 'PREPARE', 'READY'].includes(s.stage),
    resume: !running && !['IDLE', 'COMPLETED'].includes(s.stage),
    crash: running, validate: s.stage === 'READY' && !s.cdcPaused, cutover: s.stage === 'READY' && running && !s.cdcPaused,
    reset: true, 'traffic-start': !FENCED.includes(s.stage), 'traffic-stop': true
  };
  document.querySelectorAll('[data-action]').forEach(button => { button.disabled = busy || !allowed[button.dataset.action]; });
  const startKey = document.querySelector('[data-action="start"]');
  const migrating = running && s.stage !== 'IDLE' && s.stage !== 'COMPLETED';
  startKey.dataset.running = String(migrating);
  startKey.querySelector('.key__label').textContent = migrating ? 'Migration running' : 'Start migration';
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

const databaseName = primary => primary === 'SQL_SERVER' ? 'SQL Server' : 'PostgreSQL';

function renderHeader(data) {
  const s = data.migration;
  const primary = el('primary');
  const name = databaseName(s.primary);
  if (primary.textContent !== name) {
    primary.textContent = name;
    if (previous) replay(primary, 'flip');
  }
  const tone = { IDLE: 'idle', RUNNING: 'running', PAUSED: 'held', CRASHED: 'danger', FAILED: 'danger', SUCCESS: 'clear' }[s.status] || 'idle';
  const status = s.stage === 'READY' && s.status === 'RUNNING' ? 'Waiting for cutover'
    : s.stage === 'COMPLETED' ? 'Completed' : s.status.charAt(0) + s.status.slice(1).toLowerCase();
  el('run-plate').dataset.tone = tone;
  el('run-status').textContent = status;
  el('stage').textContent = STAGE_LABELS[s.stage] || s.stage;
  el('progress').value = data.progress;
  setCounter(el('percent'), String(Math.round(data.progress)));
  const mimic = el('mimic');
  mimic.dataset.stage = s.stage;
  mimic.dataset.status = s.status;
  mimic.dataset.primary = s.primary;
}

function renderTermini(data) {
  const s = data.migration;
  for (const side of ['source', 'target']) {
    const counts = data[side];
    setCounter(el(`${side}-count`), number(counts.customers + counts.orders));
    el(`${side}-detail`).textContent = `${number(counts.customers)} customers · ${number(counts.orders)} orders`;
  }
  const fenced = FENCED.includes(s.stage);
  const [sourceRole, sourceText] = s.primary === 'POSTGRESQL' ? ['fenced', 'Fenced · retired']
    : fenced ? ['fenced', 'Writes fenced'] : ['primary', 'Receives writes'];
  const [targetRole, targetText] = s.primary === 'POSTGRESQL'
    ? [data.completion.successful ? 'arrived' : 'primary', 'Primary · receives writes']
    : s.stage === 'IDLE' ? ['standby', 'Standby'] : ['standby', 'Receiving migration'];
  el('term-source').dataset.role = sourceRole;
  el('source-role').textContent = sourceText;
  el('term-target').dataset.role = targetRole;
  el('target-role').textContent = targetText;
}

function renderTrack(data) {
  const s = data.migration;
  const current = s.stage === 'COMPLETED' ? STAGES.length : STAGES.indexOf(s.stage);
  const heldReplay = s.cdcPaused && ['CATCH_UP', 'READY'].includes(s.stage);
  const snapshotShare = s.expected > 0 ? Math.min(100, (s.copied / s.expected) * 100) : 0;
  el('sections').querySelectorAll('li').forEach((section, index) => {
    const stage = section.dataset.stage;
    let state = 'waiting';
    let text = s.stage === 'IDLE' ? '—' : 'Waiting';
    if (index < current) {
      state = 'passed';
      text = stage === 'SNAPSHOT' ? `${number(s.copied)} rows` : 'Passed';
    } else if (index === current) {
      state = ['CRASHED', 'FAILED'].includes(s.status) ? 'danger' : s.status === 'PAUSED' || heldReplay ? 'held' : 'occupied';
      text = state === 'danger' ? (s.status === 'CRASHED' ? 'Crashed · resume from checkpoint' : 'Failed')
        : s.status === 'PAUSED' ? 'Paused at checkpoint'
        : heldReplay ? 'Replay held'
        : {
          SNAPSHOT: `${Math.floor(snapshotShare)}% · ${s.table.toLowerCase()}`,
          CATCH_UP: data.pending === null ? 'Replaying' : `${number(data.pending)} pending`,
          PREPARE: 'Building',
          READY: 'Awaiting cutover',
          FREEZE: 'Draining CDC',
          VALIDATION: 'Comparing rows',
          CUTOVER: 'Switching primary'
        }[stage];
    }
    section.dataset.state = state;
    section.dataset.moving = String(s.status === 'RUNNING');
    section.querySelector('.section__state').textContent = text;
    const fill = section.querySelector('.fill');
    fill.style.setProperty('--fill', stage === 'SNAPSHOT' && index === current ? String(snapshotShare / 100) : '0');
  });
}

function renderFeeder(data) {
  const s = data.migration;
  const t = data.traffic;
  const fenced = FENCED.includes(s.stage);
  const feeder = el('feeder');
  feeder.dataset.route = s.primary === 'SQL_SERVER' ? 'source' : 'target';
  feeder.dataset.fenced = String(fenced && t.running);
  el('feeder-signal').dataset.aspect = !t.running ? 'off' : fenced ? 'danger' : 'clear';
  el('feeder-rate').textContent = !t.running ? `→ ${databaseName(s.primary)} · stopped`
    : fenced ? `→ ${databaseName(t.target)} · held at fence` : `→ ${databaseName(t.target)} · ${t.operationsPerSecond.toFixed(1)} ops/s`;
}

function renderInstruments(data) {
  const s = data.migration;
  setCounter(el('rate'), number(Math.round(s.rowsPerSecond)));
  setCounter(el('batch'), number(s.batches));
  el('table').textContent = ['IDLE', 'COMPLETED'].includes(s.stage) ? '—' : s.table.toLowerCase();
  setCounter(el('cdc-pending'), data.pending === null ? '—' : number(data.pending));
  setCounter(el('cdc-applied'), number(s.applied));
  el('cdc-replay').textContent = s.stage === 'IDLE' ? '—' : s.stage === 'COMPLETED' ? 'Drained' : s.cdcPaused ? 'Held by operator' : 'Replaying';
  el('checkpoint').textContent = s.checkpoint ? new Date(s.checkpoint).toLocaleTimeString() : '—';
  el('checkpoint-key').textContent = s.checkpoint ? number(s.lastId) : '—';
  el('checkpoint-version').textContent = s.checkpoint ? `v${s.version}` : '—';
  el('validation').textContent = s.validation;
  el('validation-gauge').dataset.tone = s.stage === 'VALIDATION' && s.status === 'RUNNING' ? 'running'
    : s.validation.startsWith('FAILED') ? 'fail'
    : s.validationPassed || s.validation.startsWith('Passed') ? 'pass' : 'idle';
}

function renderCompletion(completion) {
  const panel = el('completion');
  const wasHidden = panel.hidden;
  panel.hidden = !completion.successful;
  if (!completion.successful) return;
  el('completion-progress').textContent = '100%';
  el('completion-primary').textContent = 'PostgreSQL is now Primary';
  el('completion-validation').textContent = completion.validationPassed ? 'Source/target validation passed' : 'Source/target validation not confirmed';
  el('completion-cdc').textContent = `CDC fully caught up / ${number(completion.cdcPending)} pending`;
  el('completion-summary').textContent = `${number(completion.totalMigratedRows)} total rows migrated in ${formatDuration(completion.durationMillis)}`;
  // Celebrate only a completion observed live, not one that was already true when the page opened.
  if (wasHidden && previous && !previous.completion.successful) replay(panel, 'is-new', 1600);
}

function formatDuration(milliseconds) {
  if (milliseconds < 1000) return `${milliseconds} ms`;
  const seconds = milliseconds / 1000;
  return seconds < 60 ? `${seconds.toFixed(1)} s` : `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}

function renderTraffic(t, stage) {
  const waiting = t.running && FENCED.includes(stage);
  const trafficButton = el('traffic-button');
  trafficButton.dataset.action = t.running ? 'traffic-stop' : 'traffic-start';
  trafficButton.dataset.running = String(t.running);
  trafficButton.querySelector('.key__label').textContent = t.running ? 'Stop live traffic' : 'Start live traffic';
  el('traffic-target').textContent = t.running ? (waiting ? `${databaseName(t.target)} · held at fence` : `Writing to ${databaseName(t.target)}`) : 'Stopped';
  el('traffic-lamp').dataset.lit = !t.running ? 'off' : waiting ? 'red' : 'yellow';
  setCounter(el('traffic-rate'), t.operationsPerSecond.toFixed(1));
  for (const key of ['total', 'errors']) setCounter(el(`traffic-${key}`), number(t[key]));
  el('traffic-errors').closest('div').dataset.bad = String(t.errors > 0);
  const operations = ['reads', 'inserts', 'updates', 'deletes'];
  const committed = operations.reduce((sum, key) => sum + t[key], 0);
  for (const key of operations) {
    el(`traffic-${key}`).textContent = number(t[key]);
    document.querySelector(`.mix__bar [data-op="${key}"]`).style.width = committed ? `${(t[key] / committed) * 100}%` : '0';
  }
  el('traffic-routing').textContent = `Committed operations counted by the backend: SQL Server ${number(t.sqlServerOperations)} · PostgreSQL ${number(t.postgresOperations)}`;
}

// ── Motion driven by observed deltas ──
function animateDeltas(before, after) {
  if (reducedMotion.matches || document.hidden) return;
  const a = before.migration;
  const b = after.migration;
  const sameRun = b.stage !== 'IDLE' && b.copied >= a.copied && b.applied >= a.applied;
  if (sameRun) {
    const batches = b.batches - a.batches;
    if (batches > 0) {
      for (let i = 0; i < Math.min(3, batches); i++) {
        sendPacket('snapshot', i === 0 ? `+${number(b.copied - a.copied)} rows` : '', i * 260);
      }
    }
    const applied = b.applied - a.applied;
    if (applied > 0) sendPacket('cdc', `+${number(applied)} CDC`, 120);
  }
  const operations = after.traffic.total - before.traffic.total;
  if (operations > 0 && !FENCED.includes(b.stage)) {
    const count = Math.min(5, operations);
    for (let i = 0; i < count; i++) sendPulse(after.migration.primary, (i * 900) / count);
  }
}

function sendPacket(kind, label, delay) {
  const rail = el('packets');
  const packet = document.createElement('span');
  packet.className = `packet packet--${kind}`;
  if (label) {
    const text = document.createElement('span');
    text.className = 'packet__label';
    text.textContent = label;
    packet.append(text);
  }
  rail.append(packet);
  const vertical = narrow.matches;
  const length = vertical ? rail.clientHeight : rail.clientWidth;
  const move = offset => vertical ? `translateY(${offset}px)` : `translateX(${offset}px)`;
  const turn = kind === 'cdc' ? ' rotate(45deg)' : '';
  packet.animate([
    { transform: move(-10) + turn, opacity: 0 },
    { transform: move(length * 0.08) + turn, opacity: 1, offset: 0.1 },
    { transform: move(length * 0.92) + turn, opacity: 1, offset: 0.9 },
    { transform: move(length) + turn, opacity: 0 }
  ], { duration: 1500, delay, easing: 'cubic-bezier(.45,0,.25,1)', fill: 'both' }).finished.then(() => packet.remove(), () => packet.remove());
}

function sendPulse(primary, delay) {
  if (narrow.matches) return;
  const layer = el('feeder-pulses');
  const box = layer.getBoundingClientRect();
  const branch = el(primary === 'SQL_SERVER' ? 'branch-source' : 'branch-target').getBoundingClientRect();
  const startX = box.width / 2;
  const cornerX = (primary === 'SQL_SERVER' ? branch.left : branch.right) - box.left;
  const top = branch.top - box.top + 1.5;
  const bottom = branch.bottom - box.top;
  const pulse = document.createElement('span');
  pulse.className = 'pulse';
  layer.append(pulse);
  const at = (x, y) => `translate(${x}px, ${y}px)`;
  pulse.animate([
    { transform: at(startX, top), opacity: 0 },
    { transform: at(startX, top), opacity: 1, offset: 0.08 },
    { transform: at(cornerX, top), opacity: 1, offset: 0.75 },
    { transform: at(cornerX, bottom), opacity: 0 }
  ], { duration: 950, delay, easing: 'linear', fill: 'both' }).finished.then(() => pulse.remove(), () => pulse.remove());
}

// Counters roll only the characters that changed between two backend values.
function setCounter(node, text) {
  if (node.dataset.value === text) return;
  const old = node.dataset.value ?? '';
  node.dataset.value = text;
  const animate = !reducedMotion.matches && old !== '' && old.length === text.length && old !== '—';
  node.replaceChildren(...[...text].map((character, index) => {
    const digit = document.createElement('span');
    digit.className = 'digit';
    digit.textContent = character;
    if (animate && old[index] !== character) {
      digit.animate([{ transform: 'translateY(45%)', opacity: 0 }, { transform: 'none', opacity: 1 }],
        { duration: 340, easing: 'cubic-bezier(.16,1,.3,1)' });
    }
    return digit;
  }));
}

function replay(node, className, duration = 900) {
  node.classList.remove(className);
  void node.offsetWidth;
  node.classList.add(className);
  setTimeout(() => node.classList.remove(className), duration);
}

function showMessage(message, error = false) {
  el('message').textContent = message;
  el('message').classList.toggle('error', error);
}

function connection(live) {
  const plate = el('connection-plate');
  plate.dataset.state = live ? 'live' : 'lost';
  el('connection').textContent = live ? 'Live · backend state' : 'Disconnected · displayed values may be stale';
  if (live) replay(plate.querySelector('.lamp--conn'), 'beat', 900);
}

// ── Live log ──
const logbook = (() => {
  const LIMIT = 500;
  const view = el('logs');
  const entries = [];
  let lastTop = [];
  let filter = 'all';
  let query = '';
  let follow = true;
  let unseen = 0;
  let loaded = false;

  function classify(message) {
    const traffic = /^(INSERT|UPDATE|DELETE|READ) → /.test(message);
    let cat = 'migration';
    if (traffic || /^Traffic /.test(message)) cat = 'traffic';
    else if (/^Validation|validation/i.test(message)) cat = 'validation';
    else if (/Cutover|cutover|Write freeze|fence|Migration completed|^Stage: (FREEZE|VALIDATION|CUTOVER|COMPLETED)/.test(message)) cat = 'cutover';
    else if (/Change Tracking|net changes|CDC|^Manual (INSERT|UPDATE|DELETE)/.test(message)) cat = 'cdc';
    let level = 'info';
    if (/✗$|^(CRASHED|FAILED)|FAILED|cannot|blocked|error/i.test(message) && !/✓$/.test(message)) level = 'error';
    else if (/^PAUSED|Crash armed|paused|Traffic stopped after/.test(message)) level = 'warn';
    else if (!traffic && /passed|Passed|completed successfully|successful cutover|Reset complete|ANALYZE complete|^Stage: COMPLETED/.test(message)) level = 'ok';
    return { cat, level };
  }

  function parse(line) {
    const match = /^\[(\d{2}:\d{2}:\d{2})\] ([\s\S]*)$/.exec(line);
    const time = match ? match[1] : '';
    const message = match ? match[2] : line;
    return { time, message, ...classify(message) };
  }

  const LABEL = { migration: 'Migration', cdc: 'Changes', validation: 'Validation', cutover: 'Cutover', traffic: 'Traffic' };
  const LEVEL = { info: 'INFO', ok: 'OK', warn: 'WARN', error: 'ERR' };
  const matches = entry => (entry.gap || filter === 'all' || (filter === 'problems' ? ['error', 'warn'].includes(entry.level) : entry.cat === filter))
    && (!query || (entry.message || '').toLowerCase().includes(query));

  function row(entry, fresh) {
    if (entry.gap) {
      const gap = document.createElement('p');
      gap.className = 'gap';
      gap.textContent = '… older lines rotated out of the backend log before this page saw them';
      return gap;
    }
    const line = document.createElement('div');
    line.className = 'row';
    line.dataset.level = entry.level;
    line.dataset.cat = entry.cat;
    if (fresh) line.classList.add('is-new');
    const time = document.createElement('time');
    time.textContent = entry.time;
    const level = document.createElement('span');
    level.className = `lvl lvl--${entry.level}`;
    level.textContent = LEVEL[entry.level];
    const cat = document.createElement('span');
    cat.className = 'cat';
    cat.textContent = LABEL[entry.cat];
    const message = document.createElement('span');
    message.className = 'msg';
    message.textContent = entry.message;
    line.append(time, level, cat, message);
    return line;
  }

  function counts() {
    const tally = { all: 0, problems: 0, migration: 0, cdc: 0, validation: 0, cutover: 0, traffic: 0 };
    for (const entry of entries) {
      if (entry.gap) continue;
      tally.all++;
      tally[entry.cat]++;
      if (entry.level === 'error' || entry.level === 'warn') tally.problems++;
    }
    document.querySelectorAll('#log-filters .chip').forEach(chip => {
      chip.querySelector('span').textContent = number(tally[chip.dataset.filter]);
      if (chip.dataset.filter === 'problems') chip.dataset.has = String(tally.problems > 0);
    });
  }

  function redraw() {
    const visible = entries.filter(matches);
    view.replaceChildren(...(visible.length ? visible.map(entry => row(entry, false)) : [empty()]));
    unseen = 0;
    jump();
    if (follow) view.scrollTop = view.scrollHeight;
  }

  function empty() {
    const p = document.createElement('p');
    p.className = 'terminal__empty';
    p.textContent = entries.length ? 'No lines match this filter.' : 'No log lines yet.';
    return p;
  }

  function jump() {
    el('log-jump').hidden = follow || unseen === 0;
    el('log-jump-count').textContent = `${number(unseen)} new`;
  }

  function setFollow(value) {
    follow = value;
    el('log-follow').setAttribute('aria-pressed', String(value));
    if (value) { unseen = 0; view.scrollTop = view.scrollHeight; }
    jump();
  }

  function ingest(lines) {
    if (!lines.length && !entries.length) {
      if (!loaded) { loaded = true; redraw(); }
      return;
    }
    let fresh;
    const at = lastTop.length ? lines.findIndex((line, index) => line === lastTop[0] && (lastTop[1] === undefined || lines[index + 1] === lastTop[1])) : -1;
    if (!lastTop.length) fresh = lines.slice().reverse().map(parse);
    else if (at >= 0) fresh = lines.slice(0, at).reverse().map(parse);
    else fresh = [{ gap: true }, ...lines.slice().reverse().map(parse)];
    lastTop = lines.slice(0, 2);
    if (!fresh.length) return;

    entries.push(...fresh);
    const overflow = entries.length - LIMIT;
    if (overflow > 0) entries.splice(0, overflow);
    counts();
    if (!loaded || overflow > 0) { loaded = true; redraw(); return; }

    const visible = fresh.filter(matches);
    if (!visible.length) return;
    view.querySelector('.terminal__empty')?.remove();
    view.append(...visible.map(entry => row(entry, !reducedMotion.matches)));
    if (follow) view.scrollTop = view.scrollHeight;
    else { unseen += visible.length; jump(); }
  }

  el('log-filters').addEventListener('click', event => {
    const chip = event.target.closest('.chip');
    if (!chip) return;
    filter = chip.dataset.filter;
    document.querySelectorAll('#log-filters .chip').forEach(c => c.setAttribute('aria-pressed', String(c === chip)));
    redraw();
  });
  el('log-search').addEventListener('input', event => { query = event.target.value.trim().toLowerCase(); redraw(); });
  el('log-follow').addEventListener('click', () => setFollow(!follow));
  el('log-jump').addEventListener('click', () => setFollow(true));
  view.addEventListener('scroll', () => {
    const atBottom = view.scrollHeight - view.scrollTop - view.clientHeight < 24;
    if (atBottom !== follow) setFollow(atBottom);
  }, { passive: true });

  return { ingest };
})();

async function refresh() {
  try {
    const response = await fetch('/api/status');
    if (!response.ok) throw new Error('Backend unavailable');
    render(await response.json());
  } catch (error) {
    connection(false);
    document.dispatchEvent(new CustomEvent('migration-disconnected'));
    document.querySelectorAll('[data-action]').forEach(button => { button.disabled = true; });
    el('seed-rows').disabled = true;
  }
}

function settle(button, state, duration) {
  button.dataset.state = state;
  clearTimeout(button.settleTimer);
  button.settleTimer = setTimeout(() => { delete button.dataset.state; }, duration);
}

document.querySelector('.desk').addEventListener('click', async event => {
  const button = event.target.closest('[data-action]');
  if (!button || busy) return;
  const action = button.dataset.action;
  if (action === 'reset' && !confirm('Delete demo data in both databases and reset migration progress?')) return;
  busy = true;
  clearTimeout(button.settleTimer);
  button.dataset.state = 'loading';
  button.setAttribute('aria-busy', 'true');
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
    settle(button, body.message.includes('FAILED') ? 'error' : 'success', 1400);
    if (action === 'reset') document.dispatchEvent(new CustomEvent('migration-reset'));
  } catch (error) {
    showMessage(error.message, true);
    settle(button, 'error', 1600);
  } finally {
    button.removeAttribute('aria-busy');
    busy = false;
    await refresh();
  }
});

async function poll() { await refresh(); setTimeout(poll, 1000); }
poll();
