// Kafka internals (Phase 2): a separate 3-node KRaft cluster behind the kafka-lab Compose profile.
// Renders /api/kafka-lab/state, polled with the rest of the page. Every lever is a real kill, stop
// or freeze of a node's container, or a Kafka admin call; nothing here advances on its own timer.
// Loaded before events.js and uses its helpers (h, chip, renderIf, showMessage…) at call time only.

let kafka = null; // /api/kafka-lab/state
let activeKafkaLab = null;
const kafkaProbes = []; // this page's single probe writes, newest first
let kafkaLogs = null; // a node's own truncation log lines, fetched on request

async function refreshKafka() {
  try {
    const response = await fetch('/api/kafka-lab/state');
    const body = await response.json();
    kafka = response.ok ? body : { error: body.detail || `HTTP ${response.status}` };
  } catch (e) {
    kafka = { error: e.message };
  }
}

/** A Kafka lab lever. Messages go to the Kafka section's own line. */
async function kact(method, path, pending) {
  if (busy) return;
  busy = true;
  messageTarget = 'kafka-message';
  showMessage(pending || 'Working…');
  lastRender.clear();
  renderAll();
  try {
    const response = await fetch(`/api/kafka-lab${path}`, { method });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`);
    return body;
  } catch (error) {
    showMessage(error.message, true);
    throw error;
  } finally {
    busy = false;
    await refreshKafka();
    lastRender.clear();
    renderAll();
  }
}
/** Several levers in order, stopping at the first that fails. */
async function ksteps(list, done) {
  try {
    for (const [method, path, pending] of list) await kact(method, path, pending);
    if (done) showMessage(done);
  } catch { /* the failing step's message is shown */ }
}
const kbtn = (label, onclick, tone = '', title) => h('button', { type: 'button', class: `small-button ${tone}`, disabled: busy, title, onclick }, label);
const nodeAction = (id, action, pending) => kact('POST', `/nodes/${id}/${action}`, pending);

// ---- Facts read from the snapshot -------------------------------------------------------------------
const kc = () => kafka?.cluster;
const knodes = () => kc()?.nodes || [];
const ktopic = name => (kc()?.topics || []).find(t => t.name === name);
const kpartition = (name, p = 0) => ktopic(name)?.partitions?.[p];
const quorumLeader = () => kc()?.quorum?.leaderId ?? null;
const nodesUp = () => knodes().filter(n => n.state === 'running' && n.registeredBroker).length;
const kafkaReady = () => knodes().length === 3 && nodesUp() === 3 && quorumLeader() !== null;
/** A follower of the partition that is not the quorum leader: failing it never touches the quorum's majority. */
function followerOf(name, p = 0) {
  const part = kpartition(name, p);
  if (!part) return null;
  const others = part.replicas.filter(r => r !== part.leader);
  return others.find(r => r !== quorumLeader()) ?? others[0] ?? null;
}
const run = lab => kafka?.runs?.[lab]?.[0];
const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;

// ---- Cluster strip -------------------------------------------------------------------------------------
const NODE_STATE = { running: ['up', 'good'], paused: ['frozen', 'warn'], exited: ['down', 'bad'], dead: ['down', 'bad'], created: ['created', ''], restarting: ['restarting', 'warn'] };
function nodeCard(n) {
  const [label, tone] = NODE_STATE[n.state] || [n.state, ''];
  const voter = kc()?.quorum?.voters?.find(v => v.id === n.id);
  return h('article', { class: `kafka-node st-${n.state}`, 'aria-label': `Node ${n.id}: ${label}` },
    h('header', {}, h('b', {}, `Node ${n.id}`), chip(label, tone), n.quorumLeader ? chip('quorum leader', 'info') : null),
    h('dl', {},
      h('dt', {}, 'Controller'), h('dd', {}, n.quorumLeader ? 'leads the metadata log' : voter ? `voter · ${voter.lag} behind${voter.lastFetchAgoMs != null ? ` · fetched ${Math.round(voter.lastFetchAgoMs / 1000)} s ago` : ''}` : 'not answering'),
      h('dt', {}, 'Broker'), h('dd', {}, n.registeredBroker ? `registered · leads ${plural(n.leads, 'partition')}` : 'not registered')),
    h('div', { class: 'buttons' }, ...(n.state === 'running'
      ? [kbtn('Kill', () => nodeAction(n.id, 'kill', `SIGKILL to node ${n.id}: a crash, no controlled shutdown…`), 'danger-soft', 'SIGKILL: the process dies at once'),
        kbtn('Stop', () => nodeAction(n.id, 'stop', `Stopping node ${n.id} gracefully: it hands its leaderships over first…`), '', 'SIGTERM: Kafka’s controlled shutdown'),
        kbtn('Freeze', () => nodeAction(n.id, 'pause', `Freezing node ${n.id}: alive to TCP, silent to Kafka…`), '', 'docker pause: every process stops running')]
      : n.state === 'paused' ? [kbtn('Unfreeze', () => nodeAction(n.id, 'unpause', `Unfreezing node ${n.id}…`))]
        : [kbtn('Start', () => nodeAction(n.id, 'start', `Starting node ${n.id}: it rejoins, truncates if it must, and catches up…`))])));
}
function clusterStrip() {
  if (!kafka) return [empty('Reading the Kafka lab…')];
  if (kafka.error) return [empty(`The control plane could not read the Kafka lab: ${kafka.error}`)];
  const c = kc();
  if (!c.configured || !c.nodes.length)
    return [h('div', { class: 'kafka-off' },
      h('p', {}, 'The 3-node lab cluster is not running. It is a Compose profile, so the everyday stack stays small (about 1.3 GB more when it runs):'),
      h('code', {}, 'docker compose --profile kafka-lab up -d'),
      ...c.problems.map(p => h('p', { class: 'muted' }, p)))];
  const q = c.quorum;
  return [
    h('div', { class: 'kafka-nodes' }, ...c.nodes.map(nodeCard)),
    h('div', { class: 'kafka-toolbar' },
      h('p', {}, q ? (q.leaderId !== null ? `Controller quorum: node ${q.leaderId} leads, epoch ${q.leaderEpoch}, metadata log at ${number(q.highWatermark)} · ${nodesUp()} of 3 brokers registered` : 'Controller quorum: no leader') : 'Controller quorum: not answering (no majority, or no node reachable)'),
      h('div', { class: 'buttons' },
        kbtn('Recover all nodes', () => kact('POST', '/recover', 'Starting and unfreezing every node, waiting until all three are registered…').then(() => showMessage('All three nodes are back.'), () => {})),
        kbtn('Elect preferred leaders', () => kact('POST', '/elections/preferred', 'Moving leadership back to each partition’s first replica…').then(r => showMessage(`${plural(r.partitionsMoved, 'partition')} moved back to their preferred leader.`), () => {})),
        kbtn('Reset lab', () => confirm('Recover every node, delete the scenario topics and recreate the standing ones?') && kact('POST', '/reset', 'Resetting the lab…').then(() => showMessage('Lab reset: every node up, standing topics in place.'), () => {}), 'danger-soft'))),
    c.problems.length ? h('p', { class: 'evidence-note kafka-problems' }, `Not answering right now: ${c.problems.join(' · ')}`) : null];
}

// ---- Shared evidence ----------------------------------------------------------------------------------------
function partitionTable(names) {
  const topics = names.map(ktopic).filter(Boolean);
  if (!topics.length) return empty('Not created yet.');
  return h('div', {},
    h('table', { class: 'data-table' },
      h('thead', {}, h('tr', {}, ...['Partition', 'Leader', 'Replicas', 'High watermark', 'Last stable', 'min ISR'].map(x => h('th', {}, x)))),
      h('tbody', {}, ...topics.flatMap(t => t.partitions.map(p => h('tr', {},
        h('td', { class: 'mono' }, `${t.name}-${p.partition}`),
        h('td', {}, p.leader == null ? chip('offline', 'bad') : `node ${p.leader}`),
        h('td', {}, h('div', { class: 'chips' }, ...p.replicas.map(r => {
          const inSync = p.isr.includes(r), eligible = p.elr.includes(r);
          return h('span', { class: `chip ${inSync ? 'good' : eligible ? 'warn' : 'bad'}`, title: inSync ? 'in sync (ISR)' : eligible ? 'eligible leader replica (ELR): not in sync, but has every committed record' : 'out of sync' },
            `${r}${r === p.leader ? ' ★' : ''} ${inSync ? 'ISR' : eligible ? 'ELR' : 'out'}`);
        }))),
        h('td', { class: 'num' }, p.highWatermark ?? '—'),
        h('td', { class: 'num' }, p.lastStable ?? '—'),
        h('td', { class: 'num' }, t.minInsyncReplicas ?? '—')))))),
    h('p', { class: 'evidence-note' }, '★ leader · ISR in sync · ELR eligible (has every committed record, not in sync) · out: missing records. High watermark: what consumers may read; last stable: what read_committed consumers may read.'));
}
function changesFeed(filter = () => true, limit = 12) {
  const list = (kc()?.changes || []).filter(filter).slice(0, limit);
  return list.length ? h('div', { class: 'feed' }, ...list.map(c => h('div', { class: 'feed-row kafka-change' }, h('time', {}, seconds(c.at)), chip(c.kind, c.kind === 'node' ? '' : c.kind === 'leader' ? 'info' : c.kind === 'quorum' ? 's-compensated' : 'warn'), h('p', {}, c.text))))
    : empty('No changes seen yet. Every change the observer sees between two snapshots (a second apart) lands here.');
}
const stepsTimeline = list => timeline((list || []).map(s => ['done', s.text, null, s.at]));
function kafkaUnavailable() {
  if (!kafka || kafka.error || !kc()?.configured || !knodes().length) return empty('Start the lab cluster first (see above).');
  return null;
}

// ---- Tabs ---------------------------------------------------------------------------------------------------
const KAFKA_LABS = [
  { id: 'cluster', label: 'Cluster & quorum', status: clusterStatus, render: renderClusterLab },
  { id: 'failover', label: 'Leader failure', status: failoverStatus, render: renderFailoverLab },
  { id: 'durability', label: 'acks & min ISR', status: durabilityStatus, render: renderDurabilityLab },
  { id: 'unclean', label: 'Unclean election', status: uncleanStatus, render: renderUncleanLab },
  { id: 'delivery', label: 'Delivery semantics', status: deliveryStatus, render: renderDeliveryLab }
];
function renderKafka() {
  if (!el('kafka-panel')) return;
  activeKafkaLab ??= readPref('events.kafkaLab', 'cluster');
  const c = kc();
  const nodeKey = (c?.nodes || []).map(n => [n.id, n.state, n.registeredBroker, n.quorumLeader, n.quorumLag, n.leads]);
  const voterKey = (c?.quorum?.voters || []).map(v => [v.id, v.lag, Math.round((v.lastFetchAgoMs ?? -1000) / 1000)]);
  renderIf('kafka-cluster', [kafka?.error, c?.configured, nodeKey, voterKey, c?.quorum?.leaderId, c?.quorum?.leaderEpoch, c?.problems, busy], () => el('kafka-cluster').replaceChildren(...clusterStrip().filter(Boolean)));
  renderIf('kafka-tabs', [KAFKA_LABS.map(l => l.status()), activeKafkaLab], () => el('kafka-tabs').replaceChildren(...KAFKA_LABS.map(l => {
    const [text, tone] = l.status();
    return h('button', { type: 'button', role: 'tab', id: `kafka-tab-${l.id}`, 'aria-selected': String(l.id === activeKafkaLab), 'aria-controls': 'kafka-panel',
      onclick: () => { activeKafkaLab = l.id; writePref('events.kafkaLab', l.id); renderAll(); } }, l.label, h('span', { class: `count ${tone}` }, text));
  })));
  const lab = KAFKA_LABS.find(l => l.id === activeKafkaLab) || KAFKA_LABS[0];
  el('kafka-panel').setAttribute('aria-labelledby', `kafka-tab-${lab.id}`);
  renderIf('kafka-panel', [lab.id, c?.topics, c?.groups, c?.changes?.[0]?.at, kafka?.traffic, kafka?.runs, busy, kafkaProbes.length, kafkaLogs, c?.quorum, nodeKey], () => {
    const message = el('kafka-message')?.textContent || '';
    const messageError = el('kafka-message')?.classList.contains('error');
    el('kafka-panel').replaceChildren(...[lab.render()].flat().filter(Boolean), h('p', { id: 'kafka-message', class: `composer-message ${messageError ? 'error' : ''}`, role: 'status', 'aria-live': 'polite' }, message));
  });
}

// Lab 1: the cluster itself: three nodes, a Raft quorum of controllers, replicated partitions.
function clusterStatus() {
  if (!kafka || kafka.error || !knodes().length) return ['not running', ''];
  const up = nodesUp();
  if (quorumLeader() === null) return ['no quorum', 'bad'];
  return up === 3 ? ['3 of 3 up', 'good'] : [`${up} of 3 up`, 'warn'];
}
function renderClusterLab() {
  const off = kafkaUnavailable();
  const leader = quorumLeader();
  const q = kc()?.quorum;
  const running = knodes().filter(n => n.state === 'running');
  const stopTwo = () => {
    const victims = running.map(n => n.id).filter(id => id !== leader).slice(0, 2);
    if (victims.length < 2) return showMessage('Two running nodes other than the quorum leader are needed.', true);
    if (!confirm(`Stop nodes ${victims.join(' and ')}? Node ${leader} is left alone: one voter of three is not a majority.`)) return;
    ksteps(victims.map(id => ['POST', `/nodes/${id}/stop`, `Stopping node ${id}…`]), `Nodes ${victims.join(' and ')} stopped. Node ${leader} alone has no majority: watch the quorum stop answering, and nothing change.`);
  };
  return h('div', { class: 'lab-grid' },
    steps([
      ['Learn', 'Three nodes, each both a broker (it stores partitions) and a controller. The controllers form a Raft quorum: one leader appends the cluster’s metadata (topics, leaders, ISRs) to a log the other two replicate, and a change counts only once a majority, 2 of 3, has it.'],
      ['Trigger', 'Create the lab topics: lab.replicated has 3 partitions, each on all 3 nodes, and needs 2 in sync to accept a write.', [kbtn('Create lab topics', () => kact('POST', '/setup', 'Creating the standing lab topics…').then(() => showMessage('Topics in place: every partition has a leader and 3 in-sync replicas.'), () => {}))]],
      ['Observe', 'Each node’s roles, how far each voter is behind the metadata log, and for every partition its leader and in-sync replicas.'],
      ['Break', 'Kill the quorum leader: the other two elect a new one. Then stop two nodes: one voter cannot form a majority, so nothing can change any more: no elections, no new topics, no ISR updates.',
        [leader !== null ? kbtn(`Kill quorum leader (node ${leader})`, () => nodeAction(leader, 'kill', `Killing node ${leader}, the controller quorum leader…`).then(() => showMessage('Watch the quorum: the two remaining voters elect a leader, the epoch goes up.'), () => {}), 'danger-soft') : null,
          kbtn('Stop two nodes', stopTwo, 'danger-soft')]],
      ['Understand', 'A majority survives one failure, never two. Without a quorum the brokers keep serving the partitions they already lead, but a partition whose leader dies stays leaderless until the quorum is back.'],
      ['Fix', 'Run an odd number of voters in separate failure domains: 3 tolerate 1 failure, 5 tolerate 2. More voters cost write latency on every metadata change.'],
      ['Recover', 'Start every node: the voters catch up on the metadata log, the brokers re-register and their replicas catch up and rejoin the ISR.', [kbtn('Recover all nodes', () => kact('POST', '/recover', 'Starting every node…').then(() => showMessage('All three nodes are back.'), () => {}))]]
    ]),
    h('div', { class: 'lab-evidence' }, off || [
      evidence('Controller quorum (Raft)', q ? h('table', { class: 'data-table' },
        h('thead', {}, h('tr', {}, ...['Voter', 'Role', 'Log end', 'Behind', 'Last fetch'].map(x => h('th', {}, x)))),
        h('tbody', {}, ...q.voters.map(v => h('tr', {}, h('td', {}, `node ${v.id}`), h('td', {}, v.id === q.leaderId ? chip('leader', 'info') : 'follower'),
          h('td', { class: 'num' }, number(v.logEndOffset)), h('td', { class: 'num' }, v.lag ? chip(v.lag, 'warn') : '0'), h('td', { class: 'num' }, v.lastFetchAgoMs == null ? '—' : `${(v.lastFetchAgoMs / 1000).toFixed(1)} s ago`)))))
        : empty('The quorum is not answering: no majority, or no node reachable.')),
      evidence('Partitions', partitionTable(['lab.replicated', 'lab.durability'])),
      evidence('What changed', changesFeed())]));
}

// Lab 2: a leader dies under load.
function failoverStatus() {
  const t = kafka?.traffic;
  if (t?.running) return [`traffic · ${number(t.acked)} acked`, t.failed ? 'warn' : 'good'];
  const r = run('failover');
  if (!r) return ['not run', ''];
  return r.result.lostAcknowledged ? [`${r.result.lostAcknowledged} lost`, 'bad'] : ['0 lost', 'good'];
}
function trafficChart(t) {
  const series = (t?.timeline || []).slice(-60);
  if (!series.length) return empty('No traffic yet.');
  const max = Math.max(1, ...series.map(s => s.acked + s.failed));
  return h('div', { class: 'traffic-chart', role: 'img', 'aria-label': 'Acknowledged and failed writes per second' },
    ...series.map(s => h('span', { title: `${seconds(s.at * 1000)} · ${s.acked} acked · ${s.failed} failed · ${s.consumed} consumed · slowest ${s.maxLatencyMs} ms` },
      h('i', { class: 'ok', style: `height:${s.acked / max * 100}%` }), s.failed ? h('i', { class: 'fail', style: `height:${s.failed / max * 100}%` }) : null)));
}
function renderFailoverLab() {
  const off = kafkaUnavailable();
  const t = kafka?.traffic;
  const p0 = kpartition('lab.replicated', 0);
  const leader = p0?.leader;
  const down = knodes().filter(n => n.state !== 'running');
  const group = (kc()?.groups || []).find(g => g.groupId === 'lab.replicated.reader');
  const last = run('failover');
  return h('div', { class: 'lab-grid' },
    steps([
      ['Learn', 'Each partition has one leader; producers and consumers talk only to it, followers copy its log. When the leader’s broker dies, the controller notices once its heartbeats stop (broker.session.timeout.ms, 6 s here) and makes an in-sync follower leader.'],
      ['Trigger', 'Write 20 records a second into lab.replicated with the safe producer (acks=all, idempotent) and read them back in a consumer group.',
        [kbtn(t?.running ? 'Traffic running' : 'Start traffic', () => kact('POST', '/traffic/start', 'Starting producer and consumer…').then(() => showMessage('Traffic flowing: every second shows acknowledged and failed writes.'), () => {}), '', null)]],
      ['Observe', 'Writes acknowledged and failed per second, the slowest acknowledgement, and every leader and ISR change as the controller makes it.'],
      ['Break', 'Kill the leader of lab.replicated-0: its partitions stall until the session expires. Or stop it gracefully: a controlled shutdown hands leadership over first, so the stall is milliseconds.',
        leader != null ? [kbtn(`Kill node ${leader} (leads p0)`, () => nodeAction(leader, 'kill', `Killing node ${leader}, leader of lab.replicated-0…`).then(() => showMessage('Watch the chart stall for about 6 s, then the leaders move.'), () => {}), 'danger-soft'),
          kbtn(`Stop node ${leader} gracefully`, () => nodeAction(leader, 'stop', `Controlled shutdown of node ${leader}…`).then(() => showMessage('Leadership moved before the process exited: compare the stall.'), () => {}))] : [chip('lab.replicated-0 has no leader', 'bad')]],
      ['Understand', 'The stall is failure detection, not the election, which takes milliseconds. Nothing acknowledged is lost: acks=all waited for 2 in-sync copies and the new leader is one of them; the retries after the failover are deduplicated by the idempotent producer.'],
      ['Fix', 'Nothing to fix in the producer: acks=all, idempotence and min.insync.replicas=2 on 3 replicas is the safe setting. Detection can be tuned faster only as far as GC pauses and network hiccups allow.'],
      ['Recover', 'Start the node: it truncates to the new leader’s log, catches up and rejoins the ISR; leadership drifts back to preferred replicas within 30 s, or now. Stop the traffic to count every record.',
        [...down.map(n => kbtn(`Start node ${n.id}`, () => nodeAction(n.id, n.state === 'paused' ? 'unpause' : 'start', `Starting node ${n.id}…`))),
          kbtn('Elect preferred leaders', () => kact('POST', '/elections/preferred', 'Electing preferred leaders…').then(r => showMessage(`${plural(r.partitionsMoved, 'partition')} moved back.`), () => {})),
          t?.running ? kbtn('Stop traffic and count', () => kact('POST', '/traffic/stop', 'Stopping; waiting for every in-flight record and for the consumer to drain…').then(v => showMessage(`${number(v.acked)} acknowledged, ${number(v.lostAcknowledged)} of them never consumed, ${number(v.duplicates)} consumed twice.`), () => {})) : null]]
    ]),
    h('div', { class: 'lab-evidence' }, off || [
      evidence(t?.running ? 'Traffic now' : t?.run ? 'Last traffic' : 'Traffic',
        h('div', { class: 'big-facts' },
          ...[['sent', t?.sent], ['acknowledged', t?.acked], ['failed', t?.failed, t?.failed], ['consumed', t?.consumed], ['consumed twice', t?.duplicates, t?.duplicates], ['retries', t?.retries], ['in flight', t?.inFlight], ['slowest ack', t?.maxLatencyMs != null ? `${number(t.maxLatencyMs)} ms` : null]]
            .map(([label, value, bad]) => h('div', { class: bad ? 'bad' : '' }, h('strong', {}, typeof value === 'string' ? value : number(value)), label)),
          t?.lostAcknowledged != null ? h('div', { class: t.lostAcknowledged ? 'bad' : '' }, h('strong', {}, number(t.lostAcknowledged)), 'acknowledged, never consumed') : null),
        trafficChart(t),
        Object.keys(t?.errors || {}).length ? h('p', { class: 'evidence-note' }, `Errors: ${Object.entries(t.errors).map(([k, v]) => `${k} × ${v}`).join(', ')}`) : null,
        group ? h('p', { class: 'evidence-note' }, `Consumer group ${group.groupId}: ${group.state.toLowerCase()}, ${plural(group.members, 'member')}, lag ${group.partitions.reduce((n, p) => n + (p.lag || 0), 0)}`) : null),
      evidence('lab.replicated', partitionTable(['lab.replicated'])),
      evidence('Leaders and ISRs as they changed', changesFeed(c => c.kind !== 'elr', 14)),
      last ? evidence('Last counted run', h('p', {}, last.summary), h('p', { class: 'evidence-note' }, `${seconds(last.at)} · slowest acknowledgement ${number(last.result.maxLatencyMs)} ms · ${number(last.result.retries)} producer retries`)) : null]));
}

// Lab 3: what an acknowledgement is worth.
function durabilityStatus() {
  const r = run('acks');
  if (!r) return ['not run', ''];
  return r.result.lost.length ? [`acks=${r.result.acks}: ${r.result.lost.length} lost`, 'bad'] : [`acks=${r.result.acks}: 0 lost`, 'good'];
}
function acksTable(runs) {
  const latest = ['1', 'all'].map(a => runs.find(r => r.result.acks === a)).filter(Boolean);
  if (!latest.length) return empty('Run a crash to compare.');
  return h('table', { class: 'data-table' },
    h('thead', {}, h('tr', {}, h('th', {}, 'Record'), ...latest.map(r => h('th', {}, `acks=${r.result.acks}`)))),
    h('tbody', {}, ...[6, 7, 8, 9, 10].map(seq => h('tr', {}, h('td', { class: 'mono' }, seq), ...latest.map(r => {
      const acked = r.result.acknowledged.includes(seq), present = r.result.present.includes(seq);
      return h('td', {}, acked && !present ? chip('acknowledged, then lost', 'bad') : acked ? chip('acknowledged, kept', 'good') : present ? chip('kept', 'good') : chip('never acknowledged', 'warn'));
    })))));
}
function renderDurabilityLab() {
  const off = kafkaUnavailable();
  const acksRuns = kafka?.runs?.acks || [];
  const last = acksRuns[0];
  const retries = kafka?.runs?.retries || [];
  const durability = ktopic('lab.durability');
  const minIsr = durability?.minInsyncReplicas;
  const follower = followerOf('lab.durability');
  const crashed = last ? knodes().find(n => n.id === last.result.leader && n.state !== 'running') : null;
  const probe = acks => kact('POST', `/probe?acks=${acks}`, `One write to lab.durability with acks=${acks}…`).then(p => {
    kafkaProbes.unshift({ ...p, at: new Date().toISOString() });
    kafkaProbes.splice(8);
    showMessage(p.written ? `acks=${acks}: written at offset ${p.offset} in ${p.ms} ms` : `acks=${acks}: refused, ${p.error}`, !p.written);
  }, () => {});
  const setMinIsr = value => kact('PUT', `/topics/lab.durability/min-insync-replicas?value=${value}`, `Setting min.insync.replicas=${value} on lab.durability…`).then(() => showMessage(`lab.durability now needs ${value} in-sync replicas for acks=all.`), () => {});
  const acksRun = acks => kact('POST', `/scenarios/acks?acks=${acks}`, acks === '1' ? 'Freezing the follower, writing with acks=1, killing the leader… (about 15 s)' : 'Same crash with acks=all: the producer waits, retries against the new leader… (about 30 s)')
    .then(r => { kafkaLogs = null; showMessage(r.summary); }, () => {});
  const showTruncation = () => kact('GET', `/nodes/${last.result.leader}/truncations?partition=lab.acks-0&since=${encodeURIComponent(last.at)}`, `Reading node ${last.result.leader}’s own log…`)
    .then(l => { kafkaLogs = l; showMessage(l.lines.length ? `Node ${l.node} logged ${plural(l.lines.length, 'truncation line')}.` : `Node ${l.node} has not truncated lab.acks-0 yet: start it and let it catch up.`); }, () => {});
  return h('div', { class: 'lab-grid' },
    steps([
      ['Learn', 'An acknowledgement means “the leader wrote it” under acks=1, and “every in-sync replica wrote it” under acks=all. min.insync.replicas says how many in-sync replicas acks=all needs before it accepts a write at all.'],
      ['Trigger', 'lab.acks has 2 replicas. The follower is frozen, records 6–10 are written with acks=1, then the leader is killed and the follower, still in the ISR, takes over.', [kbtn('acks=1: crash the leader', () => acksRun('1'), 'danger-soft')]],
      ['Observe', 'Which records the producer was told were written, and which exist once the follower leads. Then single writes against lab.durability as its ISR shrinks.'],
      ['Break', `Stop a follower of lab.durability, then require 3 in-sync replicas: acks=all is refused (NOT_ENOUGH_REPLICAS) while acks=1 still goes through. A producer without idempotence that times out and retries writes the record again.`,
        [follower != null ? kbtn(`Stop follower (node ${follower})`, () => nodeAction(follower, 'stop', `Stopping node ${follower}: lab.durability’s ISR shrinks to 2…`)) : null,
          kbtn(minIsr === 3 ? 'Requires 3 in sync' : 'Require 3 in sync', () => setMinIsr(3), 'danger-soft'),
          kbtn('Write acks=all', () => probe('all')), kbtn('Write acks=1', () => probe('1')),
          kbtn('Retries without idempotence', () => kact('POST', '/scenarios/retries?idempotent=false', 'Freezing a follower 4 s; one record, 800 ms request timeout…').then(r => showMessage(r.summary), () => {}), 'danger-soft')]],
      ['Understand', 'acks=1 acknowledges before replication: a leader crash in that window loses acknowledged data, and the old leader throws it away when it returns. acks=all never acknowledges what is not on every in-sync replica, and min.insync.replicas stops “every in-sync replica” from quietly meaning one. Retries are only safe when the broker can recognise them.'],
      ['Fix', 'acks=all, enable.idempotence=true and min.insync.replicas=2 on 3 replicas: one node can fail with no loss and no refused writes.',
        [kbtn('acks=all: same crash', () => acksRun('all')), kbtn('Retries with idempotence', () => kact('POST', '/scenarios/retries?idempotent=true', 'Same slow follower, idempotent producer…').then(r => showMessage(r.summary), () => {})),
          kbtn('Back to min ISR 2', () => setMinIsr(2))]],
      ['Recover', 'Start the crashed leader: it finds its log diverged from the new leader’s epoch and truncates what nobody else has. Its own log says so.',
        [crashed ? kbtn(`Start node ${crashed.id}`, () => nodeAction(crashed.id, crashed.state === 'paused' ? 'unpause' : 'start', `Starting node ${crashed.id}…`)) : null,
          last ? kbtn(`Show node ${last.result.leader}’s truncation`, showTruncation) : null,
          ...knodes().filter(n => n.state !== 'running' && n.id !== crashed?.id).map(n => kbtn(`Start node ${n.id}`, () => nodeAction(n.id, n.state === 'paused' ? 'unpause' : 'start', `Starting node ${n.id}…`)))]]
    ]),
    h('div', { class: 'lab-evidence' }, off || [
      evidence('What the producer was told vs. what survived', acksTable(acksRuns),
        last ? h('p', { class: 'evidence-note' }, `Latest: ${last.summary}`) : null),
      last ? evidence(`How the last run went · acks=${last.result.acks}`, stepsTimeline(last.result.steps)) : null,
      kafkaLogs ? evidence(`Node ${kafkaLogs.node}’s own log`, kafkaLogs.lines.length ? h('pre', { class: 'log-lines' }, kafkaLogs.lines.join('\n')) : empty('No truncation logged for this partition yet.')) : null,
      evidence('lab.durability', partitionTable(['lab.durability']),
        kafkaProbes.length ? h('div', { class: 'feed' }, ...kafkaProbes.map(p => h('div', { class: 'feed-row' }, h('time', {}, seconds(p.at)), chip(`acks=${p.acks}`, p.written ? 'good' : 'bad'),
          h('p', {}, p.written ? `written at offset ${p.offset}` : p.error, h('small', {}, `ISR ${JSON.stringify(p.isr)} · min ISR ${p.minInsyncReplicas ?? '—'} · ${p.ms} ms`))))) : empty('No single writes yet.')),
      retries.length ? evidence('Producer retries', h('div', { class: 'feed' }, ...retries.slice(0, 4).map(r => h('div', { class: 'feed-row' }, h('time', {}, seconds(r.at)), chip(r.result.idempotent ? 'idempotent' : 'plain', r.result.copies > 1 ? 'bad' : 'good'),
        h('p', {}, `${r.result.copies} ${r.result.copies === 1 ? 'copy' : 'copies'} in the log after ${r.result.retries} ${r.result.retries === 1 ? 'retry' : 'retries'}`, h('small', {}, r.summary)))))) : null]));
}

// Lab 4: the last in-sync replica dies.
function uncleanStatus() {
  const r = kafka?.runs?.unclean?.[0];
  if (!r) return ['not run', ''];
  if (r.result.phase === 'offline') return (kpartition('lab.unclean')?.leader == null) ? ['offline', 'bad'] : ['back online', 'warn'];
  return r.result.lost.length ? [`${r.result.lost.length} lost`, 'bad'] : ['nothing lost', 'good'];
}
function renderUncleanLab() {
  const off = kafkaUnavailable();
  const runs = kafka?.runs?.unclean || [];
  const broken = runs.find(r => r.mode === 'break');
  const latest = runs[0];
  const part = kpartition('lab.unclean');
  const inSyncNode = broken ? knodes().find(n => n.id === broken.result.leader) : null;
  const check = () => kact('POST', '/scenarios/unclean/check', 'Reading lab.unclean back…').then(r => showMessage(r.summary), () => {});
  return h('div', { class: 'lab-grid' },
    steps([
      ['Learn', 'When the last in-sync replica dies, Kafka takes the partition offline rather than hand leadership to a replica that may be missing acknowledged records. Kafka 4.1 tracks the replicas that are safe to wait for as ELR, eligible leader replicas.'],
      ['Trigger', 'lab.unclean: 2 replicas, min.insync.replicas=1. The follower is stopped, 3 records are acknowledged by the leader alone, the leader is killed and the stale follower comes back.', [kbtn('Break: the last in-sync replica dies', () => kact('POST', '/scenarios/unclean/break', 'Stopping the follower, writing, killing the leader, restarting the follower… (about 15 s)').then(r => showMessage(r.summary), () => {}), 'danger-soft')]],
      ['Observe', 'lab.unclean has no leader: a live replica that is out of sync, the dead one listed as ELR. Producers and consumers of this partition wait.'],
      ['Break', 'The dangerous way out: elect the stale follower anyway. The partition is writable again, without the records it never had.',
        [kbtn('Unclean election', () => confirm('Elect the out-of-sync follower leader of lab.unclean-0? Acknowledged records it does not have are lost for good.') && kact('POST', '/scenarios/unclean/elect', 'Admin.electLeaders(UNCLEAN)…').then(r => showMessage(r.summary), () => {}), 'danger-soft')]],
      ['Understand', 'Unclean election trades durability for availability: every consumer loses those acknowledged records, and when the old leader returns it truncates them to match. With unclean.leader.election.enable=false Kafka never does this on its own.'],
      ['Fix', 'Wait for the replica that has everything: start it and it leads again (it is in ELR), nothing lost. Then keep min.insync.replicas at 2 so one replica alone can never acknowledge.',
        [inSyncNode && inSyncNode.state !== 'running' ? kbtn(`Start node ${inSyncNode.id} (has every record)`, () => nodeAction(inSyncNode.id, 'start', `Starting node ${inSyncNode.id}…`)) : null, kbtn('Check what survived', check)]],
      ['Recover', 'After an unclean election, start the old leader and check again: its log shows the truncation. Reset returns every node and removes the scenario topics.',
        [inSyncNode && inSyncNode.state !== 'running' ? kbtn(`Start node ${inSyncNode.id}`, () => nodeAction(inSyncNode.id, 'start', `Starting node ${inSyncNode.id}…`)) : null, kbtn('Check what survived', check)]]
    ]),
    h('div', { class: 'lab-evidence' }, off || [
      evidence('lab.unclean now', part ? partitionTable(['lab.unclean']) : empty('Not created yet: run the break.')),
      latest ? evidence('Acknowledged vs. in the log', h('table', { class: 'data-table' }, h('tbody', {}, ...latest.result.acknowledged.map(v => h('tr', {}, h('td', { class: 'mono' }, v),
        h('td', {}, latest.result.phase === 'offline' ? chip('acknowledged', 'info') : latest.result.present.includes(v) ? chip('kept', 'good') : chip('acknowledged, then lost', 'bad')))))),
        h('p', { class: 'evidence-note' }, latest.summary)) : null,
      latest ? evidence(`How it went · ${latest.mode}`, stepsTimeline(latest.result.steps)) : null,
      latest?.result?.brokerLog?.length ? evidence('The brokers’ own log', h('pre', { class: 'log-lines' }, latest.result.brokerLog.join('\n'))) : null,
      runs.length > 1 ? evidence('Earlier', h('div', { class: 'feed' }, ...runs.slice(1, 6).map(r => h('div', { class: 'feed-row' }, h('time', {}, seconds(r.at)), chip(r.mode), h('p', {}, r.summary))))) : null]));
}

// Lab 5: where the crash falls decides the guarantee.
function deliveryStatus() {
  const r = run('delivery');
  if (!r) return ['not run', ''];
  const c = r.result.committed;
  return c.missing.length ? [`${c.missing.length} missing`, 'bad'] : c.duplicated.length ? [`${c.duplicated.length} duplicated`, 'warn'] : ['exactly once', 'good'];
}
function seqGrid(outcome) {
  return h('div', { class: 'seq-grid', role: 'img', 'aria-label': `${outcome.isolation}: ${outcome.missing.length} missing, ${outcome.duplicated.length} duplicated` },
    ...outcome.copies.map((n, i) => h('span', { class: n === 0 ? 'gap' : n > 1 ? 'dup' : 'once', title: `record ${i + 1}: ${n === 0 ? 'missing' : n === 1 ? 'once' : `${n} copies`}` }, n > 1 ? `${i + 1}×${n}` : i + 1)));
}
function renderDeliveryLab() {
  const off = kafkaUnavailable();
  const runs = kafka?.runs?.delivery || [];
  const r = runs[0];
  const go = (mode, crash) => kact('POST', `/delivery?mode=${mode}&crash=${crash}`, `Fresh topics, 30 input records, a ${mode} worker JVM${crash ? ' that crashes at record 12, then a restarted one' : ''}… (about 15 s)`).then(x => showMessage(x.summary), () => {});
  return h('div', { class: 'lab-grid' },
    steps([
      ['Learn', 'Processing a record is three things: read the input, write the output, commit the input offset. A worker can crash between any two of them; which two decides what the next worker does.'],
      ['Trigger', 'At most once: commit the offsets, then write. The worker JVM halts after committing record 12’s offset and before writing its output.', [kbtn('At-most-once, crash', () => go('at-most-once', true), 'danger-soft')]],
      ['Observe', 'Each input record 1–30 in the output: missing, once, or more than once, read as read_committed and as read_uncommitted, and each worker’s own account of what it did.'],
      ['Break', 'At least once: write, then commit. The worker halts after writing record 12 and before committing; the next one re-reads everything since the last commit.', [kbtn('At-least-once, crash', () => go('at-least-once', true), 'danger-soft')]],
      ['Understand', 'The write and the commit are two separate actions, so a crash can fall between them: commit first and records vanish (gaps), write first and they repeat (duplicates). No ordering of two separate actions is safe.'],
      ['Fix', 'Make them one: a Kafka transaction writes the outputs and the input offsets together. The crash aborts both; the restarted worker, with the same transactional.id, fences the dead one and redoes the batch. read_committed consumers see each record once; read_uncommitted consumers still see the aborted copies.',
        [kbtn('Exactly-once, crash', () => go('exactly-once', true)), kbtn('Exactly-once, no crash', () => go('exactly-once', false))]],
      ['Recover', 'Every run starts on fresh topics and a fresh consumer group, and is kept below. Runs without a crash show all three are correct when nothing goes wrong.',
        [kbtn('At-most-once, no crash', () => go('at-most-once', false)), kbtn('At-least-once, no crash', () => go('at-least-once', false))]]
    ]),
    h('div', { class: 'lab-evidence' }, off || (!r ? evidence('Evidence', empty('Run a mode to see every record’s fate.')) : [
      evidence(`${r.result.mode.replaceAll('_', '-').toLowerCase()}${r.result.crash ? `, crash at record ${r.result.crashAt}` : ', no crash'}`,
        h('p', {}, r.summary),
        h('h4', {}, `read_committed · ${r.result.committed.total} output records`), seqGrid(r.result.committed),
        h('h4', {}, `read_uncommitted · ${r.result.uncommitted.total} output records`), seqGrid(r.result.uncommitted),
        h('p', { class: 'evidence-note' }, 'Numbered cells are input records: plain = once, amber = n copies, red = missing. Committed input offsets at the end: ' + r.result.committedOffsets.map(o => `p${o.partition}→${o.committed}`).join(', '))),
      evidence('What each worker JVM did', ...r.result.workers.map(w => h('div', { class: 'worker-log' },
        h('p', {}, chip(`worker ${w.number}`, w.crashed ? 'bad' : 'good'), ` ${w.crashed ? `crashed (exit ${w.exitCode})` : `exited ${w.exitCode}`} after ${number(w.ms)} ms`),
        h('pre', { class: 'log-lines' }, w.events.join('\n'))))),
      runs.length > 1 ? evidence('All runs', runsTable(runs)) : null])));
}
const outcomeChips = o => [o.missing.length ? chip(`${o.missing.length} missing`, 'bad') : null, o.duplicated.length ? chip(`${o.duplicated.length} duplicated`, 'warn') : null,
  !o.missing.length && !o.duplicated.length ? chip('exactly once', 'good') : null];
function runsTable(runs) {
  return h('table', { class: 'data-table' },
    h('thead', {}, h('tr', {}, ...['At', 'Mode', 'Crash', 'read_committed', 'read_uncommitted'].map(x => h('th', {}, x)))),
    h('tbody', {}, ...runs.map(x => h('tr', {},
      h('td', {}, seconds(x.at)), h('td', {}, x.result.mode.replaceAll('_', '-').toLowerCase()), h('td', {}, x.result.crash ? 'yes' : 'no'),
      h('td', {}, ...outcomeChips(x.result.committed)), h('td', {}, ...outcomeChips(x.result.uncommitted))))));
}
