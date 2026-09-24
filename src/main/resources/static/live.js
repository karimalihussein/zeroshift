// This file only renders backend observations and submits operator commands.
(() => {
  const byId = id => document.getElementById(id);
  let state;
  let selected;
  let mode;
  let working = false;
  let reading = false;
  let connected = false;
  let generation = 0;
  const hint = (text, error = false) => {
    byId('live-hint').textContent = text;
    byId('live-hint').classList.toggle('error', error);
  };
  const writable = () => connected && state?.migration.status === 'RUNNING' && state.migration.primary === 'SQL_SERVER' && !['FREEZE', 'VALIDATION', 'CUTOVER'].includes(state.migration.stage);
  function controls() {
    for (const id of ['live-insert', 'live-update', 'live-delete', 'live-cdc', 'live-save']) byId(id).disabled = working || !writable();
    byId('live-cdc').querySelector('.key__label').textContent = state?.migration.cdcPaused ? 'Resume CDC replay' : 'Pause CDC replay';
    byId('live-cdc').setAttribute('aria-pressed', String(Boolean(state?.migration.cdcPaused)));
  }
  async function api(path, method = 'GET', body) {
    const response = await fetch(`/api/live${path}`, { method, headers: body ? { 'Content-Type': 'application/json' } : {}, body: body ? JSON.stringify(body) : undefined });
    const data = await response.json();
    if (!response.ok) throw new Error(data.detail || 'The live change could not be completed.');
    return data;
  }
  function render(record) {
    selected = record;
    byId('inspector').hidden = false;
    byId('inspect-title').textContent = `Order #${record.orderId} · Orders copied through ID ${record.ordersCopiedThrough}`;
    const body = byId('record-values');
    body.replaceChildren();
    for (const [label, field] of [['Record', 'id'], ['Customer', 'customerName'], ['Status', 'status'], ['Amount', 'amount']]) {
      const source = record.source?.[field] ?? 'Absent';
      const target = record.target?.[field] ?? 'Absent';
      const row = document.createElement('tr');
      if (source !== target) row.className = 'different';
      for (const value of [label, source, target]) {
        const cell = document.createElement('td'); cell.textContent = String(value); row.append(cell);
      }
      body.append(row);
    }
    byId('record-sync').textContent = `${record.message}${record.cdcPaused ? ' · replay paused' : ''}`;
    byId('record-sync').dataset.sync = String(record.inSync);
    const experiment = record.experiment;
    byId('record-before').textContent = experiment
      ? `Last manual ${experiment.operation}:\n${describe(experiment.before)} → ${describe(experiment.after)}` : '';
    const path = byId('record-path'); path.replaceChildren();
    if (experiment || record.changes.length) {
      const pending = record.changes.filter(change => change.pending);
      const steps = [
        `SQL Server · ${experiment?.operation || 'Change'} Order #${record.orderId}`,
        record.changes.length ? `CDC captured · ${record.changes.map(c => `${c.table.toLowerCase()} #${c.recordId}, version ${c.version}`).join('; ')}` : 'No retained CDC version for this record',
        pending.length ? `CDC pending · ${pending.length} changed key(s)${record.cdcPaused ? ' · replay paused' : ''}` : 'No pending CDC change for this record',
        record.inSync && record.changes.length ? `PostgreSQL · ${experiment?.operation || 'change'} applied · IN SYNC` : record.inSync ? 'Both databases have the same values' : 'PostgreSQL · waiting for snapshot or CDC replay'
      ];
      for (const text of steps) { const li = document.createElement('li'); li.textContent = text; path.append(li); }
    }
  }
  function describe(row) { return row ? `${row.customerName} · status: ${row.status} · amount: ${row.amount}` : 'Absent'; }
  async function inspect(id) { const record = await api(`/orders/${id}`); render(record); return record; }
  async function action(work) {
    if (working) return;
    working = true; controls();
    try { await work(); } catch (error) { hint(error.message, true); }
    finally { working = false; controls(); }
  }
  function edit(record, operation) {
    mode = operation;
    byId('live-edit').hidden = false;
    byId('edit-title').textContent = operation === 'insert' ? 'Create a customer and order in SQL Server' : `Update migrated Order #${record.orderId} in SQL Server`;
    byId('edit-name').value = record?.source?.customerName ?? 'Live customer';
    byId('edit-status').value = record?.source?.status ?? 'PENDING';
    byId('edit-amount').value = record?.source?.amount ?? '500';
    byId('edit-status').focus();
  }
  async function chooseCopied() {
    if (selected) {
      const record = await inspect(selected.orderId);
      if (record.source && record.target) return record;
    }
    const record = await api('/copied-order');
    byId('record-id').value = record.orderId; render(record); return record;
  }
  byId('live-insert').addEventListener('click', () => edit(null, 'insert'));
  byId('live-update').addEventListener('click', () => action(async () => edit(await chooseCopied(), 'update')));
  byId('live-delete').addEventListener('click', () => action(async () => {
    const record = await chooseCopied();
    if (!confirm(`Delete Order #${record.orderId} from SQL Server? PostgreSQL will change only when CDC replays the delete.`)) return;
    await api(`/orders/${record.orderId}`, 'DELETE');
    byId('live-edit').hidden = true;
    await inspect(record.orderId); hint(`Deleted Order #${record.orderId} in SQL Server. Its customer is preserved.`);
  }));
  byId('live-cdc').addEventListener('click', () => action(async () => {
    const result = await api(`/cdc/${state.migration.cdcPaused ? 'resume' : 'pause'}`, 'POST'); hint(result.message);
  }));
  byId('live-edit').addEventListener('submit', event => {
    event.preventDefault();
    action(async () => {
      const input = { customerName: byId('edit-name').value, status: byId('edit-status').value, amount: byId('edit-amount').value };
      const result = await api(mode === 'insert' ? '/orders' : `/orders/${selected.orderId}`, mode === 'insert' ? 'POST' : 'PUT', input);
      byId('record-id').value = result.orderId; byId('live-edit').hidden = true;
      await inspect(result.orderId); hint(`${result.operation} Order #${result.orderId} committed in SQL Server.`);
    });
  });
  byId('live-cancel').addEventListener('click', () => { byId('live-edit').hidden = true; });
  byId('inspect-form').addEventListener('submit', event => {
    event.preventDefault(); action(async () => { byId('live-edit').hidden = true; await inspect(byId('record-id').value); });
  });
  document.addEventListener('migration-state', async event => {
    state = event.detail; connected = true; controls();
    if (!selected && !working) hint(writable() ? `Migration ${Math.round(state.progress)}% · ${state.migration.cdcPaused ? 'CDC replay paused; source writes and snapshot continue.' : 'Select an already migrated order to begin.'}` : 'Start or resume a migration to try live changes.');
    if (selected && !working && !reading) {
      reading = true;
      const current = generation; const id = selected.orderId;
      try { const record = await api(`/orders/${id}`); if (generation === current && selected?.orderId === id) render(record); }
      catch (error) { hint(`Inspector unavailable: ${error.message}`, true); }
      finally { reading = false; }
    }
  });
  document.addEventListener('migration-disconnected', () => { connected = false; controls(); hint('Disconnected · inspector values may be stale', true); });
  document.addEventListener('migration-reset', () => { generation++; selected = undefined; byId('inspector').hidden = true; byId('live-edit').hidden = true; byId('record-id').value = ''; });
  controls();
})();
