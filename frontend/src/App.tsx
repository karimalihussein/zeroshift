import { useEffect, useMemo, useState } from 'react';
import {
  Activity,
  ArrowDown,
  Check,
  ChevronRight,
  CircleAlert,
  Database,
  FlaskConical,
  Gauge,
  Pause,
  Play,
  RefreshCcw,
  ShieldCheck,
  Skull,
  TrafficCone,
  Zap,
} from 'lucide-react';
const API = import.meta.env.VITE_API_URL ?? 'http://localhost:3000';
type Any = Record<string, any>;
async function post(path: string, body: Any = {}) {
  const r = await fetch(API + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await r.json();
  if (!r.ok) throw new Error(data.error ?? 'Request failed');
  return data;
}
const fmt = (n: number = 0) => new Intl.NumberFormat().format(n);
const phases = [
  'PREPARING',
  'CAPTURING_CHANGES',
  'INITIAL_COPY',
  'CATCHING_UP',
  'BUILDING_INDEXES',
  'SYNCING_SEQUENCES',
  'ANALYZING',
  'VALIDATING',
  'READY_FOR_CUTOVER',
  'FREEZING_WRITES',
  'FINAL_SYNC',
  'CUTTING_OVER',
  'MONITORING',
];
export function App() {
  const [data, setData] = useState<Any | null>(null),
    [tab, setTab] = useState('Overview'),
    [busy, setBusy] = useState(''),
    [error, setError] = useState(''),
    [batch, setBatch] = useState(5000),
    [speed, setSpeed] = useState('normal'),
    [rate, setRate] = useState(5);
  const refresh = async () => {
    try {
      setData(await (await fetch(API + '/api/status')).json());
    } catch {}
  };
  useEffect(() => {
    refresh();
    const es = new EventSource(API + '/api/events');
    es.addEventListener('status', (e: any) => setData(JSON.parse(e.data)));
    return () => es.close();
  }, []);
  const act = async (name: string, path: string, body?: Any) => {
    setBusy(name);
    setError('');
    try {
      await post(path, body);
      await refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy('');
    }
  };
  if (!data)
    return (
      <div className="boot">
        <Database /> Connecting to the lab…
      </div>
    );
  const state = data.run?.state ?? 'IDLE',
    canStart = ['IDLE', 'COMPLETED', 'FAILED'].includes(state),
    canPause = ['INITIAL_COPY', 'CATCHING_UP'].includes(state),
    canResume = ['PAUSED', 'FAILED'].includes(state),
    canCut = state === 'READY_FOR_CUTOVER';
  return (
    <div className="app">
      <header>
        <div className="brand">
          <div className="mark">
            <Database size={19} />
            <span>0</span>
          </div>
          <div>
            <h1>ZeroShift</h1>
            <p>live database migration lab</p>
          </div>
        </div>
        <div className="primary">
          <span className={data.primary === 'postgres' ? 'pg dot' : 'sql dot'}></span>
          <small>Current primary</small>
          <strong>{data.primary === 'postgres' ? 'PostgreSQL' : 'SQL Server'}</strong>
        </div>
      </header>
      <nav>
        {[
          'Overview',
          'Tables',
          'CDC',
          'Validation',
          'Schema mapping',
          'Checkpoints',
          'Performance',
          'Logs',
          'Failure lab',
          'Cutover',
        ].map((x) => (
          <button className={tab === x ? 'active' : ''} onClick={() => setTab(x)} key={x}>
            {x}
          </button>
        ))}
      </nav>
      <main>
        {error && (
          <div className="alert">
            <CircleAlert size={18} />
            {error}
          </div>
        )}
        {tab === 'Overview' && <Overview d={data} />} {tab === 'Tables' && <Tables d={data} />}{' '}
        {tab === 'CDC' && <CDC d={data} />}{' '}
        {tab === 'Validation' && <Validation d={data} act={act} />}{' '}
        {tab === 'Schema mapping' && <Mapping d={data} />}{' '}
        {tab === 'Checkpoints' && <Checkpoints d={data} />}{' '}
        {tab === 'Performance' && <Performance d={data} />} {tab === 'Logs' && <Logs d={data} />}{' '}
        {tab === 'Failure lab' && <Failure d={data} act={act} />}{' '}
        {tab === 'Cutover' && <Cutover d={data} act={act} canCut={canCut} />}
      </main>
      <aside className="controls">
        <div className="control-title">
          <Zap size={16} /> Lab controls
        </div>
        <div className="control-grid">
          <button
            onClick={() => act('seed', '/api/data/generate', { customers: 1000, orders: 5000 })}
            disabled={!!busy}
          >
            <Database />
            Generate data
          </button>
          <button
            onClick={() =>
              act(
                'traffic',
                data.runtime.trafficRunning ? '/api/traffic/stop' : '/api/traffic/start',
                { rate },
              )
            }
          >
            <TrafficCone />
            {data.runtime.trafficRunning ? 'Stop traffic' : 'Start traffic'}
          </button>
          <button
            className="accent"
            onClick={() =>
              act('migration', '/api/migration/start', {
                batchSize: batch,
                cdcBatchSize: 500,
                speed,
              })
            }
            disabled={!canStart}
          >
            <Play />
            Start migration
          </button>
          <button onClick={() => act('pause', '/api/migration/pause')} disabled={!canPause}>
            <Pause />
            Pause
          </button>
          <button onClick={() => act('resume', '/api/migration/resume')} disabled={!canResume}>
            <RefreshCcw />
            Resume
          </button>
          <button onClick={() => act('crash', '/api/migration/crash')} disabled={!canPause}>
            <Skull />
            Simulate crash
          </button>
        </div>
        <div className="sliders">
          <label>
            Traffic{' '}
            <input
              type="range"
              min="1"
              max="25"
              value={rate}
              onChange={(e) => setRate(+e.target.value)}
            />
            <b>{rate}/s</b>
          </label>
          <label>
            Batch{' '}
            <select value={batch} onChange={(e) => setBatch(+e.target.value)}>
              {[1000, 5000, 10000, 20000].map((x) => (
                <option key={x}>{x}</option>
              ))}
            </select>
          </label>
          <label>
            Speed{' '}
            <select value={speed} onChange={(e) => setSpeed(e.target.value)}>
              <option value="real">Real speed</option>
              <option value="normal">Normal demo</option>
              <option value="slow">Slow demo</option>
            </select>
          </label>
        </div>
      </aside>
    </div>
  );
}
function Overview({ d }: { d: Any }) {
  const state = d.run?.state ?? 'IDLE',
    idx = phases.indexOf(state);
  return (
    <>
      <section className="hero">
        <div>
          <p className="kicker">Copy → capture → catch-up → validate → cutover</p>
          <h2>
            Move a live system
            <br />
            without losing its pulse.
          </h2>
          <p>
            Every value below comes from a committed database record, checkpoint, or captured source
            change.
          </p>
        </div>
        <div className="phase">
          <span>Migration state</span>
          <strong>{state.replaceAll('_', ' ')}</strong>
          <div className="bar">
            <i style={{ width: `${d.totals.percentage}%` }} />
          </div>
          <footer>
            <b>{d.totals.percentage}%</b>
            <span>{fmt(d.totals.copied)} committed rows</span>
          </footer>
        </div>
      </section>
      <section className="flow">
        <DbCard
          kind="sql"
          title="SQL Server"
          rows={d.totals.source}
          sub={`${d.runtime.trafficRunning ? d.runtime.trafficRate : 0} writes scheduled/sec`}
        />
        <div className="conduit">
          <span />
          <ArrowDown />
          <b>{state === 'IDLE' ? 'Waiting' : state.replaceAll('_', ' ')}</b>
          <small>{fmt(d.runtime.rowsPerSecond)} rows/sec</small>
        </div>
        <div className="engine">
          <Activity />
          <small>Migration engine</small>
          <strong>{fmt(d.totals.copied)}</strong>
          <span>rows checkpointed</span>
          <div className="mini">
            <b>Batch</b>
            {fmt(d.run?.config?.batchSize ?? 0)}
            <b>CDC lag</b>
            {fmt(d.cdc.pending)}
          </div>
        </div>
        <div className="conduit">
          <span />
          <ArrowDown />
          <b>Committed</b>
          <small>{d.runtime.lastBatchMs} ms last batch</small>
        </div>
        <DbCard
          kind="pg"
          title="PostgreSQL"
          rows={d.totals.target}
          sub={`${fmt(d.cdc.pending)} pending changes`}
        />
      </section>
      <Timeline d={d} />
      <section className="concepts">
        <article>
          <b>Snapshot boundary</b>
          <p>
            Change capture is active before copying. Events after boundary #
            {fmt(d.run?.snapshot_boundary ?? 0)} replay after the snapshot.
          </p>
        </article>
        <article>
          <b>Keyset pagination</b>
          <p>
            Each batch continues after its last committed ID. No increasingly expensive OFFSET scan.
          </p>
        </article>
        <article>
          <b>Atomic checkpoint</b>
          <p>
            Target rows and their checkpoint commit together. A crash can repeat work, but cannot
            skip it.
          </p>
        </article>
      </section>
    </>
  );
}
function DbCard({
  kind,
  title,
  rows,
  sub,
}: {
  kind: string;
  title: string;
  rows: number;
  sub: string;
}) {
  return (
    <article className={`dbcard ${kind}`}>
      <div className="dbicon">
        <Database />
      </div>
      <small>Database</small>
      <h3>{title}</h3>
      <strong>{fmt(rows)}</strong>
      <span>total rows</span>
      <footer>{sub}</footer>
    </article>
  );
}
function Timeline({ d }: { d: Any }) {
  return (
    <section className="timeline">
      <h3>Live timeline</h3>
      <div>
        {d.logs.slice(0, 7).map((x: Any) => (
          <article key={x.id}>
            <time>{new Date(x.created_at).toLocaleTimeString()}</time>
            <i className={x.level === 'ERROR' ? 'bad' : ''} />
            <span>{x.message}</span>
          </article>
        ))}
      </div>
    </section>
  );
}
function Tables({ d }: { d: Any }) {
  return (
    <Panel
      title="Table migration"
      intro="Dependency order keeps referenced rows ahead of their children."
    >
      <table>
        <thead>
          <tr>
            <th>Table</th>
            <th>Depends on</th>
            <th>Source</th>
            <th>Target</th>
            <th>Checkpoint</th>
            <th>Progress</th>
          </tr>
        </thead>
        <tbody>
          {d.dependencies.map((x: Any) => {
            const cp = d.checkpoints.find((c: Any) => c.table_name === x.table),
              s = d.sourceCounts[x.table],
              t = d.targetCounts[x.table],
              p = s ? Math.round((t / s) * 100) : 0;
            return (
              <tr key={x.table}>
                <td>
                  <b>{x.table}</b>
                </td>
                <td>{x.dependsOn.join(', ') || '—'}</td>
                <td>{fmt(s)}</td>
                <td>{fmt(t)}</td>
                <td className="mono">{fmt(cp?.last_processed_id ?? 0)}</td>
                <td>
                  <span className="rowbar">
                    <i style={{ width: `${Math.min(100, p)}%` }} />
                  </span>
                  {p}%
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </Panel>
  );
}
function CDC({ d }: { d: Any }) {
  return (
    <Panel
      title="Change capture"
      intro="This lab uses a transactionally populated append-only log—not native SQL Server CDC. The ordering key is change_id; timestamps alone can collide."
    >
      <div className="stats">
        <Stat n={d.cdc.pending} label="Pending" />
        <Stat n={d.cdc.sourcePosition} label="Source position" />
        <Stat n={d.cdc.appliedPosition} label="Applied position" />
        <Stat n={d.logs.filter((x: Any) => x.category === 'CDC').length} label="Recent events" />
      </div>
      <Logs d={{ logs: d.logs.filter((x: Any) => x.category === 'CDC') }} embedded />
    </Panel>
  );
}
function Validation({ d, act }: { d: Any; act: any }) {
  return (
    <Panel
      title="Validation"
      intro="Counts are only level one. The lab also checks ranges, normalized deterministic samples, and relationships."
    >
      <div className="actions">
        <button onClick={() => act('validate', '/api/validation/run')}>
          <ShieldCheck />
          Run validation
        </button>
        <button className="danger" onClick={() => act('corrupt', '/api/validation/corrupt')}>
          <FlaskConical />
          Corrupt safe target value
        </button>
      </div>
      {d.validation ? (
        <>
          <div className={`verdict ${d.validation.passed ? 'pass' : 'fail'}`}>
            {d.validation.passed ? <Check /> : <CircleAlert />}
            <strong>{d.validation.passed ? 'Validation passed' : 'Mismatch detected'}</strong>
          </div>
          <table>
            <thead>
              <tr>
                <th>Table</th>
                <th>SQL Server</th>
                <th>PostgreSQL</th>
                <th>Counts</th>
                <th>Checksum sample</th>
              </tr>
            </thead>
            <tbody>
              {d.validation.tables.map((x: Any) => (
                <tr key={x.table}>
                  <td>{x.table}</td>
                  <td>{fmt(x.sourceCount)}</td>
                  <td>{fmt(x.targetCount)}</td>
                  <td>{x.countMatch ? '✓' : 'Mismatch'}</td>
                  <td>{x.checksumMatch ? '✓' : 'Mismatch'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : (
        <Empty text="No validation run yet." />
      )}
    </Panel>
  );
}
function Mapping({ d }: { d: Any }) {
  return (
    <Panel
      title="Schema mapping"
      intro="Explicit mappings prevent accidental precision, Unicode, and time semantics changes."
    >
      <table>
        <thead>
          <tr>
            <th>SQL Server</th>
            <th></th>
            <th>PostgreSQL</th>
            <th>Handling</th>
          </tr>
        </thead>
        <tbody>
          {d.mappings.map((x: Any) => (
            <tr key={x.sql}>
              <td className="mono">{x.sql}</td>
              <td>
                <ChevronRight />
              </td>
              <td className="mono">{x.pg}</td>
              <td>{x.note}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  );
}
function Checkpoints({ d }: { d: Any }) {
  return (
    <Panel
      title="Durable checkpoints"
      intro="A checkpoint advances in the same PostgreSQL transaction as its batch."
    >
      <Tables d={d} />
    </Panel>
  );
}
function Performance({ d }: { d: Any }) {
  return (
    <Panel
      title="Measured performance"
      intro="These are observed counters—not synthetic projections."
    >
      <div className="stats">
        <Stat n={d.runtime.rowsPerSecond} label="Rows / second" />
        <Stat n={d.runtime.lastBatchMs} label="Last batch ms" />
        <Stat n={d.totals.copied} label="Rows committed" />
        <Stat n={d.cdc.pending} label="CDC backlog" />
      </div>
    </Panel>
  );
}
function Logs({ d, embedded = false }: { d: Any; embedded?: boolean }) {
  const content = (
    <div className="logs">
      {d.logs.slice(0, 50).map((x: Any) => (
        <div key={x.id}>
          <time>{new Date(x.created_at).toLocaleTimeString()}</time>
          <b className={x.level}>{x.category}</b>
          <span>{x.message}</span>
        </div>
      ))}
    </div>
  );
  return embedded ? (
    content
  ) : (
    <Panel
      title="Structured event log"
      intro="Migration, CDC, validation, and failure events share one durable timeline."
    >
      {content}
    </Panel>
  );
}
function Failure({ d, act }: { d: Any; act: any }) {
  return (
    <Panel
      title="Chaos / failure lab"
      intro="Failures stop at transaction boundaries. Resume repeats only uncommitted work."
    >
      <div className="failure-grid">
        <button onClick={() => act('crash', '/api/migration/crash')}>
          <Skull />
          Kill migration worker
        </button>
        <button onClick={() => act('fail', '/api/migration/fail-next-batch')}>
          <CircleAlert />
          Fail next batch
        </button>
        <button onClick={() => act('pausecdc', '/api/cdc/pause')}>
          <Pause />
          Pause CDC replay
        </button>
        <button onClick={() => act('resumecdc', '/api/cdc/resume')}>
          <Play />
          Resume CDC replay
        </button>
        <button onClick={() => act('delay', '/api/chaos/delay', { milliseconds: 1200 })}>
          <Gauge />
          Add 1.2s network delay
        </button>
        <button onClick={() => act('delay', '/api/chaos/delay', { milliseconds: 0 })}>
          <RefreshCcw />
          Clear network delay
        </button>
      </div>
      <p className="note">
        Current artificial delay: {d.runtime.networkDelay} ms · CDC replay:{' '}
        {d.runtime.cdcPaused ? 'paused' : 'running'}
      </p>
    </Panel>
  );
}
function Cutover({ d, act, canCut }: { d: Any; act: any; canCut: boolean }) {
  const items = [
    [
      'Initial copy complete',
      d.checkpoints.length === 5 && d.checkpoints.every((x: Any) => x.status === 'complete'),
    ],
    ['CDC lag acceptable', d.cdc.pending === 0],
    ['Validation passed', d.validation?.passed],
    [
      'Sequences synchronized',
      [
        'READY_FOR_CUTOVER',
        'FREEZING_WRITES',
        'FINAL_SYNC',
        'CUTTING_OVER',
        'MONITORING',
        'COMPLETED',
      ].includes(d.run?.state),
    ],
    ['PostgreSQL ready', d.run?.state === 'READY_FOR_CUTOVER'],
  ];
  return (
    <Panel
      title="Controlled cutover"
      intro="Writes freeze only for the final drain and validation. Switching early is rejected by the state machine."
    >
      <div className="checklist">
        {items.map(([t, v]: any) => (
          <div key={t} className={v ? 'done' : ''}>
            {v ? <Check /> : <span />}
            {t}
          </div>
        ))}
      </div>
      <button className="cut" disabled={!canCut} onClick={() => act('cutover', '/api/cutover')}>
        Begin cutover
      </button>
      <div className={`rollback ${d.rollback.safe ? 'safe' : 'unsafe'}`}>
        <b>{d.rollback.safe ? 'Rollback remains straightforward' : 'Naive rollback is unsafe'}</b>
        <p>{d.rollback.explanation}</p>
        <span>PostgreSQL-only writes: {d.rollback.postgresOnlyWrites}</span>
      </div>
    </Panel>
  );
}
function Panel({ title, intro, children }: { title: string; intro: string; children: any }) {
  return (
    <section className="panel">
      <header>
        <h2>{title}</h2>
        <p>{intro}</p>
      </header>
      {children}
    </section>
  );
}
function Stat({ n, label }: { n: number; label: string }) {
  return (
    <article>
      <strong>{fmt(n)}</strong>
      <span>{label}</span>
    </article>
  );
}
function Empty({ text }: { text: string }) {
  return <div className="empty">{text}</div>;
}
