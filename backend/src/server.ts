import Fastify from 'fastify';
import cors from '@fastify/cors';
import { connectDatabases, source, target } from './db.js';
import { config } from './config.js';
import { status } from './status.js';
import { seed } from './seed.js';
import { startTraffic, stopTraffic } from './traffic.js';
import { startMigration, pauseMigration, resumeMigration, crashMigration } from './migration.js';
import { runtime, currentRun, event } from './state.js';
import { runValidation, corruptTarget } from './validation.js';
import { cutover, rollbackAssessment } from './cutover.js';
const app = Fastify({ logger: true });
await app.register(cors, { origin: true });
await connectDatabases();
app.get('/health', async () => ({ ok: true }));
app.get('/api/status', status);
app.get('/api/events', async (_req, reply) => {
  reply.raw.setHeader('Content-Type', 'text/event-stream');
  reply.raw.setHeader('Cache-Control', 'no-cache');
  reply.raw.setHeader('Connection', 'keep-alive');
  let last = 0;
  const send = async () => {
    try {
      const rows = (
        await target.query('SELECT * FROM lab_events WHERE id>$1 ORDER BY id LIMIT 100', [last])
      ).rows;
      for (const row of rows) {
        last = Number(row.id);
        reply.raw.write(`id: ${row.id}\nevent: lab\ndata: ${JSON.stringify(row)}\n\n`);
      }
      reply.raw.write(`event: status\ndata: ${JSON.stringify(await status())}\n\n`);
    } catch {}
  };
  await send();
  const timer = setInterval(send, 1000);
  reply.raw.on('close', () => clearInterval(timer));
});
app.post('/api/data/generate', async (req) => {
  const b = (req.body ?? {}) as any;
  return seed(Number(b.customers ?? 1000), Number(b.orders ?? 5000));
});
app.post('/api/traffic/start', async (req) => startTraffic(Number((req.body as any)?.rate ?? 5)));
app.post('/api/traffic/stop', async () => stopTraffic());
app.post('/api/migration/start', async (req) => ({
  runId: await startMigration(
    (req.body ?? { batchSize: 5000, cdcBatchSize: 500, speed: 'normal' }) as any,
  ),
}));
app.post('/api/migration/pause', async () => {
  await pauseMigration();
  return { ok: true };
});
app.post('/api/migration/resume', async () => ({ runId: await resumeMigration() }));
app.post('/api/migration/crash', async () => {
  await crashMigration();
  return { ok: true };
});
app.post('/api/migration/fail-next-batch', async () => {
  runtime.failNextBatch = true;
  return { ok: true };
});
app.post('/api/cdc/pause', async () => {
  runtime.cdcPaused = true;
  return { ok: true };
});
app.post('/api/cdc/resume', async () => {
  runtime.cdcPaused = false;
  return { ok: true };
});
app.post('/api/chaos/delay', async (req) => {
  runtime.networkDelay = Math.max(0, Number((req.body as any)?.milliseconds ?? 0));
  return { milliseconds: runtime.networkDelay };
});
app.post('/api/validation/run', runValidation);
app.post('/api/validation/corrupt', corruptTarget);
app.post('/api/cutover', cutover);
app.get('/api/rollback', rollbackAssessment);
app.post('/api/lab/reset', async () => {
  stopTraffic();
  runtime.workerAbort = true;
  await target.query(
    "TRUNCATE payments,order_items,orders,products,customers,applied_changes,migration_checkpoints,migration_runs,lab_events RESTART IDENTITY CASCADE; UPDATE lab_settings SET value='\"sqlserver\"' WHERE key='active_database'; UPDATE lab_settings SET value='false' WHERE key='writes_frozen'; UPDATE lab_settings SET value='0' WHERE key='postgres_only_writes'; DELETE FROM lab_settings WHERE key='latest_validation'",
  );
  await source
    .request()
    .batch(
      'DELETE FROM migration_change_log; DELETE FROM payments; DELETE FROM order_items; DELETE FROM orders; DELETE FROM products; DELETE FROM customers;',
    );
  await event('INFO', 'INFO', 'Lab reset complete');
  return { ok: true };
});
app.setErrorHandler((error, _req, reply) =>
  reply.code(400).send({ error: (error as Error).message }),
);
await app.listen({ port: config.port, host: '0.0.0.0' });
