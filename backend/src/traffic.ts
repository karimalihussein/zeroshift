import { randomUUID } from 'node:crypto';
import { source, target } from './db.js';
import { runtime, event } from './state.js';
let timer: NodeJS.Timeout | undefined;
async function sourceOperation() {
  const op = Math.random();
  if (op < 0.45) {
    const c = (await source.request().query('SELECT TOP 1 id FROM customers ORDER BY NEWID()'))
      .recordset[0];
    if (!c) return;
    await source
      .request()
      .input('u', randomUUID())
      .input('c', c.id)
      .query(
        "INSERT orders(public_id,customer_id,status,total,shipping_address,placed_at) VALUES(@u,@c,'pending',42.2500,N'القاهرة',SYSUTCDATETIME())",
      );
    runtime.trafficStats.insert++;
  } else if (op < 0.82) {
    await source
      .request()
      .query(
        "UPDATE TOP (1) orders SET status=CASE status WHEN 'pending' THEN 'paid' ELSE 'completed' END,updated_at=SYSUTCDATETIME() WHERE id=(SELECT TOP 1 id FROM orders ORDER BY NEWID())",
      );
    runtime.trafficStats.update++;
  } else {
    await source
      .request()
      .query(
        'DELETE TOP (1) FROM payments WHERE id=(SELECT TOP 1 id FROM payments ORDER BY NEWID())',
      );
    runtime.trafficStats.delete++;
  }
}
async function targetOperation() {
  const op = Math.random();
  if (op < 0.55) {
    const c = (await target.query('SELECT id FROM customers ORDER BY random() LIMIT 1')).rows[0];
    if (!c) return;
    await target.query(
      "INSERT INTO orders(public_id,customer_id,status,total,shipping_address,placed_at) VALUES($1,$2,'pending',42.2500,'Cairo',now())",
      [randomUUID(), c.id],
    );
    runtime.trafficStats.insert++;
  } else {
    await target.query(
      "UPDATE orders SET status='completed',updated_at=now() WHERE id=(SELECT id FROM orders ORDER BY random() LIMIT 1)",
    );
    runtime.trafficStats.update++;
  }
  await target.query(
    "INSERT INTO lab_settings(key,value) VALUES('postgres_only_writes','1') ON CONFLICT(key) DO UPDATE SET value=((lab_settings.value::text)::int+1)::text::jsonb",
  );
}
async function tick() {
  try {
    const frozen = (await target.query("SELECT value FROM lab_settings WHERE key='writes_frozen'"))
      .rows[0]?.value;
    if (frozen) return;
    const active =
      (
        await target.query(
          "SELECT value#>>'{}' active FROM lab_settings WHERE key='active_database'",
        )
      ).rows[0]?.active ?? 'sqlserver';
    await (active === 'postgres' ? targetOperation() : sourceOperation());
  } catch (e) {
    runtime.trafficStats.errors++;
    await event('ERROR', 'ERROR', 'Traffic operation failed', { message: (e as Error).message });
  }
}
export function startTraffic(rate = 5) {
  runtime.trafficRate = Math.max(1, Math.min(50, rate));
  runtime.trafficRunning = true;
  if (timer) clearInterval(timer);
  timer = setInterval(() => {
    for (let i = 0; i < runtime.trafficRate; i++) void tick();
  }, 1000);
  void event('INFO', 'INFO', 'Production traffic started', { rate: runtime.trafficRate });
  return runtime;
}
export function stopTraffic() {
  runtime.trafficRunning = false;
  if (timer) clearInterval(timer);
  timer = undefined;
  void event('INFO', 'INFO', 'Production traffic stopped');
  return runtime;
}
