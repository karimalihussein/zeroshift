import { connectDatabases, closeDatabases, source, target } from './db.js';
import { status } from './status.js';
import { seed } from './seed.js';
import { startTraffic } from './traffic.js';
import { startMigration, resumeMigration } from './migration.js';
import { runValidation } from './validation.js';
import { cutover } from './cutover.js';
await connectDatabases();
const [cmd, ...args] = process.argv.slice(2);
const val = (name: string, fallback: number) =>
  Number(args.find((x) => x.startsWith(`--${name}=`))?.split('=')[1] ?? fallback);
let out: unknown;
switch (cmd) {
  case 'status':
    out = await status();
    break;
  case 'generate':
    out = await seed(val('customers', 1000), val('orders', 5000));
    break;
  case 'traffic-start':
    out = startTraffic(val('rate', 5));
    break;
  case 'migrate-start':
    out = {
      runId: await startMigration({
        batchSize: val('batch', 5000),
        cdcBatchSize: 500,
        speed: 'real',
      }),
    };
    break;
  case 'migrate-resume':
    out = { runId: await resumeMigration() };
    break;
  case 'validate':
    out = await runValidation();
    break;
  case 'cutover':
    out = await cutover();
    break;
  case 'reset':
    await target.query(
      'TRUNCATE payments,order_items,orders,products,customers,applied_changes,migration_checkpoints,migration_runs,lab_events RESTART IDENTITY CASCADE',
    );
    await source
      .request()
      .batch(
        'DELETE FROM migration_change_log; DELETE FROM payments; DELETE FROM order_items; DELETE FROM orders; DELETE FROM products; DELETE FROM customers;',
      );
    out = { ok: true };
    break;
  default:
    throw new Error(`Unknown command: ${cmd}`);
}
console.log(JSON.stringify(out, null, 2));
if (cmd !== 'traffic-start') await closeDatabases();
