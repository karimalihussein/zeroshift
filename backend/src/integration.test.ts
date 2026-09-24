import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { connectDatabases, closeDatabases, source, target } from './db.js';
import { seed } from './seed.js';
import { startMigration, crashMigration, resumeMigration, syncSequences } from './migration.js';
import { runValidation, corruptTarget } from './validation.js';
import { currentRun, sleep } from './state.js';
import { cutover, rollbackAssessment } from './cutover.js';
const enabled = process.env.RUN_INTEGRATION === '1';
describe.skipIf(!enabled)('real SQL Server → PostgreSQL lifecycle', () => {
  beforeAll(connectDatabases, 120000);
  afterAll(closeDatabases);
  it('preserves Unicode, decimals, NULLs, relationships, and detects corruption', async () => {
    await seed(40, 100);
    await startMigration({ batchSize: 20, cdcBatchSize: 20, speed: 'real' });
    for (let i = 0; i < 200 && (await currentRun())?.state !== 'READY_FOR_CUTOVER'; i++)
      await sleep(100);
    expect((await currentRun())?.state).toBe('READY_FOR_CUTOVER');
    expect((await runValidation()).passed).toBe(true);
    const arabic = (await target.query("SELECT name FROM customers WHERE name ~ '[ء-ي]' LIMIT 1"))
      .rows[0];
    expect(arabic).toBeTruthy();
    expect(
      (await target.query('SELECT count(*)::int n FROM customers WHERE notes IS NULL')).rows[0].n,
    ).toBeGreaterThan(0);
    expect(
      (await target.query('SELECT price::text FROM products ORDER BY id LIMIT 1')).rows[0].price,
    ).toMatch(/\.\d{4}$/);
    await corruptTarget();
    expect((await runValidation()).passed).toBe(false);
  }, 120000);
  it('uses checkpoints after an injected worker crash', async () => {
    await seed(100, 300);
    await startMigration({ batchSize: 10, cdcBatchSize: 20, speed: 'slow' });
    await sleep(1400);
    await crashMigration();
    const before = (
      await target.query('SELECT sum(rows_processed)::int n FROM migration_checkpoints')
    ).rows[0].n;
    expect(before).toBeGreaterThan(0);
    await resumeMigration();
    expect(
      (await target.query('SELECT sum(rows_processed)::int n FROM migration_checkpoints')).rows[0]
        .n,
    ).toBeGreaterThanOrEqual(before);
  }, 30000);
  it('synchronizes identities above migrated IDs', async () => {
    await syncSequences();
    const max = Number((await target.query('SELECT max(id) n FROM customers')).rows[0].n);
    const id = Number(
      (
        await target.query(
          "INSERT INTO customers(public_id,name,email) VALUES(gen_random_uuid(),'Sequence Test','sequence@test') RETURNING id",
        )
      ).rows[0].id,
    );
    expect(id).toBeGreaterThan(max);
  });
  it('records transactionally ordered INSERT UPDATE DELETE events', async () => {
    const before = Number(
      (await source.request().query('SELECT ISNULL(MAX(change_id),0) n FROM migration_change_log'))
        .recordset[0].n,
    );
    await source
      .request()
      .query("UPDATE TOP(1) customers SET notes=N'تحديث' WHERE id=(SELECT MIN(id) FROM customers)");
    const events = (
      await source
        .request()
        .query(`SELECT * FROM migration_change_log WHERE change_id>${before} ORDER BY change_id`)
    ).recordset;
    expect(events.length).toBeGreaterThan(0);
    expect(events[0].operation).toBe('U');
  });
  it('models post-cutover rollback as unsafe', async () => {
    const run = await currentRun();
    if (run?.state === 'READY_FOR_CUTOVER') await cutover();
    await target.query("UPDATE lab_settings SET value='2' WHERE key='postgres_only_writes'");
    expect((await rollbackAssessment()).safe).toBe(false);
  });
});
