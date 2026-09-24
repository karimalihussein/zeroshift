import { createHash } from 'node:crypto';
import sql from 'mssql';
import { source, target } from './db.js';
import { TABLES, type TableName } from './types.js';
import { columns } from './schema.js';
import { event } from './state.js';
function norm(v: unknown): string {
  if (v === null || v === undefined) return '∅';
  if (v instanceof Date) return v.toISOString();
  if (typeof v === 'boolean') return v ? '1' : '0';
  if (typeof v === 'number') return String(v);
  const s = String(v);
  if (/^-?\d+\.\d+$/.test(s)) return s.replace(/0+$/, '').replace(/\.$/, '');
  return s;
}
function hashRows(rows: any[], table: TableName) {
  const h = createHash('sha256');
  for (const row of rows) for (const col of columns[table]) h.update(`${col}=${norm(row[col])}|`);
  return h.digest('hex');
}
export async function runValidation() {
  const tables: any[] = [];
  let passed = true;
  for (const table of TABLES) {
    const srcCount = Number(
      (await source.request().query(`SELECT COUNT_BIG(*) n FROM ${table}`)).recordset[0].n,
    );
    const dstCount = Number(
      (await target.query(`SELECT COUNT(*)::bigint n FROM "${table}"`)).rows[0].n,
    );
    const srcAgg = (
      await source.request().query(`SELECT MIN(id) min_id,MAX(id) max_id FROM ${table}`)
    ).recordset[0];
    const dstAgg = (
      await target.query(`SELECT MIN(id)::text min_id,MAX(id)::text max_id FROM "${table}"`)
    ).rows[0];
    const ids = (
      await source.request().query(`SELECT TOP 100 id FROM ${table} ORDER BY id`)
    ).recordset.map((r: any) => r.id);
    let checksumMatch = true;
    if (ids.length) {
      const list = ids.map((_: any, i: number) => `@i${i}`).join(',');
      const req = source.request();
      ids.forEach((id: any, i: number) => req.input(`i${i}`, sql.BigInt, id));
      const sr = (await req.query(`SELECT * FROM ${table} WHERE id IN (${list}) ORDER BY id`))
        .recordset;
      const dr = (
        await target.query(`SELECT * FROM "${table}" WHERE id=ANY($1::bigint[]) ORDER BY id`, [ids])
      ).rows;
      checksumMatch = hashRows(sr, table) === hashRows(dr, table);
    }
    const ok =
      srcCount === dstCount &&
      String(srcAgg.min_id ?? '') === String(dstAgg.min_id ?? '') &&
      String(srcAgg.max_id ?? '') === String(dstAgg.max_id ?? '') &&
      checksumMatch;
    passed &&= ok;
    tables.push({
      table,
      sourceCount: srcCount,
      targetCount: dstCount,
      countMatch: srcCount === dstCount,
      aggregatesMatch: String(srcAgg.max_id ?? '') === String(dstAgg.max_id ?? ''),
      checksumMatch,
      passed: ok,
    });
  }
  const relationships =
    (
      await target.query(
        `SELECT (SELECT count(*) FROM orders o LEFT JOIN customers c ON c.id=o.customer_id WHERE c.id IS NULL)::int + (SELECT count(*) FROM order_items i LEFT JOIN orders o ON o.id=i.order_id WHERE o.id IS NULL)::int + (SELECT count(*) FROM order_items i LEFT JOIN products p ON p.id=i.product_id WHERE p.id IS NULL)::int + (SELECT count(*) FROM payments p LEFT JOIN orders o ON o.id=p.order_id WHERE o.id IS NULL)::int AS orphans`,
      )
    ).rows[0].orphans === 0;
  passed &&= relationships;
  const result = {
    passed,
    tables,
    relationships,
    checkedAt: new Date().toISOString(),
    levels: ['counts', 'aggregates', 'deterministic samples/checksums', 'relationships'],
  };
  await target.query(
    "INSERT INTO lab_settings(key,value) VALUES('latest_validation',$1) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
    [JSON.stringify(result)],
  );
  await event(
    'VALIDATION',
    passed ? 'INFO' : 'ERROR',
    passed ? 'All validation levels passed' : 'Validation detected a mismatch',
    result,
  );
  return result;
}
export async function corruptTarget() {
  const row = await target.query(
    "UPDATE customers SET name=name || ' [CORRUPTED]' WHERE id=(SELECT min(id) FROM customers) RETURNING id",
  );
  if (!row.rowCount) throw new Error('No target customer to corrupt');
  await event('VALIDATION', 'ERROR', 'A safe target value was intentionally corrupted', {
    table: 'customers',
    id: row.rows[0].id,
  });
  return row.rows[0];
}
