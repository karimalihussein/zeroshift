import { randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import { finished } from 'node:stream/promises';
import type pg from 'pg';
import { from as copyFrom } from 'pg-copy-streams';
import type { TargetDatabase } from '../../application/ports.js';
import { TABLE_NAMES } from '../../domain/models.js';
import type { ChangeEvent, DatabaseRow, TableName } from '../../domain/models.js';
import { tableDefinition } from '../../domain/table-plan.js';
import type { DatabaseConnections } from '../database/database-connections.js';

const quote = (identifier: string) => `"${identifier}"`;

function csv(value: unknown): string {
  if (value === null || value === undefined) return '\\N';
  if (value instanceof Date) return value.toISOString();
  if (Buffer.isBuffer(value)) return value.toString();
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  const text = String(value);
  return /[",\n\r]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export class PostgresTargetDatabase implements TargetDatabase {
  constructor(private readonly connections: DatabaseConnections) {}

  async initialize(): Promise<void> {}
  async close(): Promise<void> {}

  async resetData(): Promise<void> {
    await this.connections.target.query(
      'TRUNCATE payments,order_items,orders,products,customers,applied_changes RESTART IDENTITY CASCADE',
    );
  }

  async copyBatchAndCheckpoint(
    runId: string,
    table: TableName,
    rows: DatabaseRow[],
  ): Promise<void> {
    if (!rows.length) return;
    const client = await this.connections.target.connect();
    try {
      await client.query('BEGIN');
      await this.copy(client, table, rows);
      const lastId = rows.at(-1)!.id;
      await client.query(
        `UPDATE migration_checkpoints
         SET last_processed_id=$3,rows_processed=rows_processed+$4,batch_number=batch_number+1,
             status='running',updated_at=now()
         WHERE run_id=$1 AND table_name=$2`,
        [runId, table, lastId, rows.length],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async applyChange(runId: string, change: ChangeEvent): Promise<boolean> {
    const client = await this.connections.target.connect();
    try {
      await client.query('BEGIN');
      const seen = await client.query('SELECT 1 FROM applied_changes WHERE change_id=$1', [
        change.changeId,
      ]);
      if (seen.rowCount) {
        await this.advancePosition(client, runId, change.changeId);
        await client.query('COMMIT');
        return false;
      }

      if (change.operation === 'D') {
        await client.query(`DELETE FROM ${quote(change.tableName)} WHERE id=$1`, [change.recordId]);
      } else {
        if (!change.rowData) throw new Error(`Change ${change.changeId} has no row image`);
        await this.upsert(client, change.tableName, change.rowData);
      }
      await client.query('INSERT INTO applied_changes(change_id) VALUES($1)', [change.changeId]);
      await this.advancePosition(client, runId, change.changeId);
      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async buildIndexes(): Promise<void> {
    await this.connections.target.query(`
      CREATE INDEX IF NOT EXISTS ix_orders_customer ON orders(customer_id);
      CREATE INDEX IF NOT EXISTS ix_items_order ON order_items(order_id);
      CREATE INDEX IF NOT EXISTS ix_payments_order ON payments(order_id);
    `);
  }

  async synchronizeSequences(): Promise<void> {
    for (const table of TABLE_NAMES) {
      await this.connections.target.query(
        `SELECT setval(pg_get_serial_sequence('${table}','id'),
          COALESCE((SELECT MAX(id) FROM ${quote(table)}),0)+1,false)`,
      );
    }
  }

  async analyze(table: TableName): Promise<void> {
    await this.connections.target.query(`ANALYZE ${quote(table)}`);
  }

  async count(table: TableName): Promise<number> {
    const result = await this.connections.target.query<{ count: string }>(
      `SELECT COUNT(*)::bigint count FROM ${quote(table)}`,
    );
    return Number(result.rows[0]!.count);
  }

  async idRange(table: TableName): Promise<{ min: number | null; max: number | null }> {
    const result = await this.connections.target.query<{ min: string | null; max: string | null }>(
      `SELECT MIN(id)::text min,MAX(id)::text max FROM ${quote(table)}`,
    );
    return {
      min: result.rows[0]!.min === null ? null : Number(result.rows[0]!.min),
      max: result.rows[0]!.max === null ? null : Number(result.rows[0]!.max),
    };
  }

  async readRows(table: TableName, ids: readonly number[]): Promise<DatabaseRow[]> {
    if (!ids.length) return [];
    return (
      await this.connections.target.query<DatabaseRow>(
        `SELECT * FROM ${quote(table)} WHERE id=ANY($1::bigint[]) ORDER BY id`,
        [ids],
      )
    ).rows;
  }

  async relationshipsAreValid(): Promise<boolean> {
    const result = await this.connections.target.query<{ orphans: number }>(`
      SELECT
        (SELECT count(*) FROM orders o LEFT JOIN customers c ON c.id=o.customer_id WHERE c.id IS NULL)::int +
        (SELECT count(*) FROM order_items i LEFT JOIN orders o ON o.id=i.order_id WHERE o.id IS NULL)::int +
        (SELECT count(*) FROM order_items i LEFT JOIN products p ON p.id=i.product_id WHERE p.id IS NULL)::int +
        (SELECT count(*) FROM payments p LEFT JOIN orders o ON o.id=p.order_id WHERE o.id IS NULL)::int
        AS orphans
    `);
    return result.rows[0]!.orphans === 0;
  }

  async corruptDemoRecord(): Promise<{ id: number }> {
    const result = await this.connections.target.query<{ id: number }>(
      "UPDATE customers SET name=name || ' [CORRUPTED]' WHERE id=(SELECT min(id) FROM customers) RETURNING id",
    );
    if (!result.rows[0]) throw new Error('No target customer to corrupt');
    return result.rows[0];
  }

  async performTrafficOperation(): Promise<'insert' | 'update' | null> {
    if (Math.random() < 0.55) {
      const customer = (
        await this.connections.target.query<{ id: number }>(
          'SELECT id FROM customers ORDER BY random() LIMIT 1',
        )
      ).rows[0];
      if (!customer) return null;
      await this.connections.target.query(
        "INSERT INTO orders(public_id,customer_id,status,total,shipping_address,placed_at) VALUES($1,$2,'pending',42.2500,'Cairo',now())",
        [randomUUID(), customer.id],
      );
      return 'insert';
    }
    await this.connections.target.query(
      "UPDATE orders SET status='completed',updated_at=now() WHERE id=(SELECT id FROM orders ORDER BY random() LIMIT 1)",
    );
    return 'update';
  }

  async resetLab(): Promise<void> {
    await this.connections.target.query(`
      TRUNCATE payments,order_items,orders,products,customers,applied_changes,
        migration_checkpoints,migration_runs,lab_events RESTART IDENTITY CASCADE;
      UPDATE lab_settings SET value='"sqlserver"' WHERE key='active_database';
      UPDATE lab_settings SET value='false' WHERE key='writes_frozen';
      UPDATE lab_settings SET value='0' WHERE key='postgres_only_writes';
      DELETE FROM lab_settings WHERE key='latest_validation';
    `);
  }

  private async copy(client: pg.PoolClient, table: TableName, rows: DatabaseRow[]): Promise<void> {
    const columns = tableDefinition(table).columns;
    const stream = client.query(
      copyFrom(
        `COPY ${quote(table)} (${columns.map(quote).join(',')}) FROM STDIN WITH (FORMAT csv, NULL '\\N')`,
      ),
    );
    Readable.from(
      rows.map((row) => `${columns.map((column) => csv(row[column])).join(',')}\n`),
    ).pipe(stream);
    await finished(stream);
  }

  private async upsert(client: pg.PoolClient, table: TableName, row: DatabaseRow): Promise<void> {
    const columns = tableDefinition(table).columns;
    const values = columns.map((column) => row[column] ?? null);
    const updates = columns
      .filter((column) => column !== 'id')
      .map((column) => `${quote(column)}=EXCLUDED.${quote(column)}`)
      .join(',');
    await client.query(
      `INSERT INTO ${quote(table)} (${columns.map(quote)})
       VALUES (${columns.map((_, index) => `$${index + 1}`)})
       ON CONFLICT(id) DO UPDATE SET ${updates}`,
      values,
    );
  }

  private async advancePosition(client: pg.PoolClient, runId: string, changeId: number) {
    await client.query(
      'UPDATE migration_runs SET last_applied_change=GREATEST(last_applied_change,$2),updated_at=now() WHERE id=$1',
      [runId, changeId],
    );
  }
}
