import { randomUUID } from 'node:crypto';
import sql from 'mssql';
import type { SourceDatabase } from '../../application/ports.js';
import { TABLE_NAMES } from '../../domain/models.js';
import type { ChangeEvent, ChangeOperation, DatabaseRow, TableName } from '../../domain/models.js';
import type { DatabaseConnections } from '../database/database-connections.js';

interface ChangeRow {
  change_id: string | number;
  table_name: TableName;
  operation: ChangeOperation;
  record_id: string | number;
  row_data: string | null;
  changed_at: Date;
}

type SqlType = Parameters<sql.Request['input']>[1];

export class SqlServerSourceDatabase implements SourceDatabase {
  constructor(private readonly connections: DatabaseConnections) {}

  async initialize(): Promise<void> {}
  async close(): Promise<void> {}

  async snapshotBoundary(): Promise<number> {
    const result = await this.connections.source
      .request()
      .query<{ boundary: number | string }>(
        'SELECT ISNULL(MAX(change_id),0) boundary FROM migration_change_log',
      );
    return Number(result.recordset[0]!.boundary);
  }

  async readBatch(table: TableName, afterId: number, size: number): Promise<DatabaseRow[]> {
    const request = this.connections.source.request();
    request.input('last', sql.BigInt, afterId);
    request.input('size', sql.Int, size);
    const result = await request.query<DatabaseRow>(
      `SELECT TOP (@size) * FROM ${table} WHERE id > @last ORDER BY id`,
    );
    return result.recordset;
  }

  async readChanges(afterId: number, size: number): Promise<ChangeEvent[]> {
    const request = this.connections.source.request();
    request.input('last', sql.BigInt, afterId);
    request.input('size', sql.Int, size);
    const result = await request.query<ChangeRow>(
      'SELECT TOP (@size) * FROM migration_change_log WHERE change_id > @last ORDER BY change_id',
    );
    return result.recordset.map((row) => ({
      changeId: Number(row.change_id),
      tableName: row.table_name,
      operation: row.operation,
      recordId: Number(row.record_id),
      rowData: row.row_data ? (JSON.parse(row.row_data) as DatabaseRow) : null,
      changedAt: row.changed_at,
    }));
  }

  async count(table: TableName): Promise<number> {
    const result = await this.connections.source
      .request()
      .query<{ count: number | string }>(`SELECT COUNT_BIG(*) count FROM ${table}`);
    return Number(result.recordset[0]!.count);
  }

  async idRange(table: TableName): Promise<{ min: number | null; max: number | null }> {
    const result = await this.connections.source
      .request()
      .query<{ min: number | null; max: number | null }>(
        `SELECT MIN(id) min,MAX(id) max FROM ${table}`,
      );
    return result.recordset[0]!;
  }

  async readRows(table: TableName, ids: readonly number[]): Promise<DatabaseRow[]> {
    if (!ids.length) return [];
    const request = this.connections.source.request();
    const parameters = ids.map((id, index) => {
      request.input(`id${index}`, sql.BigInt, id);
      return `@id${index}`;
    });
    return (
      await request.query<DatabaseRow>(
        `SELECT * FROM ${table} WHERE id IN (${parameters.join(',')}) ORDER BY id`,
      )
    ).recordset;
  }

  async generateData(customers: number, orders: number): Promise<Record<string, number>> {
    await this.reset();
    const arabic = ['ليلى أحمد', 'عمر خالد', 'نور علي', 'سارة محمد', 'يوسف حسن'];
    const english = ['Maya Chen', 'Noah Williams', 'Sofia Rossi', 'Ethan Brown', 'Ava Wilson'];
    const productNames = [
      'قهوة عربية',
      'دفتر جلدي',
      'سماعات لاسلكية',
      'Travel Flask',
      'Desk Lamp',
      'Mechanical Keyboard',
    ];
    const uuid = (value: number) =>
      `00000000-0000-4000-8000-${value.toString().padStart(12, '0')}`;

    const customerRows = Array.from({ length: customers }, (_, index) => [
      uuid(index + 1),
      index % 3 === 0
        ? arabic[index % arabic.length]
        : `${english[index % english.length]} ${index + 1}`,
      `user${index + 1}@example.test`,
      index % 11 !== 0,
      index % 7 === 0 ? null : index % 2 ? 'Preferred customer' : 'عميل مميز',
      new Date(Date.now() - index * 60_000),
      new Date(Date.now() - index * 30_000),
    ]);
    await this.bulkInsert('customers', [
      ['public_id', sql.UniqueIdentifier], ['name', sql.NVarChar(180)],
      ['email', sql.NVarChar(220)], ['is_active', sql.Bit], ['notes', sql.NVarChar(sql.MAX)],
      ['created_at', sql.DateTime2], ['updated_at', sql.DateTime2],
    ], customerRows);

    const productCount = Math.max(50, Math.ceil(customers / 5));
    const productRows = Array.from({ length: productCount }, (_, index) => [
      `SKU-${String(index + 1).padStart(6, '0')}`,
      `${productNames[index % productNames.length]} ${index + 1}`,
      index % 5 ? null : 'منتج تجريبي',
      `${(index % 500) + 1}.${String(index % 100).padStart(2, '0')}00`,
      index % 13 !== 0,
      new Date(), new Date(),
    ]);
    await this.bulkInsert('products', [
      ['sku', sql.VarChar(40)], ['name', sql.NVarChar(180)],
      ['description', sql.NVarChar(sql.MAX)], ['price', sql.Decimal(19, 4)],
      ['is_available', sql.Bit], ['created_at', sql.DateTime2], ['updated_at', sql.DateTime2],
    ], productRows);

    const orderRows = Array.from({ length: orders }, (_, index) => [
      uuid(1_000_000 + index), (index % customers) + 1,
      ['pending', 'paid', 'shipped', 'completed'][index % 4],
      `${(index % 300) + 20}.${String(index % 100).padStart(2, '0')}00`,
      index % 6 ? `Street ${index % 200}, Cairo` : 'شارع النيل، القاهرة',
      new Date(Date.now() - index * 10_000), new Date(),
    ]);
    await this.bulkInsert('orders', [
      ['public_id', sql.UniqueIdentifier], ['customer_id', sql.BigInt], ['status', sql.VarChar(24)],
      ['total', sql.Decimal(19, 4)], ['shipping_address', sql.NVarChar(sql.MAX)],
      ['placed_at', sql.DateTime2], ['updated_at', sql.DateTime2],
    ], orderRows);

    const itemRows = Array.from({ length: orders * 2 }, (_, index) => [
      (Math.floor(index / 2) % orders) + 1, (index % productCount) + 1,
      (index % 4) + 1, `${(index % 100) + 5}.2500`, new Date(),
    ]);
    await this.bulkInsert('order_items', [
      ['order_id', sql.BigInt], ['product_id', sql.BigInt], ['quantity', sql.Int],
      ['unit_price', sql.Decimal(19, 4)], ['created_at', sql.DateTime2],
    ], itemRows);

    const paymentCount = Math.floor(orders * 0.7);
    const paymentRows = Array.from({ length: paymentCount }, (_, index) => [
      uuid(3_000_000 + index), index + 1,
      `${(index % 300) + 20}.${String(index % 100).padStart(2, '0')}00`,
      index % 2 ? 'card' : 'cash', 'paid', new Date(), new Date(),
    ]);
    await this.bulkInsert('payments', [
      ['public_id', sql.UniqueIdentifier], ['order_id', sql.BigInt],
      ['amount', sql.Decimal(19, 4)], ['method', sql.VarChar(24)], ['status', sql.VarChar(24)],
      ['paid_at', sql.DateTime2], ['created_at', sql.DateTime2],
    ], paymentRows);
    return { customers, products: productCount, orders, orderItems: itemRows.length, payments: paymentCount };
  }

  async performTrafficOperation(): Promise<'insert' | 'update' | 'delete' | null> {
    const choice = Math.random();
    if (choice < 0.45) {
      const customer = (await this.connections.source.request().query<{ id: number }>(
        'SELECT TOP 1 id FROM customers ORDER BY NEWID()',
      )).recordset[0];
      if (!customer) return null;
      await this.connections.source.request().input('uuid', randomUUID()).input('customer', customer.id)
        .query("INSERT orders(public_id,customer_id,status,total,shipping_address,placed_at) VALUES(@uuid,@customer,'pending',42.2500,N'القاهرة',SYSUTCDATETIME())");
      return 'insert';
    }
    if (choice < 0.82) {
      await this.connections.source.request().query(
        "UPDATE TOP (1) orders SET status=CASE status WHEN 'pending' THEN 'paid' ELSE 'completed' END,updated_at=SYSUTCDATETIME() WHERE id=(SELECT TOP 1 id FROM orders ORDER BY NEWID())",
      );
      return 'update';
    }
    await this.connections.source.request().query(
      'DELETE TOP (1) FROM payments WHERE id=(SELECT TOP 1 id FROM payments ORDER BY NEWID())',
    );
    return 'delete';
  }

  async reset(): Promise<void> {
    await this.connections.source.request().batch(
      `DELETE FROM migration_change_log; DELETE FROM payments; DELETE FROM order_items;
       DELETE FROM orders; DELETE FROM products; DELETE FROM customers;
       DBCC CHECKIDENT (customers, RESEED, 0); DBCC CHECKIDENT (products, RESEED, 0);
       DBCC CHECKIDENT (orders, RESEED, 0); DBCC CHECKIDENT (order_items, RESEED, 0);
       DBCC CHECKIDENT (payments, RESEED, 0);`,
    );
  }

  private async bulkInsert(
    table: TableName,
    definitions: readonly (readonly [string, SqlType])[],
    rows: readonly (readonly unknown[])[],
  ): Promise<void> {
    const chunkSize = Math.min(500, Math.floor(2_000 / definitions.length));
    for (let start = 0; start < rows.length; start += chunkSize) {
      const request = this.connections.source.request();
      const values = rows.slice(start, start + chunkSize).map((row, rowIndex) => {
        const parameters = row.map((value, columnIndex) => {
          const name = `p${rowIndex}_${columnIndex}`;
          request.input(name, definitions[columnIndex]![1], value);
          return `@${name}`;
        });
        return `(${parameters.join(',')})`;
      });
      await request.query(
        `INSERT INTO ${table} (${definitions.map(([name]) => name).join(',')}) VALUES ${values.join(',')}`,
      );
    }
  }
}
