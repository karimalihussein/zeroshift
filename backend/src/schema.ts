import type { TableName } from './types.js';
export const columns: Record<TableName, string[]> = {
  customers: ['id', 'public_id', 'name', 'email', 'is_active', 'notes', 'created_at', 'updated_at'],
  products: [
    'id',
    'sku',
    'name',
    'description',
    'price',
    'is_available',
    'created_at',
    'updated_at',
  ],
  orders: [
    'id',
    'public_id',
    'customer_id',
    'status',
    'total',
    'shipping_address',
    'placed_at',
    'updated_at',
  ],
  order_items: ['id', 'order_id', 'product_id', 'quantity', 'unit_price', 'created_at'],
  payments: ['id', 'public_id', 'order_id', 'amount', 'method', 'status', 'paid_at', 'created_at'],
};
export const mappings = [
  { sql: 'UNIQUEIDENTIFIER', pg: 'UUID', note: 'Preserved as canonical UUID' },
  { sql: 'BIT', pg: 'BOOLEAN', note: '0/1 becomes false/true' },
  { sql: 'NVARCHAR(MAX)', pg: 'TEXT', note: 'Full Unicode, including Arabic' },
  { sql: 'NVARCHAR(n)', pg: 'VARCHAR(n)', note: 'Unicode is native in PostgreSQL UTF-8' },
  { sql: 'DATETIME2(3)', pg: 'TIMESTAMPTZ', note: 'Source values are treated as UTC' },
  {
    sql: 'DECIMAL(19,4)',
    pg: 'NUMERIC(19,4)',
    note: 'Exact decimal; never JavaScript floating point',
  },
  { sql: 'BIGINT IDENTITY', pg: 'BIGINT IDENTITY', note: 'Sequence synchronized after copy' },
];
export const dependencies = [
  { table: 'customers', dependsOn: [] },
  { table: 'products', dependsOn: [] },
  { table: 'orders', dependsOn: ['customers'] },
  { table: 'order_items', dependsOn: ['orders', 'products'] },
  { table: 'payments', dependsOn: ['orders'] },
];
