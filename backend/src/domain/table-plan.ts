import type { TableName } from './models.js';

export interface TableDefinition {
  name: TableName;
  columns: readonly string[];
  dependsOn: readonly TableName[];
}

export const TABLE_PLAN: readonly TableDefinition[] = [
  {
    name: 'customers',
    columns: ['id', 'public_id', 'name', 'email', 'is_active', 'notes', 'created_at', 'updated_at'],
    dependsOn: [],
  },
  {
    name: 'products',
    columns: ['id', 'sku', 'name', 'description', 'price', 'is_available', 'created_at', 'updated_at'],
    dependsOn: [],
  },
  {
    name: 'orders',
    columns: ['id', 'public_id', 'customer_id', 'status', 'total', 'shipping_address', 'placed_at', 'updated_at'],
    dependsOn: ['customers'],
  },
  {
    name: 'order_items',
    columns: ['id', 'order_id', 'product_id', 'quantity', 'unit_price', 'created_at'],
    dependsOn: ['orders', 'products'],
  },
  {
    name: 'payments',
    columns: ['id', 'public_id', 'order_id', 'amount', 'method', 'status', 'paid_at', 'created_at'],
    dependsOn: ['orders'],
  },
] as const;

export const tableDefinition = (name: TableName): TableDefinition => {
  const definition = TABLE_PLAN.find((candidate) => candidate.name === name);
  if (!definition) throw new Error(`No table definition for ${name}`);
  return definition;
};
