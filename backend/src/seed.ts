import sql from 'mssql';
import { source } from './db.js';
import { event } from './state.js';
const arabic = ['ليلى أحمد', 'عمر خالد', 'نور علي', 'سارة محمد', 'يوسف حسن'];
const english = ['Maya Chen', 'Noah Williams', 'Sofia Rossi', 'Ethan Brown', 'Ava Wilson'];
const products = [
  'قهوة عربية',
  'دفتر جلدي',
  'سماعات لاسلكية',
  'Travel Flask',
  'Desk Lamp',
  'Mechanical Keyboard',
];
function uuid(i: number) {
  return `00000000-0000-4000-8000-${i.toString().padStart(12, '0')}`;
}
async function insertRows(table: string, defs: [string, any][], rows: unknown[][]) {
  const chunkSize = Math.min(500, Math.floor(2000 / defs.length));
  for (let start = 0; start < rows.length; start += chunkSize) {
    const chunk = rows.slice(start, start + chunkSize),
      req = source.request();
    const values = chunk
      .map(
        (row, ri) =>
          `(${row
            .map((v, ci) => {
              const n = `p${ri}_${ci}`;
              req.input(n, defs[ci]![1], v);
              return '@' + n;
            })
            .join(',')})`,
      )
      .join(',');
    await req.query(`INSERT INTO ${table} (${defs.map((d) => d[0]).join(',')}) VALUES ${values}`);
  }
}
export async function seed(customers = 1000, orders = 5000) {
  await source
    .request()
    .batch(
      'DELETE FROM migration_change_log; DELETE FROM payments; DELETE FROM order_items; DELETE FROM orders; DELETE FROM products; DELETE FROM customers; DBCC CHECKIDENT (customers, RESEED, 0); DBCC CHECKIDENT (products, RESEED, 0); DBCC CHECKIDENT (orders, RESEED, 0); DBCC CHECKIDENT (order_items, RESEED, 0); DBCC CHECKIDENT (payments, RESEED, 0);',
    );
  const customerRows = Array.from({ length: customers }, (_, i) => [
    uuid(i + 1),
    i % 3 === 0 ? arabic[i % arabic.length] : english[i % english.length] + ` ${i + 1}`,
    `user${i + 1}@example.test`,
    i % 11 !== 0,
    i % 7 === 0 ? null : i % 2 ? 'Preferred customer' : 'عميل مميز',
    new Date(Date.now() - i * 60000),
    new Date(Date.now() - i * 30000),
  ]);
  await insertRows(
    'customers',
    [
      ['public_id', sql.UniqueIdentifier],
      ['name', sql.NVarChar(180)],
      ['email', sql.NVarChar(220)],
      ['is_active', sql.Bit],
      ['notes', sql.NVarChar(sql.MAX)],
      ['created_at', sql.DateTime2],
      ['updated_at', sql.DateTime2],
    ],
    customerRows,
  );
  const productRows = Array.from({ length: Math.max(50, Math.ceil(customers / 5)) }, (_, i) => [
    `SKU-${String(i + 1).padStart(6, '0')}`,
    products[i % products.length] + ` ${i + 1}`,
    i % 5 ? null : 'منتج تجريبي',
    (i % 500) + 1 + '.' + String(i % 100).padStart(2, '0') + '00',
    i % 13 !== 0,
    new Date(),
    new Date(),
  ]);
  await insertRows(
    'products',
    [
      ['sku', sql.VarChar(40)],
      ['name', sql.NVarChar(180)],
      ['description', sql.NVarChar(sql.MAX)],
      ['price', sql.Decimal(19, 4)],
      ['is_available', sql.Bit],
      ['created_at', sql.DateTime2],
      ['updated_at', sql.DateTime2],
    ],
    productRows,
  );
  const orderRows = Array.from({ length: orders }, (_, i) => [
    uuid(1000000 + i),
    (i % customers) + 1,
    ['pending', 'paid', 'shipped', 'completed'][i % 4],
    `${(i % 300) + 20}.${String(i % 100).padStart(2, '0')}00`,
    i % 6 ? `Street ${i % 200}, Cairo` : 'شارع النيل، القاهرة',
    new Date(Date.now() - i * 10000),
    new Date(),
  ]);
  await insertRows(
    'orders',
    [
      ['public_id', sql.UniqueIdentifier],
      ['customer_id', sql.BigInt],
      ['status', sql.VarChar(24)],
      ['total', sql.Decimal(19, 4)],
      ['shipping_address', sql.NVarChar(sql.MAX)],
      ['placed_at', sql.DateTime2],
      ['updated_at', sql.DateTime2],
    ],
    orderRows,
  );
  const itemRows = Array.from({ length: orders * 2 }, (_, i) => [
    (Math.floor(i / 2) % orders) + 1,
    (i % productRows.length) + 1,
    (i % 4) + 1,
    `${(i % 100) + 5}.2500`,
    new Date(),
  ]);
  await insertRows(
    'order_items',
    [
      ['order_id', sql.BigInt],
      ['product_id', sql.BigInt],
      ['quantity', sql.Int],
      ['unit_price', sql.Decimal(19, 4)],
      ['created_at', sql.DateTime2],
    ],
    itemRows,
  );
  const payRows = Array.from({ length: Math.floor(orders * 0.7) }, (_, i) => [
    uuid(3000000 + i),
    i + 1,
    `${(i % 300) + 20}.${String(i % 100).padStart(2, '0')}00`,
    i % 2 ? 'card' : 'cash',
    'paid',
    new Date(),
    new Date(),
  ]);
  await insertRows(
    'payments',
    [
      ['public_id', sql.UniqueIdentifier],
      ['order_id', sql.BigInt],
      ['amount', sql.Decimal(19, 4)],
      ['method', sql.VarChar(24)],
      ['status', sql.VarChar(24)],
      ['paid_at', sql.DateTime2],
      ['created_at', sql.DateTime2],
    ],
    payRows,
  );
  await event('INFO', 'INFO', 'Source dataset generated', {
    customers,
    orders,
    orderItems: itemRows.length,
    payments: payRows.length,
  });
  return {
    customers,
    products: productRows.length,
    orders,
    orderItems: itemRows.length,
    payments: payRows.length,
  };
}
