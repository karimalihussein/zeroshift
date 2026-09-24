import sql from 'mssql';
import pg from 'pg';
import fs from 'node:fs/promises';
import path from 'node:path';
import { config } from './config.js';
export const source = new sql.ConnectionPool(config.mssql);
export const target = new pg.Pool(config.postgres);
export async function connectDatabases() {
  await initializeSqlServer();
  await source.connect();
  await target.query('SELECT 1');
}
async function initializeSqlServer() {
  const candidates = [
    path.resolve('database/sqlserver/001_schema.sql'),
    path.resolve('../database/sqlserver/001_schema.sql'),
  ];
  let text = '';
  for (const file of candidates) {
    try {
      text = await fs.readFile(file, 'utf8');
      break;
    } catch {}
  }
  if (!text) throw new Error('SQL Server schema file not found');
  const batches = text
    .split(/^GO\s*$/gim)
    .map((x) => x.trim())
    .filter(Boolean);
  const bootstrap = new sql.ConnectionPool({ ...config.mssql, database: 'master' });
  await bootstrap.connect();
  try {
    await bootstrap.request().batch(batches[0]!);
  } finally {
    await bootstrap.close();
  }
  const schemaPool = new sql.ConnectionPool(config.mssql);
  await schemaPool.connect();
  try {
    for (const batch of batches.slice(2)) await schemaPool.request().batch(batch);
  } finally {
    await schemaPool.close();
  }
}
export async function closeDatabases() {
  await source.close();
  await target.end();
}
