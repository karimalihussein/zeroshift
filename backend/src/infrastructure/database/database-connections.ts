import fs from 'node:fs/promises';
import path from 'node:path';
import sql from 'mssql';
import pg from 'pg';
import type { EnvironmentConfig } from '../config/environment.js';

export class DatabaseConnections {
  readonly source: sql.ConnectionPool;
  readonly target: pg.Pool;

  constructor(private readonly config: EnvironmentConfig) {
    this.source = new sql.ConnectionPool({
      ...config.sqlServer,
      options: { encrypt: false, trustServerCertificate: true },
      pool: { max: 8, min: 0, idleTimeoutMillis: 30_000 },
    });
    this.target = new pg.Pool({ ...config.postgres, max: 10 });
  }

  async initialize(): Promise<void> {
    await this.initializeSqlServerSchema();
    await this.source.connect();
    await this.target.query('SELECT 1');
    await this.target.query('ALTER TABLE migration_runs ADD COLUMN IF NOT EXISTS resume_stage VARCHAR(40)');
  }

  async close(): Promise<void> {
    await Promise.all([this.source.close(), this.target.end()]);
  }

  private async initializeSqlServerSchema(): Promise<void> {
    const schema = await this.readSqlServerSchema();
    const batches = schema
      .split(/^GO\s*$/gim)
      .map((batch) => batch.trim())
      .filter(Boolean);

    const bootstrap = new sql.ConnectionPool({
      ...this.config.sqlServer,
      database: 'master',
      options: { encrypt: false, trustServerCertificate: true },
    });
    await bootstrap.connect();
    try {
      await bootstrap.request().batch(batches[0]!);
    } finally {
      await bootstrap.close();
    }

    const schemaPool = new sql.ConnectionPool({
      ...this.config.sqlServer,
      options: { encrypt: false, trustServerCertificate: true },
    });
    await schemaPool.connect();
    try {
      for (const batch of batches.slice(2)) await schemaPool.request().batch(batch);
    } finally {
      await schemaPool.close();
    }
  }

  private async readSqlServerSchema(): Promise<string> {
    const candidates = [
      path.resolve('database/sqlserver/001_schema.sql'),
      path.resolve('../database/sqlserver/001_schema.sql'),
    ];
    for (const candidate of candidates) {
      try {
        return await fs.readFile(candidate, 'utf8');
      } catch {
        // Try the next deployment layout.
      }
    }
    throw new Error('SQL Server schema file not found');
  }
}
