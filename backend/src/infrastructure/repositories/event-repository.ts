import type pg from 'pg';
import type { EventRepository } from '../../application/ports.js';

export class PostgresEventRepository implements EventRepository {
  constructor(private readonly pool: pg.Pool) {}

  async append(category: string, level: string, message: string, details: unknown = {}): Promise<void> {
    await this.pool.query(
      'INSERT INTO lab_events(category,level,message,details) VALUES($1,$2,$3,$4)',
      [category, level, message, JSON.stringify(details)],
    );
  }

  async recent(limit: number): Promise<readonly Record<string, unknown>[]> {
    return (await this.pool.query('SELECT * FROM lab_events ORDER BY id DESC LIMIT $1', [limit])).rows;
  }

  async after(id: number, limit: number): Promise<readonly Record<string, unknown>[]> {
    return (
      await this.pool.query('SELECT * FROM lab_events WHERE id>$1 ORDER BY id LIMIT $2', [id, limit])
    ).rows;
  }
}
