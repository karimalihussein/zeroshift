export interface EnvironmentConfig {
  readonly sqlServer: {
    readonly server: string;
    readonly port: number;
    readonly database: string;
    readonly user: string;
    readonly password: string;
  };
  readonly postgres: {
    readonly host: string;
    readonly port: number;
    readonly database: string;
    readonly user: string;
    readonly password: string;
  };
  readonly backendPort: number;
}

function integer(name: string, raw: string | undefined, fallback: number): number {
  const value = Number(raw ?? fallback);
  if (!Number.isInteger(value) || value < 1 || value > 65535) {
    throw new Error(`${name} must be an integer between 1 and 65535`);
  }
  return value;
}

function text(name: string, raw: string | undefined, fallback: string): string {
  const value = raw ?? fallback;
  if (!value.trim()) throw new Error(`${name} cannot be empty`);
  return value;
}

export function loadEnvironment(env: NodeJS.ProcessEnv = process.env): EnvironmentConfig {
  return {
    sqlServer: {
      server: text('MSSQL_HOST', env.MSSQL_HOST, 'localhost'),
      port: integer('MSSQL_PORT', env.MSSQL_PORT, 1433),
      database: text('MSSQL_DATABASE', env.MSSQL_DATABASE, 'zeroshift'),
      user: text('MSSQL_USER', env.MSSQL_USER, 'sa'),
      password: text('MSSQL_SA_PASSWORD', env.MSSQL_SA_PASSWORD, 'ZeroShift!Local2026'),
    },
    postgres: {
      host: text('POSTGRES_HOST', env.POSTGRES_HOST, 'localhost'),
      port: integer('POSTGRES_PORT', env.POSTGRES_PORT, 5432),
      database: text('POSTGRES_DB', env.POSTGRES_DB, 'zeroshift'),
      user: text('POSTGRES_USER', env.POSTGRES_USER, 'zeroshift'),
      password: text('POSTGRES_PASSWORD', env.POSTGRES_PASSWORD, 'zeroshift_local'),
    },
    backendPort: integer('BACKEND_PORT', env.BACKEND_PORT, 3000),
  };
}
