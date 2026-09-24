export const config = {
  mssql: {
    server: process.env.MSSQL_HOST ?? 'localhost',
    port: Number(process.env.MSSQL_PORT ?? 1433),
    database: process.env.MSSQL_DATABASE ?? 'zeroshift',
    user: process.env.MSSQL_USER ?? 'sa',
    password: process.env.MSSQL_SA_PASSWORD ?? 'ZeroShift!Local2026',
    options: { encrypt: false, trustServerCertificate: true },
    pool: { max: 8, min: 0, idleTimeoutMillis: 30000 },
  },
  postgres: {
    host: process.env.POSTGRES_HOST ?? 'localhost',
    port: Number(process.env.POSTGRES_PORT ?? 5432),
    database: process.env.POSTGRES_DB ?? 'zeroshift',
    user: process.env.POSTGRES_USER ?? 'zeroshift',
    password: process.env.POSTGRES_PASSWORD ?? 'zeroshift_local',
    max: 10,
  },
  port: Number(process.env.BACKEND_PORT ?? 3000),
};
