# ZeroShift

**A real SQL Server → PostgreSQL migration you can watch, interrupt and break on purpose.**

ZeroShift is a small Spring Boot application with a single dashboard. It copies two related tables from SQL Server to PostgreSQL while real traffic keeps writing to the source, then validates the data and cuts over with a short write freeze. Every number on the screen comes from the databases. No progress or event is faked in the browser.

**Java 25 · Spring Boot 4.1 · Maven · Thymeleaf · SQL Server 2022 · PostgreSQL 17 · JUnit 5 · Testcontainers**

## What it demonstrates

- **Change capture from a committed boundary** using native SQL Server Change Tracking
- **Resumable keyset snapshot** loaded with PostgreSQL `COPY`, with each checkpoint committed atomically with its rows
- **Catch-up under live traffic:** a read-heavy mix of real INSERT, UPDATE, DELETE and READ operations on random rows keeps running during the copy, catch-up, validation and cutover
- **Pause, simulated crash and process kill:** recovery always resumes from the last committed checkpoint
- **Full validation:** counts plus ordered SHA-256 fingerprints of every column, behind a real source write fence
- **Gated cutover:** fence, drain, validate, sync sequences, switch routing
- **Live Data Changes:** insert, update or delete rows in SQL Server yourself and watch them replay into PostgreSQL, or pause replay to hold the two databases apart

## Quick start

Requires Docker with about 6–8 GB of memory (SQL Server alone needs 2 GB).

```sh
git clone https://github.com/karimalihussein/zeroshift.git
cd zeroshift
docker compose up
```

Open **http://localhost:8080**. `docker compose up` runs the development stack with hot reload (see [Docker development](#docker-development)); the first start downloads Maven dependencies, and SQL Server takes a minute to become healthy. Its Linux image is amd64-only, so Compose runs it under emulation on Apple Silicon.

## Using the dashboard

1. **Generate Demo Data:** customers and related orders, including Unicode, `NULL` emails and exact decimals. The **Rows** field accepts 1–10,000,000; the default is 10,000.
2. **Start Live Traffic:** continuous real operations against whichever database is primary: SQL Server (captured by Change Tracking) before cutover, PostgreSQL directly after. The Live traffic panel shows backend-counted totals per operation, operations/sec, errors and the current target. A failed operation is counted and retried; five failures in a row stop traffic.
3. **Start Migration:** snapshot → catch-up → indexes and constraints → **Ready**.
4. **Pause / Resume / Simulate Crash:** stop and resume at durable checkpoints. A simulated crash rolls back a half-written batch.
5. **Live Data Changes:** change real rows during the migration and compare them in the Record Inspector. See [docs/live-changes.md](docs/live-changes.md).
6. **Validate:** briefly fences the source and compares every row.
7. **Cutover:** fences SQL Server permanently, drains, validates, syncs sequences and makes PostgreSQL primary. Traffic continues on PostgreSQL.
8. **Reset:** clears the lab's data in both databases.

Progress is stage-weighted: snapshot rows 0–80%, catch-up 85%, prepare 90%, ready 95%, freeze 98%, complete 100%. It is not an ETA.

## Architecture

```
web ──────────────┐
                  ▼
infrastructure ─► application ─► domain
(JDBC, Spring)    (use cases,     (records, stages,
                   ports)          rules, errors)
```

`domain` and `application` have no Spring or JDBC dependencies. Infrastructure implements the ports, and `ApplicationWiring` composes the use cases.

| Responsibility | Code |
|---|---|
| Stages, state and domain rules | `domain/` |
| Ports | `application/port/` |
| Snapshot batch, catch-up, coordination | `application/SnapshotBatch`, `ChangeCatchUp`, `MigrationCoordinator` |
| Validation and cutover | `application/ValidationService`, `RowFingerprint`, `CutoverService` |
| Live Data Changes | `application/LiveChangesService` |
| SQL Server reads and Change Tracking | `infrastructure/SqlServerReader`, `ChangeTrackingCapture` |
| PostgreSQL COPY, state and checkpoints | `infrastructure/PostgresBulkLoader`, `PostgresMigrationStore` |
| HTTP | `web/DashboardController`, `LiveChangesController`, `ApiExceptionHandler` |
| UI | `resources/templates/index.html`, `resources/static/` |

All Java packages are under `src/main/java/io/zeroshift/`.

## Documentation

- [How consistency is kept](docs/consistency.md): transaction boundaries, the change window, crash recovery and the fence
- [Live Data Changes](docs/live-changes.md): the interactive CDC experiment and its API
- [Design decisions](docs/decisions/): change capture, snapshot boundary, batching, checkpointing, cutover, rollback
- [Explaining the migration](docs/interview-notes.md): a talk track and common follow-up questions
- [Verification](docs/verification.md): what was tested and measured

## Development

### Docker development

`docker compose up` merges `docker-compose.override.yml`, which runs the app with `mvn spring-boot:run` and the `dev` profile instead of the packaged jar. `src/` and `pom.xml` are mounted read-only; no image rebuild is needed:

| You edit | What happens |
|---|---|
| Thymeleaf templates, JS, CSS | Served straight from `src/` with caching off: refresh the browser |
| Java, `application*.yml`, `db/*.sql` | Recompiled within ~1s of saving; DevTools restarts the app once the compile succeeds. A failed compile is logged and the last good build keeps running |
| `pom.xml` | Maven restarts with the new classpath |

`docker compose logs -f app` shows the compile and restart output. The Maven cache and compiled classes live in named volumes, and the SQL Server and PostgreSQL data volumes are unchanged. To run the production image instead, bypass the override:

```sh
docker compose -f docker-compose.yml up -d --build
```

### Local tooling

Requires Java 25+, Maven 3.9+ and Docker.

```sh
mvn test               # unit tests
mvn verify             # unit + Testcontainers integration tests (real SQL Server and PostgreSQL)
mvn spotless:apply     # Google Java Format
```

Integration tests fail rather than skip when Docker is missing. To run the app outside Docker:

```sh
docker compose up -d sqlserver postgres
mvn spring-boot:run -Dspring-boot.run.profiles=dev   # the dev profile also works on the host
```

End-to-end checks against a running Compose stack (**both reset the demo data**):

```sh
python3 scripts/verify_demo.py          # traffic, crash/resume, container kill, validation, cutover
python3 scripts/verify_live_changes.py  # the paused-replay experiment over HTTP
```

### Configuration

Set in `.env` (see `.env.example`) or the environment. Compose passes them to the app.

| Variable | Default | Meaning |
|---|---|---|
| `BATCH_SIZE` | 500 | Rows per snapshot batch and replay page (1–10,000) |
| `SEED_ROWS` | 10000 | Default demo rows per table (1–10,000,000) |
| `TICK_MS` | 250 | Delay between migration steps |
| `TRAFFIC_MS` | 100 | Delay between simulated transactions |
| `JAVA_APP_PORT`, `JAVA_MSSQL_PORT`, `JAVA_POSTGRES_PORT` | 8080, 1434, 55433 | Host ports (bound to 127.0.0.1) |

The snapshot copies one batch per tick. For multi-million-row demos, raise `BATCH_SIZE`: with `BATCH_SIZE=10000` and 2M rows per table, seeding took 48 s and the snapshot about 4.5 minutes on a laptop.

## Scope

ZeroShift is an educational lab for one two-table application. It is not a general schema converter or a production migration tool.

- **Net changes:** Change Tracking reports the latest state per changed key, not every intermediate image.
- **Freeze length:** the final full validation runs inside the write freeze. That is fine for demo sizes; production systems validate incrementally.
- **Source fence:** triggers enforce the fence for ordinary DML only. Schema changes during a migration are not supported.
- **Single instance:** one app instance owns recovery; there is no leader election.
- **No failback:** after cutover SQL Server stays fenced ([ADR 006](docs/decisions/006-rollback.md)).
- **Local use only:** the app has no authentication, ports bind to loopback, and the credentials in `.env.example` are local demo values. SQL Server Developer edition is licensed for development and testing only.
