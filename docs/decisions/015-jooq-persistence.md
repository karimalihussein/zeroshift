# ADR 015: jOOQ over the Flyway-built schema for the event-driven services

**Status:** Accepted

## Decision

Flyway migrations remain the only definition of each schema. jOOQ classes are generated from the
schema those migrations actually build: `JooqCodegen` (platform test-jar) applies a module's
migrations to a throwaway `postgres:17-alpine`, the same image the lab runs, and generates into
`src/generated/java`, which is committed.

```sh
mvn -Pjooq-codegen -DskipTests process-test-classes   # after adding a migration; then commit
```

Every service IT also checks that the generated tables and columns equal the migrated database
(`GeneratedSchema.assertMatchesDatabase`), so a migration committed without regenerating fails the
build.

The platform module owns the generated classes for its tables (outbox, inbox, decisions, faults);
each service owns those for its own tables. A service never queries another module's tables, which
is why the projection rebuild asks `Inbox.forget(consumer)` instead of deleting from
`processed_message` itself.

Spring Boot's jOOQ starter gives a `DSLContext` that joins Spring's transactions, so the inbox
claim, the handler's writes and the outbox row still commit or roll back together
(`OrderServiceIT.theEventAndItsOutboxRowCommitOrRollBackTogether`).

## Why

- String-built SQL had already caused a bug: the projection joined compensations with `'|'` and
  split them back with `string_to_array`, so a reason containing `|` became two entries. Bound
  arrays and typed columns remove that class of mistake, and dynamic `SET` clauses become one typed
  statement per event.
- Optional filters (`WHERE orderId = ? if given`) become `noCondition()` instead of
  `? IS NULL OR …` tricks.
- Generating from a real migrated database, rather than parsing the SQL files, sees exactly what
  PostgreSQL built (JSONB, `text[]`, partial indexes, publications) and never requires editing a
  migration that Flyway has already checksummed.
- Committing the generated code keeps Docker image builds free of a database.

## Kept as plain SQL, on purpose

- `pg_replication_slots` in `LabAdminController`: a system view, not part of our schema.
- `clock_timestamp()` (`PostgresClock`): jOOQ's `currentOffsetDateTime()` renders
  `current_timestamp`, frozen at transaction start; lease expiries need the real time.
- The Flyway Java migration that creates the replication slot.
- The whole migration lab: its SQL Server side cannot use jOOQ's open-source edition, and its
  PostgreSQL SQL is static and parameterised, with extensive integration tests. Converting it would
  be churn without a lesson.

## Not used

No JPA/Hibernate: every write here is a specific statement with a specific concurrency meaning
(upserts, `SKIP LOCKED`, version checks, `FOR SHARE`). An ORM would hide exactly those. No DAOs or
generated POJOs: the small stores map records to the domain's own types.
