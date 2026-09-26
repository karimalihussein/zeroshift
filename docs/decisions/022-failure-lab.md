# ADR 022: The failure lab: real crashes on isolated infrastructure, one comparison per experiment

**Status:** Accepted

## Decision

Phase 5 adds `/failures` to the control plane: four experiments that each run the same five
stages, **Naive design → Failure → Observable consequence → Correct design → Recovery**. A stage
acts on real infrastructure, returns what it measured, and lists the claims it checked against
those measurements (`checks`). A stage whose claim does not hold is kept as the lab's last
failure, with its measurements, but is not counted as done. Stages also contribute rows to a
naive-versus-correct comparison (`compare`) that the page builds up as they run.

- **Dangerous work runs on its own infrastructure** (Compose profile `failure-lab`, about 0.5 GB):
  - `failure-postgres`, PostgreSQL 17 with `max_prepared_transactions = 16`. The services'
    `commerce-postgres` keeps the default 0, so no prepared transaction can ever hold their rows or
    their vacuum horizon. Every database the labs touch is named `fl_*`; `FailureDb` refuses any
    other name, so a reset or a disaster drill cannot drop anything else.
  - `kafka-secure`, a single KRaft broker whose every listener (clients, inter-broker and the
    controller) is SASL/PLAIN, with the `StandardAuthorizer`, `allow.everyone.if.no.acl.found =
    false` and only `User:admin` as super user. The event lab's broker is not changed: it is the
    lab's "before".
  - Lab topics on the event lab's broker (`lab.dr.payments`, `lab.trust.payments`) and consumer
    groups prefixed `lab-dr-`/`lab-trust-` are declared in `topics.sh` and recreated by the labs.
- **The coordinator is a process, so its crash is real.** `Coordinator` is a `main` that the
  control plane launches as a child JVM (through Spring Boot's `PropertiesLauncher` when running
  from the packaged jar). It prints JSON lines and, at a named pause point, waits on stdin; the lab
  kills it there with `Process.destroyForcibly()` (SIGKILL, exit 137). The pause only chooses
  *when* the crash lands, like the race lab's controlled interleaving; what survives is only what
  the databases hold.
- **2PC uses PostgreSQL's own two-phase commit.** Each participant (fl_payments, fl_inventory)
  does its work and runs `PREPARE TRANSACTION`; the coordinator logs its decision in
  fl_coordinator before `COMMIT PREPARED`. The saga runs the same checkout as local transactions
  with a durable log and a step deadline; its recovery is a new coordinator process that
  compensates (an idempotent refund) every saga past its deadline. The two protocols use identical
  but separate rows, so the 2PC run's in-doubt locks never distort the saga run's measurements.
- **Evidence comes from PostgreSQL's catalogs.** `pg_prepared_xacts` (in doubt), `pg_locks` rows with a null pid,
  found through the prepared xid's own `transactionid` lock (locks with no session), the row's `xmax`, `pg_stat_activity` and
  `pg_blocking_pids` (0 means a prepared transaction), and `VACUUM (VERBOSE)` for dead rows it
  cannot remove. "Other work" is a real session per probe with a `lock_timeout`, rolled back after
  measuring so the lab's balances stay exact.
- **Manual recovery is the operator's procedure.** Read the coordinator's log: a durable COMMIT
  decision must be finished, no decision means presumed abort. The inspector can also resolve a
  single branch by hand, which is allowed on purpose: resolving the branches differently breaks
  atomicity, and the next reconciliation shows it.
- **Isolation anomalies reuse the race lab's engine** instead of a second one: the lab creates
  race-lab runs (controlled interleaving, one connection per request) for lost update and write
  skew under each strategy and compares their measured results. The runs stay in the race lab,
  where their timelines can be opened.
- **Disaster recovery uses a real snapshot and a real loss.** Backups are `CREATE DATABASE …
  TEMPLATE … STRATEGY FILE_COPY` (a file-level copy; writers must be stopped while it copies, which
  the lab states). The disaster is `DROP DATABASE … WITH (FORCE)`. Consumers run in batches (read
  to the end offsets taken at start, then stop) so "down" and "up" are exact points in time. The
  truth is the topic itself, read from offset 0 by a consumer with no group.
- **Security decisions are the broker's answers**: an offset, `SaslAuthenticationException`,
  `TopicAuthorizationException`/`GroupAuthorizationException`, or a connection the broker would not
  serve. The lab records each in fl_trust and never infers one.
- **Traces and metrics.** Each stage is a span (`failure-lab <lab> stage <n>`) through the race
  lab's OpenTelemetry API bridge, with span events for the crash, the blocked checkout, the
  disaster and the decisions; the page links each stage to Tempo. Micrometer counts stages by
  outcome and exposes the measured RPO and RTO as gauges.

## Excluded, and why

- **Message signing.** ACLs already bind "who may write payment events" to an authenticated
  principal, which is the lesson. Signing adds one real lesson in this system (Debezium relays
  every service's outbox under a single Connect principal, so topic ACLs cannot tell the services
  apart), but demonstrating it needs per-service keys in the outbox path of every service; it is
  documented as a limitation instead.
- **TLS.** SASL/PLAIN over an unencrypted listener sends passwords in clear on the Docker network.
  Production would use SASL_SSL or mTLS; certificates would add a PKI to the lab without changing
  what the ACL decisions show.
- **An XA transaction manager** (Narayana, Atomikos). PostgreSQL's `PREPARE TRANSACTION` is the
  resource-manager side of XA; a transaction manager would hide exactly the log and the in-doubt
  state this lab needs to show.
- **Point-in-time recovery with WAL archiving.** A template copy gives an exact, visible snapshot
  point; PITR would add an archive and a restore command without changing the offset-gap lesson.
