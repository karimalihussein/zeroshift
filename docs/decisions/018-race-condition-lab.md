# ADR 018: A race condition lab on real transactions, with forced interleavings

**Status:** Accepted

## Decision

The race condition lab (`/race`) runs each concurrent request as a real PostgreSQL transaction on its
own thread and connection. It records every step as a structured event, and the UI draws only those
events.

- **Reproducible races without fakes.** CONTROLLED interleaving orders when each request sends its
  next statement:
  - turns: A reads, then B reads;
  - barriers: nobody writes until everyone has read.

  A request that PostgreSQL reports as blocked counts as having arrived. Without that rule, a safe
  strategy would deadlock against the choreography. With it, the strategy's lock waits stay real.
  The choreography's own holds are recorded (`SYNC_POINT`) and measured apart from lock waits.
- **Lock waits from PostgreSQL, not inferred.**
  - A monitor polls `pg_stat_activity` and `pg_blocking_pids()` for the run's backends every few
    milliseconds.
  - A wait is attributed to a statement only while that statement is in flight (a token per
    statement), so a late poll never marks a statement that has already returned.
  - Aborts are PostgreSQL's SQLSTATEs: `40001` and `40P01`, with its DETAIL line.
- **Everything shown is derived from events.**
  - The metrics, key moments and the "why" narrative are computed from the recorded events.
  - The invariant is checked against committed rows, or for isolation anomalies against what the
    transactions read.
- **One run at a time,** so runs never contend with each other and blur what each shows.
- **A module of its own** (`race-lab`) inside the control plane:
  - its own pool, so up to 20 racing transactions never starve the control plane;
  - its own schema `race_lab` with its own Flyway history, so a reset touches nothing else.

  The page and assets live with the other lab pages.
- **Its own spans.**
  - The agent runs with its automatic instrumentations off and only the API bridge on. Otherwise
    the migration lab's polling (every 100–250 ms) would flood Tempo with orphan JDBC traces.
  - The lab emits run → transaction → statement spans itself, with lock waits as span events.

## Why

- A frontend timeline driven by timers can show any story. The lesson is only credible if the
  database produced it: a real row lock, a real version mismatch, a real 40001.
- Natural timing hides races: with the default settings the unsafe code usually passes. Forcing the
  interleaving shows the bug on every run, while NATURAL remains available to show why such bugs
  survive testing.
- Events, not logs: the UI, the tests and the comparison all read the same typed records, so what the
  page shows is what the tests assert.

## Consequences

- Latency under CONTROLLED includes the lab's holds. The result shows them separately
  ("Lab sync wait"), and NATURAL gives fairer latency comparisons.
- Live listeners (SSE) are fed by a dispatcher thread. Before that, a browser subscribing mid-run
  blocked the recording threads and stretched a lock-wait detection from about 5 ms to about
  900 ms. `RaceLabIT` now checks that a deliberately slow listener leaves a run's timing alone.
- A read that waited for a lock began before the lock holder committed, but saw the database as of
  its completion. Staleness is therefore judged against a statement's completion, not its start.
