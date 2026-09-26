# Race condition lab

**http://localhost:8080/race** runs concurrency bugs as real PostgreSQL transactions, then replays
each run as swimlanes: one lane per request, the database's committed state beneath, and the moments
that matter marked on the lanes. Every mark is an event the backend recorded from what PostgreSQL
returned; the browser never invents a step, a value, a lock or a timing.

## The page

Top to bottom:

1. **Experiment header.** A dropdown switches between the ten experiments. The header also shows the
   question and the invariant, and *About* holds the story and the strategy's SQL.
2. **Controls.** One sticky line: strategy, isolation, the starting value, requests, delay and
   *Run experiment*. Interleaving, retries and *Reset lab data* are under *Advanced*.
3. **Execution (the hero).** While the run is in flight, the lanes show the latest event per request.
   Once every transaction has finished, the recorded events replay in order, on one clock for every
   lane.
   - Pills mark each step, coloured by what it is: read (blue), write (ink), commit (green),
     waiting (amber) and violation, rollback or conflict (red).
   - A lock wait is an amber band with an arrow to the transaction holding the lock.
   - *Both transactions read stock = 1* brackets the shared read, and a red line marks where the race
     occurs.
   - The database panel shows the committed values as the engine read them after each transaction
     (`reservations 0 → 1 → 2`), plus writes not committed yet.
   - Controls: play/pause, step (← →), replay, speed (0.5× / 1× / 2×) and a scrubber.
   - Spacing follows event order so short steps stay readable. The ruler and the inspector show the
     real elapsed time.
4. **Result.** The verdict, expected vs actual, final state and each request's fate. It appears when
   the replay reaches the end, or on *Skip to result*.
5. **Why.** The explanation's key steps, numbered; each opens its event. *View technical execution*
   holds the full step list and every recorded event.
6. **Fix.** For a violated run, *Run with Atomic update* (or another fixing strategy) replays the same
   race with the fix. Both runs then play side by side.
7. **Compare.** Two runs on one shared time scale with synchronised playback, then correctness,
   lock waits, conflicts and latency side by side. *Change runs* picks any two finished runs.
8. **Inspector.** Click any pill for its transaction id, request id, thread, instance, pid,
   isolation, timestamp, duration, SQL, values, row versions, lock requested/acquired/wait and trace
   id.

## What a run is

- Each request is its own thread and its own connection, running one real transaction (or several,
  when it retries).
- Every step is recorded as a structured event: `TRANSACTION_STARTED`, `READ_PERFORMED`,
  `DECISION_MADE`, `LOCK_REQUESTED`, `LOCK_ACQUIRED`, `TRANSACTION_BLOCKED`, `WRITE_PERFORMED`,
  `VERSION_CONFLICT`, `SERIALIZATION_FAILURE`, `DEADLOCK_DETECTED`, `TRANSACTION_COMMITTED`,
  `TRANSACTION_ROLLED_BACK`, `LOCK_RELEASED`, `RETRY_SCHEDULED`, `STATE_OBSERVED`,
  `INVARIANT_CHECKED` and more.
- Each event carries the transaction's own `txid` (`pg_current_xact_id()`), its backend pid, the
  thread, the isolation level, the SQL with its parameters, the value and version read or written,
  the lock, who it waited for and for how long, and its trace id.
- **Lock waits** come from PostgreSQL. While a statement is in flight, a monitor asks
  `pg_blocking_pids()` every few milliseconds whether that backend is waiting and for whom.
- **Aborts** come from PostgreSQL too: SQLSTATE `40001` (serialization failure) and `40P01`
  (deadlock, with PostgreSQL's own DETAIL line). The losing transaction is rolled back and, if
  retries are configured, starts over.
- After every commit or rollback the engine reads the committed state on its own connection: that
  is the **DB** lane.
- When all requests have finished, the invariant is checked against the committed rows.
  - The metrics, the key moments ("Both transactions read stock = 1", "Transaction B is waiting for
    the row lock on item #3 held by A", "Optimistic lock rejected B because expected version=4,
    actual version=5") and the "Why did this happen?" steps are all derived from the recorded events.

### Controlled or natural interleaving

**Controlled** (the default) makes the dangerous schedule happen on every run:

- Requests take turns at named sections (A reads, then B reads).
- They meet at barriers (nobody writes until everyone has read).
- A request that PostgreSQL reports as blocked counts as having arrived, so a safe strategy still
  gets its real lock waits.

The SQL, locks, versions and aborts are unchanged; the lab only decides when each request sends its
next statement. These holds appear on the timeline as hatched "lab choreography" spans, and are
reported apart from lock waits (`syncWaitMicros`).

**Natural** starts every request at once and leaves the timing to the scheduler and the database. With
no delay the race window is tiny and the bug often hides, which is the point to notice.

## Experiments

| # | Experiment | Invariant | Breaks with | Fixed by |
|---|---|---|---|---|
| 1 | Overselling inventory | successful reservations ≤ available stock | read, check in Java, write `stock = :read − 1` | conditional `UPDATE … WHERE stock > 0`, `FOR UPDATE`, version check, SERIALIZABLE |
| 2 | Lost update | balance = initial + 10 × deposits | `balance = :read + 10` | `balance = balance + 10`, `FOR UPDATE`, version, SERIALIZABLE |
| 3 | Double payment | at most one charge per payment | `if (status == PENDING) charge()` | claim with `UPDATE … WHERE status = 'PENDING'`, lock, version, SERIALIZABLE |
| 4 | Concurrent state transition | one transition out of PAID, matching its side effect | ship and cancel both validate PAID | compare-and-set on the status, lock, version, SERIALIZABLE |
| 5 | Optimistic locking conflict | no save replaces a version it did not read | blind save of version N+1 | `UPDATE … WHERE version = N` (stale writer rejected: expected N, actual N+1) |
| 6 | Pessimistic locking: the lock queue | every reservation counted in the stock | no lock: lost updates | `FOR UPDATE` (correct, requests queue), atomic update (short lock), optimistic (retries) |
| 7 | Deadlock | every transfer commits, money conserved | opposite lock order: PostgreSQL aborts a victim (40P01) | one global lock order |
| 8 | Non-repeatable read | two reads in one transaction agree | READ COMMITTED | REPEATABLE READ |
| 9 | Phantom read | the same query returns the same rows | READ COMMITTED | REPEATABLE READ (PostgreSQL's has no phantoms) |
| 10 | Write skew | at least one doctor on call | REPEATABLE READ (snapshot) and even a conditional UPDATE | SERIALIZABLE (SSI aborts one), lock every row the decision read |

## API

The endpoints live under `/api/race-lab` and follow the [shared HTTP conventions](decisions/016-http-api-conventions.md).

| Method and path | Does |
|---|---|
| `GET /experiments` | The catalog: texts, invariant, strategies (with their SQL), limits, defaults |
| `POST /runs` | Creates a run: `{experiment, mode?, isolation?, requests?, initialValue?, delayMs?, interleaving?, maxRetries?}`; omitted fields take the experiment's defaults; fresh request, order and customer ids each time |
| `POST /runs/{id}/start` | Starts it (202); one run at a time (`409 RACE_LAB_BUSY`) |
| `GET /runs/{id}` | Configuration, status, trace id and, once done, the result: metrics, invariant, per-request outcomes, highlights, explanation, fix |
| `GET /runs/{id}/events?after=&limit=` | The recorded events, in order |
| `GET /runs/{id}/stream` | Server-sent events: `event` per recorded event, then `run` when it finishes |
| `GET /runs?experiment=&limit=` | Recent runs (summaries) |
| `GET /compare?left=&right=` | Two runs' condensed interleavings, measured differences and a verdict |
| `POST /reset` | Deletes the lab's runs and scenario rows (schema `race_lab` only); ids restart at 1 |

Error codes: `EXPERIMENT_NOT_FOUND`, `RUN_NOT_FOUND`, `INVALID_RUN_CONFIG`, `RACE_LAB_BUSY`,
`RUN_ALREADY_STARTED`, plus the shared ones.

## Tracing

The control plane runs the OpenTelemetry agent with **only the API bridge enabled**, so the migration
lab's polling produces no traces. Each run is one trace:

- `race-lab run N` → `race-lab tx A` (per attempt) → one span per statement, with `db.statement`,
  `db.rows_affected` and `db.sqlstate`.
- A lock wait is a pair of span events on the waiting statement: `lock wait` (with `race.blocked_by`)
  and `lock granted` (with `race.wait_ms`).

In the UI, the run header's *Trace* button and every event's inspector open the trace in Grafana
(Tempo).

## Where it lives

- **Code:** the `race-lab` Maven module, used by the control plane.
  - `domain`: events, configuration, results.
  - `application`: the engine, choreography, lock monitor and analysis, with ports.
  - `experiments`: the ten lessons and their SQL.
  - `infrastructure/postgres`: JDBC sessions, the run repository and schema.
  - `web`: the controller, DTOs, the SSE stream and the error advice.
- **Page and assets:** `migration-lab/src/main/resources/templates/race.html`, `static/race-lab.js`
  (model, shared time scale, swimlane renderer and replay player) and `static/race-lab.css`.
- **Data:** schema `race_lab` in the control plane's PostgreSQL, with its own Flyway history
  (`flyway_race_lab_history`) and its own connection pool (28 connections, so 20 racing
  transactions never starve the control plane). Each run seeds its own rows, tagged with its id.

## Verification

- `RaceLabIT` (19 tests, Testcontainers PostgreSQL 17) runs every experiment and asserts the
  mechanism. Examples:
  - the unsafe run violates the invariant and the fixes keep it;
  - B's wait is attributed to A;
  - the stale writer sees `expected 4, actual 5`;
  - `40001` then a retry, and `40P01` with one victim;
  - a slow live listener never slows the transactions;
  - reset restarts at run #1.
- `scripts/verify_race_lab.py` drives the running stack: all 24 experiment/strategy cases, the
  mechanisms, the event stream, the comparison, the error codes, and the trace in Tempo. Pass
  `RACE_LAB_URL` if the control plane is not on :8080.
