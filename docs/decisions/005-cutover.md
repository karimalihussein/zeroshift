# ADR 005: Gated cutover behind a real source write fence

**Status:** Accepted

## Decision

Cutover is allowed only in `READY` (snapshot copied, catch-up drained, target prepared) while CDC replay is not paused. It then:

1. commits stage `FREEZE`;
2. sets the SQL Server `migration_gate` to frozen. Triggers on both tables read the gate with `HOLDLOCK` and reject new DML, and setting the gate waits for in-flight guarded writes to commit;
3. drains the final change window;
4. validates counts and ordered SHA-256 fingerprints of every column, and validates the deferred constraints;
5. synchronizes PostgreSQL identity sequences past the final maximum IDs;
6. commits PostgreSQL as the persisted primary.

## Why

- Switching automatically after the copy, or on an unchecked button press, proves nothing about the moment of the switch. The fence makes "no further source writes" true in the database, not just in the UI.
- Validating behind the fence compares stable data, so a match really means the two databases are equal.

## Consequences

- The final full hash scan runs inside the freeze. It is short for demo datasets (15 s for 2M rows per table locally) but grows with data size. Production systems would validate incrementally and budget the freeze.
- If validation fails, SQL Server stays primary and fenced until the problem is repaired or the lab is reset. A failure after the fence commits never silently reopens source writes.
- Privileged DDL, `TRUNCATE` or disabling triggers can bypass the fence and are outside this lab's contract.
