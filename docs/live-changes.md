# Live Data Changes

While a migration is running, the dashboard's **Live Data Changes** panel lets you change real rows in SQL Server and watch change capture carry them to PostgreSQL. Nothing is simulated in the browser: every value shown comes from a query against one of the two databases.

## Actions

| Button | What happens |
|---|---|
| **Insert New Record** | Inserts a new customer and order in SQL Server. The IDs are above the snapshot bound, so the order reaches PostgreSQL only through change replay. |
| **Update Migrated Record** | Picks an order that already exists in PostgreSQL (or the one you inspected) and updates its customer name, status or amount **in SQL Server only**. |
| **Delete Migrated Record** | Deletes an already-copied order from SQL Server. Its customer is kept. |
| **Pause / Resume CDC Replay** | Pauses only the replay of changes into PostgreSQL. The snapshot keeps copying and source writes keep committing. |

Live changes are allowed while a migration is active, SQL Server is primary and the stage is not `FREEZE`. Updating or deleting an order that has not been copied yet is rejected.

## Record Inspector

Enter an order number (or let an action select one) to compare it side by side:

```
Order #100 · Orders copied through ID 2500
Field      SQL Server     PostgreSQL
Status     COMPLETED      PENDING        ← highlighted
CDC pending · replay paused

SQL Server · UPDATE Order #100
CDC captured · orders #100, version 7118
CDC pending · 1 changed key(s) · replay paused
PostgreSQL · waiting for snapshot or CDC replay
```

After you resume replay, the same record shows `✓ IN SYNC`. The inspector also shows the before and after values of the last manual change, stored in SQL Server's `live_experiment` table.

"Pending" is exact: a key is pending when its current Change Tracking version is newer than the version in PostgreSQL's `replay_receipt` table for that key.

## The pause experiment

1. Generate demo data and start the migration.
2. Wait until orders are being copied, then select a copied order.
3. Press **Pause CDC Replay**.
4. Update that order's status.
5. SQL Server shows the new value, PostgreSQL still has the old one, and the change is listed as pending.
6. Watch the snapshot progress keep moving.
7. Press **Resume CDC Replay**. PostgreSQL receives the update and the inspector shows `IN SYNC`.

Validate and Cutover are disabled while replay is paused. The pause flag is stored in PostgreSQL and survives a backend restart.

## How it is built

- `application/LiveChangesService` holds the rules: when writes are allowed, which rows count as copied, and how pending and in-sync are computed.
- `application/port/LiveSource` and `LiveTarget` are the ports. `infrastructure/SqlServerLiveSource` and `PostgresLiveTarget` implement them.
- `MigrationCoordinator` skips replay while paused. `ChangeCatchUp.drainCopied` replays, during the snapshot, only keys behind the copy frontier ([ADR 002](decisions/002-snapshot-boundary.md)).
- `web/LiveChangesController` only maps HTTP to the service, and `static/live.js` only renders backend responses.

## HTTP API

| Method | Path | Body |
|---|---|---|
| `GET` | `/api/live/orders/{id}` | inspect one order |
| `GET` | `/api/live/copied-order` | select an already-copied order |
| `POST` | `/api/live/orders` | `{customerName, status, amount}` |
| `PUT` | `/api/live/orders/{id}` | `{customerName, status, amount}` |
| `DELETE` | `/api/live/orders/{id}` | none |
| `POST` | `/api/live/cdc/pause`, `/api/live/cdc/resume` | none |

Invalid input returns `400` (`VALIDATION_FAILED`), and an action not allowed in the current state returns `409` (`INVALID_ACTION`), both as RFC 9457 problem details with a stable `code` and the `requestId` ([ADR 016](decisions/016-http-api-conventions.md)).

## Tests

`LiveChangesIT` runs against real SQL Server and PostgreSQL containers. It covers INSERT, UPDATE and DELETE both before and after a row is copied, a paused replay with the snapshot still advancing, eventual catch-up, coalesced insert-update-delete sequences, and the pause flag surviving a backend restart. `scripts/verify_live_changes.py` runs the pause experiment over HTTP against the Compose stack.
