# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Two audiences, equally:

- **Presenter and audience:** the author drives the dashboard live while explaining a SQL Server → PostgreSQL migration to engineers or interviewers (see `docs/interview-notes.md`). It is often on a shared screen, so it has to read well at a distance.
- **Self-guided learners:** engineers run the lab locally to understand change capture, resumable snapshots, validation and cutover at their own pace.

## Product Purpose

ZeroShift is a local educational lab. It runs a real SQL Server → PostgreSQL migration of two related tables (customers, orders) while real traffic keeps writing to the source. The dashboard makes every stage observable and lets the operator interrupt and break the migration on purpose: pause, simulate a crash, pause CDC replay, validate, cut over. Success means a viewer understands why the migration stays consistent, because they watched it happen.

## Positioning

Every number and event on screen comes from the databases. The browser only renders backend observations and submits operator commands. Nothing is faked, interpolated or animated ahead of the backend: no invented progress, ETAs or events.

## Operating Context

- Launched with `docker compose up` and viewed at localhost:8080. The dashboard polls `/api/status` once per second.
- Used during live demos and interviews, often with flaky or no network.
- The flow: generate demo data → start live traffic → start migration → snapshot → CDC catch-up → prepare (indexes/constraints) → ready → optional validate → cutover (freeze → validation → cutover) → completed. Reset clears everything.
- Live Data Changes: the operator inserts, updates or deletes rows in SQL Server, inspects a record side by side in both databases, and can pause CDC replay.

## Capabilities and Constraints

- Stack: Spring Boot, Thymeleaf template, plain JavaScript and CSS in `src/main/resources/static`. There is no frontend build step.
- **All assets must be self-hosted and work offline. No CDNs.**
- Progress is stage-weighted, not an ETA: snapshot 0–80%, catch-up 85, prepare 90, ready 95, freeze 96, validation 98, cutover 99, completed 100.
- Backend stages: IDLE, SNAPSHOT, CATCH_UP, PREPARE, READY, FREEZE, VALIDATION, CUTOVER, COMPLETED. Statuses: IDLE, RUNNING, PAUSED, CRASHED, FAILED, SUCCESS.
- Log lines are `[HH:mm:ss] message`, newest first, last 60 kept. Traffic lines look like `INSERT → SQL Server → customers #42 → order #7 created ✓`.
- Local educational lab: no authentication, not a production tool.

## Brand Commitments

- Name: **ZeroShift**. Tagline: **"Database migration, made visible."** Everything else about the visuals is open.

## Evidence on Hand

- Real behaviour is documented in `README.md` and `docs/` (consistency, live changes, design decisions, verification results).
- There are no customers, testimonials or benchmarks beyond `docs/verification.md`. None may be invented.

## Product Principles

1. **Truth over theatre:** motion and emphasis only show state the backend reported.
2. **The flow is the story:** source → pipeline → target is what a viewer should understand first.
3. **Legible at a distance:** a presenter's shared screen and a learner's laptop both work.
4. **Safe to break:** destructive or irreversible actions (reset, crash, cutover) look deliberate and distinct.
