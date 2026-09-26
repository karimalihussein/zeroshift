#!/usr/bin/env python3
"""End-to-end drills for the race condition lab (/race). Start the Compose stack first.

Every drill drives the running control plane over HTTP and asserts on what the run recorded:
the invariant checked against committed rows, PostgreSQL's own lock waits and aborts, the live
event stream, the comparison, the shared error format and the run's trace in Tempo.
Run all drills, or name some: scripts/verify_race_lab.py experiments stream errors
RACE_LAB_URL overrides the control plane's address (default http://localhost:8080).
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get('RACE_LAB_URL', 'http://localhost:8080').rstrip('/') + '/api/race-lab'
TEMPO = os.environ.get('TEMPO_URL', 'http://localhost:3200')

# experiment, mode, isolation (None: the mode's default), whether the invariant must hold
CASES = [
    ('oversell', 'UNSAFE', None, False), ('oversell', 'ATOMIC', None, True),
    ('oversell', 'PESSIMISTIC', None, True), ('oversell', 'OPTIMISTIC', None, True),
    ('oversell', 'SERIALIZABLE', None, True),
    ('lost-update', 'UNSAFE', None, False), ('lost-update', 'ATOMIC', None, True),
    ('double-payment', 'UNSAFE', None, False), ('double-payment', 'ATOMIC', None, True),
    ('state-transition', 'UNSAFE', None, False), ('state-transition', 'ATOMIC', None, True),
    ('optimistic-conflict', 'UNSAFE', None, False), ('optimistic-conflict', 'OPTIMISTIC', None, True),
    ('pessimistic-locking', 'UNSAFE', None, False), ('pessimistic-locking', 'PESSIMISTIC', None, True),
    ('deadlock', 'UNSAFE', None, False), ('deadlock', 'ORDERED_LOCKS', None, True),
    ('non-repeatable-read', 'UNSAFE', 'READ_COMMITTED', False),
    ('non-repeatable-read', 'UNSAFE', 'REPEATABLE_READ', True),
    ('phantom-read', 'UNSAFE', 'READ_COMMITTED', False), ('phantom-read', 'UNSAFE', 'REPEATABLE_READ', True),
    ('write-skew', 'UNSAFE', 'REPEATABLE_READ', False), ('write-skew', 'SERIALIZABLE', None, True),
    ('write-skew', 'PESSIMISTIC', None, True),
]


def call(path, method='GET', body=None, timeout=60):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(BASE + path, data=data, method=method,
                                     headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read()
        return json.loads(text) if text else None


def error_of(path, method='GET', body=None):
    try:
        call(path, method, body)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())
    raise AssertionError(f'{method} {path} should have failed')


def run(experiment, mode, isolation=None, **extra):
    body = {'experiment': experiment, 'mode': mode, **extra}
    if isolation:
        body['isolation'] = isolation
    created = call('/runs', 'POST', body)
    call(f"/runs/{created['id']}/start", 'POST')
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        r = call(f"/runs/{created['id']}")
        if r['status'] in ('COMPLETED', 'FAILED'):
            assert r['status'] == 'COMPLETED', f"run {r['id']} failed: {r.get('error')}"
            return r
        time.sleep(0.2)
    raise AssertionError(f"run {created['id']} did not finish in 90 s")


def events(run_id, kind=None):
    items = call(f'/runs/{run_id}/events?limit=5000')['data']
    return [e for e in items if kind is None or e['type'] == kind]


def ok(message):
    print(f'  ✓ {message}', flush=True)


def drill_catalog():
    catalog = call('/experiments')
    assert catalog['meta']['count'] == 10, catalog['meta']
    ok(f"{catalog['meta']['count']} experiments: " + ', '.join(e['id'] for e in catalog['data']))


def drill_experiments():
    for experiment, mode, isolation, holds in CASES:
        r = run(experiment, mode, isolation)
        inv = r['result']['invariant']
        assert inv['holds'] == holds, f'{experiment}/{mode}: expected holds={holds}, got {inv}'
        ok(f"{experiment:20} {mode:13} {r['config']['isolation']:15} {'held' if holds else 'VIOLATED'}: {inv['actual']}")


def drill_mechanisms():
    """The waits, conflicts and aborts come from PostgreSQL, and each is on the timeline."""
    r = run('oversell', 'PESSIMISTIC')
    blocked = events(r['id'], 'TRANSACTION_BLOCKED')
    assert blocked and blocked[0]['lane'] == 'B' and blocked[0]['blockedBy'] == ['A'], blocked
    ok(f"pessimistic: B blocked by A (pg_blocking_pids), waited {r['result']['metrics']['lockWaitMicros'] // 1000} ms")
    r = run('oversell', 'OPTIMISTIC')
    conflict = events(r['id'], 'VERSION_CONFLICT')[0]
    ok(f"optimistic: {conflict['message']}")
    r = run('oversell', 'SERIALIZABLE')
    failure = events(r['id'], 'SERIALIZATION_FAILURE')[0]
    assert failure['sqlState'] == '40001', failure
    ok(f"serializable: SQLSTATE {failure['sqlState']} — {failure['message']}")
    r = run('deadlock', 'UNSAFE')
    deadlock = events(r['id'], 'DEADLOCK_DETECTED')[0]
    assert deadlock['sqlState'] == '40P01', deadlock
    ok(f"deadlock: victim {deadlock['lane']}, {deadlock['data']['postgresDetail'].splitlines()[0]}")
    started = events(r['id'], 'TRANSACTION_STARTED')
    assert all(e['txId'] > 0 and e['pid'] > 0 for e in started), started
    ok('every transaction carries its PostgreSQL txid and backend pid')


def drill_stream():
    """Server-sent events: every event of a run arrives, then the finished run."""
    created = call('/runs', 'POST', {'experiment': 'oversell', 'mode': 'ATOMIC'})
    call(f"/runs/{created['id']}/start", 'POST')
    seen, finished = set(), None
    with urllib.request.urlopen(f"{BASE}/runs/{created['id']}/stream", timeout=60) as stream:
        name = None
        for raw in stream:
            line = raw.decode().rstrip('\n')
            if line.startswith('event:'):
                name = line[6:].strip()
            elif line.startswith('data:'):
                payload = json.loads(line[5:])
                if name == 'event':
                    seen.add(payload['seq'])
                elif name == 'run' and payload['status'] in ('COMPLETED', 'FAILED'):
                    finished = payload
                    break
    stored = {e['seq'] for e in events(created['id'])}
    assert finished and finished['status'] == 'COMPLETED', finished
    assert seen == stored, f'streamed {len(seen)} events, stored {len(stored)}'
    ok(f'streamed all {len(seen)} events live, then the completed run')


def drill_compare():
    unsafe = run('lost-update', 'UNSAFE')
    atomic = run('lost-update', 'ATOMIC')
    c = call(f"/compare?left={unsafe['id']}&right={atomic['id']}")
    held = next(d for d in c['differences'] if d['metric'] == 'Invariant held')
    assert held['better'] == 'right', held
    assert any('READ balance=100' in line for line in c['left']['sequence']), c['left']['sequence']
    ok(c['verdict'])


def drill_errors():
    code, body = error_of('/runs', 'POST', {'experiment': 'deadlock', 'mode': 'OPTIMISTIC'})
    assert code == 400 and body['code'] == 'INVALID_RUN_CONFIG', body
    code, body = error_of('/runs/999999')
    assert code == 404 and body['code'] == 'RUN_NOT_FOUND', body
    code, body = error_of('/runs', 'POST', {'experiment': 'nope'})
    assert code == 404 and body['code'] == 'EXPERIMENT_NOT_FOUND', body
    done = run('oversell', 'ATOMIC')
    code, body = error_of(f"/runs/{done['id']}/start", 'POST')
    assert code == 409 and body['code'] == 'RUN_ALREADY_STARTED', body
    code, body = error_of('/runs', 'POST', {'experiment': 'oversell', 'requests': 99})
    assert code == 400 and body['code'] == 'VALIDATION_FAILED', body
    ok('INVALID_RUN_CONFIG, RUN_NOT_FOUND, EXPERIMENT_NOT_FOUND, RUN_ALREADY_STARTED, VALIDATION_FAILED as problem+json')


def drill_trace():
    """The run → transaction → statement spans reach Tempo, lock waits as span events."""
    r = run('oversell', 'ATOMIC')
    trace_id = r.get('traceId')
    assert trace_id, 'the run has no trace id: is the OpenTelemetry agent attached to the control plane?'
    deadline = time.monotonic() + 30
    spans = []
    while time.monotonic() < deadline and not spans:
        try:
            with urllib.request.urlopen(f'{TEMPO}/api/traces/{trace_id}', timeout=5) as response:
                trace = json.loads(response.read())
            spans = [s for b in trace.get('batches', []) for ss in b.get('scopeSpans', []) for s in ss['spans']]
        except (urllib.error.URLError, OSError, ValueError):
            pass
        if not spans:
            time.sleep(1)
    names = {s['name'] for s in spans}
    assert f"race-lab run {r['id']}" in names and 'race-lab tx A' in names and 'race-lab tx B' in names, names
    waits = [e['name'] for s in spans for e in s.get('events', [])]
    assert 'lock wait' in waits, waits
    ok(f"trace {trace_id}: {len(spans)} spans (run → tx A, tx B → statements), lock wait recorded as a span event")


def drill_reset():
    """Destructive: deletes every run of the lab. Only when named."""
    deleted = call('/reset', 'POST')['deleted']
    first = run('oversell', 'UNSAFE')
    assert first['id'] == 1, first['id']
    ok(f"reset deleted {sum(deleted.values())} rows of race_lab; the next run is #1 again")


DRILLS = {name[len('drill_'):]: fn for name, fn in globals().items() if name.startswith('drill_')}
ORDER = ['catalog', 'experiments', 'mechanisms', 'stream', 'compare', 'errors', 'trace']

if __name__ == '__main__':
    chosen = sys.argv[1:] or ORDER
    failed = []
    for name in chosen:
        print(f'▶ {name}', flush=True)
        started = time.monotonic()
        try:
            DRILLS[name]()
            print(f'  ({time.monotonic() - started:.0f} s)', flush=True)
        except Exception as e:  # report every drill, then fail
            failed.append(name)
            detail = e.read().decode(errors='replace')[:400] if isinstance(e, urllib.error.HTTPError) else ''
            print(f'  ✗ {type(e).__name__}: {e} {detail}', flush=True)
    print('FAILED: ' + ', '.join(failed) if failed else f'All {len(chosen)} drills passed.')
    sys.exit(1 if failed else 0)
