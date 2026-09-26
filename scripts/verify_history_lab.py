#!/usr/bin/env python3
"""End-to-end drills for the Phase 4 events-over-time lab. Start the Compose stack first.

The time-travel and legacy drills check the services directly. Each lab drill runs the lab's six
steps (History → Inspect → Change/rebuild → Replay → Compare → Understand) through the control
plane and asserts what they read back from the event store, Kafka and the services, then that the
completed run was stored in lab_run.

    scripts/verify_history_lab.py                       # every drill
    scripts/verify_history_lab.py time_travel schema
    LAB_URL=http://localhost:8089 scripts/verify_history_lab.py
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

LAB = os.environ.get('LAB_URL', 'http://localhost:8080')
ORDER_A, QUERY = 'http://localhost:18081', 'http://localhost:18086'


def call(url, method='GET', body=None, timeout=300):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read()
        return json.loads(text) if text else None


def ok(message):
    print(f'  ✓ {message}', flush=True)


def wait_for(check, what, timeout=90, every=1):
    deadline, last = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (urllib.error.URLError, OSError, KeyError, ValueError, StopIteration):
            pass
        time.sleep(every)
    raise AssertionError(f'Timed out after {timeout} s waiting for {what} (last: {last})')


def finished_order():
    orders = call(f'{LAB}/api/history/orders?limit=50')
    return next(o['order']['id'] for o in orders if o['saga'] and o['saga']['state'] == 'COMPLETED')


def drill_time_travel():
    """Every version rebuilt from the event store equals the fold; Kafka's copy matches by offset."""
    order = finished_order()
    history = call(f'{LAB}/api/history/orders/{order}')
    for k, moment in enumerate(history, start=1):
        rebuilt = call(f'{LAB}/api/history/orders/{order}/rebuild?version={k}')
        assert rebuilt['state'] == moment['stateAfter'], (k, rebuilt['state'], moment['stateAfter'])
        assert len(rebuilt['applied']) == k and len(rebuilt['later']) == len(history) - k
    ok(f'order {order[:8]}: all {len(history)} versions rebuilt from the event store match the fold')
    before = history[0]['event']['recordedAt']
    early = call(f'{LAB}/api/history/orders/{order}/rebuild?at=2000-01-01T00:00:00Z')
    assert early['state']['status'] == 'NEW' and not early['applied'], early['state']
    at_first = call(f'{LAB}/api/history/orders/{order}/rebuild?at={before}')
    assert at_first['state']['status'] == 'PLACED' and len(at_first['applied']) == 1, at_first['state']
    ok('as of an instant: nothing before the order existed, PLACED at its first event')
    kafka = call(f'{LAB}/api/history/orders/{order}/kafka?at={before}')
    ids = [m['event']['eventId'] for m in history]
    found = [r['eventId'] for r in kafka['records']]
    assert found[:len(ids)] == ids, (found, ids)
    offsets = [r['offset'] for r in kafka['records']]
    assert offsets == sorted(offsets), offsets
    own = next(s for s in kafka['spans'] if s['partition'] == kafka['partition'])
    assert own['from'] <= offsets[0] < own['end'], (own, offsets)
    ok(f'Kafka time index → partition {kafka["partition"]} offset {own["from"]}: the same {len(ids)} events in order at offsets {offsets[:len(ids)]}')


def drill_legacy_v1():
    """An order written as schema v1 is stored as v1 and read as v2 everywhere."""
    placed = call(f'{ORDER_A}/lab/history/legacy-order', 'POST',
                  {'customerId': 'drill-legacy', 'items': [{'sku': 'SKU-CABLE', 'quantity': 1}]})
    order = placed['orderId']
    first = call(f'{LAB}/api/history/orders/{order}')[0]['event']
    assert first['storedVersion'] == 1 and 'currency' not in first['stored']['payload'], first['stored']
    assert first['currentVersion'] == 2 and first['decoded']['currency'] == 'USD', first['decoded']
    ok('event store: OrderPlaced stored as v1 (no currency), read as v2 with currency USD')
    record = wait_for(lambda: call(f'{LAB}/api/history/orders/{order}/kafka?at={first["recordedAt"]}')['records'], 'the record on Kafka', 30)[0]
    assert record['schemaVersion'] == 1 and record['headers'].get('schemaVersion') == '1', record
    ok(f'order.events offset {record["offset"]}: published as v1 (payload and header)')
    wait_for(lambda: call(f'{ORDER_A}/orders/{order}')['saga']['state'] == 'COMPLETED', 'the saga to complete', 90)
    view = wait_for(lambda: call(f'{QUERY}/orders/{order}'), 'the read model', 30)
    assert view['currency'] == 'USD', view
    ok('saga COMPLETED and order-query-service projected it with currency USD: every reader upcast it')


def run_lab(lab, order=None):
    before = call(f'{LAB}/api/history/runs?limit=1')
    last = before[0]['id'] if before else 0
    results = []
    for n in range(6):
        run = call(f'{LAB}/api/history/labs/{lab}/steps/{n}' + (f'?orderId={order}' if order and n == 0 else ''), 'POST')
        step = run['steps'][n]
        assert step['ok'], step
        results.append(step['result'])
        print(f'    {step["name"]}: {step["millis"]} ms', flush=True)
    stored = wait_for(lambda: [r for r in call(f'{LAB}/api/history/runs?limit=3') if r['id'] > last and r['mode'] == lab], 'the run in lab_run', 20)
    ok(f'all six steps ran; run {stored[0]["id"]} stored in lab_run')
    return results


def drill_lab_time_travel():
    r = run_lab('time-travel')
    assert r[2]['lastEqualsCurrent'], r[2]
    assert r[4]['sameEventsSameOrder'], r[4]
    ok(f'rebuilt at every version; event store and Kafka agree ({r[4]["kafkaRecords"]} records); relay delays {[d["relayMs"] for d in r[4]["relay"]]} ms')


def drill_lab_projection():
    r = run_lab('projection')
    assert r[1]['difference'], 'v1 should disagree with the truth (it counts cancelled orders)'
    assert r[2]['replay']['records'] == r[0]['records'] or r[2]['replay']['records'] >= r[0]['records'], (r[0], r[2]['replay'])
    states = r[3]['finalStates']
    assert 'CANCELLED' in states.values() and 'COMPLETED' in states.values(), states
    assert r[4]['v2MatchesTruth'] and not r[4]['v1MatchesTruth'], (r[4]['v1MinusTruth'], r[4]['v2MinusTruth'])
    over = {d['sku']: d['units'] for d in r[4]['v1MinusTruth']}
    assert all(u > 0 for u in over.values()), over
    ok(f'v1 overcounts {over} units; fixed v2 rebuilt from {r[2]["replay"]["records"]} records in {r[2]["replay"]["millis"]} ms equals the event store exactly')
    ok(f'catch-up read only new records: v1 {r[3]["v1"]["records"]}, v2 {r[3]["v2"]["records"]}; the declined order counted by v1 only')


def drill_lab_schema():
    r = run_lab('schema')
    placed = next(c for c in r[0]['contracts'] if c['type'] == 'OrderPlaced')
    assert placed['version'] == 2 and 'v1 → v2' in placed['upcasters'], placed
    assert r[1]['storedVersion'] == 1 and r[1]['currentVersion'] == 2 and r[1]['kafkaSchemaVersion'] == 1, r[1]
    assert r[1]['saga'] == 'COMPLETED' and r[1]['readModel']['currency'] == 'USD', (r[1]['saga'], r[1]['readModel'])
    ok('old event, new readers: v1 order stored, published, completed and projected')
    assert r[2]['written']['schemaVersion'] == 3
    assert r[3]['decision']['decision'] == 'DEAD_LETTERED' and r[3]['decision']['attempt'] == 1, r[3]['decision']
    assert 'newer than this consumer understands' in (r[3]['reason'] or ''), r[3]
    assert r[3]['originalTopic'] == 'order.events' and r[3]['labProjectionSkipped'], r[3]
    ok(f'new writer, old reader: v3 dead-lettered on its first delivery to order.events.dlt@{r[3]["dltOffset"]}: "{r[3]["reason"]}"')
    readable = r[4]['readableBy']
    assert readable['v1 (previous release)'] == ['v1 reader (previous release)', 'v2 reader (deployed)', 'v2 without the version check', 'v3 reader (proposed)'], readable
    assert readable['v2 (current release)'] == ['v2 reader (deployed)', 'v2 without the version check', 'v3 reader (proposed)'], readable
    assert readable['v3 (not rolled out)'] == ['v3 reader (proposed)'], readable
    ok('matrix on real records: v1 read by all, v2 by v2+ readers, v3 only by the v3 reader: roll readers out first')


def drill_lab_compaction():
    r = run_lab('compaction')
    assert r[0]['config']['cleanup.policy'] == 'compact', r[0]['config']
    assert r[2]['compacted'], r[2]
    ok(f'compacted: {r[2]["recordsBefore"]} records → {r[2]["recordsAfter"]} after {r[2]["waitedMs"] / 1000:.0f} s')
    for key in r[4]['perKey']:
        if key['tombstoned']:
            assert key['tableStatus'] is None and key['after'][-1].endswith('tombstone'), key
        else:
            assert len(key['after']) == 1 and key['tableStatus'] == key['orderServiceStatus'], key
            assert len(key['before']) > 1, key
    offsets = r[4]['offsetsAfter']
    assert offsets != list(range(offsets[0], offsets[0] + len(offsets))), 'a compacted log has gaps'
    assert r[4]['retention']['config']['cleanup.policy'] == 'delete'
    ok('one value per key survives and matches order-service; the tombstone deletes its key; offsets keep their gaps')


DRILLS = {name[len('drill_'):]: fn for name, fn in globals().items() if name.startswith('drill_')}
ORDER = ['time_travel', 'legacy_v1', 'lab_time_travel', 'lab_projection', 'lab_schema', 'lab_compaction']

if __name__ == '__main__':
    chosen = sys.argv[1:] or ORDER
    wait_for(lambda: call(f'{LAB}/api/history/labs'), f'the control plane at {LAB}', 180, 2)
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
