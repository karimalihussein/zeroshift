#!/usr/bin/env python3
"""Reset the local Java lab and verify live changes through its public HTTP API."""
import json
import time
from pathlib import Path
from verify_demo import request, action, status, wait_for, compose, ROOT
import urllib.request
from verify_demo import BASE


def live(path, method='GET', body=None):
    payload = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + '/api/live' + path, data=payload, method=method,
                                 headers={'Content-Type': 'application/json'} if payload else {})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.load(response)


def inspect(order_id):
    return live(f'/orders/{order_id}')


def wait_record(order_id, predicate, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = inspect(order_id)
        if predicate(record):
            return record
        current = status()['migration']
        assert current['status'] != 'FAILED', current['error']
        time.sleep(.1)
    raise AssertionError(f'Record {order_id} did not converge: {record}')


def edit(record, new_status):
    return dict(customerName=record['source']['customerName'],
                amount=str(record['source']['amount']), status=new_status)


def main():
    action('reset'); action('seed'); action('start')
    midpoint = wait_for(lambda d: d['progress'] >= 50 and d['migration']['stage'] == 'SNAPSHOT', '50% snapshot')
    record = inspect(100)
    assert record['source'] and record['target'] and record['inSync']
    # Establish the exact Pending → Completed example using real source DML and replay.
    live('/orders/100', 'PUT', edit(record, 'PENDING'))
    wait_record(100, lambda r: r['inSync'] and r['target']['status'] == 'PENDING')
    live('/cdc/pause', 'POST')
    record = inspect(100)
    live('/orders/100', 'PUT', edit(record, 'COMPLETED'))
    pending = inspect(100)
    assert pending['source']['status'] == 'COMPLETED'
    assert pending['target']['status'] == 'PENDING'
    assert sum(c['pending'] for c in pending['changes']) == 1
    assert status()['pending'] == 1
    batches = status()['migration']['batches']
    wait_for(lambda d: d['migration']['batches'] > batches, 'snapshot continues with CDC paused')
    assert inspect(100)['target']['status'] == 'PENDING'
    live('/cdc/resume', 'POST')
    updated = wait_record(100, lambda r: r['inSync'] and r['target']['status'] == 'COMPLETED')
    assert status()['migration']['stage'] == 'SNAPSHOT'
    print('PASS: Order #100 Pending → Completed, exactly one pending key, snapshot continued, replay converged during snapshot', flush=True)

    live('/cdc/pause', 'POST')
    inserted = live('/orders', 'POST', dict(customerName='Live verification · عميل', amount='500.1234', status='PENDING'))
    order_id = inserted['orderId']
    insert_pending = inspect(order_id)
    assert insert_pending['source'] and insert_pending['target'] is None
    assert len([c for c in insert_pending['changes'] if c['pending']]) == 2
    live('/cdc/resume', 'POST')
    insert_applied = wait_record(order_id, lambda r: r['inSync'] and r['target'] is not None)
    print('PASS: real customer + order INSERT captured and replayed', flush=True)

    live('/cdc/pause', 'POST')
    live(f'/orders/{order_id}', 'DELETE')
    delete_pending = inspect(order_id)
    assert delete_pending['source'] is None and delete_pending['target'] is not None
    assert any(c['operation'] == 'DELETE' and c['pending'] for c in delete_pending['changes'])
    compose('kill', '-s', 'SIGKILL', 'app'); compose('up', '-d', 'app')
    recovered = wait_for(lambda d: d['migration']['status'] == 'PAUSED', 'backend restart')
    assert recovered['migration']['cdcPaused']
    assert inspect(order_id)['target'] is not None
    action('resume')
    assert inspect(order_id)['cdcPaused']
    live('/cdc/resume', 'POST')
    delete_applied = wait_record(order_id, lambda r: r['inSync'] and r['target'] is None)
    print('PASS: DELETE held through backend restart, CDC pause persisted, delete eventually replayed', flush=True)

    wait_for(lambda d: d['migration']['stage'] == 'READY', 'Ready')
    assert action('validate')['message'] == 'Validation passed'
    action('cutover')
    wait_for(lambda d: d['migration']['stage'] == 'COMPLETE', 'cutover')
    evidence = dict(verifiedAt=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    midpoint=midpoint['migration'], updatePending=pending, updateApplied=updated,
                    insertPending=insert_pending, insertApplied=insert_applied,
                    deletePending=delete_pending, deleteApplied=delete_applied, final=status())
    (ROOT / 'docs' / 'live-verification.json').write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + '\n')
    print('PASS: final full validation and cutover; evidence in docs/live-verification.json', flush=True)

if __name__ == '__main__':
    main()
