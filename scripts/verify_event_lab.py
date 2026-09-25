#!/usr/bin/env python3
"""End-to-end failure drills for the event-driven lab. Start the full Compose stack first.

Every check drives the real system (HTTP, Kafka, crashes) and asserts on what it recorded:
service state, consumer decisions, Kafka offsets, the lease table, Tempo, Loki and Prometheus.
Run all drills, or name some: scripts/verify_event_lab.py happy failover lease
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

LAB = 'http://localhost:8080'
ORDER_A, ORDER_B = 'http://localhost:18081', 'http://localhost:18088'
PAYMENT, INVENTORY, SHIPPING, QUERY = (f'http://localhost:{p}' for p in (18082, 18084, 18085, 18086))
CONNECT, TEMPO, LOKI, PROMETHEUS = (
    'http://localhost:18083', 'http://localhost:3200', 'http://localhost:3100', 'http://localhost:9090')
REPLICAS = {'order-service': ORDER_A, 'order-service-b': ORDER_B}


def call(url, method='GET', body=None, timeout=30):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read()
        return json.loads(text) if text else None


def try_call(url):
    try:
        return call(url, timeout=5)
    except (urllib.error.URLError, OSError, ValueError):
        return None


def wait_for(check, what, timeout=90, every=0.5):
    deadline, last = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (urllib.error.URLError, OSError, KeyError, ValueError):
            pass
        time.sleep(every)
    raise AssertionError(f'Timed out after {timeout} s waiting for {what} (last: {last})')


def act(action, **params):
    return call(f'{LAB}/api/events/actions/{action}', 'POST', params, timeout=60)


def place(customer='drill', sku='SKU-CABLE', quantity=1, base=ORDER_A):
    return call(f'{base}/orders', 'POST', {'customerId': customer, 'items': [{'sku': sku, 'quantity': quantity}]})


def order(order_id):
    for base in REPLICAS.values():  # either replica: they share the database
        found = try_call(f'{base}/orders/{order_id}')
        if found:
            return found
    return None


def finished(order_id, timeout=90):
    return wait_for(lambda: (o := order(order_id)) and o['saga'] and o['saga']['state'] in ('COMPLETED', 'CANCELLED') and o,
                    f'order {order_id} saga to finish', timeout)


def healthy(url):
    return (try_call(f'{url}/actuator/health') or {}).get('status') == 'UP'


def group(group_id):
    return next(g for g in call(f'{LAB}/api/events/state')['groups'] if g['groupId'] == group_id)


def decisions(base, order_id):
    return call(f'{base}/lab/decisions?orderId={order_id}&limit=500')


def metric(base, name):
    """Sum of a Micrometer counter as the service exposes it right now."""
    text = urllib.request.urlopen(f'{base}/actuator/prometheus', timeout=5).read().decode()
    return sum(float(line.rsplit(' ', 1)[1]) for line in text.splitlines() if line.startswith(name))


def ok(message):
    print(f'  ✓ {message}', flush=True)


# ---- Drills ------------------------------------------------------------------------------------
def drill_health():
    for name, url in [('order-service', ORDER_A), ('order-service-b', ORDER_B), ('payment', PAYMENT),
                      ('inventory', INVENTORY), ('shipping', SHIPPING), ('order-query', QUERY), ('lab', LAB)]:
        wait_for(lambda: healthy(url), f'{name} healthy', 180)
    connectors = call(f'{CONNECT}/connectors?expand=status')
    for name, c in connectors.items():
        assert c['status']['connector']['state'] == 'RUNNING', (name, c['status'])
        assert all(t['state'] == 'RUNNING' for t in c['status']['tasks']), (name, c['status'])
    ok(f'all services healthy; {len(connectors)} outbox connectors RUNNING')


def drill_happy():
    placed = place('happy')
    done = finished(placed['orderId'])
    assert done['saga']['state'] == 'COMPLETED', done['saga']
    status = done['order']['status']
    read = wait_for(lambda: (r := try_call(f"{QUERY}/orders/{placed['orderId']}")) and r['status'] == status and r,
                    f'read model to catch up to {status}')
    ok(f"order {placed['orderId'][:8]} COMPLETED; read model agrees after {read['events_applied']} events")
    return placed


def replica_members(g):
    return {m['clientId'].split('/')[0] for m in g['members'] if m['assignments']}


def drill_replicas():
    saga = wait_for(lambda: (g := group('order-saga')) and replica_members(g) == set(REPLICAS) and g,
                    'order-saga partitions split over both replicas', 120)
    ok('order-saga: ' + ' | '.join(f"{m['clientId']} → {', '.join(m['assignments'])}" for m in saga['members']))


def compose(*args):
    subprocess.run(['docker', 'compose', *args], cwd=ROOT, check=True, capture_output=True)


def drill_failover():
    """SIGKILL replica B and keep it down: Kafka notices only when its session times out (10 s),
    then moves B's partitions to A. (The dashboard's Crash button lets Docker restart B at once,
    and B usually rejoins before the timeout, so partitions barely move.)"""
    started = time.monotonic()
    compose('kill', 'order-service-b')
    orders = [place(f'failover-{i}')['orderId'] for i in range(4)]
    one = wait_for(lambda: (g := group('order-saga')) and replica_members(g) == {'order-service'}
                   and sum(len(m['assignments']) for m in g['members']) == 9 and g,
                   'all 9 reply partitions to move to replica A', 60)
    ok(f'B killed; {time.monotonic() - started:.0f} s later the rebalance gave order-service all 9 reply partitions')
    states = [finished(o, 120)['saga']['state'] for o in orders]
    assert all(s == 'COMPLETED' for s in states), states
    ok(f'{len(orders)} orders placed during the failover all COMPLETED')
    compose('start', 'order-service-b')
    wait_for(lambda: healthy(ORDER_B), 'order-service-b to restart', 180)
    drill_replicas()
    instances = {d['instance'] for o in orders for d in decisions(ORDER_A, o) if d['consumer'] == 'order-saga'}
    ok(f'saga replies for those orders were handled by: {sorted(instances)}')


def lease():
    return try_call(f'{ORDER_A}/lab/lease') or try_call(f'{ORDER_B}/lab/lease')


def drill_lease():
    """Crash the scanner's holder: the other replica takes over with a higher fencing token."""
    for url in REPLICAS.values():
        wait_for(lambda: healthy(url), 'both replicas healthy', 180)
    before = wait_for(lambda: (l := lease()) and l['held'] and l, 'a lease holder')
    ok(f"scanner lease held by {before['owner']} with token {before['token']}")
    call(f"{REPLICAS[before['owner']]}/lab/crash", 'POST')
    after = wait_for(lambda: (l := lease()) and l['held'] and l['owner'] != before['owner'] and l, 'takeover', 30)
    assert after['token'] > before['token'], (before, after)
    ok(f"holder crashed → {after['owner']} took over within the 5 s TTL, token {before['token']} → {after['token']}")
    wait_for(lambda: healthy(REPLICAS[before['owner']]), 'crashed replica back', 180)


def drill_fencing():
    """Stall the holder past its TTL: the other replica takes over, the stale holder is fenced."""
    for url in REPLICAS.values():
        wait_for(lambda: healthy(url), 'both replicas healthy', 180)
    holder = wait_for(lambda: (l := lease()) and l['held'] and l, 'a lease holder')
    stale = REPLICAS[holder['owner']]
    fenced_before = metric(stale, 'zeroshift_lease_fenced_total')
    call(f'{stale}/lab/faults/scanner-stall?mode=8000&times=1', 'PUT')
    took = wait_for(lambda: (l := lease()) and l['owner'] != holder['owner'] and l, 'takeover during the stall', 20)
    ok(f"{holder['owner']} stalled holding token {holder['token']}; {took['owner']} took over with token {took['token']}")
    wait_for(lambda: metric(stale, 'zeroshift_lease_fenced_total') > fenced_before, 'stale holder to be fenced', 20)
    ok(f"{holder['owner']} woke up, its token failed the fence check: it wrote nothing (zeroshift_lease_fenced_total)")


def drill_duplicate():
    placed = place('duplicate')
    finished(placed['orderId'])
    journey = call(f"{LAB}/api/events/orders/{placed['orderId']}")
    message = next(m for m in journey['messages'] if m.get('type') == 'PaymentAuthorized' and m.get('kafka'))
    k = message['kafka'][0]
    act('duplicate', topic=k['topic'], partition=k['partition'], offset=k['offset'])
    wait_for(lambda: any(d['event_id'] == message['eventId'] and d['decision'] == 'DUPLICATE_SKIPPED'
                         for d in decisions(ORDER_A, placed['orderId'])), 'DUPLICATE_SKIPPED', 30)
    assert order(placed['orderId'])['saga']['state'] == 'COMPLETED'
    ok(f"re-published {k['topic']}@{k['offset']} (same event id): order-saga recorded DUPLICATE_SKIPPED, saga unchanged")


def drill_replay():
    """Rewind the projection's group to offset 0: everything is redelivered and skipped."""
    placed = drill_happy()
    before = call(f"{QUERY}/orders/{placed['orderId']}")
    g = group('order-projection')
    consumer = next(c for c in call(f'{QUERY}/lab/state')['consumers'] if c['groupId'] == 'order-projection')
    partitions = [{'topic': p['topic'], 'partition': p['partition']} for p in g['partitions'] if p['committed'] is not None]
    act('consumer-seek', group='order-projection', consumer=consumer['id'], owners=['order-query-service'],
        partitions=partitions, offset=0)
    ok(f"order-projection rewound to 0 on {len(partitions)} partitions (lag was {g['totalLag']})")
    drained = wait_for(lambda: (x := group('order-projection')) and x['totalLag'] == 0 and x['members'] and x,
                       'replay to drain', 120)
    skipped = [d for d in decisions(QUERY, placed['orderId']) if d['decision'] == 'DUPLICATE_SKIPPED']
    after = call(f"{QUERY}/orders/{placed['orderId']}")
    assert skipped, 'no duplicates recorded for the replayed order'
    assert after['events_applied'] == before['events_applied'] and after['status'] == before['status'], (before, after)
    ok(f"replayed and drained; {len(skipped)} of this order's events skipped as duplicates, read model unchanged")
    assert drained


def compensated(fault_url, fault, mode, expect):
    call(f'{fault_url}/lab/faults/{fault}?mode={mode}&times=1', 'PUT')
    done = finished(place(fault)['orderId'])
    saga = done['saga']
    assert saga['state'] == 'CANCELLED', saga
    assert set(expect) <= set(saga['compensations']), (expect, saga)
    ok(f"{fault}: CANCELLED ({saga['failureReason']}); compensations {saga['compensations'] or 'none needed'}")


def drill_compensation():
    compensated(PAYMENT, 'payment-decline', 'decline', [])
    compensated(INVENTORY, 'inventory-reject', 'reject', ['payment refunded'])
    compensated(SHIPPING, 'shipping-fail', 'fail', ['payment refunded', 'stock released'])


def drill_poison():
    dlt = lambda: next((p for t in call(f'{LAB}/api/events/state')['topics'] if t['name'] == 'payment.commands.dlt'
                        for p in t['partitions']), {'latest': 0})
    before = dlt()['latest']
    act('poison', topic='payment.commands')
    wait_for(lambda: dlt()['latest'] > before, 'poison message on payment.commands.dlt', 60)
    ok('unreadable record: no retries wasted, straight to payment.commands.dlt; the partition kept flowing')
    assert finished(place('after-poison')['orderId'])['saga']['state'] == 'COMPLETED'
    ok('next order on the same topic COMPLETED')


def drill_crash_after_commit():
    call(f'{PAYMENT}/lab/faults/crash-after-commit?mode=crash&times=1', 'PUT')
    placed = place('crash-after-commit')
    wait_for(lambda: not healthy(PAYMENT), 'payment-service to die', 30)
    wait_for(lambda: healthy(PAYMENT), 'payment-service to restart', 180)
    done = finished(placed['orderId'], 120)
    assert done['saga']['state'] == 'COMPLETED', done['saga']
    # The saga can finish before the restarted consumer gets to the redelivery: wait for it.
    wait_for(lambda: any(d['decision'] == 'DUPLICATE_SKIPPED' for d in decisions(PAYMENT, placed['orderId'])),
             'the redelivered command to be skipped', 60)
    ok('payment committed, JVM died before the offset commit, restart redelivered, inbox skipped it: charged once')


def drill_timeout():
    consumer = next(c for c in call(f'{PAYMENT}/lab/state')['consumers'] if 'payment.commands' in c['topics'])
    call(f"{PAYMENT}/lab/consumers/{consumer['id']}/pause", 'POST')
    try:
        placed = place('timeout')
        timed_out = wait_for(lambda: (o := order(placed['orderId'])) and o['saga']['state'] == 'COMPENSATING' and o['saga'],
                             'the step deadline to pass', 90)
        ok(f"payment consumer paused: the scanner timed the saga out ({timed_out['failureReason']})")
    finally:
        call(f"{PAYMENT}/lab/consumers/{consumer['id']}/resume", 'POST')
    # The refund is a command too: it waits behind the original AuthorizePayment on the same partition.
    done = finished(placed['orderId'], 60)
    assert done['saga']['state'] == 'CANCELLED', done['saga']
    late = [d for d in decisions(ORDER_A, placed['orderId']) if d['decision'] == 'IGNORED']
    ok(f"resumed: the late charge was {'IGNORED by the saga and ' if late else ''}refunded; saga CANCELLED")


def drill_breaker():
    act('gateway', mode='down')
    try:
        orders = [place(f'gateway-down-{i}')['orderId'] for i in range(3)]
        wait_for(lambda: call(f'{PAYMENT}/lab/gateway')['breakerState'] == 'OPEN', 'breaker to open', 120)
        ok('gateway 503s: retries exhausted, circuit breaker OPEN (calls now fail fast)')
    finally:
        act('gateway', mode='healthy')
        act('breaker-reset')
    for o in orders:
        finished(o, 180)
    ok('gateway healthy again; the affected sagas all reached an end state')


def drill_observability():
    placed = place('traced')
    finished(placed['orderId'])
    trace_id = placed['traceId']
    assert trace_id, 'order-service returned no trace id: is the agent attached?'

    def services_in_trace():
        trace = call(f'{TEMPO}/api/traces/{trace_id}')
        names = {a['value']['stringValue'] for b in trace['batches'] for a in b['resource']['attributes'] if a['key'] == 'service.name'}
        return names if {'order-service', 'payment-service', 'inventory-service', 'shipping-service'} <= names else None

    names = wait_for(services_in_trace, 'one trace across the saga in Tempo', 60, 2)
    ok(f"trace {trace_id[:8]} spans {sorted(names)} (context carried through the outbox and Kafka headers)")
    q = urllib.parse.urlencode({'query': f'{{service_name=~".+"}} | trace_id="{trace_id}"', 'limit': 100,
                                'start': str(int((time.time() - 600) * 1e9))})
    lines = wait_for(lambda: sum(len(s['values']) for s in call(f'{LOKI}/loki/api/v1/query_range?{q}')['data']['result']),
                     'logs for that trace in Loki', 60, 2)
    ok(f'Loki has {lines} log lines carrying that trace id')
    for expr in ['zeroshift_consumer_decisions_total', 'kafka_consumer_fetch_manager_records_lag',
                 'resilience4j_circuitbreaker_state', 'traces_service_graph_request_total']:
        r = wait_for(lambda: call(f"{PROMETHEUS}/api/v1/query?{urllib.parse.urlencode({'query': expr})}")['data']['result'],
                     f'{expr} in Prometheus', 90, 3)
        ok(f'Prometheus: {expr} ({len(r)} series)')


DRILLS = {name[len('drill_'):]: fn for name, fn in globals().items() if name.startswith('drill_')}
ORDER = ['health', 'happy', 'replicas', 'duplicate', 'replay', 'compensation', 'poison', 'observability',
         'failover', 'lease', 'fencing', 'crash_after_commit', 'timeout', 'breaker']

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
            print(f'  ✗ {type(e).__name__}: {e}', flush=True)
    print('FAILED: ' + ', '.join(failed) if failed else f'All {len(chosen)} drills passed.')
    sys.exit(1 if failed else 0)
