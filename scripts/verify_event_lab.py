#!/usr/bin/env python3
"""End-to-end failure drills for the event-driven lab. Start the full Compose stack first.

Every check drives the real system (HTTP, Kafka, crashes) and asserts on what it recorded:
service state, consumer decisions, Kafka offsets, the lease table, Tempo, Loki and Prometheus.
Run all drills, or name some: scripts/verify_event_lab.py happy failover lease
("kafka" names the Phase 2 drills, which need the kafka-lab Compose profile.)
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
    return call(f'{base}/lab/decisions?orderId={order_id}&limit=500')['data']


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
    ok(f"order {placed['orderId'][:8]} COMPLETED; read model agrees after {read['eventsApplied']} events")
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
    wait_for(lambda: any(d['eventId'] == message['eventId'] and d['decision'] == 'DUPLICATE_SKIPPED'
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
    assert after['eventsApplied'] == before['eventsApplied'] and after['status'] == before['status'], (before, after)
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


# ---- Phase 1 learning labs: each drill reproduces the failure, then the fix and the recovery ------
def labs():
    return call(f'{LAB}/api/events/labs')


def lab_order():
    return {'customerId': f'Drill {int(time.time()) % 100000}', 'items': [{'sku': 'SKU-CABLE', 'quantity': 1}]}


def drill_lab_idempotency():
    """A timed-out client that retries: two orders without a key, one with a key."""
    def run(with_key, slow):
        r = act('exp-idempotency', withKey=with_key, slowAnswers=slow, order=lab_order())
        orders = wait_for(lambda: (v := labs()) and v['idempotency'][0]['result']['customerId'] == r['customerId']
                          and len(v.get('idempotencyOrders', [])) >= min(slow + 1 if not with_key else 1, 3) and v['idempotencyOrders'], 'the run\'s orders', 30)
        time.sleep(4)  # let payment-service charge every order the run created
        return r, labs()['idempotencyOrders']
    r, orders = run(False, 1)
    assert r['attempts'][0]['outcome'].startswith('timed out') and r['clientSawOrder'], r
    assert len(orders) == 2 and sum(o['charges'] for o in orders) == 2, orders
    ok(f'no key: attempt 1 timed out after {r["clientTimeoutMs"]} ms, the retry created a 2nd order: 2 orders, 2 charges')
    r, orders = run(True, 1)
    assert len(orders) == 1 and orders[0]['orderId'] == r['clientSawOrder'] and r['attempts'][-1].get('replayed'), (r, orders)
    ok('with Idempotency-Key: the retry was answered with the first order: 1 order, 1 charge')
    r, orders = run(True, 3)
    assert r['clientSawOrder'] is None and len(orders) == 1, (r, orders)
    ok('with a key and every answer slow: the client saw only timeouts, still exactly 1 order')
    duplicate = run(False, 1)[1][0]['orderId']
    act('refund', orderId=duplicate, reason='drill: duplicate from a client retry')
    wait_for(lambda: next(o for o in labs()['idempotencyOrders'] if o['orderId'] == duplicate)['refunded'], 'the duplicate to be refunded', 30)
    ok('recovery: RefundPayment refunded the duplicate through the outbox and Kafka')


def tracking():
    return labs()['tracking']


def settle_tracking(timeout=60):
    """No lag (never-committed partitions count from their earliest offset), and no scan handled
    for a while: a recreated topic or new partition has no commits yet, so lag alone can be 0 early."""
    handled = lambda: sum(p['scansApplied'] + p['staleSkipped'] for p in tracking()['parcels'])
    wait_for(lambda: (g := group('carrier-tracking')) and g['totalLag'] == 0 and g, 'carrier-tracking to drain', timeout)
    last = -1
    while (now := handled()) != last:
        last = now
        time.sleep(2)
    return tracking()


def drill_lab_ordering():
    """Scans keyed per scan land on different partitions; a slow partition then reorders them."""
    act('fault-clear', service='shipping-service', name='carrier-slow')
    act('carrier-reset')
    act('tracking-guard', on=False)
    act('fault-arm', service='shipping-service', name='carrier-slow', mode='0:500')
    for attempt in range(3):  # random keys may all miss the slow partition; retry the trip
        act('carrier-scans', keying='scan', parcels=4)
        t = settle_tracking()
        if sum(p['regressions'] for p in t['parcels']):
            break
    regressed = [p for p in t['parcels'] if p['regressions']]
    assert regressed, t['parcels']
    ok(f'keyed by scan id with partition 0 slow: {len(regressed)} of {len(t["parcels"])} parcels went backwards '
       f'({", ".join(p["trackingNumber"] + " → " + p["status"] for p in regressed[:2])})')
    act('carrier-scans', keying='tracking', parcels=4)
    t = settle_tracking()
    assert all(p['regressions'] == 0 and p['status'] == 'DELIVERED' for p in t['parcels']), t['parcels']
    ok('keyed by tracking number, same slow partition: 0 regressions, every parcel DELIVERED')
    act('carrier-scans', keying='scan', parcels=4)
    settle_tracking()
    act('tracking-guard', on=True)
    act('tracking-replay')
    t = settle_tracking(90)
    assert all(p['status'] == 'DELIVERED' and p['regressions'] == 0 for p in t['parcels']), t['parcels']
    ok(f'recovery: guard on + replay: every parcel DELIVERED, {sum(p["staleSkipped"] for p in t["parcels"])} stale scans refused')
    act('fault-clear', service='shipping-service', name='carrier-slow')
    act('tracking-guard', on=False)


def drill_lab_repartition():
    """Correct keys still break when partitions are added while scans are in flight."""
    act('fault-clear', service='shipping-service', name='carrier-slow')
    act('carrier-reset')
    act('tracking-guard', on=False)
    act('consumer-pause', service='shipping-service', consumer='carrier-tracking')
    first = act('carrier-scans', keying='tracking', parcels=6, fromSeq=1, toSeq=2)['produced']
    act('carrier-partitions', count=6)
    time.sleep(3)
    second = act('carrier-scans', keying='tracking', parcels=6, fromSeq=3, toSeq=4)['produced']
    before = {r['trackingNumber']: r['partition'] for r in first}
    moved = sorted({r['trackingNumber'] for r in second if before[r['trackingNumber']] != r['partition']})
    assert moved, 'no key moved partition: add more parcels'
    act('fault-arm', service='shipping-service', name='carrier-slow', mode='0,1,2:600')
    act('consumer-resume', service='shipping-service', consumer='carrier-tracking')
    t = settle_tracking(90)
    wrong = [p for p in t['parcels'] if p['trackingNumber'] in moved and p['status'] != 'DELIVERED']
    assert wrong, (moved, t['parcels'])
    ok(f'3 → 6 partitions in flight: {len(moved)} parcels changed partition, {len(wrong)} ended in the wrong status')
    act('fault-clear', service='shipping-service', name='carrier-slow')
    act('carrier-reset')
    assert tracking()['partitions'] == 3
    ok('recovery: topic recreated with 3 partitions (partitions can only be added, never removed)')


def drill_lab_read_your_writes():
    """A fresh write read back from the read model: stale, refused honestly, or waited for."""
    consumer = next(c for c in call(f'{QUERY}/lab/state')['consumers'] if c['groupId'] == 'order-projection')['id']
    act('consumer-pause', service='order-query-service', consumer=consumer)
    try:
        naive = act('exp-ryw', mode='naive', order=lab_order())
        assert naive['read']['status'] == 404, naive
        ok(f'projection paused, naive read: 404, the user\'s own order "not found" ({naive["read"]["ms"]} ms)')
        token = act('exp-ryw', mode='token', order=lab_order())
        assert token['read']['status'] == 409 and token['read']['projectedVersion'] == 0, token
        ok(f'token read: 409 after {token["read"]["waitedMs"]} ms: read model at v0 < write v{token["write"]["version"]}, not served stale')
        write_model = act('exp-ryw', mode='write-model', order=lab_order())
        assert write_model['read']['status'] == 200, write_model
        ok('write-model read: 200, always consistent')
    finally:
        act('consumer-resume', service='order-query-service', consumer=consumer)
    token = act('exp-ryw', mode='token', order=lab_order())
    assert token['read']['status'] == 200 and token['read']['orderStatus'], token
    ok(f'recovered: token read 200 ({token["read"]["orderStatus"]}) after waiting {token["read"].get("waitedMs", 0)} ms')


# ---- Phase 2: Kafka internals, on the 3-node lab cluster (Compose profile kafka-lab) ----------------
KLAB = f'{LAB}/api/kafka-lab'


def klab(path, method='POST', timeout=180):
    return call(f'{KLAB}{path}', method, timeout=timeout)


def kstate():
    return call(f'{KLAB}/state', timeout=10)


def kpart(state, topic, p=0):
    """The partition as last observed; KeyError while the cluster is not answering (wait_for retries)."""
    for t in state['cluster']['topics']:
        if t['name'] == topic:
            return t['partitions'][p]
    raise KeyError(f'{topic} not in this snapshot')


def kafka_running():
    state = try_call(f'{KLAB}/state')
    return bool(state and state['cluster']['reachable'] and len(state['cluster']['nodes']) == 3)


def drill_kafka_quorum():
    """Kill the controller quorum leader: the two remaining voters elect another."""
    klab('/reset')
    s = kstate()
    leader = s['cluster']['quorum']['leaderId']
    assert all(n['state'] == 'running' and n['registeredBroker'] for n in s['cluster']['nodes']), s['cluster']['nodes']
    assert all(p['isr'] == [1, 2, 3] for p in next(t for t in s['cluster']['topics'] if t['name'] == 'lab.replicated')['partitions'])
    ok(f'3 nodes up, quorum leader node {leader}, lab.replicated: 3 partitions, ISR [1, 2, 3] each')
    klab(f'/nodes/{leader}/kill')
    new = wait_for(lambda: (q := kstate()['cluster']['quorum']) and q['leaderId'] not in (None, leader) and q, 'a new quorum leader', 30)
    ok(f'killed node {leader}: node {new["leaderId"]} leads the quorum, epoch {new["leaderEpoch"]}')
    klab('/recover')
    ok('recovered: all three nodes registered again')


def drill_kafka_failover():
    """Kill a partition leader under traffic: nothing acknowledged is lost, nothing is duplicated."""
    klab('/recover')
    klab('/traffic/start')
    wait_for(lambda: kstate()['traffic']['consumed'] > 40, 'traffic to flow', 30)
    leader = wait_for(lambda: kpart(kstate(), 'lab.replicated')['leader'], 'lab.replicated-0 to have a leader', 30)
    klab(f'/nodes/{leader}/kill')
    moved = wait_for(lambda: (p := kpart(kstate(), 'lab.replicated')) and p['leader'] not in (None, leader) and leader not in p['isr'] and p, 'a new leader', 30)
    ok(f'killed node {leader} under 20 writes/s: lab.replicated-0 now led by node {moved["leader"]}, ISR {moved["isr"]}')
    klab('/recover')
    wait_for(lambda: all(len(p['isr']) == 3 for p in next(t for t in kstate()['cluster']['topics'] if t['name'] == 'lab.replicated')['partitions']), 'the ISR to be whole again', 60)
    v = klab('/traffic/stop')
    assert v['lostAcknowledged'] == 0 and v['duplicates'] == 0 and v['acked'] > 40, v
    ok(f'recovered, node {leader} back in the ISR: {v["acked"]} acknowledged, 0 lost, 0 duplicated, {v["retries"]} retries, slowest ack {v["maxLatencyMs"]} ms')


def drill_kafka_acks():
    """acks=1 loses acknowledged records when the leader crashes first; acks=all does not."""
    klab('/recover')
    one = klab('/scenarios/acks?acks=1')['result']
    assert one['acknowledged'] == [6, 7, 8, 9, 10] and one['lost'], one
    ok(f'acks=1: records 6–10 acknowledged by node {one["leader"]} alone, {len(one["lost"])} lost after it crashed')
    klab('/recover')
    lines = wait_for(lambda: call(f'{KLAB}/nodes/{one["leader"]}/truncations?partition=lab.acks-0')['lines'], 'the old leader to truncate', 30)
    ok(f'node {one["leader"]} came back and truncated: "{lines[-1].split("] ")[-1][:60]}"')
    every = klab('/scenarios/acks?acks=all')['result']
    assert not every['lost'] and set(range(6, 11)) <= set(every['present']), every
    ok('acks=all, same crash: nothing acknowledged lost; the producer retried against the new leader')
    klab('/recover')


def drill_kafka_min_isr():
    """min.insync.replicas refuses acks=all when too few replicas are in sync; acks=1 bypasses it."""
    klab('/reset')
    s = wait_for(lambda: (st := kstate()) and kpart(st, 'lab.durability')['leader'] and st, 'lab.durability to be visible', 30)
    p = kpart(s, 'lab.durability')
    follower = next(r for r in p['replicas'] if r != p['leader'] and r != s['cluster']['quorum']['leaderId'])
    klab(f'/nodes/{follower}/stop')
    wait_for(lambda: len(kpart(kstate(), 'lab.durability')['isr']) == 2, 'the ISR to shrink', 30)
    first = klab('/probe?acks=all')
    assert first['written'], first
    klab('/topics/lab.durability/min-insync-replicas?value=3', 'PUT')
    refused, accepted = klab('/probe?acks=all'), klab('/probe?acks=1')
    assert not refused['written'] and 'NotEnoughReplicas' in refused['error'] and accepted['written'], (refused, accepted)
    ok(f'node {follower} stopped, ISR {refused["isr"]}, min ISR 3: acks=all refused ({refused["error"].split(":")[0]}), acks=1 written')
    klab('/topics/lab.durability/min-insync-replicas?value=2', 'PUT')
    again = klab('/probe?acks=all')
    assert again['written'], again
    klab('/recover')
    ok('min ISR back to 2: acks=all accepted again; node recovered')


def drill_kafka_retries():
    """A retrying producer without idempotence writes duplicates; an idempotent one writes once."""
    klab('/recover')
    plain, idem = klab('/scenarios/retries?idempotent=false')['result'], klab('/scenarios/retries?idempotent=true')['result']
    assert plain['copies'] > 1 and idem['copies'] == 1, (plain, idem)
    ok(f'plain producer: 1 send, {plain["retries"]} retries, {plain["copies"]} copies; idempotent: {idem["retries"]} retries, 1 copy')


def drill_kafka_unclean():
    """The last in-sync replica dies: offline; an unclean election loses data, waiting does not."""
    klab('/reset')
    broken = klab('/scenarios/unclean/break')['result']
    assert broken['partition']['leader'] is None and broken['partition']['elr'] == [broken['leader']], broken
    ok(f'offline: no leader, ELR {broken["partition"]["elr"]}, {len(broken["acknowledged"])} acknowledged records')
    elected = klab('/scenarios/unclean/elect')['result']
    assert len(elected['lost']) == 3, elected
    ok(f'unclean election: available again, {len(elected["lost"])} acknowledged records gone')
    klab('/reset')
    broken = klab('/scenarios/unclean/break')['result']
    klab(f'/nodes/{broken["leader"]}/start')
    wait_for(lambda: kpart(kstate(), 'lab.unclean')['leader'] is not None, 'the in-sync replica to lead', 60)
    checked = klab('/scenarios/unclean/check')['result']
    assert not checked['lost'] and len(checked['present']) == 4, checked
    ok(f'waited for node {broken["leader"]} instead: it leads again, all 4 records kept')
    klab('/reset')


def drill_kafka_delivery():
    """A worker JVM crashing mid-batch: gaps, duplicates, or neither with transactions."""
    klab('/recover')
    amo = klab('/delivery?mode=at-most-once&crash=true')['result']
    assert amo['committed']['missing'] and not amo['committed']['duplicated'], amo['committed']
    ok(f'at-most-once, crash: {len(amo["committed"]["missing"])} missing {amo["committed"]["missing"]}, none duplicated')
    alo = klab('/delivery?mode=at-least-once&crash=true')['result']
    assert alo['committed']['duplicated'] and not alo['committed']['missing'], alo['committed']
    ok(f'at-least-once, crash: {len(alo["committed"]["duplicated"])} duplicated {alo["committed"]["duplicated"]}, none missing')
    eos = klab('/delivery?mode=exactly-once&crash=true')['result']
    assert not eos['committed']['missing'] and not eos['committed']['duplicated'] and eos['uncommitted']['duplicated'], eos
    ok(f'exactly-once, crash: read_committed every record once; read_uncommitted sees aborted copies of {eos["uncommitted"]["duplicated"]}')


DRILLS = {name[len('drill_'):]: fn for name, fn in globals().items() if name.startswith('drill_')}
ORDER = ['health', 'happy', 'replicas', 'duplicate', 'replay', 'compensation', 'poison', 'observability',
         'failover', 'lease', 'fencing', 'crash_after_commit', 'timeout', 'breaker',
         'lab_idempotency', 'lab_ordering', 'lab_repartition', 'lab_read_your_writes']
KAFKA = ['kafka_quorum', 'kafka_failover', 'kafka_acks', 'kafka_min_isr', 'kafka_retries', 'kafka_unclean', 'kafka_delivery']

if __name__ == '__main__':
    chosen = [d for name in sys.argv[1:] for d in (KAFKA if name == 'kafka' else [name])]
    if not chosen:
        chosen = ORDER + KAFKA if kafka_running() else ORDER
        if chosen == ORDER:
            print('(Kafka lab drills skipped: start them with `docker compose --profile kafka-lab up -d`)')
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
