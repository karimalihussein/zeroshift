#!/usr/bin/env python3
"""End-to-end drills for the Phase 3 resilience lab. Start the full Compose stack first; the network
experiments also need the chaos overlay (docker-compose.chaos.yml).

Each experiment drill runs the experiment automatically through the control plane (Hypothesis →
Inject → Observe → Explain → Mitigate → Recover → Verify), fails if any claim did not hold, and then
checks independently, from the services and Toxiproxy themselves, that the lab was put back: links
healed, edge guards off, default gateway policy with a closed breaker, no slow consumer, and the
run stored in lab_run. The load and chaos drills check the levers on their own.

    scripts/verify_resilience_lab.py                  # every drill (network ones only with Toxiproxy)
    scripts/verify_resilience_lab.py load slow-consumer
    LAB_URL=http://localhost:8089 scripts/verify_resilience_lab.py
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

LAB = os.environ.get('LAB_URL', 'http://localhost:8080')
ORDER_A, ORDER_B = 'http://localhost:18081', 'http://localhost:18088'
PAYMENT = 'http://localhost:18082'
TOXIPROXY = os.environ.get('TOXIPROXY_URL', 'http://localhost:8474')
EXPERIMENTS = ['slow-consumer', 'poll-interval', 'gateway-breaker', 'bulkhead', 'retry-storm',
               'kafka-partition', 'cdc-degraded']
NETWORK = {'bulkhead', 'retry-storm', 'kafka-partition', 'cdc-degraded'}


def call(url, method='GET', body=None, timeout=30):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read()
        return json.loads(text) if text else None


def lab(path, method='GET', body=None):
    return call(LAB + path, method, body)


def ok(message):
    print(f'  ✓ {message}', flush=True)


def wait_for(check, what, timeout=60, every=1):
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


def chaos_running():
    try:
        call(TOXIPROXY + '/version', timeout=3)
        return lab('/api/resilience/state')['chaos']['configured']
    except (urllib.error.URLError, OSError):
        return False


def assert_reset():
    """Independently of the control plane: every lever is back at its default."""
    for name, base in (('order-service', ORDER_A), ('order-service-b', ORDER_B)):
        s = call(base + '/lab/edge')['settings']
        assert not (s['rateLimit'] or s['shedding'] or s['bulkhead']), f'{name} guards still on: {s}'
    g = call(PAYMENT + '/lab/gateway')
    assert g['mode'] == 'healthy', g['mode']
    assert g['policy'] == {'timeoutMs': 1500, 'retry': 'exponential', 'maxAttempts': 3, 'breaker': True, 'pauseOnOpen': False}, g['policy']
    assert g['breakerState'] == 'CLOSED', g['breakerState']
    p = call(PAYMENT + '/lab/state')
    assert not [f for f in p['faults'] if f['name'] == 'slow-processing'], p['faults']
    # Restoring the default poll settings restarts the consumer in the background: allow for it.
    payment_consumer = lambda: next(c for c in call(PAYMENT + '/lab/state')['consumers'] if c['id'] == 'payment-service')
    consumer = wait_for(lambda: (lambda c: c if c['running'] else None)(payment_consumer()), 'the payment consumer running', 30)
    assert not consumer['pauseRequested'] and not consumer['configOverrides'], consumer
    if chaos_running():
        proxies = call(TOXIPROXY + '/proxies')
        broken = {n: (p['enabled'], p['toxics']) for n, p in proxies.items() if not p['enabled'] or p['toxics']}
        assert not broken, f'links still broken: {broken}'
    assert not lab('/api/resilience/state')['load']['running'], 'load still running'


def run_experiment(experiment_id):
    before = lab('/api/resilience/runs?limit=1')
    last_id = before[0]['id'] if before else 0
    run = lab(f'/api/resilience/experiments/{experiment_id}/start?auto=true', 'POST')
    seen = {}
    while run['status'] in ('running', 'waiting'):
        time.sleep(2)
        run = lab('/api/resilience/state')['run']
        for i, step in enumerate(run['steps']):
            if step['status'] in ('passed', 'failed', 'done') and seen.get(i) != step['status']:
                seen[i] = step['status']
                mark = {'passed': '✓', 'done': '·', 'failed': '✗'}[step['status']]
                print(f'  {mark} {step["phase"].lower()}: {step["title"]}' + (f' — {step["did"]}' if step['did'] else ''), flush=True)
                for c in step['checks']:
                    print(f'      {"✓" if c["status"] == "held" else "✗"} {c["claim"]} [{c["observed"]}]', flush=True)
    failed = [f'{s["phase"]}: {c["claim"]} [{c["observed"]}]' for s in run['steps'] for c in s['checks'] if c['status'] != 'held']
    failed += [f'{s["phase"]}: {s["did"]}' for s in run['steps'] if s['status'] == 'failed' and not s['checks']]
    assert run['status'] == 'passed' and not failed, f'{run["status"]}: ' + '; '.join(failed)
    stored = wait_for(lambda: [r for r in lab('/api/resilience/runs?limit=3') if r['id'] > last_id and r['mode'] == experiment_id], 'the run in lab_run', 20)
    assert stored[0]['result']['status'] == 'passed', stored[0]['summary']
    ok(f'every claim held; run {stored[0]["id"]} stored in lab_run')
    assert_reset()
    ok('lab reset: guards off, default gateway policy, breaker closed, consumer defaults, links healed, load stopped')


def drill_load():
    """The generator's own controls, cross-checked against the order service's server-side counts."""
    lab('/api/resilience/reset', 'POST')
    lab('/api/resilience/load/reset', 'POST')
    admitted = lambda: sum(call(b + '/lab/edge')['counters']['admitted'] for b in (ORDER_A, ORDER_B))
    before = admitted()
    lab('/api/resilience/load/start', 'POST', {'ratePerSecond': 5, 'concurrency': 50, 'readPercent': 0, 'retry': 'jitter', 'maxAttempts': 3, 'timeoutMs': 3000})
    time.sleep(20)
    lab('/api/resilience/load/stop', 'POST')
    time.sleep(4)  # the last clients finish
    totals = lab('/api/resilience/state')['load']
    assert not totals['running']
    assert 90 <= totals['arrivals'] <= 110, f'5/s for 20 s gave {totals["arrivals"]} arrivals'
    server = admitted() - before
    # A retried attempt whose first try did succeed is admitted twice (the Idempotency-Key makes it
    # one order), so the server's count sits between the successes and the attempts.
    assert totals['succeeded'] <= server <= totals['attempts'], f'order-service admitted {server}; clients: {totals}'
    ok(f'{totals["arrivals"]} clients in 20 s at 5/s; order-service admitted {server} for {totals["succeeded"]} successes in {totals["attempts"]} attempts')
    arrivals = totals['arrivals']
    time.sleep(3)
    assert lab('/api/resilience/state')['load']['arrivals'] == arrivals, 'arrivals continued after stop'
    reset = lab('/api/resilience/load/reset', 'POST')
    assert reset['arrivals'] == 0 and reset['attempts'] == 0
    ok('stop halts arrivals; reset zeroes the counters')
    samples = lab('/api/resilience/state')['samples'][-20:]
    assert any(s['sagas']['completed'] for s in samples) and any(s['relay']['records'] for s in samples), 'no saga completions or relayed records sampled'
    ok('samples carry saga completions and outbox → Kafka relay delays measured under the load')


def drill_chaos():
    """A toxic injected through the control plane is really in Toxiproxy, and really slows the link."""
    lab('/api/resilience/reset', 'POST')
    def timed_read():
        started = time.monotonic()
        call(ORDER_A + '/orders?limit=1')
        return (time.monotonic() - started) * 1000
    base = min(timed_read() for _ in range(5))
    lab('/api/resilience/chaos/order-db/latency?value=300&jitter=0', 'POST')
    toxics = call(TOXIPROXY + '/proxies/order-db/toxics')
    assert [t for t in toxics if t['type'] == 'latency' and t['attributes']['latency'] == 300], toxics
    slow = min(timed_read() for _ in range(3))
    assert slow >= base + 250, f'GET /orders took {slow:.0f} ms with +300 ms latency (baseline {base:.0f} ms)'
    ok(f'order-db +300 ms: a read went from {base:.0f} ms to {slow:.0f} ms')
    lab('/api/resilience/chaos/order-db/down', 'POST')
    assert not call(TOXIPROXY + '/proxies/order-db')['enabled']
    ok('order-db down: Toxiproxy reports the proxy disabled')
    lab('/api/resilience/chaos/heal', 'POST')
    proxies = call(TOXIPROXY + '/proxies')
    assert all(p['enabled'] and not p['toxics'] for p in proxies.values()), proxies
    wait_for(lambda: timed_read() < base + 200, 'reads fast again', 60)
    ok('heal: every link enabled with no toxics; reads fast again')


DRILLS = {'load': drill_load, 'chaos': drill_chaos}
for experiment in EXPERIMENTS:
    DRILLS[experiment] = (lambda e: lambda: run_experiment(e))(experiment)

if __name__ == '__main__':
    chosen = sys.argv[1:] or ['load', 'chaos'] + EXPERIMENTS
    wait_for(lambda: lab('/api/resilience/state'), f'the control plane at {LAB}', 180, 2)
    if not chaos_running():
        skipped = [d for d in chosen if d in NETWORK or d == 'chaos']
        chosen = [d for d in chosen if d not in skipped]
        if skipped:
            print(f'(skipped {", ".join(skipped)}: start Toxiproxy with docker-compose.chaos.yml)')
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
            try:
                lab('/api/resilience/experiments/abort', 'POST')
            except Exception:
                pass
            try:
                lab('/api/resilience/reset', 'POST')
            except Exception:
                pass
    print('FAILED: ' + ', '.join(failed) if failed else f'All {len(chosen)} drills passed.')
    sys.exit(1 if failed else 0)
