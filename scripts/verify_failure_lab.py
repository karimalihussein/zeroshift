#!/usr/bin/env python3
"""End-to-end drills for the Phase 5 failure lab. Start the stack with the failure-lab profile:

    docker compose --profile failure-lab up -d
    scripts/verify_failure_lab.py                       # every drill
    scripts/verify_failure_lab.py two_phase trust
    LAB_URL=http://localhost:8089 scripts/verify_failure_lab.py

Each experiment drill runs the five stages (Naive design → Failure → Observable consequence →
Correct design → Recovery) through the control plane. A stage only succeeds when every claim it
checked against what it measured holds; the drill then asserts the key measurements again, and
that the completed run was stored in lab_run. The other drills check the manual 2PC recovery,
the error codes, the traces and metrics, the resets, and that nothing leaked into the event lab.
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

LAB = os.environ.get('LAB_URL', 'http://localhost:8080')
ORDER_A = 'http://localhost:18081'
TEMPO = os.environ.get('TEMPO_URL', 'http://localhost:3200')
PROMETHEUS = os.environ.get('PROMETHEUS_URL', 'http://localhost:9090')


def call(url, method='GET', body=None, timeout=300):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read()
        return json.loads(text) if text else None


def problem(url, method='POST'):
    """The status and problem+json body of a request expected to fail."""
    try:
        call(url, method)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b'{}')
    raise AssertionError(f'{method} {url} succeeded')


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


def psql(service, database, sql):
    out = subprocess.run(['docker', 'compose', 'exec', '-T', service, 'psql', '-U', 'zeroshift', '-d', database, '-qAt', '-c', sql],
                         capture_output=True, text=True, check=True)
    return out.stdout.strip()


def stages(lab):
    """Runs the five stages; returns their results. Fails with the broken claims if one does not hold."""
    before = call(f'{LAB}/api/failures/runs?limit=1')
    last = before[0]['id'] if before else 0
    results = []
    for n in range(5):
        try:
            run = call(f'{LAB}/api/failures/labs/{lab}/stages/{n}', 'POST')
        except urllib.error.HTTPError as e:
            info = next(l for l in call(f'{LAB}/api/failures/labs') if l['id'] == lab)
            failure = info['run'] and info['run']['lastFailure']
            broken = [c for c in ((failure or {}).get('result') or {}).get('checks', []) if not c['ok']]
            raise AssertionError(f'{lab} stage {n + 1} failed: {e.read().decode()[:400]} broken claims: {broken}')
        stage = run['stages'][n]
        assert stage['ok'], stage
        held = sum(1 for c in stage['result'].get('checks', []) if c['ok'])
        print(f'    {stage["name"]}: {stage["millis"]:,} ms, {held} claims held', flush=True)
        results.append(stage)
    stored = wait_for(lambda: [r for r in call(f'{LAB}/api/failures/runs?limit=3') if r['id'] > last and r['mode'] == lab], 'the run in lab_run', 20)
    ok(f'all five stages held; run {stored[0]["id"]} stored in lab_run')
    return results


def drill_status():
    """The lab's own infrastructure answers: prepared transactions enabled, both brokers up."""
    s = call(f'{LAB}/api/failures/status')
    assert s['failurePostgres']['up'] and int(s['failurePostgres']['max_prepared_transactions']) > 0, s
    assert s['kafka']['up'] and s['kafkaSecure']['up'], s
    assert s['kafkaSecure']['authentication'].startswith('SASL'), s
    ok(f'failure-postgres (max_prepared_transactions={s["failurePostgres"]["max_prepared_transactions"]}), kafka ({s["kafka"]["authentication"]}), kafka-secure ({s["kafkaSecure"]["authentication"]})')
    assert psql('commerce-postgres', 'orders', 'SHOW max_prepared_transactions') == '0'
    ok('the services\' commerce-postgres still has max_prepared_transactions = 0: 2PC cannot touch it')


def drill_two_phase():
    """2PC in doubt after a SIGKILL versus a saga after the same crash, then recovery of both."""
    r = stages('two-phase')
    crash = r[1]['result']
    assert crash['exitCode'] == 137 and len(crash['preparedTransactions']) == 2, crash
    ok(f'coordinator pid {crash["coordinatorPid"]} SIGKILLed after PREPARE: {", ".join(p["gid"] for p in crash["preparedTransactions"])} in doubt')
    harm = r[2]['result']
    same = next(p for p in harm['otherWork'] if p['name'] == 'checkout, same SKU')
    other = next(p for p in harm['otherWork'] if p['name'] == 'checkout, other SKU')
    assert same['outcome'] == 'LOCK_TIMEOUT' and same['blockedBy'] == [0] and same['sqlstate'] == '55P03', same
    assert other['outcome'] == 'DONE' and other['elapsedMs'] < 1000, other
    assert harm['vacuum']['deadNotRemovable'] >= 200, harm['vacuum']
    assert len(harm['preparedLocks']) >= 4 and all(l['pid'] is None for l in harm['preparedLocks']), harm['preparedLocks']
    ok(f'checkout blocked {same["elapsedMs"]:,} ms by pid 0 (SQLSTATE 55P03); other SKU in {other["elapsedMs"]} ms; '
       f'{len(harm["preparedLocks"])} locks held by no session; {len(harm["sessionsOnParticipants"])} sessions left on the participants; '
       f'VACUUM left {harm["vacuum"]["deadNotRemovable"]} dead rows')
    saga = r[3]['result']
    assert all(p['outcome'] == 'DONE' and p['elapsedMs'] < 1000 for p in saga['otherWork'][:2]), saga['otherWork']
    assert saga['halfDoneOrder']['payment'] == 'CAPTURED' and saga['halfDoneOrder']['reservation'] is None
    ok('saga killed at the same point: no locks, probes done at once, the half-done order visible to all')
    rec = r[4]['result']
    assert [a['action'] for a in rec['twoPhaseRecovery']] == ['ROLLBACK PREPARED', 'ROLLBACK PREPARED'], rec['twoPhaseRecovery']
    assert rec['twoPhaseOrderOutcome'] == '0 payment + 0 reservation' and rec['sagaOrder']['payment'] == 'REFUNDED'
    assert rec['invariants']['balancesMatchPayments'] and rec['invariants']['stockMatchesReservations']
    ok(f'2PC: presumed abort from the log (no decision) → ROLLBACK PREPARED ×2; saga: new coordinator compensated in {rec["sagaRecoveryMs"]:,} ms; money and stock reconcile')
    return r


def drill_manual_recovery():
    """A logged COMMIT must be finished; the inspector's buttons resolve single branches."""
    call(f'{LAB}/api/failures/labs/two-phase/reset', 'POST')
    drill = call(f'{LAB}/api/failures/two-phase/in-doubt?crashAt=after-decision', 'POST')
    assert drill['exitCode'] == 137 and drill['decision'] == 'COMMIT', drill
    state = call(f'{LAB}/api/failures/labs/two-phase/state')
    prepared = state['preparedTransactions']
    assert len(prepared) == 2 and all(p['loggedDecision'] == 'COMMIT' for p in prepared), prepared
    ok(f'coordinator killed after logging COMMIT: 2 branches in doubt, the log says COMMIT')
    payments = next(p['gid'] for p in prepared if p['gid'].endswith('-payments'))
    call(f'{LAB}/api/failures/two-phase/prepared/{payments}/commit', 'POST')
    left = call(f'{LAB}/api/failures/labs/two-phase/state')['preparedTransactions']
    assert [p['gid'] for p in left] == [payments.replace('-payments', '-inventory')], left
    ok(f'operator: COMMIT PREPARED {payments}; the inventory branch is still in doubt')
    actions = call(f'{LAB}/api/failures/two-phase/recover', 'POST')
    assert [a['action'] for a in actions] == ['COMMIT PREPARED'], actions
    rows = call(f'{LAB}/api/failures/labs/two-phase/state')['rows']
    assert any(p['order_id'] == drill['order'] for p in rows['payments']) and any(r['order_id'] == drill['order'] for r in rows['reservations'])
    ok('recovery finished the logged decision: payment and reservation both committed')
    drill = call(f'{LAB}/api/failures/two-phase/in-doubt?crashAt=after-prepare', 'POST')
    for p in call(f'{LAB}/api/failures/labs/two-phase/state')['preparedTransactions']:
        call(f'{LAB}/api/failures/two-phase/prepared/{p["gid"]}/rollback', 'POST')
    rows = call(f'{LAB}/api/failures/labs/two-phase/state')
    assert not rows['preparedTransactions'] and not any(p['order_id'] == drill['order'] for p in rows['rows']['payments'])
    ok('no decision logged: ROLLBACK PREPARED on both branches by hand, nothing of the order remains')


def drill_isolation():
    """Lost update and write skew under every strategy, compared."""
    r = stages('isolation')
    naive = r[2]['result']
    ok(f'naive: {naive["lostUpdate"]["actual"]} (expected {naive["lostUpdate"]["expected"]}); write skew: {naive["writeSkew"]["actual"]}; every request committed')
    matrix = r[4]['result']['matrix']
    assert len(matrix) == 9, matrix
    for row in matrix:
        print(f'      {row["scenario"]:<11} {row["strategy"]:<24} {row["isolation"]:<16} invariant={"holds" if row["invariant"] else "BROKEN":<7}'
              f' committed={row["committed"]} aborted={row["aborted"]} conflicts={row["version conflicts"]}+{row["40001"]} retries={row["retries"]} lock wait={row["lock wait ms"]} ms')
    broken = [row for row in matrix if not row['invariant']]
    assert {(b['scenario'], b['isolation']) for b in broken} == {('lost-update', 'READ COMMITTED'), ('write-skew', 'READ COMMITTED'), ('write-skew', 'REPEATABLE READ')}, broken
    ok('only the naive designs broke their invariant; SERIALIZABLE without retry kept it but failed users')


def drill_disaster_recovery():
    """Backup, more events, lose the databases, restore: the offset gap, RPO, RTO, replay."""
    r = stages('disaster-recovery')
    gap = r[2]['result']
    ok(f'naive restore: {gap["comparedWithTopic"]["missing"]} events ({gap["comparedWithTopic"]["missingAmount"]}) missing, '
       f'offset gap {sum(p["gap"] for p in gap["offsetGap"].values())}, RPO window {gap["rpo"]["dataLossWindowSeconds"]} s')
    safe = r[3]['result']
    ok(f'safe restore: replayed {safe["safeRun"]["applied"]} from the backup\'s positions {safe["resumedFromPositionsInBackup"]}; '
       f'rewound to 0: {safe["rewoundToZero"]["read"]} redelivered, {safe["rewoundToZero"]["applied"]} applied; RTO {safe["rto"]["rtoMillis"]:,} ms')
    rec = r[4]['result']
    assert all(b['match'] for b in rec['balances']), rec['balances']
    assert rec['naiveRepair']['skippedDuplicates'] > 0, rec['naiveRepair']
    ok(f'naive repair from Kafka\'s time index {rec["replayFromTimeIndex"]["offsets"]}: applied {rec["naiveRepair"]["applied"]}, '
       f'skipped {rec["naiveRepair"]["skippedDuplicates"]} known ids; both ledgers = topic, every balance matches')
    status = call(f'{LAB}/api/failures/labs/disaster-recovery/state')
    assert status['rpoEvents'] == 25 and status['rtoMillisSafe'] > 0
    ok('inspector exposes RPO and RTO as measured')


def drill_trust():
    """A forged event ships an unpaid order on the open broker; ACLs stop it on the secure one."""
    r = stages('trust')
    attack = r[1]['result']['attackerDecision']
    ok(f'open broker: forged PaymentCaptured accepted at offset {attack["offset"]} ({r[0]["result"]["openBroker"]["describeAcls"][:60]}…)')
    ok(f'reconciliation: {r[2]["result"]["reconciliation"]["unpaid"]} shipment without payment, worth {r[2]["result"]["reconciliation"]["unpaidAmount"]}')
    for a in r[3]['result']['attempts']:
        print(f'      {a["principal"]:<32} {a["operation"]:<5} {a["resource"]:<44} {a["decision"]:<7} {a["error"] or ""}')
    denied = [a for a in r[3]['result']['attempts'] if a['decision'] == 'DENIED']
    assert len(denied) == 5, denied
    acls = r[3]['result']['acls']
    assert {a['principal'] for a in acls} == {'User:payments', 'User:fulfilment', 'User:checkout'}, acls
    ok(f'secure broker: {len(acls)} ACLs, 5 attempts denied, only User:payments published payment events')
    assert r[4]['result']['reconciliation']['open']['unpaid'] == 0 and r[4]['result']['reconciliation']['secure']['unpaid'] == 0
    ok('forged shipment held; every active shipment has a payment on both brokers')


def drill_errors():
    """Stages in order, unknown labs, foreign transactions and bad actions are refused with codes."""
    call(f'{LAB}/api/failures/labs/trust/reset', 'POST')
    status, body = problem(f'{LAB}/api/failures/labs/trust/stages/3')
    assert status == 409 and body['code'] == 'STAGE_OUT_OF_ORDER', body
    status, body = problem(f'{LAB}/api/failures/labs/nope/stages/0')
    assert status == 404 and body['code'] == 'UNKNOWN_FAILURE_LAB', body
    status, body = problem(f'{LAB}/api/failures/two-phase/prepared/not-ours/commit')
    assert status == 400 and body['code'] == 'INVALID_FAILURE_ACTION', body
    status, body = problem(f'{LAB}/api/failures/two-phase/in-doubt?crashAt=whenever')
    assert status == 400 and body['code'] == 'INVALID_FAILURE_ACTION', body
    ok('409 STAGE_OUT_OF_ORDER, 404 UNKNOWN_FAILURE_LAB, 400 INVALID_FAILURE_ACTION (foreign gid, bad crash point)')


def drill_observability():
    """Every stage is a trace in Tempo; stage outcomes and RPO/RTO are Prometheus metrics."""
    lab = next(l for l in call(f'{LAB}/api/failures/labs') if l['id'] == 'disaster-recovery')
    trace = lab['run']['stages'][1]['traceId'] if lab['run'] and lab['run']['stages'] else None
    if trace:
        found = wait_for(lambda: call(f'{TEMPO}/api/traces/{trace}', timeout=10), f'trace {trace} in Tempo', 60, 3)
        names = [s['name'] for b in found.get('batches', []) for ss in b.get('scopeSpans', []) for s in ss.get('spans', [])]
        assert any(n.startswith('failure-lab disaster-recovery stage 2') for n in names), names
        ok(f'trace {trace[:12]}…: {names[0]} in Tempo')
    else:
        print('    (no trace id: the control plane runs without the OpenTelemetry agent)')
    series = wait_for(lambda: call(f'{PROMETHEUS}/api/v1/query?query=zeroshift_failure_lab_stages_total')['data']['result'], 'failure-lab stage metrics in Prometheus', 60, 5)
    held = sum(float(s['value'][1]) for s in series if s['metric'].get('outcome') == 'held')
    rpo = call(f'{PROMETHEUS}/api/v1/query?query=zeroshift_failure_lab_dr_rpo_events')['data']['result']
    ok(f'Prometheus: {len(series)} stage series ({held:.0f} held stages), rpo_events={rpo[0]["value"][1] if rpo else "not scraped yet"}')


def drill_resets_and_isolation():
    """Resets leave nothing in doubt, and the event lab keeps working next to the failure lab."""
    for lab in ('two-phase', 'disaster-recovery', 'trust', 'isolation'):
        call(f'{LAB}/api/failures/labs/{lab}/reset', 'POST')
    assert psql('failure-postgres', 'failure_lab', 'SELECT count(*) FROM pg_prepared_xacts') == '0'
    dbs = psql('failure-postgres', 'failure_lab', "SELECT string_agg(datname, ',' ORDER BY datname) FROM pg_database WHERE datname LIKE 'fl_%'")
    assert 'backup' not in dbs, dbs
    ok(f'every lab reset: no prepared transactions, no backups; databases {dbs}')
    customer = call(f'{ORDER_A}/customers?limit=1')['data'][0]['id']
    order = call(f'{ORDER_A}/orders', 'POST', {'customerId': customer, 'items': [{'sku': 'SKU-CABLE', 'quantity': 1}]})['orderId']
    state = wait_for(lambda: (lambda s: s if s in ('COMPLETED', 'CANCELLED') else None)(call(f'{ORDER_A}/orders/{order}')['saga']['state']), 'the order saga', 90)
    assert state == 'COMPLETED', state
    ok(f'event lab unaffected: a new order\'s saga COMPLETED ({order[:8]})')


DRILLS = {
    'status': drill_status,
    'two_phase': drill_two_phase,
    'manual_recovery': drill_manual_recovery,
    'isolation': drill_isolation,
    'disaster_recovery': drill_disaster_recovery,
    'trust': drill_trust,
    'observability': drill_observability,
    'errors': drill_errors,
    'resets': drill_resets_and_isolation,
}


def main(names):
    failed = []
    for name in names or DRILLS:
        print(f'▶ {name}: {DRILLS[name].__doc__}', flush=True)
        started = time.monotonic()
        try:
            DRILLS[name]()
            print(f'  passed in {time.monotonic() - started:.1f} s', flush=True)
        except Exception as e:  # noqa: BLE001 - report every drill
            failed.append(name)
            body = e.read().decode(errors='replace')[:500] if isinstance(e, urllib.error.HTTPError) else ''
            print(f'  ✗ FAILED: {type(e).__name__}: {e} {body}', flush=True)
    print(f'\n{len(names or DRILLS) - len(failed)}/{len(names or DRILLS)} drills passed' + (f'; failed: {", ".join(failed)}' if failed else ''))
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
