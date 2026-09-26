#!/usr/bin/env python3
"""Destructive end-to-end check of only the Java demo's data. Start Compose first."""
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

BASE = os.environ.get('ZEROSHIFT_URL', 'http://localhost:8080')
ROOT = Path(__file__).resolve().parents[1]

def request(path, method='GET'):
    with urllib.request.urlopen(urllib.request.Request(BASE + path, method=method), timeout=90) as response:
        return json.load(response)

def action(name):
    result = request('/api/actions/' + name, 'POST')
    print(name + ': ' + result['message'], flush=True)
    return result

def status():
    return request('/api/status')

def wait_for(predicate, description, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = status()
            if predicate(last):
                return last
            if last['migration']['status'] == 'FAILED':
                raise AssertionError(last['migration']['error'])
        except (ConnectionError, OSError):
            pass
        time.sleep(0.2)
    raise AssertionError(f'Timed out waiting for {description}: {last}')

def compose(*args):
    subprocess.run(['docker', 'compose', *args], cwd=ROOT, check=True)

def sql_scalar(statement):
    result = subprocess.run(['docker', 'compose', 'exec', '-T', 'postgres', 'sh', '-c',
                             'psql -U "$POSTGRES_USER" -d zeroshift -At -c "$1"',
                             'sh', statement], cwd=ROOT, check=True, capture_output=True, text=True)
    return result.stdout.strip()

def main():
    wait_for(lambda _: True, 'healthy backend')
    action('reset')
    action('seed')
    action('traffic-start')
    action('start')
    wait_for(lambda d: d['migration']['batches'] >= 2, 'snapshot batches')
    action('crash')
    crashed = wait_for(lambda d: d['migration']['status'] == 'CRASHED', 'simulated crash')
    action('resume')
    wait_for(lambda d: d['migration']['batches'] > crashed['migration']['batches'], 'resumed batch')
    compose('kill', '-s', 'SIGKILL', 'app')
    compose('up', '-d', 'app')
    recovered = wait_for(lambda d: d['migration']['status'] == 'PAUSED', 'backend restart recovery')
    assert recovered['migration']['batches'] >= crashed['migration']['batches']
    action('resume')
    wait_for(lambda d: d['migration']['stage'] == 'READY', 'ready for validation')
    validation = action('validate')
    assert validation['message'] == 'Validation passed'
    before = status()
    action('cutover')
    completed = wait_for(lambda d: d['migration']['primary'] == 'POSTGRESQL', 'cutover')
    frozen_source = completed['source']
    source_max = int(sql_scalar('SELECT max(id) FROM customers'))
    wait_for(lambda _: int(sql_scalar('SELECT max(id) FROM customers')) > source_max, 'new PostgreSQL identity')
    action('traffic-stop')
    after = status()
    assert after['source'] == frozen_source, 'Source changed after cutover'
    assert after['migration']['stage'] == 'COMPLETED'
    assert after['migration']['validation'].startswith('Passed')
    evidence = {
        'verifiedAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'checks': ['HTTP seeding', 'concurrent traffic', 'simulated crash rollback/resume',
                   'SIGKILL backend restart recovery', 'full row validation', 'cutover',
                   'new target identity/write', 'source unchanged after cutover'],
        'beforeCutover': before,
        'afterCutover': after,
    }
    output = ROOT / 'docs' / 'verification.json'
    output.write_text(json.dumps(evidence, indent=2) + '\n')
    print(f'PASS: complete demo verified; evidence in {output}', flush=True)

if __name__ == '__main__':
    main()
