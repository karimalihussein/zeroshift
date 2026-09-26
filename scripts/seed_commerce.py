#!/usr/bin/env python3
"""Places randomized demo orders through the running commerce services.

Customers and vouchers (order-service) and the product catalog (inventory-service) are seeded by the
services themselves on an empty database. Orders are never inserted directly: each one goes through
POST /orders, so its payment, stock reservation, invoice, shipment and read-model row are produced by
the real saga, exactly as for an order placed from the dashboard.

  scripts/seed_commerce.py            # 25 orders
  scripts/seed_commerce.py 60 --seed 7
"""
import argparse
from decimal import Decimal
import json
import random
import sys
import time
import urllib.error
import urllib.request
import uuid

ORDERS = 'http://localhost:18081'
INVENTORY = 'http://localhost:18084'
QUERY = 'http://localhost:18086'


def call(url, method='GET', body=None, headers=None, timeout=30):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method,
                                     headers={'Content-Type': 'application/json', **(headers or {})})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read()
        # Money stays exact: never parse amounts as floats.
        return json.loads(text, parse_float=Decimal) if text else None


def data(url):
    return call(url)['data']


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('orders', nargs='?', type=int, default=25)
    parser.add_argument('--seed', type=int, default=None, help='random seed, for a repeatable mix')
    parser.add_argument('--voucher-share', type=float, default=0.35, help='share of orders that try a voucher')
    args = parser.parse_args()
    rnd = random.Random(args.seed)

    customers = data(f'{ORDERS}/customers?limit=100')
    products = [p for p in data(f'{INVENTORY}/products?limit=100') if p['active'] and p['stock'] > 0]
    vouchers = [v['code'] for v in data(f'{ORDERS}/vouchers') if v['active']]
    if not customers or not products:
        sys.exit('No customers or products: start the stack with COMMERCE_DEMO_DATA=true (the Compose default).')
    print(f'{len(customers)} customers, {len(products)} products in stock, {len(vouchers)} active vouchers')

    placed, refused = [], {}
    for n in range(args.orders):
        customer = rnd.choice(customers)
        items = [{'sku': p['sku'], 'quantity': rnd.randint(1, 3)}
                 for p in rnd.sample(products, k=min(len(products), rnd.randint(1, 4)))]
        body = {'customerId': customer['id'], 'items': items}
        if vouchers and rnd.random() < args.voucher_share:
            body['voucherCode'] = rnd.choice(vouchers)
        try:
            accepted = call(f'{ORDERS}/orders', 'POST', body, {'Idempotency-Key': f'seed-{uuid.uuid4()}'})
        except urllib.error.HTTPError as e:
            code = json.loads(e.read() or b'{}', parse_float=Decimal).get('code', f'HTTP {e.code}')
            if 'voucherCode' in body and code.startswith('VOUCHER'):
                refused[code] = refused.get(code, 0) + 1
                del body['voucherCode']  # a voucher that does not apply: buy without it
                accepted = call(f'{ORDERS}/orders', 'POST', body, {'Idempotency-Key': f'seed-{uuid.uuid4()}'})
            else:
                refused[code] = refused.get(code, 0) + 1
                continue
        placed.append(accepted['orderId'])
        voucher = f" with {body['voucherCode']}" if 'voucherCode' in body else ''
        print(f"  {accepted['invoiceNumber']}  {customer['name']:<24} {len(items)} item(s){voucher:<16}"
              f" {accepted['total']:>9} {accepted['currency']}")

    print(f'Placed {len(placed)} orders' + (f'; refused: {refused}' if refused else ''))
    # The saga and the read model finish asynchronously: wait until the projection has them all.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        projected = {o['orderId']: o['status'] for o in data(f'{QUERY}/orders?limit=500')}
        pending = [o for o in placed if projected.get(o) in (None, 'PLACED', 'PAID', 'RESERVED')]
        if not pending:
            break
        time.sleep(1)
    statuses = {}
    for o in placed:
        statuses[projected.get(o, 'not projected')] = statuses.get(projected.get(o, 'not projected'), 0) + 1
    print('Final statuses in the read model:', statuses)


if __name__ == '__main__':
    main()
