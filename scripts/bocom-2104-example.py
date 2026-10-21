#!/usr/bin/env python3
"""离线生成2104合成正文；参见详细设计「交行 2104 通知实施细化」。不发网络请求、不加载密钥。"""
import argparse
import datetime
import json
import secrets
import time

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--operation', choices=['order', 'redeemed', 'cancelled'], default='order')
    parser.add_argument('--protocol', default='OFFLINE-EXAMPLE-ONLY')
    options = parser.parse_args()
    if not options.protocol.strip() or len(options.protocol) > 200:
        parser.error('protocol must contain 1..200 characters')
    common = dict(order_id='SYNTHETIC-ORDER', out_order_sn='SYNTHETIC-MERCHANT-ORDER',
                  timestamp=str(time.time_ns() // 1_000_000), nonce_str=secrets.token_hex(16), account='synthetic-account')
    if options.operation == 'order':
        fields = dict(operate_type='1', order_id=common['order_id'], out_order_sn=common['out_order_sn'],
                      total_price=100, type='1', total_settle_price=100, timestamp=common['timestamp'],
                      nonce_str=common['nonce_str'], order_status='12', account=common['account'])
    else:
        event_time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        redeemed = options.operation == 'redeemed'
        fields = dict(operate_type='2' if redeemed else '3', **common,
                      use_time=event_time if redeemed else '', state_type='3', use_state='2' if redeemed else '3',
                      invalid_time='' if redeemed else event_time)
    if any(',' in str(value) for value in fields.values()):
        parser.error('business values may not contain comma')
    encoded = ','.join(str(key) + ':' + str(value) for key, value in fields.items())
    if len(encoded) > 2500:
        parser.error('trx_data_imprt exceeds 2500 characters')
    print(json.dumps(dict(head={}, body=dict(trx_data_imprt=encoded, protocol=options.protocol)), ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
