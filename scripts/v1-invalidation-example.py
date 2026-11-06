#!/usr/bin/env python3
"""离线合成作废响应与调用方判定，不加载凭据、不发送请求。参见详细设计9.3.6。"""
import argparse
import json


def classify(response):
    code = str(response.get('code', ''))
    if code == '503':
        return 'failed'
    if code != '200':
        return 'protocol-error'
    data = response.get('data') or {}
    completed = (str(data.get('order_status')) == '12'
                 and str(data.get('state_type')) in ('2', '3')
                 and str(data.get('use_state')) == '3'
                 and isinstance(data.get('invalid_time'), str)
                 and bool(data['invalid_time'].strip())
                 and data.get('use_time') in (None, ''))
    return 'completed' if completed else 'protocol-error'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=('completed', 'failed'), default='completed')
    scenario = parser.parse_args().scenario
    codes = {'completed': '200', 'failed': '503'}
    response = {'code': codes[scenario], 'msg': {
        'completed': '操作成功', 'failed': '作废失败'}[scenario], 'data': None}
    if scenario == 'completed':
        response['data'] = dict(order_id='SYNTHETIC-ORDER', out_order_sn='SYNTHETIC-OUT',
                                order_status='12', order_status_desc='成功' if scenario == 'completed' else '作废结果待确认',
                                state_type='3' if scenario == 'completed' else '',
                                use_state='3' if scenario == 'completed' else '', use_time='',
                                invalid_time='2026-11-05 10:00:00' if scenario == 'completed' else '')
    print(json.dumps(dict(http_status=200, response=response, classification=classify(response)), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
