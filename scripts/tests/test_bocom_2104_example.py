"""合成通知示例只生成正文，不调用银行。"""
import json
import pathlib
import subprocess
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]

class BocomExampleTest(unittest.TestCase):
    def test_generates_all_three_operations_without_network_or_business_signature(self):
        for operation, code in [('order', '1'), ('redeemed', '2'), ('cancelled', '3')]:
            result = subprocess.run(['python3', str(ROOT / 'scripts/bocom-2104-example.py'), '--operation', operation],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            body = json.loads(result.stdout)['body']
            payload = body['trx_data_imprt']
            self.assertIsInstance(payload, str)
            fields = json.loads(payload)
            self.assertEqual(fields['operate_type'], code)
            self.assertEqual(fields['order_id'], 'SYNTHETIC-ORDER')
            self.assertNotIn('keyId', fields)
            self.assertNotIn('sign', fields)
            self.assertLessEqual(len(payload), 2500)
            if operation == 'order':
                self.assertIsInstance(fields['total_price'], int)
                self.assertEqual(fields['total_price'], 100)
                self.assertEqual(fields['order_status'], '12')
            else:
                self.assertEqual(fields['state_type'], '3')
                self.assertEqual(fields['use_state'], '2' if operation == 'redeemed' else '3')
                self.assertEqual(fields['invalid_time' if operation == 'redeemed' else 'use_time'], '')

if __name__ == '__main__':
    unittest.main()
