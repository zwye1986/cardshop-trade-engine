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
            self.assertTrue(payload.startswith('operate_type:' + code + ','))
            self.assertNotIn('keyId:', payload)
            self.assertNotIn('sign:', payload)
            self.assertLessEqual(len(payload), 2500)

if __name__ == '__main__':
    unittest.main()
