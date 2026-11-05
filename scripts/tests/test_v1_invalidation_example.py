"""作废消费合同：充值12与券状态不足以确认完成，见详细设计9.3.6。"""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'v1-invalidation-example.py'


class InvalidationExampleTest(unittest.TestCase):
    def test_only_trusted_cancelled_coupon_with_time_is_completed(self):
        spec = importlib.util.spec_from_file_location('invalidation_example', SCRIPT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for data, expected in [
            ({'order_status': '12', 'state_type': '1', 'use_state': '1'}, 'pending'),
            ({'state_type': '3', 'use_state': '3', 'invalid_time': ''}, 'pending'),
            ({'order_status': '6', 'state_type': '3', 'use_state': '3', 'invalid_time': '2026-11-05 10:00:00'}, 'pending'),
            ({'order_status': '12', 'state_type': '3', 'use_state': '3', 'invalid_time': '2026-11-05 10:00:00', 'use_time': ''}, 'completed'),
        ]:
            self.assertEqual(module.classify({'code': '200', 'data': data}), expected)
        self.assertEqual(module.classify({'code': '202'}), 'pending')
        self.assertEqual(module.classify({'code': '503'}), 'not-accepted')
        self.assertEqual(module.classify({'code': '999'}), 'protocol-error')

    def test_cli_only_outputs_synthetic_offline_responses(self):
        for scenario in ('completed', 'pending', 'not-accepted'):
            result = subprocess.run([sys.executable, str(SCRIPT), '--scenario', scenario], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            parsed = json.loads(result.stdout)
            self.assertEqual(parsed['classification'], scenario)
            self.assertEqual(parsed['http_status'], 200)
            self.assertNotIn('token', result.stdout)


if __name__ == '__main__':
    unittest.main()
