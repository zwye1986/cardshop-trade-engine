"""Task19示例通过真实回环HTTP核对状态、重投和脱敏；参见详细设计9.8。"""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'miniso-writeoff-example.py'
SPEC = importlib.util.spec_from_file_location('example', SCRIPT)
EXAMPLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXAMPLE)


class ProtocolExampleTest(unittest.TestCase):
    def run_scenario(self, scenario, responses):
        captured = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                captured.append((self.path, json.loads(body), dict(self.headers)))
                status, code = responses[len(captured) - 1]
                self.send_response(status)
                self.end_headers()
                self.wfile.write(json.dumps({'code': code, 'msg': 'PRIVATE-RESPONSE'}).encode())

            def log_message(self, *args):
                pass

        with HTTPServer(('127.0.0.1', 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                result = subprocess.run([sys.executable, str(SCRIPT), '--url',
                                         f'http://127.0.0.1:{server.server_port}/api/common/mcNotify',
                                         '--scenario', scenario], capture_output=True, text=True,
                                        env=dict(os.environ, MINISO_CALLBACK_KEY2='LOCAL-PRIVATE-KEY'), timeout=10)
            finally:
                server.shutdown()
                thread.join()
        output = result.stdout + result.stderr
        for secret in ('LOCAL-PRIVATE-KEY', 'PRIVATE-RESPONSE', 'TEST-MINISO-0001', 'couponCode', 'Traceback'):
            self.assertNotIn(secret, output)
        return result, captured

    def test_same_time_replay_resigns_same_fact_at_only_legacy_path(self):
        result, captured = self.run_scenario('same-time', [(200, 200), (200, 200)])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(captured), 2)
        self.assertEqual(captured[0][1], captured[1][1])
        self.assertEqual({row[0] for row in captured}, {'/api/common/mcNotify'})
        self.assertNotEqual(captured[0][2]['Nonce'], captured[1][2]['Nonce'])
        self.assertIn('accepted_only', result.stdout)
        wrong, _ = self.run_scenario('same-time', [(200, 'PRIVATE-RESPONSE')])
        self.assertEqual(wrong.returncode, 1)

    def test_other_time_conflict_requires_numeric_business_500(self):
        result, captured = self.run_scenario('conflict', [(200, 200), (200, 500)])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(captured), 2)
        self.assertEqual(captured[0][1]['couponCode'], captured[1][1]['couponCode'])
        self.assertNotEqual(captured[0][1]['useTime'], captured[1][1]['useTime'])
        for _, body, headers in captured:
            _, signed = EXAMPLE.notice('LOCAL-PRIVATE-KEY', headers['Nonce'], headers['Time'], body)
            self.assertEqual(signed['sign'], headers['Sign'])
        wrong, _ = self.run_scenario('conflict', [(200, 200), (200, 200)])
        self.assertEqual(wrong.returncode, 1)

    def test_pause_requires_http_503_and_does_not_fall_back(self):
        result, captured = self.run_scenario('paused', [(503, 503)])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(captured), 1)
        self.assertIn('503', result.stdout)
        wrong, _ = self.run_scenario('paused', [(200, 200)])
        self.assertEqual(wrong.returncode, 1)

    def test_offline_vectors_include_stable_event_and_changed_time_signature(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            EXAMPLE.self_test()
        self.assertIn('same-time', output.getvalue())
        self.assertIn('conflict', output.getvalue())
        self.assertIn('event-id', output.getvalue())

    def test_remote_or_wrong_path_and_missing_key_are_rejected_before_network(self):
        for url in ('https://example.invalid/api/common/mcNotify', 'http://127.0.0.1/2104',
                    'http://user@localhost/api/common/mcNotify', 'http://localhost/api/common/mcNotify?key=x'):
            result = subprocess.run([sys.executable, str(SCRIPT), '--url', url], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
        with patch.dict(os.environ, {}, clear=True):
            result = subprocess.run([sys.executable, str(SCRIPT), '--url', 'http://127.0.0.1/api/common/mcNotify'],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)


if __name__ == '__main__':
    unittest.main()
