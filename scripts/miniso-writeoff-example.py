#!/usr/bin/env python3
"""名创入站和V1.2核销/作废通知的可执行协议示例；参见详细设计9.5。
默认仅离线自检。发送仅允许显式本机URL和环境变量key2，不打印真实密钥或报文。
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

FIXTURE_KEY2 = "miniso-callback-local-test-key"
FIXTURE_NONCE = "0123456789abcdef0123456789abcdef"
FIXTURE_TIME = "1792200600000"
FIXTURE_BODY = {"couponCode": "TEST-MINISO-0001", "useTime": "2026-10-17 09:29:00"}
CALLBACK_FIELDS = {
    "order_id", "out_order_sn", "timestamp", "nonce_str", "account", "use_time",
    "state_type", "use_state", "invalid_time", "keyId", "sign",
}


def notice(key2, nonce, timestamp, body):
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    source = key2 + "nonce" + nonce + "tenantMINISOtime" + timestamp + "versiontripartite1.0" + canonical
    sign = hashlib.md5(source.encode("utf-8")).hexdigest().upper()
    return canonical.encode("utf-8"), {"Content-Type": "application/json", "nonce": nonce, "time": timestamp, "sign": sign}


def verify_callback(payload, key_id, encoded_key):
    if set(payload) != CALLBACK_FIELDS or payload["keyId"] != key_id:
        return False
    if len(encoded_key) != 43:
        return False
    key = base64.urlsafe_b64decode(encoded_key + "=")
    if len(key) != 32 or base64.urlsafe_b64encode(key).decode().rstrip("=") != encoded_key:
        return False
    if any(not isinstance(value, str) for value in payload.values()):
        return False
    source = ",".join(field + "=" + payload[field] for field in sorted(CALLBACK_FIELDS - {"sign"}))
    expected = hmac.new(key, source.encode("utf-8"), "sm3").hexdigest()
    return hmac.compare_digest(expected, payload["sign"])


def self_test():
    _, headers = notice(FIXTURE_KEY2, FIXTURE_NONCE, FIXTURE_TIME, FIXTURE_BODY)
    assert headers["sign"] == "62088F92CF0CB4452F9A95098E60E350"
    # 文档V1.2通用协议验签向量：2/2属于其他直充分类，不作为名创业务状态示例。
    # 空invalid_time与keyId都必须参与签名，原固定向量保持不变。
    payload = {
        "order_id": "2024051410460831111495", "out_order_sn": "2024022120200772347024",
        "timestamp": "1715592615713", "nonce_str": "00000000000000000000000000000006",
        "account": "13423456789", "use_time": "2025-11-01 10:05:30", "state_type": "2",
        "use_state": "2", "invalid_time": "", "keyId": "demo-key-20261015-001",
        "sign": "f70181a433e0acb9dfce122729f106f1b0d6db6139e811ec23c69e3877cdcb9a",
    }
    assert verify_callback(payload, "demo-key-20261015-001", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
    # 名创已核销固定向量：state_type=3/use_state=2，不复用其他直充分类的state_type=2。
    miniso_payload = dict(payload, state_type="3",
                          sign="5cd69af0b03123180ea6b49b50fd055c02c84e899fce0d3cbc853ddae89df72c")
    assert verify_callback(miniso_payload, "demo-key-20261015-001", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
    # 名创作废完成固定向量：state_type=3/use_state=3，use_time留空，invalid_time非空。
    # 核销/作废均为券状态通知，不增加order_status；发券成功后的订单状态仍保留12。
    invalid_payload = dict(miniso_payload, use_time="", use_state="3", invalid_time="2025-11-01 10:06:30",
                           sign="27e954d989258511fabe57ea6441ae8e0fb7650767cf755f03d4d1311007bc68")
    assert verify_callback(invalid_payload, "demo-key-20261015-001", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
    # 原文重投保持签名不变，文档向量及两种名创状态都应再次通过验签。
    for original in (payload, miniso_payload, invalid_payload):
        replay = json.loads(json.dumps(original))
        assert verify_callback(replay, "demo-key-20261015-001", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
    invalid_payload["invalid_time"] = "2025-11-01 10:06:31"
    assert not verify_callback(invalid_payload, "demo-key-20261015-001", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
    miniso_payload["use_time"] = "2025-11-01 10:05:31"
    assert not verify_callback(miniso_payload, "demo-key-20261015-001", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
    payload["use_time"] = "2025-11-01 10:05:31"
    assert not verify_callback(payload, "demo-key-20261015-001", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
    print("PASS: legacy key2 MD5, generic V1.2 2/2 vector, MINISO 3/2 redemption and 3/3 invalidation, replay and tamper rejection")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--url", help="仅显式发送本机dev/test的/api/common/mcNotify")
    group.add_argument("--verify-callback", type=Path, help="验证已保存的商户回调JSON，不输出报文")
    parser.add_argument("--key-id", help="本商户可信配置中的预期keyId")
    args = parser.parse_args()
    if args.verify_callback:
        key = os.environ.get("V12_SIGN_KEY", "")
        if not args.key_id or not key:
            parser.error("验证需要--key-id和V12_SIGN_KEY环境变量")
        valid = verify_callback(json.loads(args.verify_callback.read_text(encoding="utf-8")), args.key_id, key)
        print("PASS" if valid else "FAIL")
        return 0 if valid else 1
    if not args.url:
        self_test()
        return 0
    target = urllib.parse.urlsplit(args.url)
    if (target.scheme not in {"http", "https"} or target.hostname not in {"localhost", "127.0.0.1", "::1"}
            or target.username or target.password or target.query or target.fragment or target.path != "/api/common/mcNotify"):
        parser.error("只允许本机固定核销路径，不允许鉴权URL、query或fragment")
    key2 = os.environ.get("MINISO_CALLBACK_KEY2", "")
    if not key2:
        parser.error("发送需要显式配置MINISO_CALLBACK_KEY2，不回退示例密钥")
    body, headers = notice(key2, secrets.token_hex(16), str(int(time.time() * 1000)), FIXTURE_BODY)
    # 禁止重定向把带签名的请求转发到其它地址；默认代理也不参与本机测试。
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, hdrs, newurl):
            return None
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    request = urllib.request.Request(args.url, data=body, headers=headers, method="POST")
    try:
        with opener.open(request, timeout=10) as response:
            payload = json.loads(response.read(4096))
            code = payload.get("code")
            print("HTTP", response.status, "business_code", code)
            return 0 if response.status == 200 and type(code) is int and code == 200 else 1
    except (urllib.error.URLError, ValueError):
        print("FAIL: callback request rejected or unavailable")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
