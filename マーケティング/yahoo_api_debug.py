"""
Yahoo!広告APIエンドポイント調査スクリプト（JWT・search配下パス集中調査版）
"""

import os
import time
import json
import base64
import hashlib
import hmac
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv("YAHOO_ADS_CLIENT_ID")
CLIENT_SECRET = os.getenv("YAHOO_ADS_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("YAHOO_ADS_REFRESH_TOKEN")
ACCOUNT_ID    = os.getenv("YAHOO_ADS_ACCOUNT_ID", "1003214")

TOKEN_URL = "https://biz-oauth.yahoo.co.jp/oauth/v1/token"


def get_access_token():
    res = requests.post(TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    })
    res.raise_for_status()
    return res.json()["access_token"]


def make_jwt(client_id, client_secret):
    """
    LINEヤフー広告スタイルのJWT assertion を生成する
    （HS256: client_secretで署名）
    """
    header  = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iss": client_id,
        "sub": client_id,
        "aud": "https://ads.line.me",
        "iat": int(time.time()),
        "exp": int(time.time()) + 3600,
    }

    def b64url(data):
        return base64.urlsafe_b64encode(
            json.dumps(data, separators=(",", ":")).encode()
        ).rstrip(b"=").decode()

    header_b64  = b64url(header)
    payload_b64 = b64url(payload)
    signing_input = f"{header_b64}.{payload_b64}"

    sig = hmac.new(
        client_secret.encode(),
        signing_input.encode(),
        hashlib.sha256
    ).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=").decode()

    return f"{signing_input}.{sig_b64}"


def probe(url, token, method="GET", body=None, auth_type="bearer"):
    if auth_type == "bearer":
        auth_header = f"Bearer {token}"
    else:
        auth_header = f"Bearer {token}"  # JWT用も一旦Bearerで送る

    headers = {
        "Authorization": auth_header,
        "Content-Type":  "application/json",
    }
    try:
        fn  = requests.post if method == "POST" else requests.get
        res = fn(url, headers=headers, json=body, timeout=10)
        return res.status_code, res.text[:400]
    except Exception as e:
        return "ERR", str(e)[:100]


def show(label, status, body):
    is_notable = str(status) not in ["404", "ERR"]
    mark = "✓" if is_notable else "✗"
    print(f"  {mark} {status}  {label}")
    if is_notable:
        print(f"       → {body[:200]}")


REPORT_BODY = {
    "reportName": "test",
    "reportType": "CAMPAIGN",
    "dateRangeType": "YESTERDAY",
    "fields": ["CAMPAIGN_NAME", "IMPRESSIONS"],
    "format": "TSV",
}

if __name__ == "__main__":
    print("アクセストークンを取得中...")
    bearer_token = get_access_token()
    print("取得しました\n")

    # ── テスト①：ads.yahoo.co.jp/search/ 配下のAPIパス ────────
    print("=" * 70)
    print("【テスト①】ads.yahoo.co.jp/search/ 配下")
    print("=" * 70)
    base = "https://ads.yahoo.co.jp"
    for v in [13, 14, 15, 12, 11, 10]:
        paths = [
            f"/search/api/v{v}/{ACCOUNT_ID}/reports",
            f"/search/api/v{v}/reports",
            f"/search/api/v{v}/accounts/{ACCOUNT_ID}/reports",
            f"/search/v{v}/api/{ACCOUNT_ID}/reports",
            f"/search/v{v}/api/reports",
            f"/search/api/reports",
        ]
        for p in paths:
            status, body = probe(base + p, bearer_token, method="POST", body=REPORT_BODY)
            show(base + p, status, body)

    print()

    # ── テスト②：ads.line.me にJWT形式で送る ─────────────────
    print("=" * 70)
    print("【テスト②】ads.line.me — JWT assertionを生成して送る")
    print("=" * 70)

    jwt_token = make_jwt(CLIENT_ID, CLIENT_SECRET)
    print(f"  生成したJWT（先頭50文字）: {jwt_token[:50]}...")
    print(f"  JWT のピリオド数: {jwt_token.count('.')}（3つあればOK）")
    print()

    for v in [13, 14, 15, 12, 11, 10]:
        for path in [
            f"/api/v{v}/{ACCOUNT_ID}/reports",
            f"/api/v{v}/reports",
            f"/api/v{v}/accounts/{ACCOUNT_ID}/reports",
        ]:
            url = f"https://ads.line.me{path}"
            status, body = probe(url, jwt_token, method="POST", body=REPORT_BODY)
            show(url, status, body)

    print()

    # ── テスト③：ads.line.me のトークン取得エンドポイント ─────
    print("=" * 70)
    print("【テスト③】ads.line.me トークン取得を試みる")
    print("=" * 70)
    token_endpoints = [
        "https://ads.line.me/oauth/v1/token",
        "https://ads.line.me/api/v1/token",
        "https://api.line.me/oauth2/v2.1/token",
    ]
    for url in token_endpoints:
        try:
            res = requests.post(url, data={
                "grant_type":            "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "client_assertion":      jwt_token,
                "scope":                 "yahooads",
            }, timeout=10)
            print(f"  {res.status_code}  {url}")
            print(f"       → {res.text[:200]}")
        except Exception as e:
            print(f"  ERR  {url} → {e}")

    print()

    # ── テスト④：アクセストークンのデコードで送り先を確認 ─────
    print("=" * 70)
    print("【テスト④】取得済みアクセストークンの中身を確認")
    print("=" * 70)
    parts = bearer_token.split(".")
    print(f"  ピリオド数: {len(parts) - 1}")
    if len(parts) == 3:
        try:
            # JWTならデコードしてペイロードを見る
            padded = parts[1] + "=" * (4 - len(parts[1]) % 4)
            payload_str = base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")
            print(f"  JWTペイロード: {payload_str[:300]}")
        except Exception as e:
            print(f"  デコード失敗: {e}")
    else:
        print(f"  JWTではない形式（opaque token）")
        print(f"  トークン先頭: {bearer_token[:30]}...")

    print("\n完了。")
