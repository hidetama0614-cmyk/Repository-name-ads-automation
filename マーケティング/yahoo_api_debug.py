"""
Yahoo!広告APIエンドポイント調査スクリプト（トークン交換・JWT詳細調査版）
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

YAHOO_TOKEN_URL = "https://biz-oauth.yahoo.co.jp/oauth/v1/token"


def get_yahoo_token():
    res = requests.post(YAHOO_TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    })
    res.raise_for_status()
    data = res.json()
    print(f"  Yahoo OAuthレスポンスキー: {list(data.keys())}")
    return data


def make_jwt(client_id, client_secret, aud):
    """HS256 JWT を生成"""
    def b64url(data):
        return base64.urlsafe_b64encode(
            json.dumps(data, separators=(",", ":")).encode()
        ).rstrip(b"=").decode()

    header  = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iss": client_id,
        "sub": client_id,
        "aud": aud,
        "iat": int(time.time()),
        "exp": int(time.time()) + 3600,
    }
    signing_input = f"{b64url(header)}.{b64url(payload)}"
    sig = hmac.new(
        client_secret.encode(), signing_input.encode(), hashlib.sha256
    ).digest()
    return f"{signing_input}.{base64.urlsafe_b64encode(sig).rstrip(b'=').decode()}"


def post(url, headers, body):
    try:
        res = requests.post(url, headers=headers, data=body, timeout=10)
        return res.status_code, res.text[:400]
    except Exception as e:
        return "ERR", str(e)[:100]


def post_json(url, headers, body):
    try:
        res = requests.post(url, headers=headers, json=body, timeout=10)
        return res.status_code, res.text[:400]
    except Exception as e:
        return "ERR", str(e)[:100]


def get_req(url, token):
    try:
        res = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        return res.status_code, res.text[:400]
    except Exception as e:
        return "ERR", str(e)[:100]


if __name__ == "__main__":
    print("Yahoo OAuthトークンを取得中...")
    yahoo_data  = get_yahoo_token()
    yahoo_token = yahoo_data.get("access_token", "")
    print(f"  取得完了\n")

    # ── テスト①：ads.line.me/api/v1/token にYahoo tokenを渡す ──
    print("=" * 70)
    print("【テスト①】ads.line.me/api/v1/token にYahoo OAuthトークンを渡す")
    print("=" * 70)

    url = "https://ads.line.me/api/v1/token"

    # パターンA: Authorizationヘッダー + grant_type
    grant_types = [
        "client_credentials",
        "urn:ietf:params:oauth:grant-type:token-exchange",
    ]
    for gt in grant_types:
        status, body = post(url,
            headers={"Authorization": f"Bearer {yahoo_token}", "Content-Type": "application/x-www-form-urlencoded"},
            body={"grant_type": gt, "scope": "yahooads"}
        )
        print(f"  grant_type={gt}")
        print(f"  → {status}: {body[:150]}\n")

    # パターンB: subject_token形式（RFC 8693 Token Exchange）
    status, body = post(url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        body={
            "grant_type":          "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token":       yahoo_token,
            "subject_token_type":  "urn:ietf:params:oauth:token-type:access_token",
        }
    )
    print(f"  Token Exchange (subject_token)")
    print(f"  → {status}: {body[:150]}\n")

    # パターンC: client_credentials（認証ヘッダーなし・body内に認証情報）
    status, body = post(url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        body={
            "grant_type":    "client_credentials",
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
    )
    print(f"  client_credentials (body auth)")
    print(f"  → {status}: {body[:150]}\n")

    print()

    # ── テスト②：異なるaud値でJWTを変える ─────────────────────
    print("=" * 70)
    print("【テスト②】JWTのaud値を変えて試す")
    print("=" * 70)

    aud_values = [
        "https://ads.line.me",
        "https://ads.line.me/",
        "https://ads.line.me/api",
        "ads.line.me",
        "LINE_ADS",
        CLIENT_ID,
    ]
    for aud in aud_values:
        jwt = make_jwt(CLIENT_ID, CLIENT_SECRET, aud)
        status, body = post_json(
            "https://ads.line.me/api/v13/reports",
            headers={"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"},
            body={"reportName": "test", "reportType": "CAMPAIGN",
                  "dateRangeType": "YESTERDAY", "fields": ["CAMPAIGN_NAME"]}
        )
        print(f"  aud={aud}")
        print(f"  → {status}: {body[:150]}\n")

    print()

    # ── テスト③：GETでユーザー情報・アカウント一覧を取得 ───────
    print("=" * 70)
    print("【テスト③】アカウント情報の取得を試みる（Bearer / JWT両方）")
    print("=" * 70)

    endpoints = [
        "https://ads.line.me/api/v13/accounts",
        "https://ads.line.me/api/v13/me",
        "https://ads.line.me/api/v1/me",
        "https://ads.yahoo.co.jp/api/v13/me",
        "https://biz-oauth.yahoo.co.jp/oauth/v1/userinfo",
    ]
    for url in endpoints:
        status, body = get_req(url, yahoo_token)
        mark = "✓" if str(status) not in ["404", "ERR"] else "✗"
        print(f"  {mark} {status}  {url}")
        if str(status) not in ["404", "ERR"]:
            print(f"       → {body[:200]}")

    print("\n完了。")
