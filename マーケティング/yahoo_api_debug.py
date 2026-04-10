"""
Yahoo!広告APIのエンドポイントを調べるデバッグスクリプト
どのURLが正しいかを自動で確認します。
"""

import os
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


def probe(token, url, method="GET", body=None):
    """URLにリクエストを送り、ステータスコードとレスポンスを返す"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    try:
        if method == "POST":
            res = requests.post(url, headers=headers, json=body, timeout=10)
        else:
            res = requests.get(url, headers=headers, timeout=10)
        return res.status_code, res.text[:200]
    except Exception as e:
        return "ERR", str(e)


if __name__ == "__main__":
    print("アクセストークンを取得中...")
    token = get_access_token()
    print("取得しました\n")

    # ── テスト①：APIバージョンを変えて試す ──────────────────
    print("=" * 60)
    print("【テスト①】APIバージョン別 /reports エンドポイント")
    print("=" * 60)

    report_body = {
        "reportName": "test",
        "reportType": "CAMPAIGN",
        "dateRangeType": "YESTERDAY",
        "fields": ["CAMPAIGN_NAME", "IMPRESSIONS"],
        "format": "TSV",
    }

    for version in range(6, 16):
        url = f"https://ads.yahoo.co.jp/api/v{version}/{ACCOUNT_ID}/reports"
        status, body = probe(token, url, method="POST", body=report_body)
        mark = "✓" if str(status) not in ["404", "ERR"] else "✗"
        print(f"  {mark} v{version:2d}  {status}  {url}")
        if str(status) not in ["404", "ERR"]:
            print(f"       レスポンス: {body}")

    print()

    # ── テスト②：アカウントIDなしのURL ──────────────────────
    print("=" * 60)
    print("【テスト②】アカウントIDなしのURL")
    print("=" * 60)

    for version in range(6, 16):
        url = f"https://ads.yahoo.co.jp/api/v{version}/reports"
        status, body = probe(token, url, method="POST", body=report_body)
        mark = "✓" if str(status) not in ["404", "ERR"] else "✗"
        print(f"  {mark} v{version:2d}  {status}  {url}")
        if str(status) not in ["404", "ERR"]:
            print(f"       レスポンス: {body}")

    print()

    # ── テスト③：ヘッダーにアカウントIDを入れる形式 ──────────
    print("=" * 60)
    print("【テスト③】X-LY-AdsAccountId ヘッダー形式")
    print("=" * 60)

    for version in range(6, 16):
        url = f"https://ads.yahoo.co.jp/api/v{version}/reports"
        try:
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "X-LY-AdsAccountId": ACCOUNT_ID,
            }
            res = requests.post(url, headers=headers, json=report_body, timeout=10)
            status = res.status_code
            body = res.text[:200]
        except Exception as e:
            status, body = "ERR", str(e)
        mark = "✓" if str(status) not in ["404", "ERR"] else "✗"
        print(f"  {mark} v{version:2d}  {status}  {url}")
        if str(status) not in ["404", "ERR"]:
            print(f"       レスポンス: {body}")

    print()
    print("完了しました。「✓」がついた行が正しいエンドポイントです。")
