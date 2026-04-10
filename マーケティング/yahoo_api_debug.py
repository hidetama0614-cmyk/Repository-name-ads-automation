"""
Yahoo!広告APIのエンドポイントを調べるデバッグスクリプト（ドメイン拡張版）
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


def probe(token, url, method="GET", body=None, extra_headers=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    try:
        if method == "POST":
            res = requests.post(url, headers=headers, json=body, timeout=10)
        else:
            res = requests.get(url, headers=headers, timeout=10)
        return res.status_code, res.text[:300]
    except Exception as e:
        return "ERR", str(e)[:100]


def print_result(label, status, body):
    mark = "✓" if str(status) not in ["404", "ERR"] else "✗"
    print(f"  {mark} {status}  {label}")
    if str(status) not in ["404", "ERR"]:
        print(f"       → {body}")


REPORT_BODY = {
    "reportName": "test",
    "reportType": "CAMPAIGN",
    "dateRangeType": "YESTERDAY",
    "fields": ["CAMPAIGN_NAME", "IMPRESSIONS"],
    "format": "TSV",
}

# 試すドメイン一覧
DOMAINS = [
    "ads.yahoo.co.jp",
    "ads-search.yahooapis.jp",
    "ads.yahooapis.jp",
    "ss.yahooapis.jp",
    "lyads.yahooapis.jp",
    "ads-display.yahoo.co.jp",
    "businessmanager.yahoo.co.jp",
]

VERSIONS = range(6, 16)

if __name__ == "__main__":
    print("アクセストークンを取得中...")
    token = get_access_token()
    print("取得しました\n")

    # ── テスト①：ドメイン × バージョン × アカウントIDあり ─────
    print("=" * 70)
    print("【テスト①】各ドメイン /{accountId}/reports")
    print("=" * 70)
    for domain in DOMAINS:
        for v in VERSIONS:
            url = f"https://{domain}/api/v{v}/{ACCOUNT_ID}/reports"
            status, body = probe(token, url, method="POST", body=REPORT_BODY)
            print_result(url, status, body)

    print()

    # ── テスト②：ドメイン × バージョン × アカウントIDなし ─────
    print("=" * 70)
    print("【テスト②】各ドメイン /reports（アカウントIDなし）")
    print("=" * 70)
    for domain in DOMAINS:
        for v in VERSIONS:
            url = f"https://{domain}/api/v{v}/reports"
            status, body = probe(token, url, method="POST", body=REPORT_BODY)
            print_result(url, status, body)

    print()

    # ── テスト③：旧SOAP系サービス名パス ──────────────────────
    print("=" * 70)
    print("【テスト③】サービス名パス")
    print("=" * 70)
    services = ["ReportDefinitionService", "ReportService", "StatsService"]
    for domain in ["ads.yahoo.co.jp", "ss.yahooapis.jp", "ads-search.yahooapis.jp"]:
        for v in VERSIONS:
            for svc in services:
                url = f"https://{domain}/api/v{v}/services/{svc}"
                status, body = probe(token, url, method="GET")
                print_result(url, status, body)

    print()

    # ── テスト④：GETでルートを叩いてみる ─────────────────────
    print("=" * 70)
    print("【テスト④】ルートURL確認（何かが返るか）")
    print("=" * 70)
    for domain in DOMAINS:
        url = f"https://{domain}/"
        status, body = probe(token, url, method="GET")
        print_result(f"{domain}/", status, body)

    print("\n完了。「✓」がついた行が正しいエンドポイントです。")
