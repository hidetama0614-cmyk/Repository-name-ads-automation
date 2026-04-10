"""
Yahoo!広告APIエンドポイント調査スクリプト（パス構造拡張版）
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

REPORT_BODY = {
    "reportName": "test",
    "reportType": "CAMPAIGN",
    "dateRangeType": "YESTERDAY",
    "fields": ["CAMPAIGN_NAME", "IMPRESSIONS"],
    "format": "TSV",
}


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
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    try:
        fn  = requests.post if method == "POST" else requests.get
        res = fn(url, headers=headers, json=body, timeout=10)
        return res.status_code, res.text[:300]
    except Exception as e:
        return "ERR", str(e)[:100]


def show(label, status, body):
    is_ok = str(status) not in ["404", "ERR", "400"]
    mark  = "✓" if is_ok else "✗"
    # 400も詳細を表示（パスは存在するがリクエストが悪い場合）
    show_body = str(status) not in ["404", "ERR"]
    print(f"  {mark} {status}  {label}")
    if show_body:
        print(f"       → {body}")


if __name__ == "__main__":
    print("アクセストークンを取得中...")
    token = get_access_token()
    print(f"取得しました\n")
    print(f"アカウントID: {ACCOUNT_ID}\n")

    VERSIONS = range(6, 16)

    # ── テスト①：/api/ なしのパス ────────────────────────────
    print("=" * 70)
    print("【テスト①】ads.yahoo.co.jp — /api/ なしのパス")
    print("=" * 70)
    base = "https://ads.yahoo.co.jp"
    for v in VERSIONS:
        paths = [
            f"/v{v}/{ACCOUNT_ID}/reports",
            f"/v{v}/reports",
            f"/search/v{v}/{ACCOUNT_ID}/reports",
            f"/search/v{v}/reports",
            f"/management/v{v}/{ACCOUNT_ID}/reports",
            f"/management/v{v}/reports",
            f"/api/search/v{v}/{ACCOUNT_ID}/reports",
        ]
        for p in paths:
            status, body = probe(token, base + p, method="POST", body=REPORT_BODY)
            show(base + p, status, body)

    print()

    # ── テスト②：サブドメインを変える ────────────────────────
    print("=" * 70)
    print("【テスト②】サブドメイン違い")
    print("=" * 70)
    subdomains = [
        "api.ads.yahoo.co.jp",
        "ads-api.yahoo.co.jp",
        "search-ads.yahoo.co.jp",
        "api.line.me",
        "ads.line.me",
    ]
    for domain in subdomains:
        for v in VERSIONS:
            for path in [f"/api/v{v}/{ACCOUNT_ID}/reports", f"/api/v{v}/reports", f"/v{v}/reports"]:
                url = f"https://{domain}{path}"
                status, body = probe(token, url, method="POST", body=REPORT_BODY)
                show(url, status, body)

    print()

    # ── テスト③：ss.yahooapis.jp（旧API系） ──────────────────
    print("=" * 70)
    print("【テスト③】ss.yahooapis.jp 系")
    print("=" * 70)
    for domain in ["ss.yahooapis.jp", "ads-search.yahooapis.jp", "ads.yahooapis.jp"]:
        for v in VERSIONS:
            for path in [
                f"/services/v{v}/ReportDefinitionService",
                f"/api/v{v}/{ACCOUNT_ID}/reports",
                f"/v{v}/{ACCOUNT_ID}/reports",
            ]:
                url = f"https://{domain}{path}"
                status, body = probe(token, url, method="POST", body=REPORT_BODY)
                show(url, status, body)

    print()

    # ── テスト④：GETで各ドメインのルート・ヘルスチェック ─────
    print("=" * 70)
    print("【テスト④】ドメイン生死確認（GET /）")
    print("=" * 70)
    check_domains = [
        "ads.yahoo.co.jp", "api.ads.yahoo.co.jp",
        "ss.yahooapis.jp", "ads-search.yahooapis.jp", "ads.yahooapis.jp",
        "api.line.me", "ads.line.me",
    ]
    for domain in check_domains:
        status, body = probe(token, f"https://{domain}/", method="GET")
        show(f"https://{domain}/", status, body)

    print("\n完了。「✓」または400（パスは存在するがリクエスト内容が問題）の行に注目してください。")
