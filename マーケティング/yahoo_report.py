"""
Yahoo!広告（LINEヤフー広告 検索広告）実績レポート取得スクリプト
・期間: 当月1日〜前日
・出力先: Googleスプレッドシート「【添付】N月Yahoo!広告」タブ
・毎日 GitHub Actions で自動実行

必須の環境変数（GitHub Secrets）:
  YAHOO_ADS_CLIENT_ID     : LINEヤフー広告アプリのクライアントID
  YAHOO_ADS_CLIENT_SECRET : LINEヤフー広告アプリのクライアントシークレット
  YAHOO_ADS_REFRESH_TOKEN : yahoo_auth.py で取得したリフレッシュトークン
"""

import os
import sys
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

# ─── 設定 ─────────────────────────────────────────────────────
YAHOO_ADS_CLIENT_ID     = os.getenv("YAHOO_ADS_CLIENT_ID")
YAHOO_ADS_CLIENT_SECRET = os.getenv("YAHOO_ADS_CLIENT_SECRET")
YAHOO_ADS_REFRESH_TOKEN = os.getenv("YAHOO_ADS_REFRESH_TOKEN")
YAHOO_ADS_ACCOUNT_ID    = os.getenv("YAHOO_ADS_ACCOUNT_ID", "1003214")  # 検索広告アカウントID

SPREADSHEET_ID = "1u1wH7WiCjYoN0p4IFNPXfYsr_h5bnAxEb0tdBgTEx-8"

# ─── エンドポイント（LINEヤフー広告 公式URL）───────────────────
OAUTH_BASE      = "https://biz-oauth.yahoo.co.jp/oauth"
TOKEN_URL       = f"{OAUTH_BASE}/v1/token"
YAHOO_API_BASE  = f"https://ads.yahoo.co.jp/api/v13/{YAHOO_ADS_ACCOUNT_ID}"

# ─── 必須環境変数チェック ─────────────────────────────────────
def check_env():
    required = {
        "YAHOO_ADS_CLIENT_ID":     YAHOO_ADS_CLIENT_ID,
        "YAHOO_ADS_CLIENT_SECRET": YAHOO_ADS_CLIENT_SECRET,
        "YAHOO_ADS_REFRESH_TOKEN": YAHOO_ADS_REFRESH_TOKEN,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print("[エラー] 以下の環境変数（GitHub Secrets）が設定されていません：")
        for k in missing:
            print(f"  - {k}")
        print()
        print("  GitHub → Settings → Secrets and variables → Actions で登録してください")
        sys.exit(1)

# ─── 日付範囲（当月1日〜前日）────────────────────────────────
today = date.today()

if today.day == 1:
    print("本日は月初のため、当月データがありません。処理をスキップします。")
    sys.exit(0)

start_date = today.replace(day=1).strftime("%Y%m%d")
end_date   = (today - timedelta(days=1)).strftime("%Y%m%d")

# ─── スプレッドシートのヘッダー行 ────────────────────────────
HEADER = [
    "配信設定",
    "キャンペーン名",
    "コンバージョン名",
    "配信状況",
    "入札戦略の状況",
    "キャンペーンタイプ",
    "入札戦略",
    "1日の予算",
    "インプレッション数",
    "クリック数",
    "クリック率",
    "コスト",
    "平均CPC",
    "コンバージョン数",
    "コンバージョン率",
    "ラベル",
    "インプレッションにおけるページ上部の割合",
    "インプレッションにおけるページ最上部の割合",
    "コンバージョン数（全て）",
    "ページ上部のインプレッションシェア",
    "ページ最上部のインプレッションシェア",
]

# ─── Yahoo広告APIのレポートフィールド（HEADERと同じ順番）────
REPORT_FIELDS = [
    "CAMPAIGN_DISTRIBUTION_SETTINGS",           # 配信設定
    "CAMPAIGN_NAME",                            # キャンペーン名
    "CONVERSION_NAME",                          # コンバージョン名
    "CAMPAIGN_STATUS",                          # 配信状況
    "BID_STRATEGY_STATUS",                      # 入札戦略の状況
    "CAMPAIGN_TYPE",                            # キャンペーンタイプ
    "BID_STRATEGY_TYPE",                        # 入札戦略
    "DAILY_BUDGET",                             # 1日の予算
    "IMPRESSIONS",                              # インプレッション数
    "CLICKS",                                   # クリック数
    "CTR",                                      # クリック率
    "COST",                                     # コスト
    "AVG_CPC",                                  # 平均CPC
    "CONVERSIONS",                              # コンバージョン数
    "CONV_RATE",                                # コンバージョン率
    "LABELS",                                   # ラベル
    "SEARCH_TOP_IMPRESSION_RATE",               # ページ上部の割合
    "SEARCH_ABSOLUTE_TOP_IMPRESSION_RATE",      # ページ最上部の割合
    "ALL_CONVERSIONS",                          # コンバージョン数（全て）
    "SEARCH_TOP_IMPRESSION_SHARE",              # ページ上部インプレッションシェア
    "SEARCH_ABSOLUTE_TOP_IMPRESSION_SHARE",     # ページ最上部インプレッションシェア
]


def get_access_token():
    """
    refresh_token でアクセストークンを取得
    grant_type: refresh_token
    """
    print(f"  URL        : {TOKEN_URL}")
    print(f"  grant_type : refresh_token")

    res = requests.post(TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "client_id":     YAHOO_ADS_CLIENT_ID,
        "client_secret": YAHOO_ADS_CLIENT_SECRET,
        "refresh_token": YAHOO_ADS_REFRESH_TOKEN,
    })

    if not res.ok:
        print(f"\n[エラー] アクセストークンの取得に失敗しました")
        print(f"  HTTP status : {res.status_code}")
        print(f"  URL         : {TOKEN_URL}")
        print(f"  grant_type  : refresh_token")
        print(f"  レスポンス  : {res.text}")
        sys.exit(1)

    return res.json()["access_token"]


def create_report_job(token):
    """レポートジョブを作成し、ジョブIDを返す"""
    url = f"{YAHOO_API_BASE}/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }
    body = {
        "reportName":             f"キャンペーンレポート_{start_date}_{end_date}",
        "reportType":             "CAMPAIGN",
        "dateRangeType":          "CUSTOM_DATE",
        "dateRange":              {"startDate": start_date, "endDate": end_date},
        "fields":                 REPORT_FIELDS,
        "sortFields":             [{"field": "CAMPAIGN_NAME", "sortOrder": "ASCENDING"}],
        "format":                 "TSV",
        "encode":                 "UTF-8",
        "includeZeroImpressions": True,
    }

    res = requests.post(url, headers=headers, json=body)

    if not res.ok:
        print(f"\n[エラー] レポートジョブの作成に失敗しました")
        print(f"  HTTP status  : {res.status_code}")
        print(f"  URL          : {url}")
        print(f"  アカウントID : {YAHOO_ADS_ACCOUNT_ID}")
        print(f"  レスポンス   : {res.text}")
        sys.exit(1)

    return res.json()["reportJobId"]


def wait_for_completion(token, job_id):
    """レポートが完成するまで最大20回（約10分）待機"""
    url     = f"{YAHOO_API_BASE}/reports/{job_id}"
    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(20):
        res = requests.get(url, headers=headers)
        if not res.ok:
            print(f"[エラー] レポート状態の確認に失敗: {res.status_code} {res.text}")
            sys.exit(1)

        data   = res.json()
        status = data.get("reportJobStatus", "UNKNOWN")
        print(f"  レポート状況: {status}（{attempt + 1}回目）")

        if status == "COMPLETED":
            return
        elif status == "FAILED":
            print(f"\n[エラー] レポート生成が失敗しました")
            print(f"  レスポンス: {data}")
            sys.exit(1)

        time.sleep(30)

    print("[エラー] レポート生成がタイムアウトしました（10分以上かかっています）")
    sys.exit(1)


def download_report(token, job_id):
    """完成したレポートをダウンロードし、行データのリストを返す"""
    url     = f"{YAHOO_API_BASE}/reports/{job_id}/download"
    headers = {"Authorization": f"Bearer {token}"}
    res     = requests.get(url, headers=headers)

    if not res.ok:
        print(f"\n[エラー] レポートのダウンロードに失敗しました")
        print(f"  HTTP status : {res.status_code}")
        print(f"  URL         : {url}")
        print(f"  レスポンス  : {res.text}")
        sys.exit(1)

    # TSV形式: 1行目がヘッダーなのでスキップ
    lines = res.text.strip().split("\n")
    rows  = [line.split("\t") for line in lines[1:] if line.strip()]
    return rows


def write_to_spreadsheet(rows):
    """当月タブにヘッダーとデータを書き込む"""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds  = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    gc     = gspread.authorize(creds)
    sh     = gc.open_by_key(SPREADSHEET_ID)

    tab_name = f"【添付】{today.month}月Yahoo!広告"

    try:
        ws = sh.worksheet(tab_name)
        ws.clear()
        print(f"  既存タブ「{tab_name}」をクリアして上書きします")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=500, cols=len(HEADER) + 2)
        print(f"  新しいタブ「{tab_name}」を作成しました")

    ws.update("A1", [HEADER] + rows)
    print(f"  {len(rows)}行のデータを書き込みました")


# ─── メイン処理 ──────────────────────────────────────────────
if __name__ == "__main__":
    check_env()

    print(f"Yahoo!広告レポート取得開始")
    print(f"期間: {start_date} 〜 {end_date}")
    print(f"アカウントID: {YAHOO_ADS_ACCOUNT_ID}")
    print()

    print("① アクセストークンを取得中...")
    token = get_access_token()
    print("  取得しました")

    print("② レポートジョブを作成中...")
    job_id = create_report_job(token)
    print(f"  ジョブID: {job_id}")

    print("③ レポート完成を待機中...")
    wait_for_completion(token, job_id)
    print("  完成しました")

    print("④ レポートをダウンロード中...")
    rows = download_report(token, job_id)
    print(f"  {len(rows)}件取得しました")

    print("⑤ スプレッドシートに書き込み中...")
    write_to_spreadsheet(rows)

    print()
    print("完了しました！")
