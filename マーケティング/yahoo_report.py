"""
Yahoo!広告（LINEヤフー広告 検索広告）実績レポート取得スクリプト
・期間: 当月1日〜前日
・出力先: Googleスプレッドシート「【添付】N月Yahoo!広告」タブ
・毎日 GitHub Actions で自動実行

必須の環境変数（GitHub Secrets）:
  YAHOO_ADS_CLIENT_ID     : LINEヤフー広告アプリのクライアントID
  YAHOO_ADS_CLIENT_SECRET : LINEヤフー広告アプリのクライアントシークレット
  YAHOO_ADS_REFRESH_TOKEN : リフレッシュトークン
  YAHOO_ADS_ACCOUNT_ID    : 検索広告アカウントID（例: 1003214）
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
YAHOO_ADS_CLIENT_ID       = os.getenv("YAHOO_ADS_CLIENT_ID")
YAHOO_ADS_CLIENT_SECRET   = os.getenv("YAHOO_ADS_CLIENT_SECRET")
YAHOO_ADS_REFRESH_TOKEN   = os.getenv("YAHOO_ADS_REFRESH_TOKEN")
YAHOO_ADS_ACCOUNT_ID      = int(os.getenv("YAHOO_ADS_ACCOUNT_ID", "1003214"))  # キャンペーンアカウントID
YAHOO_ADS_BASE_ACCOUNT_ID = "1001894160"                                           # ベースアカウントID（固定）

SPREADSHEET_ID = "1u1wH7WiCjYoN0p4IFNPXfYsr_h5bnAxEb0tdBgTEx-8"

# ─── エンドポイント（公式 v19）────────────────────────────────
OAUTH_TOKEN_URL = "https://biz-oauth.yahoo.co.jp/oauth/v1/token"
API_BASE        = "https://ads-search.yahooapis.jp/api/v19/ReportDefinitionService"

# ─── 必須環境変数チェック ─────────────────────────────────────
def check_env():
    required = {
        "YAHOO_ADS_CLIENT_ID":     YAHOO_ADS_CLIENT_ID,
        "YAHOO_ADS_CLIENT_SECRET": YAHOO_ADS_CLIENT_SECRET,
        "YAHOO_ADS_REFRESH_TOKEN": YAHOO_ADS_REFRESH_TOKEN,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print("[エラー] 以下のGitHub Secretsが設定されていません：")
        for k in missing:
            print(f"  - {k}")
        sys.exit(1)

# ─── 日付範囲（当月1日〜前日）────────────────────────────────
today = date.today()

if today.day == 1:
    print("本日は月初のため、当月データがありません。スキップします。")
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

# ─── Yahoo広告 APIフィールド（HEADERと同じ順番）────────────
# 出典: https://yahoojp-marketing.github.io/ads-search-api-documents/reports/v19/CAMPAIGN.csv
REPORT_FIELDS = [
    "CAMPAIGN_DISTRIBUTION_SETTINGS",       # 配信設定
    "CAMPAIGN_NAME",                        # キャンペーン名
    "CONVERSION_NAME",                      # コンバージョン名
    "CAMPAIGN_DISTRIBUTION_STATUS",         # 配信状況
    "BID_STRATEGY_STATUS",                  # 入札戦略の状況
    "CAMPAIGN_TYPE",                        # キャンペーンタイプ
    "BID_STRATEGY_TYPE",                    # 入札戦略
    "DAILY_SPENDING_LIMIT",                 # 1日の予算
    "IMPS",                                 # インプレッション数
    "CLICKS",                               # クリック数
    "CLICK_RATE",                           # クリック率
    "COST",                                 # コスト
    "AVG_CPC",                              # 平均CPC
    "CONVERSIONS",                          # コンバージョン数
    "CONV_RATE",                            # コンバージョン率
    "LABELS",                               # ラベル
    "TOP_IMPRESSION_PERCENTAGE",            # ページ上部の割合
    "ABSOLUTE_TOP_IMPRESSION_PERCENTAGE",   # ページ最上部の割合
    "ALL_CONV",                             # コンバージョン数（全て）
    "SEARCH_TOP_IMPRESSION_SHARE",          # ページ上部インプレッションシェア
    "SEARCH_ABSOLUTE_TOP_IMPRESSION_SHARE", # ページ最上部インプレッションシェア
]


def get_access_token():
    """リフレッシュトークンでアクセストークンを取得"""
    res = requests.post(OAUTH_TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "client_id":     YAHOO_ADS_CLIENT_ID,
        "client_secret": YAHOO_ADS_CLIENT_SECRET,
        "refresh_token": YAHOO_ADS_REFRESH_TOKEN,
    })
    if not res.ok:
        print(f"[エラー] トークン取得失敗: {res.status_code} {res.text}")
        sys.exit(1)
    return res.json()["access_token"]


def make_headers(token):
    """全APIリクエストに共通のヘッダーを返す"""
    return {
        "Authorization":       f"Bearer {token}",
        "Content-Type":        "application/json",
        "x-z-base-account-id": YAHOO_ADS_BASE_ACCOUNT_ID,
    }


def add_report_job(token):
    """レポートジョブを登録してジョブIDを返す"""
    headers = make_headers(token)
    # THIS_MONTH = 当月1日〜本日（日次9時実行のため当日データはほぼゼロ）
    operand = {
        "reportName":          f"キャンペーンレポート_{today.strftime('%Y%m')}",
        "reportType":          "CAMPAIGN",
        "reportDateRangeType": "THIS_MONTH",
        "fields":              REPORT_FIELDS,
        "reportDownloadFormat": "CSV",
    }
    body = {
        "accountId": YAHOO_ADS_ACCOUNT_ID,
        "operand":   [operand],
    }
    import json as _json
    print(f"  送信リクエスト: {_json.dumps(body, ensure_ascii=False)[:300]}")
    res = requests.post(f"{API_BASE}/add", headers=headers, json=body)
    if not res.ok:
        print(f"[エラー] レポート登録失敗: {res.status_code}")
        print(f"  URL       : {API_BASE}/add")
        print(f"  レスポンス: {res.text}")
        sys.exit(1)

    data = res.json()
    # レスポンス構造: {"rval": {"values": [{"reportDefinition": {"reportJobId": ...}}]}}
    try:
        job_id = data["rval"]["values"][0]["reportDefinition"]["reportJobId"]
    except (KeyError, IndexError) as e:
        print(f"[エラー] レポートジョブIDの取得失敗: {e}")
        print(f"  レスポンス: {data}")
        sys.exit(1)
    return job_id


def wait_for_completion(token, job_id):
    """レポートが完成するまで最大20回（約10分）待機"""
    headers = make_headers(token)
    body = {
        "accountId": YAHOO_ADS_ACCOUNT_ID,
        "selector": {
            "reportJobIds":  [job_id],
            "startIndex":    1,
            "numberResults": 1,
        }
    }
    for attempt in range(20):
        res = requests.post(f"{API_BASE}/get", headers=headers, json=body)
        if not res.ok:
            print(f"[エラー] ステータス確認失敗: {res.status_code} {res.text}")
            sys.exit(1)

        data   = res.json()
        status = data["rval"]["values"][0]["reportDefinition"].get("reportJobStatus", "UNKNOWN")
        print(f"  レポート状況: {status}（{attempt + 1}回目）")

        if status == "COMPLETED":
            return
        elif status == "FAILED":
            print(f"[エラー] レポート生成失敗: {data}")
            sys.exit(1)

        time.sleep(30)

    print("[エラー] タイムアウト（10分以上かかっています）")
    sys.exit(1)


def download_report(token, job_id):
    """完成したレポートをダウンロードして行データのリストを返す"""
    headers = make_headers(token)
    body    = {"accountId": YAHOO_ADS_ACCOUNT_ID, "reportJobId": job_id}
    res     = requests.post(f"{API_BASE}/download", headers=headers, json=body)

    if not res.ok:
        print(f"[エラー] ダウンロード失敗: {res.status_code} {res.text}")
        sys.exit(1)

    # CSV形式: 1行目はヘッダーなのでスキップ
    lines = res.text.strip().split("\n")
    rows  = []
    for line in lines[1:]:
        if line.strip():
            # CSVをパース（カンマ区切り・ダブルクォート対応）
            import csv
            import io
            row = next(csv.reader(io.StringIO(line)))
            rows.append(row)
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
    print(f"API: {API_BASE}")
    print()

    print("① アクセストークンを取得中...")
    token = get_access_token()
    print("  取得しました")

    print("② レポートジョブを登録中...")
    job_id = add_report_job(token)
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
