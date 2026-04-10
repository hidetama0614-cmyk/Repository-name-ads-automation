"""
Yahoo!広告（LINEヤフー広告 検索広告）実績レポート取得スクリプト
・期間: 当月1日〜前日
・出力先①: Googleスプレッドシート「【添付】N月Yahoo!広告」タブ（キャンペーン実績）
・出力先②: Googleスプレッドシート「【添付】N月Yahoo!広告_CV内訳」タブ（コンバージョン名別）
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
import csv
import io
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
YAHOO_ADS_BASE_ACCOUNT_ID = "1001894160"                                        # ベースアカウントID（固定）

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

# ─── レポート①：キャンペーン実績（コンバージョン名なし）─────
# ※ CONVERSION_NAME はインプレッション・費用などと同じレポートに入れられないため別タブで取得
HEADER_CAMPAIGN = [
    "配信設定",
    "キャンペーン名",
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

FIELDS_CAMPAIGN = [
    "CAMPAIGN_DISTRIBUTION_SETTINGS",       # 配信設定
    "CAMPAIGN_NAME",                        # キャンペーン名
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

# ─── レポート②：コンバージョン名別内訳 ──────────────────────
# ※ CONVERSION_NAME を含む場合は、コンバージョン系の指標のみ指定可能
HEADER_CV = [
    "キャンペーン名",
    "コンバージョン名",
    "コンバージョン数",
    "コンバージョン率",
    "コンバージョン数（全て）",
]

FIELDS_CV = [
    "CAMPAIGN_NAME",    # キャンペーン名
    "CONVERSION_NAME",  # コンバージョン名
    "CONVERSIONS",      # コンバージョン数
    "CONV_RATE",        # コンバージョン率
    "ALL_CONV",         # コンバージョン数（全て）
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


def add_report_job(token, report_name, fields):
    """レポートジョブを登録してジョブIDを返す"""
    import json as _json
    headers = make_headers(token)
    operand = {
        "reportName":           report_name,
        "reportType":           "CAMPAIGN",
        "reportDateRangeType":  "THIS_MONTH",
        "fields":               fields,
        "reportDownloadFormat": "CSV",
    }
    body = {
        "accountId": YAHOO_ADS_ACCOUNT_ID,
        "operand":   [operand],
    }
    print(f"  送信リクエスト: {_json.dumps(body, ensure_ascii=False)[:300]}")
    res = requests.post(f"{API_BASE}/add", headers=headers, json=body)
    if not res.ok:
        print(f"[エラー] レポート登録失敗: {res.status_code}")
        print(f"  URL       : {API_BASE}/add")
        print(f"  レスポンス: {res.text}")
        sys.exit(1)

    data = res.json()
    print(f"  レスポンス全体: {data}")

    # v19のレスポンス構造から reportJobId を取得
    job_id = None
    try:
        values = data.get("rval", {}).get("values", [])
        if values:
            v = values[0]
            if v.get("reportDefinition", {}) and v["reportDefinition"].get("reportJobId"):
                job_id = v["reportDefinition"]["reportJobId"]
            elif v.get("reportJobId"):
                job_id = v["reportJobId"]
        if not job_id and data.get("rval", {}).get("reportJobId"):
            job_id = data["rval"]["reportJobId"]
    except Exception as e:
        print(f"[エラー] レスポンスのパース失敗: {e}")
        sys.exit(1)

    if not job_id:
        print(f"[エラー] reportJobIdが見つかりません")
        print(f"  レスポンス全体: {data}")
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
            row = next(csv.reader(io.StringIO(line)))
            rows.append(row)
    return rows


def write_to_spreadsheet(gc, sh, tab_name, header, rows):
    """指定タブにヘッダーとデータを書き込む"""
    try:
        ws = sh.worksheet(tab_name)
        ws.clear()
        print(f"  既存タブ「{tab_name}」をクリアして上書きします")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=500, cols=len(header) + 2)
        print(f"  新しいタブ「{tab_name}」を作成しました")

    ws.update("A1", [header] + rows)
    print(f"  {len(rows)}行のデータを書き込みました")


# ─── メイン処理 ──────────────────────────────────────────────
if __name__ == "__main__":
    check_env()

    print(f"Yahoo!広告レポート取得開始")
    print(f"アカウントID: {YAHOO_ADS_ACCOUNT_ID}")
    print(f"API: {API_BASE}")
    print()

    print("① アクセストークンを取得中...")
    token = get_access_token()
    print("  取得しました")

    # ── レポート① キャンペーン実績 ───────────────────────────
    print()
    print("② キャンペーンレポートを登録中...")
    job_id_campaign = add_report_job(
        token,
        f"キャンペーンレポート_{today.strftime('%Y%m')}",
        FIELDS_CAMPAIGN,
    )
    print(f"  ジョブID: {job_id_campaign}")

    print("③ キャンペーンレポート完成を待機中...")
    wait_for_completion(token, job_id_campaign)
    print("  完成しました")

    print("④ キャンペーンレポートをダウンロード中...")
    rows_campaign = download_report(token, job_id_campaign)
    print(f"  {len(rows_campaign)}件取得しました")

    # ── レポート② コンバージョン名別内訳 ────────────────────
    print()
    print("⑤ コンバージョン名別レポートを登録中...")
    job_id_cv = add_report_job(
        token,
        f"CV内訳レポート_{today.strftime('%Y%m')}",
        FIELDS_CV,
    )
    print(f"  ジョブID: {job_id_cv}")

    print("⑥ CV内訳レポート完成を待機中...")
    wait_for_completion(token, job_id_cv)
    print("  完成しました")

    print("⑦ CV内訳レポートをダウンロード中...")
    rows_cv = download_report(token, job_id_cv)
    print(f"  {len(rows_cv)}件取得しました")

    # ── スプレッドシート書き込み ──────────────────────────────
    print()
    print("⑧ スプレッドシートに書き込み中...")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds  = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    gc     = gspread.authorize(creds)
    sh     = gc.open_by_key(SPREADSHEET_ID)

    write_to_spreadsheet(gc, sh, f"【添付】{today.month}月Yahoo!広告", HEADER_CAMPAIGN, rows_campaign)
    write_to_spreadsheet(gc, sh, f"【添付】{today.month}月Yahoo!広告_CV内訳", HEADER_CV, rows_cv)

    print()
    print("完了しました！")
