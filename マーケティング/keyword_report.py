"""
keyword_report.py — キーワード・キャンペーン別実績の週次レポートを生成・通知するエントリーポイント

処理の流れ:
    1. Google Ads API からキーワード実績（費用・CV）を取得
    2. キャンペーン実績を取得
    3. スプレッドシートに保存
    4. 分析レポートを生成・追記
    5. Slack に整形済みメッセージを通知
"""

import os
import json
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import gspread
from google.oauth2.service_account import Credentials
from datetime import date
import analyze

load_dotenv()

with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

ads_credentials = {
    "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
    "client_id":       os.getenv("GOOGLE_ADS_CLIENT_ID"),
    "client_secret":   os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
    "refresh_token":   os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
    "use_proto_plus":  True,
}

customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID") or config["customer_id"]

# 常に合算するCVアクション
ALWAYS_INCLUDE_ACTIONS = {
    "2W無料_完了&アンケート",
    "3大CP_完了&アンケート",
    "op3大CP_完了&アンケート",
    "pre3大CP_完了&アンケート",
}
# 2つを比較して大きい方のみ合算するCVアクション（1回体験系）
TAIKEN_ACTIONS = {
    "1回体験_最初の受付完了",
    "1回体験_申込完了",
}
ALL_CV_ACTIONS = ALWAYS_INCLUDE_ACTIONS | TAIKEN_ACTIONS


def _calc_hon_cv(always: float, taiken: dict) -> int:
    """always合計 + taiken系の最大値 を本CVとして返す。"""
    return int(always + max(taiken.values(), default=0.0))


def fetch_keyword_hon_cv(client):
    """キーワード×CVアクション別にコンバージョンを取得し、本CVを集計した辞書を返す。
    キー: (campaign_name, ad_group_name, keyword_text, match_type)
    """
    service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
            campaign.name,
            ad_group.name,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            segments.conversion_action_name,
            metrics.conversions
        FROM keyword_view
        WHERE segments.date DURING {config["date_range"]}
          AND campaign.status = 'ENABLED'
          AND ad_group.status = 'ENABLED'
          AND ad_group_criterion.status = 'ENABLED'
    """
    raw = {}
    for row in service.search(customer_id=customer_id, query=query):
        action = row.segments.conversion_action_name
        if action not in ALL_CV_ACTIONS:
            continue
        key = (
            row.campaign.name,
            row.ad_group.name,
            row.ad_group_criterion.keyword.text,
            row.ad_group_criterion.keyword.match_type.name,
        )
        if key not in raw:
            raw[key] = {"always": 0.0, "taiken": {}}
        if action in ALWAYS_INCLUDE_ACTIONS:
            raw[key]["always"] += row.metrics.conversions
        else:
            raw[key]["taiken"][action] = raw[key]["taiken"].get(action, 0.0) + row.metrics.conversions

    return {key: _calc_hon_cv(v["always"], v["taiken"]) for key, v in raw.items()}


def fetch_campaign_hon_cv(client):
    """キャンペーン×CVアクション別にコンバージョンを取得し、本CVを集計した辞書を返す。
    キー: campaign_name
    """
    service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
            campaign.name,
            segments.conversion_action_name,
            metrics.conversions
        FROM campaign
        WHERE segments.date DURING {config["date_range"]}
    """
    raw = {}
    for row in service.search(customer_id=customer_id, query=query):
        action = row.segments.conversion_action_name
        if action not in ALL_CV_ACTIONS:
            continue
        key = row.campaign.name
        if key not in raw:
            raw[key] = {"always": 0.0, "taiken": {}}
        if action in ALWAYS_INCLUDE_ACTIONS:
            raw[key]["always"] += row.metrics.conversions
        else:
            raw[key]["taiken"][action] = raw[key]["taiken"].get(action, 0.0) + row.metrics.conversions

    return {key: _calc_hon_cv(v["always"], v["taiken"]) for key, v in raw.items()}


def fetch_keywords(client):
    service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
            campaign.name,
            ad_group.name,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.ctr
        FROM keyword_view
        WHERE segments.date DURING {config["date_range"]}
          AND campaign.status = 'ENABLED'
          AND ad_group.status = 'ENABLED'
          AND ad_group_criterion.status = 'ENABLED'
          AND metrics.cost_micros > 1000000
        ORDER BY campaign.name, ad_group.name, metrics.clicks DESC
    """
    rows = []
    for row in service.search(customer_id=customer_id, query=query):
        rows.append([
            row.campaign.name,
            row.ad_group.name,
            row.ad_group_criterion.keyword.text,
            row.ad_group_criterion.keyword.match_type.name,
            row.metrics.impressions,
            row.metrics.clicks,
            round(row.metrics.cost_micros / 1_000_000),
            round(row.metrics.ctr * 100, 2),
        ])
    return rows


def fetch_campaigns(client):
    service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
        FROM campaign
        WHERE segments.date DURING {config["date_range"]}
        ORDER BY metrics.clicks DESC
    """
    rows = []
    for row in service.search(customer_id=customer_id, query=query):
        rows.append([
            row.campaign.id,
            row.campaign.name,
            row.campaign.status.name,
            row.metrics.impressions,
            row.metrics.clicks,
            round(row.metrics.cost_micros / 1_000_000),
        ])
    return rows


def get_or_create_worksheet(sh, title, rows=200, cols=20):
    try:
        ws = sh.worksheet(title)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    return ws


def main():
    print("=== 【キーワード】週次レポート ===")

    client = GoogleAdsClient.load_from_dict(ads_credentials)

    # キーワードメトリクス取得
    print(f"キーワードデータを取得中（{config['date_range']}）...")
    try:
        keyword_rows = fetch_keywords(client)
        print(f"  → {len(keyword_rows)}件取得")
    except GoogleAdsException as ex:
        print(f"キーワード取得エラー: {ex.error.code().name}")
        for e in ex.failure.errors:
            print(f"  {e.message}")
        return

    # キーワード 本CV取得（別クエリで結合）
    print("キーワード 本CVを取得中...")
    try:
        kw_hon_cv = fetch_keyword_hon_cv(client)
        print(f"  → 本CVあり: {len(kw_hon_cv)}キーワード")
    except GoogleAdsException as ex:
        print(f"キーワード本CV取得エラー: {ex.error.code().name}")
        for e in ex.failure.errors:
            print(f"  {e.message}")
        return

    # キーワード行に本CVを結合
    for row in keyword_rows:
        key = (row[0], row[1], row[2], row[3])
        row.append(int(kw_hon_cv.get(key, 0)))

    # キャンペーンメトリクス取得
    print("キャンペーンデータを取得中...")
    try:
        campaign_rows = fetch_campaigns(client)
        print(f"  → {len(campaign_rows)}件取得")
    except GoogleAdsException as ex:
        print(f"キャンペーン取得エラー: {ex.error.code().name}")
        for e in ex.failure.errors:
            print(f"  {e.message}")
        return

    # キャンペーン 本CV取得（別クエリで結合）
    print("キャンペーン 本CVを取得中...")
    try:
        cp_hon_cv = fetch_campaign_hon_cv(client)
        print(f"  → 本CVあり: {len(cp_hon_cv)}キャンペーン")
    except GoogleAdsException as ex:
        print(f"キャンペーン本CV取得エラー: {ex.error.code().name}")
        for e in ex.failure.errors:
            print(f"  {e.message}")
        return

    # キャンペーン行に本CVを結合
    for row in campaign_rows:
        key = row[1]  # campaign_name
        row.append(int(cp_hon_cv.get(key, 0)))

    # スプレッドシート書き込み
    print("スプレッドシートに書き込み中...")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(config["spreadsheet_id"])

    today = date.today().strftime("%Y-%m-%d")

    ws_kw = get_or_create_worksheet(sh, today)
    kw_header = ["キャンペーン", "広告グループ", "キーワード", "マッチタイプ", "表示回数", "クリック数", "費用(円)", "CTR(%)", "本CV"]
    ws_kw.update([kw_header] + keyword_rows, "A1")
    print(f"  → キーワードシート「{today}」に{len(keyword_rows)}件保存")

    ws_cp = get_or_create_worksheet(sh, config["sheet"]["campaign_tab"])
    cp_header = ["キャンペーンID", "キャンペーン名", "ステータス", "表示回数", "クリック数", "費用(円)", "本CV"]
    ws_cp.update([cp_header] + campaign_rows, "A1")
    print(f"  → キャンペーンシート「{config['sheet']['campaign_tab']}」に{len(campaign_rows)}件保存")

    # 分析・レポート出力
    print("分析レポートを生成中...")
    analyze.run(keyword_rows, sh, config)

    print("\n完了しました！")
    print(f"  スプレッドシート: https://docs.google.com/spreadsheets/d/{config['spreadsheet_id']}")


if __name__ == "__main__":
    main()
