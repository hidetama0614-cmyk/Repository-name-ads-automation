from dotenv import load_dotenv
import os
import gspread
from google.oauth2.service_account import Credentials
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from datetime import date

load_dotenv()

# Google Ads APIの設定
credentials = {
    "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
    "client_id": os.getenv("GOOGLE_ADS_CLIENT_ID"),
    "client_secret": os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
    "refresh_token": os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
    "use_proto_plus": True,
}

customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")

# Google Ads APIからキーワードデータを取得
print("Google広告のデータを取得中...")

client = GoogleAdsClient.load_from_dict(credentials)
service = client.get_service("GoogleAdsService")

query = """
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
    WHERE segments.date DURING LAST_30_DAYS
      AND campaign.status = 'ENABLED'
      AND ad_group.status = 'ENABLED'
      AND ad_group_criterion.status = 'ENABLED'
    ORDER BY metrics.clicks DESC
    LIMIT 50
"""

rows = []
try:
    response = service.search(customer_id=customer_id, query=query)
    for row in response:
        keyword = row.ad_group_criterion.keyword.text
        match_type = row.ad_group_criterion.keyword.match_type.name
        impressions = row.metrics.impressions
        clicks = row.metrics.clicks
        cost = round(row.metrics.cost_micros / 1_000_000)
        ctr = round(row.metrics.ctr * 100, 2)
        campaign_name = row.campaign.name
        ad_group_name = row.ad_group.name
        rows.append([campaign_name, ad_group_name, keyword, match_type, impressions, clicks, cost, ctr])
    print(f"{len(rows)}件のデータを取得しました")
except GoogleAdsException as ex:
    print(f"エラーが発生しました: {ex.error.code().name}")
    for error in ex.failure.errors:
        print(f"  詳細: {error.message}")
    exit()

# スプレッドシートに書き込む
print("スプレッドシートに書き込み中...")

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
gc = gspread.authorize(creds)

spreadsheet_id = "1u2hsFMpS0DMQZitEqET2imRPqTJbDUFMkZJyzcvuoJI"
sh = gc.open_by_key(spreadsheet_id)

today = date.today().strftime("%Y-%m-%d")
try:
    worksheet = sh.worksheet(today)
    worksheet.clear()
except gspread.exceptions.WorksheetNotFound:
    worksheet = sh.add_worksheet(title=today, rows=100, cols=10)

header = ["キャンペーン", "広告グループ", "キーワード", "マッチタイプ", "表示回数", "クリック数", "費用(円)", "CTR(%)"]
worksheet.update("A1", [header] + rows)

print(f"完了しました！シート名「{today}」に{len(rows)}件保存しました")
