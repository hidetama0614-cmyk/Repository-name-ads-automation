from dotenv import load_dotenv
import os
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

load_dotenv()

credentials = {
    "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
    "client_id": os.getenv("GOOGLE_ADS_CLIENT_ID"),
    "client_secret": os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
    "refresh_token": os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
    "use_proto_plus": True,
}

customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")

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

try:
    response = service.search(customer_id=customer_id, query=query)
    print("=== キーワード実績（直近30日・上位50件）===")
    print(f"{'キーワード':<30} {'マッチ':<10} {'表示回数':>8} {'クリック':>8} {'費用(円)':>10} {'CTR':>8}")
    print("-" * 80)
    for row in response:
        keyword = row.ad_group_criterion.keyword.text
        match_type = row.ad_group_criterion.keyword.match_type.name
        impressions = row.metrics.impressions
        clicks = row.metrics.clicks
        cost = row.metrics.cost_micros / 1_000_000
        ctr = row.metrics.ctr * 100
        print(f"{keyword:<30} {match_type:<10} {impressions:>8} {clicks:>8} {cost:>10.0f} {ctr:>7.2f}%")
except GoogleAdsException as ex:
    print(f"エラーが発生しました: {ex.error.code().name}")
    for error in ex.failure.errors:
        print(f"  詳細: {error.message}")
