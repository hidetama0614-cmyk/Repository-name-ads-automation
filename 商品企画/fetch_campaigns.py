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
        campaign.id,
        campaign.name,
        campaign.status
    FROM campaign
    ORDER BY campaign.name
"""

try:
    response = service.search(customer_id=customer_id, query=query)
    print("=== キャンペーン一覧 ===")
    for row in response:
        campaign = row.campaign
        print(f"ID: {campaign.id}  名前: {campaign.name}  状態: {campaign.status.name}")
except GoogleAdsException as ex:
    print(f"エラーが発生しました: {ex.error.code().name}")
    for error in ex.failure.errors:
        print(f"  詳細: {error.message}")
