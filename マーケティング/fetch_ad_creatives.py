"""
fetch_ad_creatives.py — Google検索広告の広告アセット（見出し・説明文）実績を取得するモジュール
"""

from google.ads.googleads.errors import GoogleAdsException


def fetch_ad_asset_performance(client, customer_id: str, config: dict) -> list[dict]:
    """広告アセット（見出し・説明文）の実績データを取得する。

    取得条件:
        - 検索キャンペーン（SEARCH）のみ
        - 種別: 見出し（HEADLINE）または説明文（DESCRIPTION）
        - 期間: config["date_range"]（デフォルト: LAST_30_DAYS）
        - 費用: 1円以上

    戻り値:
        list[dict] — 1件 = 1アセットの実績
    """
    service = client.get_service("GoogleAdsService")
    date_range = config.get("date_range", "LAST_30_DAYS")

    query = f"""
        SELECT
            campaign.name,
            ad_group.name,
            ad_group_ad.ad.id,
            asset.text_asset.text,
            ad_group_ad_asset_view.field_type,
            ad_group_ad_asset_view.performance_label,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.ctr,
            metrics.conversions
        FROM ad_group_ad_asset_view
        WHERE segments.date DURING {date_range}
          AND campaign.advertising_channel_type = 'SEARCH'
          AND ad_group_ad_asset_view.field_type IN ('HEADLINE', 'DESCRIPTION')
          AND ad_group_ad.status = 'ENABLED'
          AND campaign.status = 'ENABLED'
          AND ad_group.status = 'ENABLED'
          AND metrics.impressions > 0
        ORDER BY metrics.cost_micros DESC
    """

    rows = []
    try:
        for row in service.search(customer_id=customer_id, query=query):
            cost = round(row.metrics.cost_micros / 1_000_000)
            if cost < 1:
                continue
            rows.append({
                "campaign":          row.campaign.name,
                "ad_group":          row.ad_group.name,
                "ad_id":             row.ad_group_ad.ad.id,
                "text":              row.asset.text_asset.text,
                "field_type":        row.ad_group_ad_asset_view.field_type.name,
                "performance_label": row.ad_group_ad_asset_view.performance_label.name,
                "impressions":       int(row.metrics.impressions),
                "clicks":            int(row.metrics.clicks),
                "cost":              cost,
                "ctr":               round(row.metrics.ctr * 100, 2),
                "conversions":       round(row.metrics.conversions, 1),
            })
    except GoogleAdsException as ex:
        print(f"広告アセット取得エラー: {ex.error.code().name}")
        for e in ex.failure.errors:
            print(f"  {e.message}")

    return rows
