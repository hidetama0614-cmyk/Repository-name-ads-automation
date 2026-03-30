"""
fetch_pmax_assets.py — P-MAX広告のアセット（見出し・ロング見出し・説明文）実績を取得するモジュール
"""

from google.ads.googleads.errors import GoogleAdsException


def fetch_pmax_asset_performance(client, customer_id: str, config: dict) -> list[dict]:
    """P-MAXアセット（見出し・ロング見出し・説明文）の実績データを取得する。

    取得条件:
        - P-MAXキャンペーン（PERFORMANCE_MAX）のみ
        - 種別: 見出し（HEADLINE）・ロング見出し（LONG_HEADLINE）・説明文（DESCRIPTION）
        - 期間: config["date_range"]（デフォルト: LAST_30_DAYS）
        - ステータス: 有効なもののみ

    戻り値:
        list[dict] — 1件 = 1アセットの実績
    """
    service = client.get_service("GoogleAdsService")
    date_range = config.get("date_range", "LAST_30_DAYS")

    query = f"""
        SELECT
            campaign.name,
            asset_group.name,
            asset.text_asset.text,
            asset_group_asset.field_type,
            asset_group_asset.performance_label,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions
        FROM asset_group_asset
        WHERE segments.date DURING {date_range}
          AND campaign.advertising_channel_type = 'PERFORMANCE_MAX'
          AND asset_group_asset.field_type IN ('HEADLINE', 'LONG_HEADLINE', 'DESCRIPTION')
          AND asset_group_asset.status = 'ENABLED'
          AND campaign.status = 'ENABLED'
          AND asset_group.status = 'ENABLED'
        ORDER BY metrics.cost_micros DESC
    """

    rows = []
    try:
        for row in service.search(customer_id=customer_id, query=query):
            impressions = int(row.metrics.impressions)
            clicks      = int(row.metrics.clicks)
            cost        = round(row.metrics.cost_micros / 1_000_000)
            ctr         = round(clicks / impressions * 100, 2) if impressions > 0 else 0.0

            rows.append({
                "campaign":          row.campaign.name,
                "asset_group":       row.asset_group.name,
                "text":              row.asset.text_asset.text,
                "field_type":        row.asset_group_asset.field_type.name,
                "performance_label": row.asset_group_asset.performance_label.name,
                "impressions":       impressions,
                "clicks":            clicks,
                "cost":              cost,
                "ctr":               ctr,
                "conversions":       round(row.metrics.conversions, 1),
            })
    except GoogleAdsException as ex:
        print(f"P-MAXアセット取得エラー: {ex.error.code().name}")
        for e in ex.failure.errors:
            print(f"  {e.message}")

    return rows
