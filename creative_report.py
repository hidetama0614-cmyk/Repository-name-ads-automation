"""
creative_report.py — 広告クリエイティブの週次レポートを生成・通知するエントリーポイント

処理の流れ:
    1. Google Ads API から広告アセット実績を取得
    2. 生データをスプレッドシートに保存
    3. creative-analyst エージェントのプロンプトで Claude API を呼び出し → 分析
    4. 分析結果をスプレッドシートに追記 ＋ Slack に通知
"""

import os
import re
import json
import requests
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from google.ads.googleads.client import GoogleAdsClient
import anthropic

from fetch_ad_creatives import fetch_ad_asset_performance

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


# ---------------------------------------------------------------------------
# 1. エージェントプロンプトの読み込み
# ---------------------------------------------------------------------------

def _load_agent_prompt(agent_name: str) -> str:
    """claude/agents/{agent_name}.md からシステムプロンプトを読み込む。
    frontmatter (--- ... ---) は除去して本文のみ返す。
    """
    path = Path(__file__).parent / "claude" / "agents" / f"{agent_name}.md"
    content = path.read_text(encoding="utf-8")
    content = re.sub(r"^---.*?---\s*", "", content, flags=re.DOTALL)
    return content.strip()


# ---------------------------------------------------------------------------
# 2. Claude への入力データ整形
# ---------------------------------------------------------------------------

PERFORMANCE_LABEL_MAP = {
    "BEST":     "◎ BEST",
    "GOOD":     "○ GOOD",
    "LOW":      "× LOW",
    "LEARNING": "学習中",
    "UNKNOWN":  "-",
}


def _format_for_claude(rows: list[dict]) -> str:
    """アセット実績データを Claude に渡す Markdown テーブルに整形する。"""
    lines = [
        "以下はGoogle検索広告の広告アセット（見出し・説明文）の直近30日間の実績データです。分析をお願いします。\n",
        "| 種別 | テキスト | パフォーマンスラベル | 表示回数 | クリック数 | 費用(円) | CTR(%) | CV数 | キャンペーン | 広告グループ |",
        "| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | :--- | :--- |",
    ]
    for r in rows:
        label = PERFORMANCE_LABEL_MAP.get(r["performance_label"], r["performance_label"])
        lines.append(
            f"| {r['field_type']} | {r['text']} | {label} "
            f"| {r['impressions']:,} | {r['clicks']:,} | {r['cost']:,} "
            f"| {r['ctr']} | {r['conversions']} | {r['campaign']} | {r['ad_group']} |"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 3. Claude API 呼び出し
# ---------------------------------------------------------------------------

def analyze_with_claude(rows: list[dict]) -> str:
    """creative-analyst エージェントを使って Claude に分析させる。"""
    system_prompt = _load_agent_prompt("creative-analyst")
    user_message  = _format_for_claude(rows)

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    )
    return message.content[0].text


# ---------------------------------------------------------------------------
# 4. スプレッドシート書き込み
# ---------------------------------------------------------------------------

def _get_or_create_worksheet(sh, title: str, rows: int = 1000, cols: int = 15):
    try:
        ws = sh.worksheet(title)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    return ws


def write_raw_sheet(sh, rows: list[dict], config: dict):
    """生データをスプレッドシートの専用タブに上書き保存する。"""
    tab_name = config.get("sheet", {}).get("creative_tab", "クリエイティブ実績")
    ws = _get_or_create_worksheet(sh, tab_name)

    header = [
        "種別", "テキスト", "パフォーマンスラベル",
        "表示回数", "クリック数", "費用(円)", "CTR(%)", "CV数",
        "広告ID", "キャンペーン", "広告グループ",
    ]
    data = [header] + [
        [
            r["field_type"], r["text"], r["performance_label"],
            r["impressions"], r["clicks"], r["cost"], r["ctr"],
            r["conversions"], r["ad_id"], r["campaign"], r["ad_group"],
        ]
        for r in rows
    ]
    ws.update(data, "A1")
    print(f"  → 生データ {len(rows)}件 → シート「{tab_name}」に保存")


def write_analysis_sheet(sh, analysis_text: str, config: dict):
    """Claude の分析テキストをスプレッドシートの専用タブに日付付きで追記する。"""
    tab_name = config.get("sheet", {}).get("creative_analysis_tab", "クリエイティブ分析")
    today = date.today().strftime("%Y-%m-%d")

    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=2000, cols=3)
        ws.append_row(["日付", "分析結果"])
        ws.freeze(rows=1)

    ws.append_row([today, analysis_text])
    print(f"  → 分析結果 → シート「{tab_name}」に追記")


# ---------------------------------------------------------------------------
# 5. Slack 通知
# ---------------------------------------------------------------------------

def notify_slack(analysis_text: str, config: dict):
    """分析結果を Slack に通知する。"""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL") or config.get("slack_webhook_url", "")
    if not webhook_url:
        return

    today = date.today().strftime("%Y-%m-%d")
    payload = {
        "text": f"*📝 クリエイティブ週次レポート（{today}）*\n\n{analysis_text}"
    }
    requests.post(webhook_url, json=payload, timeout=10)
    print("  → Slack に通知完了")


# ---------------------------------------------------------------------------
# 6. エントリーポイント
# ---------------------------------------------------------------------------

def main():
    print("=== クリエイティブ週次レポート ===")

    # Google Ads クライアント初期化
    ads_client = GoogleAdsClient.load_from_dict(ads_credentials)

    # アセット実績データ取得
    print(f"広告アセットデータを取得中（{config['date_range']}）...")
    rows = fetch_ad_asset_performance(ads_client, customer_id, config)
    print(f"  → {len(rows)}件取得")

    if not rows:
        print("対象データがありません。処理を終了します。")
        return

    # スプレッドシート接続
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(config["spreadsheet_id"])

    # 生データをシートに保存
    write_raw_sheet(sh, rows, config)

    # Claude で分析
    print("Claude（creative-analyst）で分析中...")
    analysis_text = analyze_with_claude(rows)
    print("  → 分析完了")

    # 分析結果をシートに追記
    write_analysis_sheet(sh, analysis_text, config)

    # Slack 通知
    notify_slack(analysis_text, config)

    print("\n完了しました！")
    print(f"  スプレッドシート: https://docs.google.com/spreadsheets/d/{config['spreadsheet_id']}")


if __name__ == "__main__":
    main()
