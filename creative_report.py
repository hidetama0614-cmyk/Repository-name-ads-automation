"""
creative_report.py — 広告クリエイティブの週次レポートを生成・通知するエントリーポイント

処理の流れ:
    1. Google Ads API から広告アセット実績を取得
    2. 生データをスプレッドシートに保存
    3. creative-analyst エージェントのプロンプトで Claude API を呼び出し → 分析（JSON）
    4. 分析結果を専用スプレッドシートに行単位で追記
    5. Slack に整形済みメッセージを通知
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

IMPORTANCE_ICON = {"高": "🔴", "中": "🟡", "低": "🟢"}
DIVIDER = "━━━━━━━━━━━━━━━━━━━━━"


# ---------------------------------------------------------------------------
# 1. エージェントプロンプトの読み込み
# ---------------------------------------------------------------------------

def _load_agent_prompt(agent_name: str) -> str:
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
# 3. Claude API 呼び出し → JSON パース
# ---------------------------------------------------------------------------

def analyze_with_claude(rows: list[dict]) -> dict:
    """creative-analyst エージェントで分析し、JSONとして返す。"""
    system_prompt = _load_agent_prompt("creative-analyst")
    user_message  = _format_for_claude(rows)

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    )
    raw_text = message.content[0].text

    # JSON パース（コードブロック記法を除去して再試行）
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        cleaned = re.sub(r"```(?:json)?\s*|\s*```", "", raw_text).strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return {"_raw": raw_text, "stop": [], "winning": [], "new_ads": []}


# ---------------------------------------------------------------------------
# 4. Slack メッセージ整形
# ---------------------------------------------------------------------------

def _format_slack_message(analysis: dict, today: str) -> str:
    lines = [f"*📊 クリエイティブ週次レポート（{today}）*"]

    # 今週の結論
    conclusion = analysis.get("conclusion", "")
    if conclusion:
        lines.append("")
        lines.append(f"*【今週の結論】*\n{conclusion}")

    # 停止推奨（重要度順にソート）
    stop_items = analysis.get("stop", [])
    if stop_items:
        priority_order = {"高": 0, "中": 1, "低": 2}
        stop_items = sorted(stop_items, key=lambda x: priority_order.get(x.get("importance", "低"), 2))
        lines.append("")
        lines.append(DIVIDER)
        lines.append(f"*🚨 停止・修正すべきクリエイティブ（{len(stop_items)}件）*")
        lines.append(DIVIDER)
        for i, item in enumerate(stop_items, 1):
            icon = IMPORTANCE_ICON.get(item.get("importance", "中"), "🟡")
            kind = "見出し" if item.get("field_type") == "HEADLINE" else "説明文"
            lines.append(f"{icon} *{i}. {kind}：「{item['text']}」*")
            lines.append(f"📁 {item.get('campaign', '-')} ＞ {item.get('ad_group', '-')}")
            lines.append(f"課題：{item.get('issue', '-')}")
            lines.append(f"▶ 次にやること：{item.get('next_action', '-')}")
            lines.append("")

    # 勝ちパターン
    winning_items = analysis.get("winning", [])
    if winning_items:
        lines.append(DIVIDER)
        lines.append(f"*✅ 勝ちパターン（{len(winning_items)}件）*")
        lines.append(DIVIDER)
        for item in winning_items:
            kind = "見出し" if item.get("field_type") == "HEADLINE" else "説明文"
            lines.append(f"*{kind}：「{item['text']}」*（{item.get('appeal_axis', '-')}）")
            lines.append(f"📁 {item.get('campaign', '-')} ＞ {item.get('ad_group', '-')}")
            lines.append(f"→ {item.get('reason', '-')}")
            lines.append(f"▶ 次にやること：{item.get('next_action', '-')}")
            lines.append("")

    # 新規広告案
    new_ads = analysis.get("new_ads", [])
    if new_ads:
        lines.append(DIVIDER)
        lines.append(f"*💡 今週追加する新規広告案（{len(new_ads)}件）*")
        lines.append(DIVIDER)
        for i, item in enumerate(new_ads, 1):
            kind = "見出し" if item.get("type") == "HEADLINE" else "説明文"
            lines.append(f"*{i}. 【{kind}】「{item.get('text', '-')}」*（{item.get('appeal_axis', '-')}）")
            lines.append(f"📁 追加先：{item.get('target_campaign', '-')} ＞ {item.get('target_ad_group', '-')}")
            lines.append(f"理由：{item.get('reason', '-')}")
            lines.append("")

    # フォールバック（JSON解析失敗時）
    if "_raw" in analysis:
        lines.append(analysis["_raw"])

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 5. スプレッドシート書き込み
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
        [r["field_type"], r["text"], r["performance_label"],
         r["impressions"], r["clicks"], r["cost"], r["ctr"],
         r["conversions"], r["ad_id"], r["campaign"], r["ad_group"]]
        for r in rows
    ]
    ws.update(data, "A1")
    print(f"  → 生データ {len(rows)}件 → シート「{tab_name}」に保存")


def write_analysis_sheet(sh, analysis: dict, config: dict):
    """Claude の分析結果（JSON）をサマリーシートに日付付きで追記する。"""
    tab_name = config.get("sheet", {}).get("creative_analysis_tab", "クリエイティブ分析")
    today = date.today().strftime("%Y-%m-%d")

    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=2000, cols=3)
        ws.append_row(["日付", "分析結果（JSON）"])
        ws.freeze(rows=1)

    ws.append_row([today, json.dumps(analysis, ensure_ascii=False)])
    print(f"  → 分析結果 → シート「{tab_name}」に追記")


def write_detail_spreadsheet(gc, analysis: dict, config: dict):
    """分析結果を行単位で専用スプレッドシートに追記する。"""
    log_spreadsheet_id = config.get("creative_log_spreadsheet_id", "")
    if not log_spreadsheet_id:
        print("  → creative_log_spreadsheet_id 未設定のためスキップ")
        return

    sh = gc.open_by_key(log_spreadsheet_id)
    today = date.today().strftime("%Y-%m-%d")
    tab_name = "クリエイティブ分析ログ"

    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=5000, cols=8)
        ws.append_row(["日付", "分析区分", "重要度", "種別", "テキスト", "課題/訴求軸/理由", "次にやること", "結論"])
        ws.freeze(rows=1)

    conclusion = analysis.get("conclusion", "")
    rows_to_append = []

    for item in analysis.get("stop", []):
        rows_to_append.append([
            today, "停止推奨",
            item.get("importance", "-"),
            item.get("field_type", "-"),
            item.get("text", "-"),
            item.get("issue", "-"),
            item.get("next_action", "-"),
            conclusion,
        ])

    for item in analysis.get("winning", []):
        rows_to_append.append([
            today, "継続強化",
            "高",
            item.get("field_type", "-"),
            item.get("text", "-"),
            item.get("appeal_axis", "-"),
            item.get("next_action", "-"),
            conclusion,
        ])

    for item in analysis.get("new_ads", []):
        rows_to_append.append([
            today, "新規広告案",
            "-",
            item.get("type", "-"),
            item.get("text", "-"),
            item.get("reason", "-"),
            "-",
            conclusion,
        ])

    if rows_to_append:
        ws.append_rows(rows_to_append)

    print(f"  → 分析ログ {len(rows_to_append)}件 → 専用スプレッドシートに追記")


# ---------------------------------------------------------------------------
# 6. Slack 通知
# ---------------------------------------------------------------------------

def notify_slack(analysis: dict, config: dict):
    """分析結果を整形して Slack に通知する。"""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL") or config.get("slack_webhook_url", "")
    if not webhook_url:
        return

    today = date.today().strftime("%Y-%m-%d")
    message = _format_slack_message(analysis, today)
    requests.post(webhook_url, json={"text": message}, timeout=10)
    print("  → Slack に通知完了")


# ---------------------------------------------------------------------------
# 7. エントリーポイント
# ---------------------------------------------------------------------------

def main():
    print("=== クリエイティブ週次レポート ===")

    ads_client = GoogleAdsClient.load_from_dict(ads_credentials)

    print(f"広告アセットデータを取得中（{config['date_range']}）...")
    rows = fetch_ad_asset_performance(ads_client, customer_id, config)
    print(f"  → {len(rows)}件取得")

    if not rows:
        print("対象データがありません。処理を終了します。")
        return

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(config["spreadsheet_id"])

    write_raw_sheet(sh, rows, config)

    print("Claude（creative-analyst）で分析中...")
    analysis = analyze_with_claude(rows)
    print("  → 分析完了")

    write_analysis_sheet(sh, analysis, config)
    write_detail_spreadsheet(gc, analysis, config)
    notify_slack(analysis, config)

    print("\n完了しました！")
    print(f"  スプレッドシート: https://docs.google.com/spreadsheets/d/{config['spreadsheet_id']}")


if __name__ == "__main__":
    main()
