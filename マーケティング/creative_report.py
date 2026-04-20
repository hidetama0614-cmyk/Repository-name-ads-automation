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
import json
import time
import requests
from datetime import date
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from google.ads.googleads.client import GoogleAdsClient
from groq import Groq

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
# 1. エージェントプロンプト（インライン埋め込み）
# ---------------------------------------------------------------------------

CREATIVE_ANALYST_PROMPT = """【重要】あなたの返答は必ず有効なJSONオブジェクトのみで構成してください。`{` で始まり `}` で終わること。前置き・見出し・マークダウン・コードブロック記法（```）は一切含めないでください。

あなたはD2C領域の運用型広告において、クリエイティブの摩耗と勝ちパターンを的確に見抜く優秀なアナリストです。
提供された広告見出し・説明文の配信データ（インプレッション、CTR、CPA、CV数など）を読み解き、以下のルールに従って分析レポートを作成してください。

# 分析・判断ルール
1. **停止判断（摩耗・負けクリエイティブの特定）:**
   - 十分なインプレッションがあるにも関わらずCTRが著しく低い、またはCPAが高騰している見出し・説明文を特定してください。
   - 各アセットに「高・中・低」の重要度を付与してください（すぐに停止すべきものは「高」）。
   - 課題・要因仮説・改善提案をセットで記載してください。改善提案は抽象的でなく、そのまま広告に使える具体的なテキストレベルで記載してください。
2. **勝ち筋の言語化（ベストクリエイティブの特定）:**
   - CTRが高く、安定してCVを獲得しているクリエイティブに共通する訴求軸を言語化してください。なぜそのターゲットに刺さったのか、心理的背景も短く考察してください。
3. **次の一手（テスト仮説の立案）:**
   - 次に検証すべき仮説を3つ立て、それぞれにそのまま使える具体的な広告文案を添えてください。

# 出力形式
必ず以下のJSON形式のみで出力してください。JSON以外のテキスト（前置き・解説・コードブロック記法など）は一切含めないでください。

{
  "conclusion": "今週全体の結論（2〜3文。最も重要なアクションと勝ちパターンを端的にまとめる）",
  "stop": [
    {
      "text": "対象の広告テキスト（完全な文言をそのまま記載）",
      "field_type": "HEADLINE または DESCRIPTION",
      "campaign": "キャンペーン名（データから正確に抜き出す）",
      "ad_group": "広告グループ名（データから正確に抜き出す）",
      "importance": "高 または 中 または 低",
      "action_type": "停止 または 修正",
      "issue": "課題の説明（1文）",
      "operation": "管理画面での具体的な操作指示（例：このアセットを無効化する）",
      "improved_copy": "改善後のテキスト（停止の場合は代替案、修正の場合は修正後テキスト。文字数制限を守ること）"
    }
  ],
  "winning": [
    {
      "text": "対象の広告テキスト（完全な文言をそのまま記載）",
      "field_type": "HEADLINE または DESCRIPTION",
      "campaign": "キャンペーン名（データから正確に抜き出す）",
      "ad_group": "広告グループ名（データから正確に抜き出す）",
      "appeal_axis": "訴求軸（例：価格、権威性、共感など）",
      "reason": "なぜ刺さっているか（1文）",
      "next_action": "この勝ちパターンをどう横展開・強化するか（具体的なアクション）"
    }
  ],
  "new_ads": [
    {
      "type": "HEADLINE または DESCRIPTION",
      "text": "そのまま入稿できる広告文（文字数制限を必ず守ること：見出し15文字以内、説明文45文字以内）",
      "target_campaign": "追加先のキャンペーン名",
      "target_ad_group": "追加先の広告グループ名",
      "appeal_axis": "訴求軸",
      "reason": "追加する理由（1文）",
      "operation": "管理画面での操作指示（例：〇〇広告グループの広告編集画面で見出し欄に追加）"
    }
  ]
}

## 重要ルール
- 同一テキストが複数のキャンペーン・広告グループに存在する場合、必ずそれぞれ個別のエントリとして出力する。まとめたり「複数」「計〇件」などと表現することは禁止。
- campaign / ad_group はデータに記載された名称をそのまま使用する。省略・要約禁止。"""


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
    system_prompt = CREATIVE_ANALYST_PROMPT
    user_message  = _format_for_claude(rows)

    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_message},
        ],
        max_tokens=8192,
    )
    raw_text = response.choices[0].message.content

    # 1回目：そのままパース
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        pass

    # 2回目：{ 〜 } の範囲を抽出してパース（最も堅牢な方法）
    start = raw_text.find("{")
    end   = raw_text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(raw_text[start:end + 1])
        except json.JSONDecodeError:
            pass

    # 3回目：Geminiに再変換させる（レートリミット回避のため65秒待機）
    print("  → JSON形式ではないため65秒待機後に再変換中...")
    time.sleep(65)
    json_schema = """{
  "conclusion": "今週全体の結論（2〜3文）",
  "stop": [{"text":"","field_type":"HEADLINE or DESCRIPTION","campaign":"","ad_group":"","importance":"高/中/低","action_type":"停止 or 修正","issue":"","improved_copy":""}],
  "winning": [{"text":"","field_type":"HEADLINE or DESCRIPTION","campaign":"","ad_group":"","appeal_axis":"","reason":"","next_action":""}],
  "new_ads": [{"type":"HEADLINE or DESCRIPTION","text":"","target_campaign":"","target_ad_group":"","appeal_axis":"","reason":""}]
}"""
    retry_response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": (
                "あなたはJSONフォーマッターです。"
                "与えられたテキストを指定のJSON形式に変換し、JSONオブジェクトのみを出力してください。"
                "前置き・説明・コードブロック記法は一切含めないでください。"
            )},
            {"role": "user", "content": (
                f"以下の分析テキストを、このJSON形式に変換してください。\n\n"
                f"## 必要なJSON形式\n{json_schema}\n\n"
                f"## 変換対象の分析テキスト\n{raw_text}"
            )},
        ],
        max_tokens=8192,
    )
    retry_raw = retry_response.choices[0].message.content
    rs = retry_raw.find("{")
    re_ = retry_raw.rfind("}")
    if rs != -1 and re_ > rs:
        try:
            return json.loads(retry_raw[rs:re_ + 1])
        except json.JSONDecodeError:
            pass
    return {"_raw": raw_text, "stop": [], "winning": [], "new_ads": []}


# ---------------------------------------------------------------------------
# 4. Slack メッセージ整形
# ---------------------------------------------------------------------------

def _format_slack_message(analysis: dict, today: str) -> str:
    lines = [f"*📊 【検索広告】クリエイティブ週次レポート（{today}）*"]

    # 今週の結論
    conclusion = analysis.get("conclusion", "")
    if conclusion:
        lines.append("")
        lines.append(f"*【今週の結論】*\n{conclusion}")

    # 停止・修正指示（重要度順にソート）
    stop_items = analysis.get("stop", [])
    if stop_items:
        priority_order = {"高": 0, "中": 1, "低": 2}
        stop_items = sorted(stop_items, key=lambda x: priority_order.get(x.get("importance", "低"), 2))
        lines.append("")
        lines.append(DIVIDER)
        lines.append(f"*🚨 停止・修正指示（{len(stop_items)}件）*")
        lines.append(DIVIDER)
        for i, item in enumerate(stop_items, 1):
            icon = IMPORTANCE_ICON.get(item.get("importance", "中"), "🟡")
            field = item.get("field_type", "-")
            action = item.get("action_type", "-")
            lines.append(f"{icon} *[{item.get('importance', '-')}] {i}. {field} — {action}*")
            lines.append(f"・キャンペーン：{item.get('campaign', '-')}")
            lines.append(f"・広告グループ：{item.get('ad_group', '-')}")
            lines.append(f"・テキスト：「{item.get('text', '-')}」")
            lines.append(f"・課題：{item.get('issue', '-')}")
            lines.append(f"・操作：{item.get('operation', '-')}")
            lines.append(f"・改善コピー：「{item.get('improved_copy', '-')}」")
            lines.append("")

    # 勝ちパターン
    winning_items = analysis.get("winning", [])
    if winning_items:
        lines.append(DIVIDER)
        lines.append(f"*✅ 勝ちパターン — 継続強化（{len(winning_items)}件）*")
        lines.append(DIVIDER)
        for i, item in enumerate(winning_items, 1):
            field = item.get("field_type", "-")
            lines.append(f"*{i}. {field} — 継続強化*")
            lines.append(f"・キャンペーン：{item.get('campaign', '-')}")
            lines.append(f"・広告グループ：{item.get('ad_group', '-')}")
            lines.append(f"・テキスト：「{item.get('text', '-')}」")
            lines.append(f"・訴求軸：{item.get('appeal_axis', '-')}")
            lines.append(f"・理由：{item.get('reason', '-')}")
            lines.append(f"・次にやること：{item.get('next_action', '-')}")
            lines.append("")

    # 新規追加指示
    new_ads = analysis.get("new_ads", [])
    if new_ads:
        lines.append(DIVIDER)
        lines.append(f"*💡 新規追加指示（{len(new_ads)}件）*")
        lines.append(DIVIDER)
        for i, item in enumerate(new_ads, 1):
            field = item.get("type", "-")
            lines.append(f"*{i}. {field} — 新規追加*")
            lines.append(f"・追加先キャンペーン：{item.get('target_campaign', '-')}")
            lines.append(f"・追加先広告グループ：{item.get('target_ad_group', '-')}")
            lines.append(f"・テキスト：「{item.get('text', '-')}」")
            lines.append(f"・訴求軸：{item.get('appeal_axis', '-')}")
            lines.append(f"・理由：{item.get('reason', '-')}")
            lines.append(f"・操作：{item.get('operation', '-')}")
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

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(config["spreadsheet_id"])

    write_raw_sheet(sh, rows, config)

    print("Claude（creative-analyst）で分析中...")
    analysis_rows = sorted(rows, key=lambda r: r["cost"], reverse=True)[:150]
    analysis = analyze_with_claude(analysis_rows)
    print("  → 分析完了")

    write_analysis_sheet(sh, analysis, config)
    write_detail_spreadsheet(gc, analysis, config)
    notify_slack(analysis, config)

    print("\n完了しました！")
    print(f"  スプレッドシート: https://docs.google.com/spreadsheets/d/{config['spreadsheet_id']}")


if __name__ == "__main__":
    main()
