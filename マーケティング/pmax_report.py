"""
pmax_report.py — P-MAX広告クリエイティブの週次レポートを生成・通知するエントリーポイント

処理の流れ:
    1. Google Ads API から P-MAX アセット実績を取得
    2. 生データをスプレッドシートに保存
    3. pmax-analyst エージェントのプロンプトで Claude API を呼び出し → 分析（JSON）
    4. 分析結果を専用スプレッドシートに行単位で追記
    5. Slack に整形済みメッセージを通知

初回実行時:
    config.json の pmax_log_spreadsheet_id が空の場合、
    Google Drive の共有フォルダに新規スプレッドシートを自動作成します。
    作成後に表示されるスプレッドシートIDを config.json に設定してください。
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

from fetch_pmax_assets import fetch_pmax_asset_performance

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
LABEL_ICON      = {"BEST": "◎", "GOOD": "○", "LOW": "×", "LEARNING": "△"}
DIVIDER = "━━━━━━━━━━━━━━━━━━━━━"

DRIVE_FOLDER_ID = config.get("drive_folder_id", "")
PMAX_SPREADSHEET_TITLE = "P-MAX クリエイティブレポート"


# ---------------------------------------------------------------------------
# 1. エージェントプロンプト（インライン埋め込み）
# ---------------------------------------------------------------------------

PMAX_ANALYST_PROMPT = """あなたはD2C領域の運用型広告において、P-MAX（Performance Max）キャンペーンのアセット品質改善と訴求軸分析を専門とするアナリストです。
提供されたアセット（見出し・ロング見出し・説明文）の配信データを読み解き、以下のルールに従って分析レポートを作成してください。

# P-MAX アセットの特性
- **HEADLINE（見出し）**: 30文字以内、最大5個。短く訴求力のあるコピー。
- **LONG_HEADLINE（ロング見出し）**: 90文字以内、最大5個。より詳細な訴求が可能。
- **DESCRIPTION（説明文）**: 90文字以内、最大5個。ベネフィットや詳細情報。
- **パフォーマンスラベル**: GoogleのAIが評価した品質スコア
  - BEST（◎）: 最も高いパフォーマンスを発揮しているアセット
  - GOOD（○）: 良好なパフォーマンス
  - LOW（×）: 改善または置き換えが必要なアセット
  - LEARNING（学習中）: データ収集中のため判断保留
- P-MAXはGoogleAIが自動でアセットを組み合わせて配信するため、LOWアセットはAIに使われにくくなる。

# 分析・判断ルール
1. **停止・置き換え判断（LOWアセットの特定）:**
   - パフォーマンスラベルが「LOW」のアセットを特定し、停止または置き換えを推奨してください。
   - インプレッションが多いにも関わらずCTRが低いアセットも対象にしてください。
   - 各アセットに「高・中・低」の重要度を付与（LOWラベルかつ費用消化が大きい場合は「高」）。
   - 課題・要因仮説・改善提案をセットで記載。改善案はそのまま入稿できる具体的なテキストで。
2. **勝ち筋の言語化（BESTアセットの特定）:**
   - パフォーマンスラベルが「BEST」のアセットに共通する訴求軸・表現パターンを言語化してください。
   - なぜそのターゲットに刺さっているか、心理的背景も短く考察してください。
3. **次の一手（テスト仮説の立案）:**
   - 次に検証すべき仮説を3つ立て、それぞれにそのまま使える具体的な広告文案を添えてください。
   - 文字数制限を必ず守ること（HEADLINE: 30文字以内、LONG_HEADLINE/DESCRIPTION: 90文字以内）。

# 出力形式
必ず以下のJSON形式のみで出力してください。JSON以外のテキスト（前置き・解説・コードブロック記法など）は一切含めないでください。

{
  "conclusion": "今週全体の結論（2〜3文。最も重要なアクションと勝ちパターンを端的にまとめる）",
  "stop": [
    {
      "text": "対象のアセットテキスト（完全な文言をそのまま記載）",
      "field_type": "HEADLINE または LONG_HEADLINE または DESCRIPTION",
      "campaign": "キャンペーン名（データから正確に抜き出す）",
      "asset_group": "アセットグループ名（データから正確に抜き出す）",
      "performance_label": "LOW または GOOD または BEST",
      "importance": "高 または 中 または 低",
      "action_type": "停止 または 置き換え",
      "issue": "課題の説明（1文）",
      "operation": "管理画面での具体的な操作指示（例：このアセットを削除して新しいテキストに置き換える）",
      "improved_copy": "改善後のテキスト（文字数制限を必ず守ること）"
    }
  ],
  "winning": [
    {
      "text": "対象のアセットテキスト（完全な文言をそのまま記載）",
      "field_type": "HEADLINE または LONG_HEADLINE または DESCRIPTION",
      "campaign": "キャンペーン名（データから正確に抜き出す）",
      "asset_group": "アセットグループ名（データから正確に抜き出す）",
      "performance_label": "BEST",
      "appeal_axis": "訴求軸（例：価格、権威性、共感、緊急性など）",
      "reason": "なぜ刺さっているか（1文）",
      "next_action": "この勝ちパターンをどう横展開・強化するか（具体的なアクション）"
    }
  ],
  "new_ads": [
    {
      "type": "HEADLINE または LONG_HEADLINE または DESCRIPTION",
      "text": "そのまま入稿できるテキスト（文字数制限を必ず守ること）",
      "target_campaign": "追加先のキャンペーン名",
      "target_asset_group": "追加先のアセットグループ名",
      "appeal_axis": "訴求軸",
      "reason": "追加する理由（1文）",
      "operation": "管理画面での操作指示（例：〇〇アセットグループの編集画面で見出し欄に追加）"
    }
  ]
}

## 重要ルール
- 同一テキストが複数のキャンペーン・アセットグループに存在する場合、必ずそれぞれ個別のエントリとして出力する。まとめたり「複数」「計〇件」などと表現することは禁止。
- campaign / asset_group はデータに記載された名称をそのまま使用する。省略・要約禁止。
- LEARNING（学習中）のアセットは停止推奨に含めないこと。"""


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

FIELD_TYPE_MAP = {
    "HEADLINE":      "見出し",
    "LONG_HEADLINE": "ロング見出し",
    "DESCRIPTION":   "説明文",
}


def _format_for_claude(rows: list[dict]) -> str:
    lines = [
        "以下はGoogle P-MAX広告のアセット（見出し・ロング見出し・説明文）の直近30日間の実績データです。分析をお願いします。\n",
        "| 種別 | テキスト | パフォーマンスラベル | 表示回数 | クリック数 | 費用(円) | CTR(%) | CV数 | キャンペーン | アセットグループ |",
        "| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | :--- | :--- |",
    ]
    for r in rows:
        label     = PERFORMANCE_LABEL_MAP.get(r["performance_label"], r["performance_label"])
        type_name = FIELD_TYPE_MAP.get(r["field_type"], r["field_type"])
        lines.append(
            f"| {type_name} | {r['text']} | {label} "
            f"| {r['impressions']:,} | {r['clicks']:,} | {r['cost']:,} "
            f"| {r['ctr']} | {r['conversions']} | {r['campaign']} | {r['asset_group']} |"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 3. Claude API 呼び出し → JSON パース
# ---------------------------------------------------------------------------

def analyze_with_claude(rows: list[dict]) -> dict:
    """pmax-analyst エージェントで分析し、JSONとして返す。"""
    system_prompt = PMAX_ANALYST_PROMPT
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
  "stop": [{"text":"","field_type":"HEADLINE/LONG_HEADLINE/DESCRIPTION","campaign":"","asset_group":"","importance":"高/中/低","action_type":"停止 or 修正","issue":"","improved_copy":""}],
  "winning": [{"text":"","field_type":"HEADLINE/LONG_HEADLINE/DESCRIPTION","campaign":"","asset_group":"","appeal_axis":"","reason":"","next_action":""}],
  "new_ads": [{"type":"HEADLINE/LONG_HEADLINE/DESCRIPTION","text":"","target_campaign":"","target_asset_group":"","appeal_axis":"","reason":""}]
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
    lines = [f"*📊 【P-MAX】クリエイティブ週次レポート（{today}）*"]

    conclusion = analysis.get("conclusion", "")
    if conclusion:
        lines.append("")
        lines.append(f"*【今週の結論】*\n{conclusion}")

    # 停止・置き換え指示（上位5件のみ表示）
    stop_items = analysis.get("stop", [])
    if stop_items:
        priority_order = {"高": 0, "中": 1, "低": 2}
        stop_items = sorted(stop_items, key=lambda x: priority_order.get(x.get("importance", "低"), 2))
        total_stop = len(stop_items)
        lines.append("")
        lines.append(DIVIDER)
        lines.append(f"*🚨 停止・置き換え指示（{total_stop}件）*")
        lines.append(DIVIDER)
        for i, item in enumerate(stop_items[:5], 1):
            icon  = IMPORTANCE_ICON.get(item.get("importance", "中"), "🟡")
            field = FIELD_TYPE_MAP.get(item.get("field_type", "-"), item.get("field_type", "-"))
            lines.append(f"{icon} *{i}. 「{item.get('text', '-')}」*")
            lines.append(f"　{field}｜{item.get('campaign', '-')} / {item.get('asset_group', '-')}")
            lines.append(f"　課題：{item.get('issue', '-')}")
            lines.append(f"　改善コピー：「{item.get('improved_copy', '-')}」")
            lines.append("")
        if total_stop > 5:
            lines.append(f"　_ほか {total_stop - 5} 件 → 詳細はスプレッドシートを確認_")
            lines.append("")

    # 勝ちパターン（上位3件）
    winning_items = analysis.get("winning", [])
    if winning_items:
        lines.append(DIVIDER)
        lines.append(f"*✅ 勝ちパターン — 継続強化（{len(winning_items)}件）*")
        lines.append(DIVIDER)
        for i, item in enumerate(winning_items[:3], 1):
            field = FIELD_TYPE_MAP.get(item.get("field_type", "-"), item.get("field_type", "-"))
            lines.append(f"*{i}. {field} — 継続強化*")
            lines.append(f"・キャンペーン：{item.get('campaign', '-')}")
            lines.append(f"・アセットグループ：{item.get('asset_group', '-')}")
            lines.append(f"・テキスト：「{item.get('text', '-')}」")
            lines.append(f"・訴求軸：{item.get('appeal_axis', '-')}")
            lines.append(f"・理由：{item.get('reason', '-')}")
            lines.append(f"・次にやること：{item.get('next_action', '-')}")
            lines.append("")

    # 新規追加指示
    new_ads = analysis.get("new_ads", [])
    if new_ads:
        total_new = len(new_ads)
        lines.append(DIVIDER)
        lines.append(f"*💡 新規追加指示（{total_new}件）*")
        lines.append(DIVIDER)
        for i, item in enumerate(new_ads[:3], 1):
            field = FIELD_TYPE_MAP.get(item.get("type", "-"), item.get("type", "-"))
            lines.append(f"*{i}. 「{item.get('text', '-')}」*")
            lines.append(f"　{field}｜{item.get('target_campaign', '-')} / {item.get('target_asset_group', '-')}")
            lines.append(f"　理由：{item.get('reason', '-')}")
            lines.append("")
        if total_new > 3:
            lines.append(f"　_ほか {total_new - 3} 件 → 詳細はスプレッドシートを確認_")
            lines.append("")

    # フォールバック（JSON解析失敗時）
    if "_raw" in analysis and not analysis.get("stop") and not analysis.get("winning"):
        spreadsheet_id = config.get("pmax_log_spreadsheet_id", "")
        lines.append("")
        lines.append("⚠️ 分析結果の整形に失敗しました。スプレッドシートで詳細を確認してください。")
        if spreadsheet_id:
            lines.append(f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 5. スプレッドシート作成・書き込み
# ---------------------------------------------------------------------------

def _get_or_create_spreadsheet(gc) -> tuple:
    """P-MAX専用スプレッドシートを取得する。(sh, spreadsheet_id) を返す。"""
    spreadsheet_id = config.get("pmax_log_spreadsheet_id", "")

    if not spreadsheet_id:
        print("\n【設定エラー】config.json の pmax_log_spreadsheet_id が未設定です。")
        print("以下の手順でスプレッドシートIDを設定してください：")
        print("  1. Googleスプレッドシートを新規作成する")
        print("  2. サービスアカウントのメールアドレスを編集者として共有する")
        print("  3. URLの /d/〇〇〇/ の部分（〇〇〇）をコピーする")
        print("  4. config.json の pmax_log_spreadsheet_id に貼り付ける")
        raise SystemExit(1)

    sh = gc.open_by_key(spreadsheet_id)
    return sh, spreadsheet_id


def _get_or_create_worksheet(sh, title: str, rows: int = 2000, cols: int = 15):
    try:
        ws = sh.worksheet(title)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    return ws


def write_raw_sheet(sh, rows: list[dict]):
    """生データを「P-MAXアセット実績」タブに上書き保存する。"""
    ws = _get_or_create_worksheet(sh, "P-MAXアセット実績")
    header = [
        "種別", "テキスト", "パフォーマンスラベル",
        "表示回数", "クリック数", "費用(円)", "CTR(%)", "CV数",
        "キャンペーン", "アセットグループ",
    ]
    data = [header] + [
        [
            FIELD_TYPE_MAP.get(r["field_type"], r["field_type"]),
            r["text"],
            r["performance_label"],
            r["impressions"],
            r["clicks"],
            r["cost"],
            r["ctr"],
            r["conversions"],
            r["campaign"],
            r["asset_group"],
        ]
        for r in rows
    ]
    ws.update(data, "A1")
    print(f"  → 生データ {len(rows)}件 → シート「P-MAXアセット実績」に保存")


def write_analysis_sheet(sh, analysis: dict):
    """Claude の分析結果（JSON）を「P-MAX分析」タブに日付付きで追記する。"""
    tab_name = "P-MAX分析"
    today    = date.today().strftime("%Y-%m-%d")

    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=2000, cols=3)
        ws.append_row(["日付", "分析結果（JSON）"])
        ws.freeze(rows=1)

    ws.append_row([today, json.dumps(analysis, ensure_ascii=False)])
    print(f"  → 分析結果 → シート「{tab_name}」に追記")


def write_detail_sheet(sh, analysis: dict):
    """分析結果を行単位で「P-MAX分析ログ」タブに追記する。"""
    tab_name = "P-MAX分析ログ"
    today    = date.today().strftime("%Y-%m-%d")

    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=5000, cols=9)
        ws.append_row([
            "日付", "分析区分", "重要度", "種別", "テキスト",
            "課題/訴求軸/理由", "次にやること/操作", "アセットグループ", "結論",
        ])
        ws.freeze(rows=1)

    conclusion     = analysis.get("conclusion", "")
    rows_to_append = []

    for item in analysis.get("stop", []):
        rows_to_append.append([
            today, "停止・置き換え推奨",
            item.get("importance", "-"),
            FIELD_TYPE_MAP.get(item.get("field_type", "-"), item.get("field_type", "-")),
            item.get("text", "-"),
            item.get("issue", "-"),
            item.get("operation", "-"),
            item.get("asset_group", "-"),
            conclusion,
        ])

    for item in analysis.get("winning", []):
        rows_to_append.append([
            today, "継続強化",
            "高",
            FIELD_TYPE_MAP.get(item.get("field_type", "-"), item.get("field_type", "-")),
            item.get("text", "-"),
            item.get("appeal_axis", "-"),
            item.get("next_action", "-"),
            item.get("asset_group", "-"),
            conclusion,
        ])

    for item in analysis.get("new_ads", []):
        rows_to_append.append([
            today, "新規追加案",
            "-",
            FIELD_TYPE_MAP.get(item.get("type", "-"), item.get("type", "-")),
            item.get("text", "-"),
            item.get("reason", "-"),
            item.get("operation", "-"),
            item.get("target_asset_group", "-"),
            conclusion,
        ])

    if rows_to_append:
        ws.append_rows(rows_to_append)

    print(f"  → 分析ログ {len(rows_to_append)}件 → シート「{tab_name}」に追記")


# ---------------------------------------------------------------------------
# 6. Slack 通知
# ---------------------------------------------------------------------------

def notify_slack(analysis: dict):
    """分析結果を整形して Slack に通知する。"""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL") or config.get("slack_webhook_url", "")
    if not webhook_url:
        return

    today   = date.today().strftime("%Y-%m-%d")
    message = _format_slack_message(analysis, today)
    requests.post(webhook_url, json={"text": message}, timeout=10)
    print("  → Slack に通知完了")


# ---------------------------------------------------------------------------
# 7. エントリーポイント
# ---------------------------------------------------------------------------

def main():
    print("=== P-MAX クリエイティブ週次レポート ===")

    ads_client = GoogleAdsClient.load_from_dict(ads_credentials)

    print(f"P-MAXアセットデータを取得中（{config['date_range']}）...")
    rows = fetch_pmax_asset_performance(ads_client, customer_id, config)
    print(f"  → {len(rows)}件取得")

    if not rows:
        print("対象データがありません。処理を終了します。")
        return

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    gc    = gspread.authorize(creds)

    sh, spreadsheet_id = _get_or_create_spreadsheet(gc)

    write_raw_sheet(sh, rows)

    print("Claude（pmax-analyst）で分析中...")
    analysis_rows = sorted(rows, key=lambda r: r["cost"], reverse=True)[:30]
    analysis = analyze_with_claude(analysis_rows)
    print("  → 分析完了")

    write_analysis_sheet(sh, analysis)
    write_detail_sheet(sh, analysis)
    notify_slack(analysis)

    print("\n完了しました！")
    print(f"  スプレッドシート: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")


if __name__ == "__main__":
    main()
