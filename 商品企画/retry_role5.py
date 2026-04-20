"""
専門家⑤（クリエイティブ・LP担当）だけ再実行して既存タブに書き込む
"""

import os
import sys
import json
import time
import re

import requests
from bs4 import BeautifulSoup
from groq import Groq
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TECH_DIR = os.path.join(BASE_DIR, "技術（ツール開発・自動化）")
load_dotenv(os.path.join(TECH_DIR, ".env"))

SPREADSHEET_ID = "1UgaBWytuz08spedLGybh44j0S2XLQQidpzRB4LaWjR4"
TAB_NAME       = "【04/09】メンズ用エイジ_2"
MODEL          = "llama-3.3-70b-versatile"
MAX_TOKENS     = 8192
LP_MAX_CHARS   = 6000

THEME    = "メンズ用エイジングケア"
CATEGORY = "メンズコスメ"
LP_URLS  = [
    "https://www.almado.jp/cellula/item/000804/",
    "https://www.sukkiri-life.com/item/detail/1700/?srsltid=AfmBOorMinPvR2H5yN7LSOzWGXqjvd6TNcrGiQkHKfaklmBY0trvpbJz",
]


def fetch_lp_text(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "lxml")
        for tag in soup(["script", "style", "header", "footer", "nav"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        cleaned = "\n".join(lines)
        if len(cleaned) > LP_MAX_CHARS:
            cleaned = cleaned[:LP_MAX_CHARS] + "\n...(省略)"
        return cleaned
    except Exception as e:
        return f"（取得失敗: {e}）"


def connect_sheets():
    sa_path = os.path.join(TECH_DIR, "service_account.json")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


def ask_claude(client, system_prompt, user_message, label=""):
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_message},
                ],
            )
            text = response.choices[0].message.content
            match = re.search(r'\{[\s\S]*\}', text)
            if match:
                return json.loads(match.group())
            else:
                print(f"  [!] JSON未検出 (試行 {attempt + 1}/3)")
                if attempt < 2:
                    time.sleep(5)
        except json.JSONDecodeError as e:
            print(f"  [!] JSON解析失敗 (試行 {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(5)
        except Exception as e:
            print(f"  [!] API エラー (試行 {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(10)
    print(f"  [X] {label} の分析に失敗しました。")
    return {}


ROLE1_SYSTEM = """あなたは通信販売（化粧品・健康食品）の市場リサーチと競合分析の専門家です。
提供された競合LP（ランディングページ）のテキストを分析し、以下を読み解いてください：

・各競合のターゲット・訴求軸・強みのポジショニング
・競合が使っている価格帯・容量・販売チャネル
・競合のLPが強調していること（メインメッセージ・社会的証明・保証など）
・競合が「触れていない弱点・不満」（レビューや記載から読み取れる課題）
・市場全体の訴求トレンドと空白地帯

必ず以下のJSON形式のみで回答してください（他の文章は不要）：
{
  "market_research": "競合LP分析から読み解いた市場概況\\n（改行は\\\\nで表現、訴求トレンド・空白地帯を含む詳細な内容）",
  "competitors": [
    {
      "name": "商品名／ブランド名／販売元（LPから読み取る）",
      "price": "価格（税込）",
      "volume": "容量・内容量",
      "sales": "推定売上・販売実績（LPの実績表示から）",
      "features": "商品特長（LPのメッセージから2〜3文）",
      "appeal": "メイン訴求（LPのキャッチコピー・ファーストビューのメッセージ）",
      "reasons": "選ばれる理由（LPで強調している根拠・証拠・社会的証明）",
      "complaints": "弱点・不満（LPで触れていないこと・改善余地）"
    }
  ]
}"""

ROLE2_SYSTEM = """あなたは通信販売（化粧品・健康食品）で年商10億円を目指す商品設計の専門家です。
競合LPの分析結果をもとに、「競合に勝てる商品コンセプト」を設計してください。

必ず以下のJSON形式のみで回答してください：
{
  "title": "商品タイトル（仮）10〜20文字のキャッチーな名前",
  "one_line": "この商品をひとことで言うと（30〜50文字）",
  "target": "ターゲット（年代・性別・生活状況を具体的に）",
  "problems": "ターゲットの悩み・困りごと（箇条書き、発生状況を含む、改行は\\n）",
  "emotional_job": "感情ジョブ（なりたい気持ち・なりたくない気持ち、改行は\\n）",
  "entry_point": "参入余地・勝ち筋の一言（20〜30文字）",
  "entry_detail": "参入余地の詳細（競合との比較・空白地帯の説明、改行は\\n）"
}"""

ROLE3_SYSTEM = """あなたは化粧品・健康食品の処方設計から商品化まで担う商品開発の専門家です。
競合LPの分析・商品コンセプトをもとに、「実際に作れる・売れる商品」の開発プランを立ててください。

必ず以下のJSON形式のみで回答してください：
{
  "product_form": "商品形状（容器タイプ・テクスチャー・使用方法・使用感）",
  "key_ingredients": "キー成分（差別化成分と採用理由、カンマ区切り）",
  "price_volume": "売価と容量（例：3,980円／60ml・約1ヶ月分）",
  "formulation_notes": "処方・開発の方向性と検討事項（箇条書き、改行は\\n）",
  "commercialization": "商品化ネクストアクション（ODM先選定・試作・薬機法対応等）"
}"""

ROLE4_SYSTEM = """あなたは通信販売のWEB広告（Google広告・Meta広告・Amazon広告）で新規顧客獲得を最大化する広告運用の専門家です。
競合LPの分析・商品コンセプトをもとに、「競合に勝てる広告戦略」を設計してください。

必ず以下のJSON形式のみで回答してください：
{
  "keywords": [
    {"word": "キーワード", "monthly_volume": "月間検索数（推定、数字のみ）"}
  ],
  "ad_strategy": "広告戦略サマリー（Google/Meta/Amazonごとの方針、改行は\\n）",
  "cpa_target": "目標CPA（金額と根拠）",
  "acquisition_scenario": "10億円達成の顧客獲得シナリオ（改行は\\n）"
}"""

ROLE5_SYSTEM = """あなたは通信販売の販売LP（ランディングページ）を設計するコピーライター・クリエイティブディレクターです。
競合LP・商品コンセプト・広告戦略をすべて受け取り、「競合LPに勝てるLP骨格」を設計してください。

・競合LPにない独自の切り口でのファーストビュー訴求
・商品の強み（競合との明確な差別化ポイント3〜5つ）
・LP構成（問題提起→共感→解決策→証拠→CTA）
・競合LPに対して優位に立てる訴求ポイント

必ず以下のJSON形式のみで回答してください：
{
  "main_appeal": "メイン訴求（LPファーストビューキャッチコピー、30〜40文字）",
  "product_strengths": "商品の強み（箇条書き3〜5点、競合との差別化を含む、改行は\\n）",
  "lp_structure": "LP構成案（各セクションのタイトルと概要、改行は\\n）",
  "sales_copy_tips": "競合LPに勝つための訴求ポイント（心理トリガー・差別化軸、改行は\\n）"
}"""


def main():
    print("専門家⑤ 再実行")
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("[X] GROQ_API_KEY が設定されていません。")
        sys.exit(1)

    client = Groq(api_key=api_key)
    user_context = f"商品テーマ：{THEME}\nカテゴリ：{CATEGORY}"

    # LP取得
    print(f"LP取得中（{len(LP_URLS)}件）...")
    lp_contents = []
    for i, url in enumerate(LP_URLS, 1):
        print(f"  [{i}] {url[:60]}...")
        text = fetch_lp_text(url)
        lp_contents.append({"url": url, "text": text})
        print(f"      -> {len(text)}文字")

    lp_summary = "\n\n".join([
        f"=== 競合LP {i+1}: {item['url']} ===\n{item['text']}"
        for i, item in enumerate(lp_contents)
    ])

    # 役割1〜4を再実行してコンテキストを揃える
    print("\n[1/4] 市場リサーチ...")
    r1 = ask_claude(client, ROLE1_SYSTEM,
        f"{user_context}\n\n【競合LP情報】\n{lp_summary}", label="市場リサーチ")
    print("  -> 完了")

    print("[2/4] 商品設計...")
    r2 = ask_claude(client, ROLE2_SYSTEM,
        f"{user_context}\n\n【競合LP情報】\n{lp_summary}\n\n【競合分析】\n{json.dumps(r1, ensure_ascii=False, indent=2)}",
        label="商品設計")
    print("  -> 完了")

    print("[3/4] 商品開発...")
    r3 = ask_claude(client, ROLE3_SYSTEM,
        f"{user_context}\n\n【競合分析】\n{json.dumps(r1, ensure_ascii=False, indent=2)}\n\n【商品コンセプト】\n{json.dumps(r2, ensure_ascii=False, indent=2)}",
        label="商品開発")
    print("  -> 完了")

    print("[4/4] 広告運用...")
    r4 = ask_claude(client, ROLE4_SYSTEM,
        f"{user_context}\n\n【競合LP情報（抜粋）】\n{lp_summary[:3000]}\n\n【商品コンセプト】\n{json.dumps(r2, ensure_ascii=False, indent=2)}\n\n【商品開発プラン】\n{json.dumps(r3, ensure_ascii=False, indent=2)}",
        label="広告運用")
    print("  -> 完了")

    # 専門家⑤を実行
    print("\n[5/5] クリエイティブ・LP担当が分析中...")
    r5 = ask_claude(
        client, ROLE5_SYSTEM,
        (
            f"{user_context}\n\n"
            f"【競合LP情報（抜粋）】\n{lp_summary[:2000]}\n\n"
            f"【競合分析のポイント】\n市場概況：{r1.get('market_research', '')[:500]}\n\n"
            f"【商品コンセプト】\n{json.dumps(r2, ensure_ascii=False, indent=2)}\n\n"
            f"【商品開発プラン】\n{json.dumps(r3, ensure_ascii=False, indent=2)}\n\n"
            f"【広告戦略】\n{json.dumps(r4, ensure_ascii=False, indent=2)}"
        ),
        label="クリエイティブ"
    )

    if not r5:
        print("[X] 専門家⑤の分析に失敗しました。")
        sys.exit(1)

    print("  -> 完了")

    # 既存タブに書き込み
    print("\nスプレッドシートに接続...")
    sh = connect_sheets()
    ws = sh.worksheet(TAB_NAME)

    updates = [
        {"range": "E82", "values": [[r5.get("main_appeal", "")]]},
        {"range": "E83", "values": [[r5.get("product_strengths", "")]]},
    ]

    # A95の既存内容を読んでから⑤分を追記
    extra_parts = []
    if r5.get("lp_structure"):
        extra_parts.append("【LP構成案】\n" + r5["lp_structure"])
    if r5.get("sales_copy_tips"):
        extra_parts.append("【競合に勝つ訴求ポイント】\n" + r5["sales_copy_tips"])
    if extra_parts:
        # A95の既存値を取得して末尾に追記
        try:
            existing = ws.acell("A95").value or ""
            new_value = existing + "\n\n" + "\n\n".join(extra_parts) if existing else "\n\n".join(extra_parts)
        except Exception:
            new_value = "\n\n".join(extra_parts)
        updates.append({"range": "A95", "values": [[new_value]]})

    ws.batch_update(updates)
    print("  -> 書き込み完了")

    print("\n完了！")
    print(f"タブ: {TAB_NAME}")
    print(f"URL: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
