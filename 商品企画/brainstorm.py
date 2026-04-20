"""
商品企画ブレインストーミングスクリプト (brainstorm.py)

【使い方】
    python brainstorm.py

起動後、競合LPのURLをいくつか入力してください。
スクリプトがLP内容を取得し、5名の専門家AIが順番に分析して企画書を作成します。

  [1] 市場リサーチ・競合分析担当  → LPから市場感・競合の訴求を読み解く
  [2] 商品設計担当                → 競合の弱点をついた勝てるコンセプトを設計
  [3] 商品開発担当                → 処方・形状・売価を設計
  [4] WEB広告運用担当             → キーワード・広告獲得戦略を設計
  [5] クリエイティブ・LP担当      → 勝てるLPの骨格とメイン訴求を仕上げる

【出力】
    商品コンセプトシート（Google スプレッドシート）に新しいタブとして出力
    URL: https://docs.google.com/spreadsheets/d/1UgaBWytuz08spedLGybh44j0S2XLQQidpzRB4LaWjR4
"""

import os
import sys
import json
import time
import re
from datetime import date

import requests
from bs4 import BeautifulSoup
from groq import Groq
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# ─── パス設定 ────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TECH_DIR = os.path.join(BASE_DIR, "技術（ツール開発・自動化）")
load_dotenv(os.path.join(TECH_DIR, ".env"))

# ─── 設定 ────────────────────────────────────────────────
SPREADSHEET_ID = "1UgaBWytuz08spedLGybh44j0S2XLQQidpzRB4LaWjR4"
TEMPLATE_TAB   = "商品コンセプトシート"
MODEL          = "llama-3.3-70b-versatile"
MAX_TOKENS     = 8192

# LPのテキストを取得するときの最大文字数（長すぎるとAPIに送れないため）
LP_MAX_CHARS = 6000


# ─── LPテキストの取得 ────────────────────────────────────
def fetch_lp_text(url: str) -> str:
    """
    URLからLPのテキストを取得する。
    JavaScriptで動くページは完全には取得できないが、主要な文章は取れることが多い。
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "lxml")

        # scriptやstyleタグを除去してテキストだけ取り出す
        for tag in soup(["script", "style", "header", "footer", "nav"]):
            tag.decompose()

        text = soup.get_text(separator="\n")
        # 空行を詰める
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        cleaned = "\n".join(lines)

        # 長すぎる場合は先頭部分だけ使う（LPの重要情報は上部にあることが多い）
        if len(cleaned) > LP_MAX_CHARS:
            cleaned = cleaned[:LP_MAX_CHARS] + "\n...(省略)"
        return cleaned

    except Exception as e:
        return f"（取得失敗: {e}）"


# ─── Google Sheets 接続 ──────────────────────────────────
def connect_sheets():
    sa_path = os.path.join(TECH_DIR, "service_account.json")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


# ─── テンプレートをコピーして新タブを作成 ────────────────
def create_new_tab(sh, product_name: str) -> gspread.Worksheet:
    today = date.today().strftime("%m/%d")
    base_name = f"【{today}】{product_name[:15]}"

    existing = [ws.title for ws in sh.worksheets()]
    tab_name = base_name
    count = 2
    while tab_name in existing:
        tab_name = f"{base_name}_{count}"
        count += 1

    template = sh.worksheet(TEMPLATE_TAB)
    sh.batch_update({
        "requests": [{
            "duplicateSheet": {
                "sourceSheetId": template.id,
                "newSheetName": tab_name,
                "insertSheetIndex": len(sh.worksheets())
            }
        }]
    })
    return sh.worksheet(tab_name)


# ─── Claudeに質問する共通関数 ────────────────────────────
def ask_claude(client: Groq, system_prompt: str, user_message: str, label: str = "") -> dict:
    """AIに質問してJSON形式の回答を受け取る。最大3回まで再試行する。"""
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

            # JSONを抽出（{〜}の範囲を取り出す）
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


# ─── 専門家プロンプト ─────────────────────────────────────

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

・競合が攻めていない「空白の訴求軸」を見つける
・競合LPの弱点をついた差別化ポイント
・ターゲットの「機能ジョブ（何を解決したいか）」と「感情ジョブ（どんな気持ちになりたいか）」
・通販（D2C）で年商10億円を狙えるUSP（独自の強み）

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

・競合が使っていない差別化成分・処方の方向性
・商品形状（容器・テクスチャー・使用感・使いやすさ）
・想定売価と容量設定（通販で利益が出る原価率20〜30%・LTV考慮）
・薬機法・成分制限・製造上の課題

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

・競合LPのメッセージと差別化した広告訴求軸
・ECモールで狙うキーワードと月間検索数（推定）10〜15個
・Google広告・Meta広告でのターゲティング方針
・年商10億円に向けた顧客獲得シナリオ（CPA・LTV・回収期間）

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


# ─── スプレッドシートへの書き込み ────────────────────────
def write_to_sheet(ws: gspread.Worksheet, results: dict, today_str: str):
    """
    分析結果を所定のセルに書き込む。
    セル位置は商品コンセプトシートのフォーマットに準拠。
    """
    r1 = results.get("role1", {})
    r2 = results.get("role2", {})
    r3 = results.get("role3", {})
    r4 = results.get("role4", {})
    r5 = results.get("role5", {})
    competitors = r1.get("competitors", [])
    keywords    = r4.get("keywords", [])

    updates = []

    # ── 作成日 ──
    updates.append({"range": "D3", "values": [[today_str]]})

    # ── 企画サマリー（J列）──
    updates.append({"range": "J6",  "values": [[r2.get("title", "")]]})
    updates.append({"range": "J7",  "values": [[r2.get("one_line", "")]]})
    updates.append({"range": "J8",  "values": [[r2.get("target", "")]]})
    updates.append({"range": "J9",  "values": [[r2.get("problems", "")]]})
    updates.append({"range": "J19", "values": [[r2.get("emotional_job", "")]]})

    # ── 市場調査（A33 マージドセル）──
    updates.append({"range": "A33", "values": [[r1.get("market_research", "")]]})

    # ── 競合（最大5社）──
    # 各競合の開始行：①=43, ②=49, ③=55, ④=61, ⑤=67
    competitor_rows = [43, 49, 55, 61, 67]
    for i, comp in enumerate(competitors[:5]):
        r = competitor_rows[i]
        updates.append({"range": f"I{r}",     "values": [[comp.get("name", "")]]})
        updates.append({"range": f"D{r + 1}", "values": [[comp.get("price", "")]]})
        updates.append({"range": f"J{r + 1}", "values": [[comp.get("volume", "")]]})
        updates.append({"range": f"S{r + 1}", "values": [[comp.get("sales", "")]]})
        updates.append({"range": f"E{r + 2}", "values": [[comp.get("features", "")]]})
        updates.append({"range": f"E{r + 3}", "values": [[comp.get("appeal", "")]]})
        updates.append({"range": f"F{r + 4}", "values": [[comp.get("reasons", "")]]})
        updates.append({"range": f"E{r + 5}", "values": [[comp.get("complaints", "")]]})

    # ── 自社（商品詳細）──
    updates.append({"range": "E75", "values": [[r2.get("entry_detail", "")]]})
    updates.append({"range": "E82", "values": [[r5.get("main_appeal", "")]]})
    updates.append({"range": "E83", "values": [[r5.get("product_strengths", "")]]})
    updates.append({"range": "E90", "values": [[r3.get("product_form", "")]]})
    updates.append({"range": "Z90", "values": [[r3.get("price_volume", "")]]})
    updates.append({"range": "E91", "values": [["自社EC／Amazon／楽天"]]})
    updates.append({"range": "R91", "values": [[r3.get("key_ingredients", "")]]})

    # ── 商品開発・広告戦略メモ（テンプレート外に追記）──
    extra_notes = []
    if r3.get("formulation_notes"):
        extra_notes.append("【処方・開発メモ】\n" + r3["formulation_notes"])
    if r3.get("commercialization"):
        extra_notes.append("【商品化ネクストアクション】\n" + r3["commercialization"])
    if r4.get("ad_strategy"):
        extra_notes.append("【広告戦略】\n" + r4["ad_strategy"])
    if r4.get("acquisition_scenario"):
        extra_notes.append("【獲得シナリオ】\n" + r4["acquisition_scenario"])
    if r5.get("lp_structure"):
        extra_notes.append("【LP構成案】\n" + r5["lp_structure"])
    if r5.get("sales_copy_tips"):
        extra_notes.append("【競合に勝つ訴求ポイント】\n" + r5["sales_copy_tips"])

    if extra_notes:
        updates.append({"range": "A95", "values": [["\n\n".join(extra_notes)]]})

    # ── キーワード（A122〜）──
    for j, kw in enumerate(keywords[:20]):
        row_num = 122 + j
        updates.append({"range": f"A{row_num}", "values": [[kw.get("word", "")]]})
        updates.append({"range": f"Q{row_num}", "values": [[str(kw.get("monthly_volume", ""))]]})

    # バッチで一括書き込み
    ws.batch_update(updates)


# ─── メイン処理 ──────────────────────────────────────────
def main():
    print("=" * 60)
    print("  商品企画ブレインストーミング")
    print("  競合LPをもとに5名の専門家AIが企画書を作成します")
    print("=" * 60)

    # APIキー確認
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("\n❌ エラー: GROQ_API_KEY が .env に設定されていません。")
        print(f"   {os.path.join(TECH_DIR, '.env')} に以下を追加してください：")
        print("   GROQ_API_KEY=gsk_xxxxxxxxxx")
        sys.exit(1)

    # ─── ① 商品テーマの入力 ──────────────────────────────
    print("\n① 企画する商品テーマを入力してください")
    print("   例：メンズ用プレシャンプー、40代向け美容液、産後ケアサプリ")
    theme = input("   > ").strip()
    if not theme:
        print("テーマが入力されていません。終了します。")
        return

    print("\n② カテゴリを入力してください")
    print("   例：スカルプケア、アンチエイジング、ヘアケア、ボディケア")
    category = input("   > ").strip() or "化粧品"

    # ─── ② 競合LPのURL入力 ──────────────────────────────
    print("\n③ 競合LPのURLを入力してください（1行に1つ。入力が終わったら空のままEnterを押す）")
    lp_urls = []
    while True:
        url = input(f"   LP{len(lp_urls) + 1} URL > ").strip()
        if not url:
            break
        lp_urls.append(url)
        if len(lp_urls) >= 5:
            print("   （最大5件に達しました）")
            break

    if not lp_urls:
        print("\n❌ LPのURLが入力されていません。終了します。")
        return

    # ─── ③ LP内容の取得 ─────────────────────────────────
    print(f"\nLP内容を取得中（{len(lp_urls)}件）...")
    lp_contents = []
    for i, url in enumerate(lp_urls, 1):
        print(f"  [{i}/{len(lp_urls)}] {url[:60]}...")
        text = fetch_lp_text(url)
        lp_contents.append({"url": url, "text": text})
        print(f"       → {len(text)}文字取得")

    # LP情報をまとめたテキスト（各専門家への入力に使う）
    lp_summary = "\n\n".join([
        f"=== 競合LP {i+1}: {item['url']} ===\n{item['text']}"
        for i, item in enumerate(lp_contents)
    ])
    user_context = f"商品テーマ：{theme}\nカテゴリ：{category}"

    # ─── ④ スプレッドシートへの接続と新タブ作成 ─────────
    print("\nスプレッドシートに接続中...")
    sh = connect_sheets()
    print("新しいタブを作成中...")
    ws = create_new_tab(sh, theme)
    print(f"  → タブ「{ws.title}」を作成しました")

    client = Groq(api_key=api_key)
    today_str = date.today().strftime("%Y/%m/%d")
    results = {}

    # ─── 専門家①: 市場リサーチ・競合分析 ──────────────
    print("\n[1/5] 市場リサーチ・競合分析担当が分析中...")
    r1 = ask_claude(
        client, ROLE1_SYSTEM,
        f"{user_context}\n\n【競合LP情報】\n{lp_summary}",
        label="市場リサーチ・競合分析"
    )
    results["role1"] = r1
    print("  → 完了")

    # ─── 専門家②: 商品設計 ──────────────────────────────
    print("\n[2/5] 商品設計担当がコンセプトを設計中...")
    r2 = ask_claude(
        client, ROLE2_SYSTEM,
        f"{user_context}\n\n【競合LP情報】\n{lp_summary}\n\n【競合分析の結果】\n{json.dumps(r1, ensure_ascii=False, indent=2)}",
        label="商品設計"
    )
    results["role2"] = r2
    print("  → 完了")

    # ─── 専門家③: 商品開発 ──────────────────────────────
    print("\n[3/5] 商品開発担当が処方・商品化プランを設計中...")
    r3 = ask_claude(
        client, ROLE3_SYSTEM,
        f"{user_context}\n\n【競合分析】\n{json.dumps(r1, ensure_ascii=False, indent=2)}\n\n【商品コンセプト】\n{json.dumps(r2, ensure_ascii=False, indent=2)}",
        label="商品開発"
    )
    results["role3"] = r3
    print("  → 完了")

    # ─── 専門家④: WEB広告運用 ───────────────────────────
    print("\n[4/5] WEB広告運用担当が獲得戦略を設計中...")
    r4 = ask_claude(
        client, ROLE4_SYSTEM,
        f"{user_context}\n\n【競合LP情報（抜粋）】\n{lp_summary[:3000]}\n\n【商品コンセプト】\n{json.dumps(r2, ensure_ascii=False, indent=2)}\n\n【商品開発プラン】\n{json.dumps(r3, ensure_ascii=False, indent=2)}",
        label="広告運用"
    )
    results["role4"] = r4
    print("  → 完了")

    # ─── 専門家⑤: クリエイティブ・LP ───────────────────
    print("\n[5/5] クリエイティブ担当がLP骨格と訴求を設計中...")
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
    results["role5"] = r5
    print("  → 完了")

    # ─── スプレッドシートに書き込み ─────────────────────
    print("\nスプレッドシートに書き込み中...")
    write_to_sheet(ws, results, today_str)
    print("  → 完了")

    print("\n" + "=" * 60)
    print("  ブレインストーミング完了！")
    print(f"  タブ名  : {ws.title}")
    print(f"  URL     : https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print("=" * 60)


if __name__ == "__main__":
    main()
