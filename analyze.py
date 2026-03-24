"""
analyze.py — 広告データの分析・レポート出力モジュール

拡張方法:
    新プラットフォーム（Meta / Yahoo など）を追加する場合は
    normalize_<platform>() を追加し、main.py から渡すだけでよい。
"""

import requests
import gspread
from datetime import date
from typing import Optional


# ---------------------------------------------------------------------------
# 1. 正規化（プラットフォームごとに追加）
# ---------------------------------------------------------------------------

def normalize_google_ads(keyword_rows: list) -> list[dict]:
    """Google Ads キーワード行リストを共通スキーマに変換する。

    共通スキーマ:
        platform, campaign, ad_group, keyword, match_type,
        impressions, clicks, cost, ctr, hon_cv
    """
    records = []
    for r in keyword_rows:
        records.append({
            "platform":    "Google Ads",
            "campaign":    r[0],
            "ad_group":    r[1],
            "keyword":     r[2],
            "match_type":  r[3],
            "impressions": int(r[4]),
            "clicks":      int(r[5]),
            "cost":        int(r[6]),
            "ctr":         float(r[7]),
            "hon_cv":      int(r[8]),
        })
    return records


# def normalize_meta_ads(rows): ...
# def normalize_yahoo_ads(rows): ...


# ---------------------------------------------------------------------------
# 2. 分析ロジック
# ---------------------------------------------------------------------------

def _cpa(record: dict) -> Optional[float]:
    return record["cost"] / record["hon_cv"] if record["hon_cv"] > 0 else None


def _flag(record: dict, category: str, priority: str, action: str, reason: str) -> dict:
    """分析フラグを付与した辞書を返す共通ヘルパー。"""
    return {
        **record,
        "cpa":      _cpa(record),
        "category": category,
        "priority": priority,
        "action":   action,
        "reason":   reason,
    }


def run_analysis(records: list[dict], config: dict) -> dict:
    """共通スキーマのレコードリストを受け取り、分析結果を返す。"""
    a              = config.get("analysis", {})
    target_cpa     = a.get("target_cpa", 0)
    wasted_min     = a.get("wasted_cost_min", 5000)
    ctr_alert      = a.get("ctr_alert_threshold", 1.0)   # 強警戒
    ctr_watch      = a.get("ctr_watch_threshold", 3.0)   # 要確認
    growth_ratio   = a.get("growth_cpa_ratio", 0.7)
    growth_clicks  = a.get("growth_min_clicks", 10)

    high_cpa, wasted_cost, growth, low_ctr_alert, low_ctr_watch = [], [], [], [], []

    for r in records:
        cpa = _cpa(r)

        # CPA高騰: 本CVあり & CPA > 目標CPA
        if cpa is not None and target_cpa > 0 and cpa > target_cpa:
            ratio = cpa / target_cpa * 100
            high_cpa.append(_flag(
                r, "CPA高騰", "高",
                action="入札引き下げ",
                reason=f"CPA ¥{int(cpa):,}（目標 ¥{target_cpa:,} の {ratio:.0f}%）",
            ))

        # 無駄コスト: 費用5,000円以上 & 本CV=0
        if r["cost"] >= wasted_min and r["hon_cv"] == 0:
            wasted_cost.append(_flag(
                r, "無駄コスト", "高",
                action="停止 or マッチタイプ変更",
                reason=f"費用 ¥{r['cost']:,} / 本CV 0件",
            ))

        # 伸びしろ: 本CVあり & CPA ≤ 目標×70% & クリック10以上
        if (cpa is not None
                and r["clicks"] >= growth_clicks
                and (target_cpa == 0 or cpa <= target_cpa * growth_ratio)):
            ratio = (cpa / target_cpa * 100) if target_cpa > 0 else 0
            growth.append(_flag(
                r, "伸びしろ", "中",
                action="予算増額 / 入札引き上げ",
                reason=f"CPA ¥{int(cpa):,}（目標の {ratio:.0f}%）/ クリック {r['clicks']}",
            ))

        # 低CTR 強警戒: CTR < 1%
        if r["impressions"] > 0 and r["ctr"] < ctr_alert:
            low_ctr_alert.append(_flag(
                r, "低CTR（強警戒）", "高",
                action="広告文の見直し・キーワード除外を検討",
                reason=f"CTR {r['ctr']:.2f}%（{ctr_alert}%未満）",
            ))

        # 低CTR 要確認: 1% ≤ CTR < 3%
        elif r["impressions"] > 0 and r["ctr"] < ctr_watch:
            low_ctr_watch.append(_flag(
                r, "低CTR（要確認）", "中",
                action="広告文改善の検討",
                reason=f"CTR {r['ctr']:.2f}%（{ctr_watch}%未満）",
            ))

    # 各カテゴリ内のソート
    high_cpa.sort(key=lambda x: x["cpa"], reverse=True)
    wasted_cost.sort(key=lambda x: x["cost"], reverse=True)
    growth.sort(key=lambda x: x["cpa"])
    low_ctr_alert.sort(key=lambda x: x["ctr"])
    low_ctr_watch.sort(key=lambda x: x["ctr"])

    return {
        "date":           date.today().strftime("%Y-%m-%d"),
        "high_cpa":       high_cpa,
        "wasted_cost":    wasted_cost,
        "growth":         growth,
        "low_ctr_alert":  low_ctr_alert,
        "low_ctr_watch":  low_ctr_watch,
    }


# ---------------------------------------------------------------------------
# 3. Sheets 出力（日付ごとに追記）
# ---------------------------------------------------------------------------

REPORT_HEADER = [
    "日付", "プラットフォーム", "分析区分", "優先度", "推奨アクション", "理由",
    "キャンペーン", "広告グループ", "キーワード", "マッチタイプ",
    "費用(円)", "クリック数", "CTR(%)", "本CV", "CPA(円)",
]

# 出力順: CPA高騰 → 無駄コスト → 伸びしろ → 低CTR強警戒 → 低CTR要確認
SECTIONS = [
    "high_cpa",
    "wasted_cost",
    "growth",
    "low_ctr_alert",
    "low_ctr_watch",
]


def write_report(sh, results: dict, config: dict) -> int:
    """分析レポートタブに追記する。初回のみヘッダーを書き込む。"""
    tab_name = config.get("sheet", {}).get("report_tab", "分析レポート")

    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=2000, cols=len(REPORT_HEADER))
        ws.append_row(REPORT_HEADER)
        ws.freeze(rows=1)

    today = results["date"]
    rows_to_append = []

    for key in SECTIONS:
        for r in results[key]:
            cpa_val = r.get("cpa")
            rows_to_append.append([
                today,
                r["platform"],
                r["category"],
                r["priority"],
                r["action"],
                r["reason"],
                r["campaign"],
                r["ad_group"],
                r["keyword"],
                r["match_type"],
                r["cost"],
                r["clicks"],
                r["ctr"],
                r["hon_cv"],
                int(cpa_val) if cpa_val is not None else "",
            ])

    if rows_to_append:
        ws.append_rows(rows_to_append)

    return len(rows_to_append)


# ---------------------------------------------------------------------------
# 4. Slack 通知（高優先度のみ）
# ---------------------------------------------------------------------------

def notify_slack(results: dict, config: dict):
    """無駄コスト・CPA悪化・伸びしろ上位3件のみ Slack に送信する。低CTRは送らない。"""
    import os
    webhook_url = os.getenv("SLACK_WEBHOOK_URL") or config.get("slack_webhook_url", "")
    if not webhook_url:
        return

    wasted   = results["wasted_cost"]
    high_cpa = results["high_cpa"]
    growth   = results["growth"][:3]

    if not wasted and not high_cpa and not growth:
        return

    target_cpa = config.get("analysis", {}).get("target_cpa", 0)
    today = results["date"]
    lines = [f"*📊 Google Ads レポート（{today}）*"]

    # ① 無駄コスト
    if wasted:
        total_wasted = sum(r["cost"] for r in wasted)
        lines.append(f"\n*🚨 無駄コスト  {len(wasted)}件｜合計 ¥{total_wasted:,}*")
        for r in wasted[:5]:
            lines.append(f"  • {r['keyword']}  ｜  ¥{r['cost']:,}  ｜  CV 0件  →  停止 or マッチタイプ変更")
        if len(wasted) > 5:
            lines.append(f"  ほか {len(wasted) - 5} 件")

    # ② CPA悪化
    if high_cpa:
        lines.append(f"\n*⚠️ CPA悪化  {len(high_cpa)}件*")
        for r in high_cpa[:5]:
            cpa = int(r["cpa"])
            lines.append(
                f"  • {r['keyword']}  ｜  CPA ¥{cpa:,}（目標 ¥{target_cpa:,}）｜  CV {r['hon_cv']}件  ｜  費用 ¥{r['cost']:,}"
            )
        if len(high_cpa) > 5:
            lines.append(f"  ほか {len(high_cpa) - 5} 件")

    # ③ 伸びしろ TOP3
    if growth:
        lines.append(f"\n*💡 伸びしろ TOP3*")
        for r in growth:
            cpa = int(r["cpa"])
            ratio = int(cpa / target_cpa * 100) if target_cpa > 0 else 0
            lines.append(
                f"  • {r['keyword']}  ｜  CPA ¥{cpa:,}（目標の {ratio}%）｜  CV {r['hon_cv']}件  ｜  クリック {r['clicks']}"
            )

    requests.post(webhook_url, json={"text": "\n".join(lines)}, timeout=10)


# ---------------------------------------------------------------------------
# 5. エントリーポイント
# ---------------------------------------------------------------------------

def run(keyword_rows: list, sh, config: dict):
    """main.py から呼び出す。keyword_rows は main.py が構築したリストをそのまま渡す。"""
    records = normalize_google_ads(keyword_rows)
    results = run_analysis(records, config)
    count   = write_report(sh, results, config)
    notify_slack(results, config)

    h = len(results["high_cpa"])
    w = len(results["wasted_cost"])
    g = len(results["growth"])
    la = len(results["low_ctr_alert"])
    lw = len(results["low_ctr_watch"])
    print(f"  → 分析レポート: {count}件書き込み"
          f"（CPA高騰:{h} / 無駄コスト:{w} / 伸びしろ:{g}"
          f" / 低CTR強警戒:{la} / 低CTR要確認:{lw}）")
