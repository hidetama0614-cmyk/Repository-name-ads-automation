"""
meta_frequency_alert.py — Meta広告のフリークエンシーを毎日監視してSlack通知するスクリプト

処理の流れ:
    1. Meta Marketing API から直近7日間のキャンペーン別フリークエンシーを取得
    2. スプレッドシートに記録
    3. フリークエンシーが2.5以上のキャンペーンがあればSlackにアラート通知
"""

import os
import re
import json
import requests
from datetime import date, timedelta
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()

META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
ACCOUNT_ID        = "act_2212511425637711"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
SPREADSHEET_ID    = "1IyZDqKKumh2CY6x152lWGArB1v9qQSJIlmzFtOvRoAA"
SHEET_NAME        = "フリークエンシー記録"
FREQUENCY_THRESHOLD = 3.5
API_VERSION       = "v25.0"


# ---------------------------------------------------------------------------
# 1. Meta API からデータ取得
# ---------------------------------------------------------------------------

def fetch_campaign_frequency():
    """直近7日間のキャンペーン別フリークエンシーを取得する。"""
    today = date.today()
    since = (today - timedelta(days=7)).isoformat()
    until = (today - timedelta(days=1)).isoformat()

    url = f"https://graph.facebook.com/{API_VERSION}/{ACCOUNT_ID}/insights"
    params = {
        "access_token": META_ACCESS_TOKEN,
        "level": "campaign",
        "fields": "campaign_id,campaign_name,frequency,reach,impressions,spend",
        "time_range": json.dumps({"since": since, "until": until}),
        "limit": 100,
    }

    response = requests.get(url, params=params)
    result = response.json()

    if "error" in result:
        raise Exception(f"Meta API エラー: {result['error']}")

    return result.get("data", []), since, until


def _clean_ad_name(name):
    """広告名を見やすく整形する。"""
    # 末尾の「 YYYY-MM-DD-英数字32文字」を除去
    name = re.sub(r"\s+\d{4}-\d{2}-\d{2}-[a-f0-9]{32}$", "", name).strip()
    # カタログ広告のテンプレート変数を日本語に置換
    name = name.replace("{{product.name}}", "カタログ広告")
    return name


def fetch_creative_names(campaign_id):
    """キャンペーンIDから配下の広告名を取得する。重複除去して最大5件。"""
    url = f"https://graph.facebook.com/{API_VERSION}/{campaign_id}/ads"
    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": "name",
        "limit": 50,
    }
    try:
        response = requests.get(url, params=params)
        result = response.json()
        if "error" in result:
            return []
        names = []
        for ad in result.get("data", []):
            name = _clean_ad_name(ad.get("name", ""))
            if name and name not in names:
                names.append(name)
        return names[:5]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# 2. スプレッドシートに記録
# ---------------------------------------------------------------------------

def save_to_spreadsheet(campaigns, checked_date):
    """スプレッドシートにデータを追記する。シートがなければ自動作成する。"""
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    try:
        sheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=10)
        sheet.append_row([
            "確認日", "キャンペーン名",
            "フリークエンシー(7日)", "リーチ", "インプレッション", "消化金額(円)"
        ])

    for c in campaigns:
        sheet.append_row([
            checked_date,
            c.get("campaign_name", ""),
            round(float(c.get("frequency", 0)), 2),
            int(c.get("reach", 0)),
            int(c.get("impressions", 0)),
            round(float(c.get("spend", 0))),
        ])


# ---------------------------------------------------------------------------
# 3. Slack 通知
# ---------------------------------------------------------------------------

def send_slack(alert_campaigns, all_campaigns, since, until):
    """Slackに通知を送る。アラートあり・なしで内容を変える。"""
    today = date.today().strftime("%Y/%m/%d")
    period = f"{since} 〜 {until}（直近7日間）"

    if alert_campaigns:
        lines = [
            f"⚠️ *【Meta広告】フリークエンシーアラート* — {today}",
            f"閾値（{FREQUENCY_THRESHOLD}）以上のキャンペーンがあります",
            f"集計期間：{period}",
            "",
        ]
        for c in alert_campaigns:
            freq  = round(float(c.get("frequency", 0)), 2)
            spend = round(float(c.get("spend", 0)))
            lines.append(f"🔴 *{c['campaign_name']}*")
            lines.append(f"　フリークエンシー：*{freq}*　消化金額：{spend:,}円")
            campaign_id = c.get("campaign_id", "")
            if campaign_id:
                creative_names = fetch_creative_names(campaign_id)
                if creative_names:
                    lines.append("　クリエイティブ名：" + "\n　".join(creative_names))
        lines.append("")
        lines.append("クリエイティブの差し替えを検討してください。")
    else:
        lines = [
            f"✅ *【Meta広告】フリークエンシー確認* — {today}",
            f"全キャンペーンが閾値（{FREQUENCY_THRESHOLD}）以下です",
            f"集計期間：{period}",
            "",
        ]
        for c in all_campaigns:
            freq = round(float(c.get("frequency", 0)), 2)
            lines.append(f"・{c['campaign_name']}：{freq}")

    requests.post(SLACK_WEBHOOK_URL, json={"text": "\n".join(lines)})


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main():
    print("Meta広告フリークエンシー確認を開始...")

    campaigns, since, until = fetch_campaign_frequency()
    print(f"  取得キャンペーン数：{len(campaigns)}")

    save_to_spreadsheet(campaigns, date.today().isoformat())
    print("  スプレッドシートに記録しました")

    alert_campaigns = [
        c for c in campaigns
        if float(c.get("frequency", 0)) >= FREQUENCY_THRESHOLD
    ]
    send_slack(alert_campaigns, campaigns, since, until)

    if alert_campaigns:
        print(f"  ⚠️ アラート送信：{len(alert_campaigns)}件が閾値超え")
    else:
        print("  ✅ 全キャンペーン正常")

    print("完了")


if __name__ == "__main__":
    main()
