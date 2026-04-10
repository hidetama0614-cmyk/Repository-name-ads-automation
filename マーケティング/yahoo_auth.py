"""
Yahoo!広告（LINEヤフー広告）リフレッシュトークン取得スクリプト
実行すると自動でブラウザが開き、認証後にリフレッシュトークンが表示されます。

使い方:
  1. LINEヤフー広告の管理画面でリダイレクトURIを「http://localhost:8080」に変更
  2. python マーケティング/yahoo_auth.py を実行
  3. ブラウザでログインして「許可する」をクリック
  4. 表示されたリフレッシュトークンをGitHub Secretsに登録
"""

import webbrowser
import requests
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# ─── 設定 ─────────────────────────────────────────────────────
CLIENT_ID     = "a683d440413c3ff40cb0a8a25ea8d133adfed9f6d60745b6ac730b11e9771457"
CLIENT_SECRET = "d5686cbf87be53bd4097a787da23d8c9c46f15543e80adfbe568f95f05a51434"
REDIRECT_URI  = "http://localhost:8080"

# ─── エンドポイント（LINEヤフー広告 公式URL）───────────────────
AUTH_BASE  = "https://biz-oauth.yahoo.co.jp/oauth"
AUTH_URL   = f"{AUTH_BASE}/v1/authorize"    # 認可URL（/authorize）
TOKEN_URL  = f"{AUTH_BASE}/v1/token"        # トークンURL
REVOKE_URL = f"{AUTH_BASE}/v1/revoke"       # 失効URL

# 認証コードを受け取るための変数
received_code = None


class CallbackHandler(BaseHTTPRequestHandler):
    """ブラウザからのリダイレクトを受け取るミニサーバー"""

    def do_GET(self):
        global received_code
        params = parse_qs(urlparse(self.path).query)

        if "code" in params:
            received_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            html = "<h2>認証に成功しました。このページは閉じてください。</h2>"
            self.wfile.write(html.encode("utf-8"))
        elif "error" in params:
            error = params.get("error", ["不明"])[0]
            desc  = params.get("error_description", [""])[0]
            self.send_response(400)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            html = f"<h2>認証エラー: {error}</h2><p>{desc}</p>"
            self.wfile.write(html.encode("utf-8"))
            print(f"\n[エラー] 認証が拒否されました")
            print(f"  error             : {error}")
            print(f"  error_description : {desc}")
        else:
            self.send_response(400)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # サーバーログを非表示


def exchange_code_for_token(code):
    """
    authorization_code でリフレッシュトークンを取得
    grant_type: authorization_code
    """
    print(f"\n[トークン取得] grant_type=authorization_code")
    print(f"  URL         : {TOKEN_URL}")
    print(f"  redirect_uri: {REDIRECT_URI}")

    res = requests.post(TOKEN_URL, data={
        "grant_type":    "authorization_code",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri":  REDIRECT_URI,
        "code":          code,
    })

    if not res.ok:
        print(f"\n[エラー] トークン取得に失敗しました")
        print(f"  HTTP status : {res.status_code}")
        print(f"  URL         : {TOKEN_URL}")
        print(f"  grant_type  : authorization_code")
        print(f"  レスポンス  : {res.text}")
        return None

    return res.json()


def check_env():
    """必須項目の確認"""
    missing = []
    if not CLIENT_ID:
        missing.append("CLIENT_ID（yahoo_auth.py 内に直接記載）")
    if not CLIENT_SECRET:
        missing.append("CLIENT_SECRET（yahoo_auth.py 内に直接記載）")
    if missing:
        print("[エラー] 以下の設定が空です：")
        for m in missing:
            print(f"  - {m}")
        sys.exit(1)


# ─── メイン処理 ──────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("Yahoo!広告 リフレッシュトークン取得")
    print("=" * 60)

    check_env()

    print()
    print("【事前準備】LINEヤフー広告の管理画面で")
    print("アプリの「リダイレクトURI」を以下に変更してください：")
    print()
    print("  http://localhost:8080")
    print()
    input("変更できたら Enter キーを押してください...")

    # 認可URLを組み立て
    full_auth_url = (
        f"{AUTH_URL}"
        f"?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope=yahooads"
    )

    print(f"\n[認可URL] {full_auth_url}")
    print("\nブラウザを開いています...")
    webbrowser.open(full_auth_url)
    print("ブラウザでYahoo!アカウントにログインして「許可する」をクリックしてください")
    print("（認証完了まで待機中 ... ポート8080で受信）\n")

    # ローカルサーバーで認証コードを1回だけ受け取る
    server = HTTPServer(("localhost", 8080), CallbackHandler)
    server.handle_request()

    if not received_code:
        print("\n[失敗] 認証コードを受け取れませんでした。もう一度試してください。")
        sys.exit(1)

    print(f"[OK] 認証コードを受け取りました")
    token_data = exchange_code_for_token(received_code)

    if token_data:
        refresh_token = token_data.get("refresh_token", "")
        if not refresh_token:
            print("\n[警告] レスポンスにrefresh_tokenが含まれていません")
            print(f"  レスポンス全体: {token_data}")
        else:
            print()
            print("=" * 60)
            print("✓ 取得成功！")
            print("=" * 60)
            print()
            print("【リフレッシュトークン】")
            print("↓ GitHubのSecrets → YAHOO_ADS_REFRESH_TOKEN に登録してください")
            print()
            print(refresh_token)
            print()
