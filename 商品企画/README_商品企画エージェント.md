# 商品企画エージェント（Claude Code完結版）

`brainstorm.py`（Groq API + Google Sheets版）とは別に、**外部APIキーやGoogle認証を一切使わず、
Claude Code自身が競合分析〜自社商品企画〜シート生成まで完結させる**仕組み。
APIキーやservice_account.jsonが無い環境（このリポジトリをどこにクローンしても）で動作する。

## 使い方
Claude Codeに次のように依頼する。

```
商品テーマ：〇〇
カテゴリ：〇〇
競合ページURL：
1. https://...
2. https://...
3. https://...

claude/agents/product-planner.md に沿って競合分析と自社商品企画を提案してください。
```

## 処理の流れ
1. Claudeが各URLをWebFetchで取得し、競合分析（商品特長・メイン訴求・選ばれる理由・不満な声など）を行う
2. 市場調査・自社商品コンセプト（参入余地・メイン訴求・商品の強み・商品形状・想定売り場・売価／容量・キー成分・ボトルネックとそれを超える策・解雇条件）を設計する
3. 分析結果をJSONにまとめ、`generate_concept_sheet.py` を実行して `.xlsx` を生成する
   ```
   python 商品企画/generate_concept_sheet.py <入力JSON>
   ```
4. 生成物は `商品企画/output/` に保存される（Gitには含めない。企画内容は機密情報のため）

## 必要なもの
- Python 3 と `openpyxl`（`pip install openpyxl`）のみ
- APIキー・サービスアカウント・Googleアカウントは不要

## ファイル構成
- `template/商品コンセプトシート_フォーマット.xlsx` … 出力テンプレート（項目名のみの空欄フォーマット）
- `generate_concept_sheet.py` … JSON→xlsx変換スクリプト（入力JSONの形式はスクリプト冒頭のdocstring参照）
- `../claude/agents/product-planner.md` … 分析・提案の役割定義（進め方・出力項目の仕様）
