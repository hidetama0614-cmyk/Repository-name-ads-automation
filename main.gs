// main.gs
// Google Apps Script — スプレッドシートのスクリプトエディタに貼り付けて使用
// Python (main.py) がデータを書き込んだ後、このスクリプトで整形・集計を行う

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('広告レポート')
    .addItem('最新シートを整形', 'formatLatestSheet')
    .addItem('サマリーを更新', 'updateSummary')
    .addSeparator()
    .addItem('自動トリガーを設定（毎日9時）', 'setupDailyTrigger')
    .addToUi();
}

// 本日付のキーワードシートを整形する
function formatLatestSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const today = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd');
  const ws = ss.getSheetByName(today);

  if (!ws) {
    SpreadsheetApp.getUi().alert(
      `シート「${today}」が見つかりません。\nPython スクリプト (main.py) を先に実行してください。`
    );
    return;
  }

  const lastRow = ws.getLastRow();
  const lastCol = ws.getLastColumn();
  if (lastRow < 2) return;

  // ヘッダー行
  const headerRange = ws.getRange(1, 1, 1, lastCol);
  headerRange.setFontWeight('bold')
             .setBackground('#4472C4')
             .setFontColor('#FFFFFF')
             .setHorizontalAlignment('center');

  // データ行の交互背景
  for (let i = 2; i <= lastRow; i++) {
    const color = i % 2 === 0 ? '#EAF0FB' : '#FFFFFF';
    ws.getRange(i, 1, 1, lastCol).setBackground(color);
  }

  // 数値フォーマット
  // E列: 表示回数, F列: クリック数, G列: 費用, H列: CTR, I列: 本CV
  ws.getRange(2, 5, lastRow - 1, 1).setNumberFormat('#,##0');
  ws.getRange(2, 6, lastRow - 1, 1).setNumberFormat('#,##0');
  ws.getRange(2, 7, lastRow - 1, 1).setNumberFormat('¥#,##0');
  ws.getRange(2, 8, lastRow - 1, 1).setNumberFormat('0.00"%"');
  ws.getRange(2, 9, lastRow - 1, 1).setNumberFormat('#,##0');

  // CTR が 1% 未満の行を薄赤でハイライト
  const ctrValues = ws.getRange(2, 8, lastRow - 1, 1).getValues();
  ctrValues.forEach((row, idx) => {
    if (Number(row[0]) < 1.0) {
      ws.getRange(idx + 2, 8).setBackground('#FFE0E0');
    }
  });

  ws.autoResizeColumns(1, lastCol);
  ws.setFrozenRows(1);

  SpreadsheetApp.getUi().alert(`「${today}」の整形が完了しました。`);
}

// 本日付シートからキャンペーン別サマリーを生成する
function updateSummary() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const today = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd');
  const srcSheet = ss.getSheetByName(today);

  if (!srcSheet) {
    SpreadsheetApp.getUi().alert(`シート「${today}」が見つかりません。`);
    return;
  }

  const data = srcSheet.getDataRange().getValues();
  if (data.length < 2) {
    SpreadsheetApp.getUi().alert('データがありません。');
    return;
  }

  // キャンペーン別集計
  const campaignMap = {};
  for (let i = 1; i < data.length; i++) {
    const [campaign, , , , impressions, clicks, cost] = data[i];
    if (!campaignMap[campaign]) {
      campaignMap[campaign] = { impressions: 0, clicks: 0, cost: 0, kwCount: 0 };
    }
    campaignMap[campaign].impressions += Number(impressions);
    campaignMap[campaign].clicks      += Number(clicks);
    campaignMap[campaign].cost        += Number(cost);
    campaignMap[campaign].kwCount     += 1;
  }

  // サマリーシート準備
  let summarySheet = ss.getSheetByName('サマリー');
  if (!summarySheet) {
    summarySheet = ss.insertSheet('サマリー');
  } else {
    summarySheet.clearContents();
  }

  const summaryHeader = ['キャンペーン', '表示回数', 'クリック数', '費用(円)', 'CTR(%)', 'KW数', '更新日時'];
  summarySheet.appendRow(summaryHeader);

  const now = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm');
  Object.entries(campaignMap).forEach(([campaign, s]) => {
    const ctr = s.impressions > 0
      ? Math.round((s.clicks / s.impressions) * 10000) / 100
      : 0;
    summarySheet.appendRow([campaign, s.impressions, s.clicks, s.cost, ctr, s.kwCount, now]);
  });

  const lastRow = summarySheet.getLastRow();
  const lastCol = summarySheet.getLastColumn();

  // 合計行
  summarySheet.appendRow([
    '【合計】',
    `=SUM(B2:B${lastRow})`,
    `=SUM(C2:C${lastRow})`,
    `=SUM(D2:D${lastRow})`,
    `=IFERROR(ROUND(C${lastRow + 1}/B${lastRow + 1}*100,2),0)`,
    `=SUM(F2:F${lastRow})`,
    ''
  ]);
  summarySheet.getRange(lastRow + 1, 1, 1, lastCol)
    .setFontWeight('bold')
    .setBackground('#D9E1F2');

  // 整形
  const headerRange = summarySheet.getRange(1, 1, 1, lastCol);
  headerRange.setFontWeight('bold')
             .setBackground('#4472C4')
             .setFontColor('#FFFFFF')
             .setHorizontalAlignment('center');
  summarySheet.getRange(2, 2, lastRow - 1, 1).setNumberFormat('#,##0');
  summarySheet.getRange(2, 3, lastRow - 1, 1).setNumberFormat('#,##0');
  summarySheet.getRange(2, 4, lastRow - 1, 1).setNumberFormat('¥#,##0');
  summarySheet.getRange(2, 5, lastRow - 1, 1).setNumberFormat('0.00"%"');
  summarySheet.autoResizeColumns(1, lastCol);
  summarySheet.setFrozenRows(1);

  SpreadsheetApp.getUi().alert('サマリーシートを更新しました。');
}

// 毎日9時に formatLatestSheet を自動実行するトリガーを設定
function setupDailyTrigger() {
  // 重複登録を防ぐため既存トリガーを削除
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'formatLatestSheet') {
      ScriptApp.deleteTrigger(t);
    }
  });

  ScriptApp.newTrigger('formatLatestSheet')
    .timeBased()
    .atHour(9)
    .everyDays(1)
    .inTimezone('Asia/Tokyo')
    .create();

  SpreadsheetApp.getUi().alert('毎日9時（JST）に自動整形するトリガーを設定しました。');
}
