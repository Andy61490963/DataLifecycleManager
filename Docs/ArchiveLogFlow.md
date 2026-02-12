# Archive Log 頁面與資料流程說明

此頁面提供搬移執行紀錄的查詢介面，使用者可透過 Bootstrap Tabs 在「Archive Settings」與「Archive Log」間切換。

## 資料來源與組裝

1. `ArchiveJobLogService.GetJobRunsWithDetailsAsync` 以 Dapper 一次查詢 `ArchiveJobRun` 與 `ArchiveJobRunDetail` 兩張表。
2. 服務將結果分組後組成 `ArchiveJobRunLogWithDetails`，包含整體工作與底下所有資料表的執行明細。
3. `ArchiveLogController.Index` 注入上述服務，將組裝好的 `IEnumerable<ArchiveJobRunLogWithDetails>` 傳入 Razor View。

## Razor View 呈現邏輯

- 使用 `_ArchiveTabs` partial 產生 Bootstrap nav-tabs，標記目前頁籤 active 狀態。
- 主列表以卡片呈現每筆 `ArchiveJobRunLog` 的基本資訊（時間、狀態、來源主機、表數量等）。
- 每筆卡片內使用 Bootstrap Accordion 展開對應的 `ArchiveJobRunDetailLog`，顯示資料表名稱、批次設定、CSV 選項、計數與錯誤訊息等欄位。
- 日期時間使用 `ToLocalTime()` 搭配 `yyyy-MM-dd HH:mm:ss` 以利閱讀。

## 穩定性與效能

- QueryMultiple 一次取回主檔與明細，避免 N+1 連線負擔。
- 以字典分組明細，時間複雜度 O(n) 且維持良好可讀性。
- UI 採用標準 Bootstrap 5 組件，無需額外的前端套件或腳本。
