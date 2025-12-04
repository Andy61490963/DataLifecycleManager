# MVC 版資料歸檔與搬移流程說明

本專案已改寫為 ASP.NET Core 8 MVC 單一 Web 專案。所有搬移參數改由 Web 介面填寫並寫入設定資料表，使用者在需要時按下「開始搬移」才會同步執行一次 DB1 → DB2 → CSV → Delete 的流程。

## 專案分層
- **Controllers**：`ArchiveSettingsController` 提供設定維護與觸發搬移的 Action。
- **ViewModels**：`ArchiveSettingInputModel`、`ArchiveSettingsPageViewModel` 對應設定表單、列表與執行結果。
- **Repositories**：`ArchiveSettingRepository` 以 Dapper 讀寫設定資料表（ConnectionName=ConfigurationDb）。
- **Services**：`ArchiveExecutionService` 執行搬移主流程，`RetryPolicyExecutor` 提供可設定的重試機制，`SqlConnectionFactory` 依連線名稱建立 `SqlConnection`。
- **Views**：`Views/ArchiveSettings/Index.cshtml` 使用 Bootstrap 呈現設定表單、設定列表與「開始搬移」按鈕。
- **Configuration**：`CsvOptions` 與 `RetryPolicySettings` 為固定參數（分隔符、檔名格式、重試次數等），透過 `appsettings.json` 載入。

## 設定資料表建議結構
```sql
CREATE TABLE dbo.ArchiveSettings
(
    Id INT IDENTITY(1,1) PRIMARY KEY,
    SourceConnectionName NVARCHAR(100) NOT NULL,
    TargetConnectionName NVARCHAR(100) NOT NULL,
    TableName            NVARCHAR(128) NOT NULL,
    DateColumn           NVARCHAR(128) NOT NULL,
    PrimaryKeyColumn     NVARCHAR(128) NOT NULL,
    OnlineRetentionDate    DATE NOT NULL,
    HistoryRetentionDate   DATE NOT NULL,
    BatchSize              INT NOT NULL,
    CsvEnabled             BIT NOT NULL,
    CsvRootFolder          NVARCHAR(512) NOT NULL,
    Enabled                BIT NOT NULL
);
```

## 執行流程
1. **儲存設定**：使用者於 `/ArchiveSettings/Index` 填完表單後提交，`ArchiveSettingsController.Save` 透過 `ArchiveSettingRepository.UpsertAsync` 寫入設定表。
2. **啟用 / 停用**：每筆設定帶有 `Enabled`，`ArchiveExecutionService` 只會對啟用的設定執行搬移，未啟用的設定會直接略過並回報提示訊息。
3. **開始搬移**：按下「開始搬移」按鈕後，`ArchiveSettingsController.Run` 呼叫 `ArchiveExecutionService.RunOnceAsync`。流程中若任一表發生例外，會記錄詳細錯誤並回傳到頁面顯示。
   - 前端送出「開始搬移」時會透過 SweetAlert2 顯示不可關閉的 Loading 視窗（含 Spinner），流程結束後再依成功 / 失敗彈出提示並呈現訊息。
4. **搬移線上庫 → 歷史庫**：`ArchiveExecutionService` 依設定抓取「早於 `OnlineRetentionDate`」的資料，使用 `DynamicSqlHelper.BuildInsertSql`（內含 `NOT EXISTS`）寫入目標庫，再以 `IN (@Ids)` 批次刪除來源庫資料。
5. **歷史庫 → CSV**：若 `CsvEnabled`，則針對「早於 `HistoryRetentionDate`」的歷史資料批次匯出 CSV（依 `CsvOptions.MaxRowsPerFile` 分段，檔名使用 `CsvOptions.FileNameFormat`），成功後以 `IN (@Ids)` 刪除目標庫批次。
6. **重試與日誌**：每個批次包在 `RetryPolicyExecutor` 中，依 `RetryPolicySettings` 決定重試次數與間隔；Serilog 透過 `AppLoggingOptions` 設定輸出。

## 重要實作注意事項
- **無背景排程**：不再註冊 `BackgroundService`，所有搬移皆由使用者手動觸發。
- **Dapper 操作**：查詢、插入、刪除全數使用 Dapper 的 `CommandDefinition`，並透過 `SqlConnectionFactory` 以連線名稱存取不同資料庫。
- **避免分散式交易**：不使用 `TransactionScope`；採取「先插入目標庫，再刪除來源庫」的順序，並以冪等 `NOT EXISTS` 保證重複執行安全。
- **CSV 安全性**：匯出時自動建立目錄，值會依分隔符號跳脫並以 UTF-8 BOM 編碼輸出。

## 前端互動
- 使用 Bootstrap 5 表單與表格，提供基本的前端驗證（jQuery Validate）。
- 設定列表提供「編輯」與「刪除」按鈕，編輯時會回填表單並可切換啟用 / 停用旗標。
- 來源 / 目標連線欄位允許直接輸入連線名稱或完整連線字串（例如 `Server=localhost;Database=DB1;...`）。
- 最新執行結果會在右側卡片顯示，包含成功 / 失敗標示與訊息列表，例外內容會直接呈現以利排錯。

## 延伸建議
- 可將「開始搬移」改成發送 BackgroundJob（例如 Hangfire）以避免長時間佔用要求，若日後需要非同步化。
- 可加入設定層級的角色權限或審核流程，避免誤刪 / 誤停用關鍵設定。
