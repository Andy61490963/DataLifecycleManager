# 資料歸檔程式碼運作流程

本文件以目前提交的程式碼為基礎，說明 Worker 與後台設定 UI 的運作方式與模組邏輯。

## 模組與責任

- `Domain`：定義可重用的設定模型（`ArchiveJobDefinition`、`ArchiveJobRuntimeSettings`）與介面 `IArchiveSettingsProvider`、`IArchiveJobRepository`。
- `Configuration/ArchiveDefaultsOptions`：提供全域預設值，當 DB 未給定時仍能安全執行。
- `Infrastructure/Repositories/ArchiveJobRepository`：透過 Dapper 從管理資料庫讀取 `ArchiveJob`、`ArchiveJobRetryPolicy` 設定。
- `Infrastructure/Providers/DbArchiveSettingsProvider`：將資料庫設定與全域預設合併，輸出執行期可用的 `ArchiveJobRuntimeSettings` 集合。
- `Services/ArchiveCoordinator`：核心批次流程，依設定抓批次、搬移、匯出 CSV、刪除，並以 `RetryPolicyExecutor` 保護。
- `WebAdmin`（獨立 Web 專案）：提供 MVC CRUD 介面（`ArchiveJobsController` + `ArchiveJobViewModel`），可直接維護管理庫設定。

## 執行流程

1. **設定載入**：`DbArchiveSettingsProvider` 透過 `IArchiveJobRepository.GetEnabledJobsAsync` 取得啟用任務，並合併 `ArchiveDefaultsOptions` 的批次大小、保留月數、CSV 根目錄與重試策略。
2. **Worker 啟動**：`Program` 註冊 `ArchiveCoordinator`、`DbArchiveSettingsProvider`、`RetryPolicyExecutor`，週期性由 `Worker` 呼叫 `RunOnceAsync`。
3. **搬移線上資料**：`ArchiveCoordinator` 的 `ArchiveOnlineAsync` 依 `OnlineRetentionMonths` 篩選來源庫批次資料，使用 `MoveBatchAsync` 先插入目標庫再刪除來源，避免 MSDTC。
4. **歷史 CSV 匯出**：若任務啟用 CSV，`ExportHistoryAsync` 以 `HistoryRetentionMonths` 篩選歷史庫，`WriteCsvFilesAsync` 依 `MaxRowsPerFile` 分段寫檔後呼叫 `DeleteBatchAsync` 清除資料。
5. **重試控制**：每個任務在 Archive/CSV 兩段皆以 `RetryPolicyExecutor` 包裝，優先採任務重試設定，否則回退全域預設。
6. **後台 CRUD**：`ArchiveJobsController` 提供 Index/Create/Edit/Delete，透過 `SqlConnectionFactory` 和 Dapper 寫入/更新 `ArchiveJob` 與 `ArchiveJobRetryPolicy` 表。

## 效能與風險防範

- **批次控制**：`ChunkRows` 以批次大小分段，避免記憶體暴衝；CSV 亦以 `MaxRowsPerFile` 切檔。
- **交易風險**：搬移流程先寫目標庫再刪來源庫，避免跨庫交易升級 MSDTC。
- **資料驗證**：`ArchiveJobViewModel` 採用 DataAnnotations；`ArchiveJobDefinition`/`RuntimeSettings` 也以屬性限制確保資料正確性。
- **錯誤重試**：重試間隔與次數可由後台指定；若後台未填則使用預設值，確保即時恢復又避免無限循環。

## 相關檔案

- Worker 流程：`Services/ArchiveCoordinator.cs`
- 設定模型：`Domain/ArchiveJobDefinition.cs`
- 設定 Provider：`Infrastructure/Providers/DbArchiveSettingsProvider.cs`
- 後台 MVC：`WebAdmin/Controllers/ArchiveJobsController.cs`、`WebAdmin/Models/ArchiveJobViewModel.cs`
- 全域預設：`Configuration/ArchiveDefaultsOptions.cs`
