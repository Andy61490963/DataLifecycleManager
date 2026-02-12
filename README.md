# MVC 版資料歸檔與搬移流程說明

本專案為 ASP.NET Core 8 MVC 單一 Web 專案。
所有搬移參數改由 Web 介面填寫並寫入設定資料表，使用者在需要時按下「開始搬移」才會同步執行一次 DB1 → DB2 → CSV → Delete 的流程。

## 使用方法(只說重點)

### 1. 設定資料庫連線字串

輸入來源（Source DB）與目標（Target DB）資料庫的連線字串，例如：

```
Server=127.0.0.1,1433;Database=MyDb;User Id=sa;Password=myPassword;TrustServerCertificate=True;
```

* * *

### 2. 設定資料表資訊

請設定：

* 搬移資料表名稱（TableName）

* 主鍵欄位名稱（PrimaryKeyColumn）

* 日期判斷欄位名稱（DateColumn）


> 系統將以 `DateColumn` 作為資料搬移與匯出的判斷依據。

* * *

### 3. 設定資料保留截止日期

需設定兩個日期：

* **線上資料保留截止日期（OnlineRetentionDate）**

* **歷史資料保留截止日期（HistoryRetentionDate）**


⚠ **重要規則：**

```
OnlineRetentionDate 必須晚於 HistoryRetentionDate
```

* * *

## 判斷邏輯說明

系統以 `DateColumn < 截止日期` 作為判斷條件  
（僅比較日期部分，不含時間）

資料生命週期如下：

```
Date >= OnlineRetentionDate
    → 保留在線上資料庫（Source DB）

HistoryRetentionDate <= Date < OnlineRetentionDate
    → 存放於歷史資料庫（Target DB）

Date < HistoryRetentionDate
    → 匯出為 CSV，並從歷史資料庫刪除
```

* * *

## 範例說明

```
現在時間                  = 2025-07-01
線上資料保留截止日期        = 2025-01-01
歷史資料保留截止日期        = 2024-01-01
```

則：

| 資料日期 | 狀態 |
| --- | --- |
| 2025-03-01 | 存於線上（Source DB） |
| 2024-06-01 | 存於歷史（Target DB） |
| 2023-05-01 | 已匯出為 CSV |
* * *

## 專案分層
- **Controllers**：`ArchiveSettingsController` 提供設定維護與觸發搬移的 Action。
- **ViewModels**：`ArchiveSettingInputModel`、`ArchiveSettingsPageViewModel` 對應設定表單、列表與執行結果。
- **Repositories**：`ArchiveSettingRepository` 以 Dapper 讀寫設定資料表（ConnectionName=ConfigurationDb）。
- **Services**：`ArchiveExecutionService` 執行搬移主流程，`RetryPolicyExecutor` 提供可設定的重試機制，`SqlConnectionFactory` 依連線名稱建立 `SqlConnection`。
- **Views**：`Views/ArchiveSettings/Index.cshtml` 使用 Bootstrap 呈現設定表單、設定列表與「開始搬移」按鈕。
- **Configuration**：`CsvOptions` 與 `RetryPolicySettings` 為固定參數（分隔符、檔名格式、重試次數等），透過 `appsettings.json` 載入。

## 設定資料表建議結構
```sql
USE []
GO
/****** Object:  Table [dbo].[ArchiveJobRun] ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ArchiveJobRun](
   [JobRunId] [uniqueidentifier] NOT NULL,
   [StartedAt] [datetime] NOT NULL,
   [EndedAt] [datetime] NULL,
   [Status] [nvarchar](max) NOT NULL,
   [HostName] [nvarchar](max) NULL,
   [TotalTables] [int] NOT NULL,
   [SucceededTables] [int] NOT NULL,
   [FailedTables] [int] NOT NULL,
   [Message] [nvarchar](max) NULL,
   CONSTRAINT [PK_ArchiveJobRun] PRIMARY KEY CLUSTERED
(
[JobRunId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
   ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
   GO
/****** Object:  Table [dbo].[ArchiveJobRunDetail] ******/
   SET ANSI_NULLS ON
   GO
   SET QUOTED_IDENTIFIER ON
   GO
CREATE TABLE [dbo].[ArchiveJobRunDetail](
   [TableRunId] [uniqueidentifier] NOT NULL,
   [JobRunId] [uniqueidentifier] NOT NULL,
   [SettingId] [int] NOT NULL,
   [SourceConnectionName] [nvarchar](max) NOT NULL,
   [TargetConnectionName] [nvarchar](max) NOT NULL,
   [TableName] [nvarchar](max) NOT NULL,
   [DateColumn] [nvarchar](max) NOT NULL,
   [PrimaryKeyColumn] [sysname] NOT NULL,
   [OnlineRetentionDate] [nvarchar](max) NOT NULL,
   [HistoryRetentionDate] [date] NOT NULL,
   [BatchSize] [int] NOT NULL,
   [CsvEnabled] [bit] NOT NULL,
   [CsvRootFolder] [nvarchar](max) NOT NULL,
   [IsPhysicalDeleteEnabled] [bit] NOT NULL,
   [StartedAt] [datetime] NOT NULL,
   [EndedAt] [datetime] NULL,
   [Status] [nvarchar](max) NOT NULL,
   [TotalSourceScanned] [int] NOT NULL,
   [TotalInsertedToHistory] [int] NOT NULL,
   [TotalDeletedFromSource] [int] NULL,
   [TotalExportedToCsv] [int] NOT NULL,
   [TotalDeletedFromHistory] [int] NULL,
   [LastProcessedDate] [datetime] NULL,
   [LastProcessedPrimaryKey] [nvarchar](max) NULL,
   [ErrorMessage] [nvarchar](max) NULL,
   CONSTRAINT [PK_ArchiveJobRunDetail] PRIMARY KEY CLUSTERED
(
[TableRunId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
   ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
   GO
/****** Object:  Table [dbo].[ArchiveSettings] ******/
   SET ANSI_NULLS ON
   GO
   SET QUOTED_IDENTIFIER ON
   GO
CREATE TABLE [dbo].[ArchiveSettings](
   [Id] [int] IDENTITY(1,1) NOT NULL,
   [SourceConnectionName] [nvarchar](max) NOT NULL,
   [TargetConnectionName] [nvarchar](max) NOT NULL,
   [TableName] [nvarchar](max) NOT NULL,
   [DateColumn] [nvarchar](max) NOT NULL,
   [PrimaryKeyColumn] [nvarchar](max) NOT NULL,
   [OnlineRetentionDate] [date] NOT NULL,
   [HistoryRetentionDate] [date] NOT NULL,
   [BatchSize] [int] NOT NULL,
   [CsvEnabled] [bit] NOT NULL,
   [CsvRootFolder] [nvarchar](max) NOT NULL,
   [IsPhysicalDeleteEnabled] [bit] NOT NULL,
   [Enabled] [bit] NOT NULL,
   CONSTRAINT [PK_ArchiveSettings] PRIMARY KEY CLUSTERED
(
[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
   ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
   GO
ALTER TABLE [dbo].[ArchiveJobRun] ADD  CONSTRAINT [DF_ArchiveJobRun_Status]  DEFAULT (N'Running') FOR [Status]
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail] ADD  CONSTRAINT [DF_ArchiveJobRunDetail_CsvRootFolder]  DEFAULT (N'') FOR [CsvRootFolder]
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail] ADD  CONSTRAINT [DF_ArchiveJobRunDetail_Status]  DEFAULT (N'Running') FOR [Status]
   GO
ALTER TABLE [dbo].[ArchiveSettings] ADD  CONSTRAINT [DF_ArchiveSettings_BatchSize]  DEFAULT ((0)) FOR [BatchSize]
   GO
ALTER TABLE [dbo].[ArchiveSettings] ADD  CONSTRAINT [DF_ArchiveSettings_CsvEnabled]  DEFAULT ((0)) FOR [CsvEnabled]
   GO
ALTER TABLE [dbo].[ArchiveSettings] ADD  CONSTRAINT [DF_ArchiveSettings_CsvRootFolder]  DEFAULT (N'') FOR [CsvRootFolder]
   GO
ALTER TABLE [dbo].[ArchiveSettings] ADD  CONSTRAINT [DF_ArchiveSettings_IsPhysicalDeleteEnabled]  DEFAULT ((1)) FOR [IsPhysicalDeleteEnabled]
   GO
ALTER TABLE [dbo].[ArchiveSettings] ADD  CONSTRAINT [DF_ArchiveSettings_Enabled]  DEFAULT ((1)) FOR [Enabled]
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail]  WITH CHECK ADD  CONSTRAINT [FK_ArchiveJobRunDetail_ArchiveJobRun] FOREIGN KEY([JobRunId])
   REFERENCES [dbo].[ArchiveJobRun] ([JobRunId])
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail] CHECK CONSTRAINT [FK_ArchiveJobRunDetail_ArchiveJobRun]
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail]  WITH CHECK ADD  CONSTRAINT [FK_ArchiveJobRunDetail_ArchiveSettings] FOREIGN KEY([SettingId])
   REFERENCES [dbo].[ArchiveSettings] ([Id])
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail] CHECK CONSTRAINT [FK_ArchiveJobRunDetail_ArchiveSettings]
   GO
ALTER TABLE [dbo].[ArchiveJobRun]  WITH CHECK ADD  CONSTRAINT [CK_ArchiveJobRun_Status] CHECK  (([Status]=N'Skipped' OR [Status]=N'Fail' OR [Status]=N'PartialFail' OR [Status]=N'Success' OR [Status]=N'Running'))
   GO
ALTER TABLE [dbo].[ArchiveJobRun] CHECK CONSTRAINT [CK_ArchiveJobRun_Status]
   GO
ALTER TABLE [dbo].[ArchiveJobRun]  WITH CHECK ADD  CONSTRAINT [CK_ArchiveJobRun_TableCounters] CHECK  (([TotalTables]>=(0) AND [SucceededTables]>=(0) AND [FailedTables]>=(0) AND ([SucceededTables]+[FailedTables])<=[TotalTables]))
   GO
ALTER TABLE [dbo].[ArchiveJobRun] CHECK CONSTRAINT [CK_ArchiveJobRun_TableCounters]
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail]  WITH CHECK ADD  CONSTRAINT [CK_ArchiveJobRunDetail_Counters] CHECK  (([TotalSourceScanned]>=(0) AND [TotalInsertedToHistory]>=(0) AND [TotalExportedToCsv]>=(0) AND ([TotalDeletedFromSource] IS NULL OR [TotalDeletedFromSource]>=(0)) AND ([TotalDeletedFromHistory] IS NULL OR [TotalDeletedFromHistory]>=(0)) AND [BatchSize]>=(0)))
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail] CHECK CONSTRAINT [CK_ArchiveJobRunDetail_Counters]
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail]  WITH CHECK ADD  CONSTRAINT [CK_ArchiveJobRunDetail_CsvRootFolder_WhenEnabled] CHECK  (([CsvEnabled]=(0) OR len(ltrim(rtrim([CsvRootFolder])))>(0)))
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail] CHECK CONSTRAINT [CK_ArchiveJobRunDetail_CsvRootFolder_WhenEnabled]
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail]  WITH CHECK ADD  CONSTRAINT [CK_ArchiveJobRunDetail_Status] CHECK  (([Status]=N'Skipped' OR [Status]=N'Fail' OR [Status]=N'PartialFail' OR [Status]=N'Success' OR [Status]=N'Running'))
   GO
ALTER TABLE [dbo].[ArchiveJobRunDetail] CHECK CONSTRAINT [CK_ArchiveJobRunDetail_Status]
   GO
ALTER TABLE [dbo].[ArchiveSettings]  WITH CHECK ADD  CONSTRAINT [CK_ArchiveSettings_BatchSize_NonNegative] CHECK  (([BatchSize]>=(0)))
   GO
ALTER TABLE [dbo].[ArchiveSettings] CHECK CONSTRAINT [CK_ArchiveSettings_BatchSize_NonNegative]
   GO
ALTER TABLE [dbo].[ArchiveSettings]  WITH CHECK ADD  CONSTRAINT [CK_ArchiveSettings_CsvRootFolder_WhenEnabled] CHECK  (([CsvEnabled]=(0) OR len(ltrim(rtrim([CsvRootFolder])))>(0)))
   GO
ALTER TABLE [dbo].[ArchiveSettings] CHECK CONSTRAINT [CK_ArchiveSettings_CsvRootFolder_WhenEnabled]
   GO

```

## 執行流程
1. **儲存設定**：使用者於 `/ArchiveSettings/Index` 填完表單後提交，`ArchiveSettingsController.Save` 透過 `ArchiveSettingRepository.UpsertAsync` 寫入設定表。
2. **啟用 / 停用**：每筆設定帶有 `Enabled`，`ArchiveExecutionService` 只會對啟用的設定執行搬移，未啟用的設定會直接略過並回報提示訊息。
3. **開始搬移**：按下「開始搬移」按鈕後，`ArchiveSettingsController.Run` 呼叫 `ArchiveExecutionService.RunOnceAsync`。流程中若任一表發生例外，會記錄詳細錯誤並回傳到頁面顯示。
   - 前端送出「開始搬移」時會透過 SweetAlert2 顯示不可關閉的 Loading 視窗（含 Spinner），流程結束後再依成功 / 失敗彈出提示並呈現訊息。
4. **搬移線上庫 → 歷史庫**：`ArchiveExecutionService` 依設定抓取「早於 `OnlineRetentionDate`」的資料，使用 `DynamicSqlHelper.BuildInsertSql`（內含 `NOT EXISTS`）寫入目標庫，再以 `IN (@Ids)` 批次刪除來源庫資料。
5. **歷史庫 → CSV**：若 `CsvEnabled`，則針對「早於 `HistoryRetentionDate`」的歷史資料批次匯出 CSV（依 `CsvOptions.MaxRowsPerFile` 分段，檔名使用 `CsvOptions.FileNameFormat`），成功後以 `IN (@Ids)` 刪除目標庫批次。
6. **重試與日誌**：每個批次包在 `RetryPolicyExecutor` 中，依 `RetryPolicySettings` 決定重試次數與間隔；Serilog 透過 `AppLoggingOptions` 設定輸出。

## 前端互動
- 使用 Bootstrap 5 表單與表格，提供基本的前端驗證（jQuery Validate）。
- 設定列表提供「編輯」與「刪除」按鈕，編輯時會回填表單並可切換啟用 / 停用旗標。
- 來源 / 目標連線欄位允許直接輸入連線名稱或完整連線字串（例如 `Server=localhost;Database=DB1;...`）。
- 最新執行結果會在右側卡片顯示，包含成功 / 失敗標示與訊息列表，例外內容會直接呈現以利排錯。