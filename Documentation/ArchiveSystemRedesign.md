# 資料歸檔排程系統重構藍圖（可視化設定版）

本文針對現有僅以 appsettings.json 硬寫設定的歸檔排程，提供完整的架構、資料表設計、API/UI、Worker 與設定 Provider 重構方案。所有範例以 .NET 8、C#、Dapper、MS SQL Server 為基礎，並強調可維護、可重用與效能考量。

## 1. Solution / 專案拆分建議

```
DataLifecycleManager.sln
├─ apps
│  ├─ DataLifecycleManager.Worker (背景排程主機，擴充後仍為 Worker Service)
│  └─ DataLifecycleManager.WebAdmin (MVC/Razor 後台，提供 CRUD 設定 UI 與 API)
├─ src
│  ├─ DataLifecycleManager.Domain (設定 Domain Model、介面、Policy、共用常數)
│  ├─ DataLifecycleManager.Application (Use Case / Service：ArchiveJobService、CsvExporter、RetryPolicyExecutor 等)
│  └─ DataLifecycleManager.Infrastructure (Dapper Repository、SqlConnectionFactory、Logging、跨專案的設定 Provider)
└─ docs
   └─ ArchiveSystemRedesign.md (本文件)
```

### 拆分理由
- **模組化與重用**：Domain 定義設定模型與介面；Application 封裝用例，Worker 與 WebAdmin 皆可重用；Infrastructure 集中存取邏輯避免散落。
- **獨立部署**：Worker 與 WebAdmin 可各自部署與擴充，避免互相阻塞。
- **測試友善**：介面 + DI 使得設定 Provider、Repository、Service 容易替換為 InMemory/Stubs 以利自動化測試。

## 2. 設定儲存策略

| 設定 | 儲存位置 | 理由 |
| --- | --- | --- |
| DB 連線字串（來源/目標/後台） | appsettings.json (`ConnectionStrings`) | 部署時由 Ops 管理，安全性與秘密管理可交由 Key Vault/環境變數覆寫。 |
| 全域 CSV RootFolder、預設 BatchSize/Retention | appsettings.json (`ArchiveDefaults`) | 系統級預設，變動頻率低。 |
| 每個歸檔任務的表級設定 | DB | 需要後台 CRUD 與版本控管。 |
| RetryPolicy 預設值（可被任務覆寫） | appsettings.json (`RetryPolicy`) | 全球化預設，方便 Ops 調整。 |
| UI 權限/使用者 | DB | 後台登入/授權。 |

### 建議資料表（SQL Server）
```sql
CREATE TABLE dbo.ArchiveJob (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    JobName NVARCHAR(128) NOT NULL UNIQUE,
    TableName NVARCHAR(256) NOT NULL,
    SourceConnection NVARCHAR(128) NOT NULL, -- 對應 ConnectionStrings key
    TargetConnection NVARCHAR(128) NOT NULL,
    DateColumn NVARCHAR(128) NOT NULL,
    PrimaryKeyColumn NVARCHAR(128) NOT NULL,
    OnlineRetentionMonths INT NOT NULL CHECK (OnlineRetentionMonths > 0),
    HistoryRetentionMonths INT NOT NULL CHECK (HistoryRetentionMonths >= 0),
    BatchSize INT NOT NULL CHECK (BatchSize > 0),
    CsvEnabled BIT NOT NULL DEFAULT(0),
    CsvRootFolder NVARCHAR(400) NULL,
    Enabled BIT NOT NULL DEFAULT(1),
    CreatedAt DATETIME2 NOT NULL DEFAULT (SYSUTCDATETIME()),
    UpdatedAt DATETIME2 NOT NULL DEFAULT (SYSUTCDATETIME())
);

CREATE TABLE dbo.ArchiveJobRetryPolicy (
    JobId INT NOT NULL PRIMARY KEY FOREIGN KEY REFERENCES dbo.ArchiveJob(Id) ON DELETE CASCADE,
    RetryCount INT NOT NULL CHECK (RetryCount >= 0),
    RetryDelaySeconds INT NOT NULL CHECK (RetryDelaySeconds >= 0)
);

CREATE TABLE dbo.ArchiveJobAudit (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    JobId INT NOT NULL FOREIGN KEY REFERENCES dbo.ArchiveJob(Id) ON DELETE CASCADE,
    Status NVARCHAR(32) NOT NULL, -- Started/Success/Failed/Partial
    Message NVARCHAR(4000) NULL,
    StartedAt DATETIME2 NOT NULL DEFAULT (SYSUTCDATETIME()),
    FinishedAt DATETIME2 NULL
);
```

> 若需分層連線權限，可額外建 `ConnectionProfile` 表儲存 Name/ConnectionString，後台僅引用 Name；部署時由 appsettings 或 KeyVault 映射 Name → Secret。

## 3. 後台設定 UI（MVC）

### ViewModel 範例（C#，含資料驗證）
```csharp
/// <summary>
/// 後台設定畫面使用的歸檔任務 ViewModel。
/// </summary>
public class ArchiveJobViewModel
{
    [HiddenInput]
    public int? Id { get; set; }

    [Required, StringLength(128)]
    public string JobName { get; set; } = string.Empty;

    [Required, StringLength(256)]
    public string TableName { get; set; } = string.Empty;

    [Required, StringLength(128)]
    public string SourceConnection { get; set; } = string.Empty;

    [Required, StringLength(128)]
    public string TargetConnection { get; set; } = string.Empty;

    [Required, StringLength(128)]
    public string DateColumn { get; set; } = string.Empty;

    [Required, StringLength(128)]
    public string PrimaryKeyColumn { get; set; } = string.Empty;

    [Range(1, 120)]
    public int OnlineRetentionMonths { get; set; }

    [Range(0, 120)]
    public int HistoryRetentionMonths { get; set; }

    [Range(1, 200000)]
    public int BatchSize { get; set; }

    public bool CsvEnabled { get; set; }

    [StringLength(400)]
    public string? CsvRootFolder { get; set; }

    public bool Enabled { get; set; } = true;

    [Range(0, 10)]
    public int RetryCount { get; set; }

    [Range(0, 3600)]
    public int RetryDelaySeconds { get; set; }
}
```

### MVC Controller（重點動作）
```csharp
/// <summary>
/// 歸檔任務設定的 MVC 控制器，提供 CRUD 與基本驗證。
/// </summary>
[Route("archive-jobs")]
public class ArchiveJobsController : Controller
{
    private readonly IArchiveJobService _service;

    public ArchiveJobsController(IArchiveJobService service)
    {
        _service = service;
    }

    /// <summary>
    /// 列出所有歸檔任務設定，供後台瀏覽。
    /// </summary>
    [HttpGet]
    public async Task<IActionResult> Index() => View(await _service.ListAsync());

    /// <summary>
    /// 顯示新增頁面，預設帶入空白模型。
    /// </summary>
    [HttpGet("create")]
    public IActionResult Create() => View(new ArchiveJobViewModel());

    /// <summary>
    /// 建立新的歸檔任務；驗證失敗回到表單，成功後導向列表。
    /// </summary>
    [HttpPost("create")]
    public async Task<IActionResult> Create(ArchiveJobViewModel model)
    {
        if (!ModelState.IsValid) return View(model);
        await _service.CreateAsync(model);
        TempData["Success"] = "新增成功";
        return RedirectToAction(nameof(Index));
    }

    /// <summary>
    /// 取得指定任務並展示編輯頁面。
    /// </summary>
    [HttpGet("edit/{id:int}")]
    public async Task<IActionResult> Edit(int id)
    {
        var vm = await _service.FindAsync(id);
        return vm is null ? NotFound() : View(vm);
    }

    /// <summary>
    /// 更新任務設定，若路由 id 與模型不符則回傳 400。
    /// </summary>
    [HttpPost("edit/{id:int}")]
    public async Task<IActionResult> Edit(int id, ArchiveJobViewModel model)
    {
        if (id != model.Id) return BadRequest();
        if (!ModelState.IsValid) return View(model);
        await _service.UpdateAsync(model);
        TempData["Success"] = "儲存成功";
        return RedirectToAction(nameof(Index));
    }

    /// <summary>
    /// 刪除指定任務並回到列表。
    /// </summary>
    [HttpPost("delete/{id:int}")]
    public async Task<IActionResult> Delete(int id)
    {
        await _service.DeleteAsync(id);
        TempData["Success"] = "刪除成功";
        return RedirectToAction(nameof(Index));
    }
}
```

### CRUD 畫面欄位
- JobName（唯一）、TableName、SourceConnection、TargetConnection
- DateColumn、PrimaryKeyColumn
- OnlineRetentionMonths、HistoryRetentionMonths、BatchSize
- CsvEnabled、CsvRootFolder（可空，可覆寫 global root）
- RetryCount、RetryDelaySeconds、Enabled

### 驗證規則
- Required：JobName、TableName、SourceConnection、TargetConnection、DateColumn、PrimaryKeyColumn
- Range：OnlineRetentionMonths > 0、HistoryRetentionMonths ≥ 0、BatchSize > 0、RetryCount ≥ 0、RetryDelaySeconds ≥ 0
- StringLength 防止超長；CsvRootFolder 可空但若填寫需存在於白名單路徑（可加自訂驗證）。

## 4. 設定 Provider 與 Worker 重構

### 介面設計
```csharp
public interface IArchiveSettingsProvider
{
    /// <summary>
    /// 讀取所有啟用的歸檔設定與重試策略。
    /// </summary>
    Task<IReadOnlyCollection<ArchiveJobDefinition>> GetEnabledJobsAsync(CancellationToken ct);
}

public record ArchiveJobDefinition(
    int Id,
    string JobName,
    string TableName,
    string SourceConnection,
    string TargetConnection,
    string DateColumn,
    string PrimaryKeyColumn,
    int OnlineRetentionMonths,
    int HistoryRetentionMonths,
    int BatchSize,
    bool CsvEnabled,
    string? CsvRootFolder,
    RetryPolicyOptions RetryPolicy
);

public record RetryPolicyOptions(int RetryCount, TimeSpan RetryDelay);
```

### Provider 實作（Dapper）
```csharp
/// <summary>
/// 透過 Dapper 由資料庫讀取最新歸檔設定。
/// </summary>
public class DbArchiveSettingsProvider : IArchiveSettingsProvider
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly ILogger<DbArchiveSettingsProvider> _logger;

    public DbArchiveSettingsProvider(
        SqlConnectionFactory connectionFactory,
        ILogger<DbArchiveSettingsProvider> logger)
    {
        _connectionFactory = connectionFactory;
        _logger = logger;
    }

    public async Task<IReadOnlyCollection<ArchiveJobDefinition>> GetEnabledJobsAsync(CancellationToken ct)
    {
        const string sql = @"SELECT j.*, r.RetryCount, r.RetryDelaySeconds
                              FROM dbo.ArchiveJob j
                              LEFT JOIN dbo.ArchiveJobRetryPolicy r ON r.JobId = j.Id
                              WHERE j.Enabled = 1";

        await using var conn = _connectionFactory.CreateAdminConnection();
        var rows = await conn.QueryAsync<dynamic>(sql);

        return rows.Select(r => new ArchiveJobDefinition(
            r.Id,
            r.JobName,
            r.TableName,
            r.SourceConnection,
            r.TargetConnection,
            r.DateColumn,
            r.PrimaryKeyColumn,
            r.OnlineRetentionMonths,
            r.HistoryRetentionMonths,
            r.BatchSize,
            r.CsvEnabled,
            r.CsvRootFolder,
            new RetryPolicyOptions(r.RetryCount ?? 3, TimeSpan.FromSeconds(r.RetryDelaySeconds ?? 5))
        )).ToList();
    }
}
```

> **效能**：一次性抓取全部啟用設定，避免 Worker 每張表重複 round-trip。可加 MemoryCache TTL（例如 5 分鐘）減少 DB 壓力，並在後台儲存後透過訊息或變數失效快取。

### Worker 重構重點
- **啟動時改注入 `IArchiveSettingsProvider`**，改為讀 DB 設定，而非 IOptions。
- **批次搬移流程**：
  1. 依 Job 定義以日期判斷「線上 → 歷史」以及「歷史 → CSV」。
  2. `FetchBatch`：以 `TOP (@BatchSize)`、`ORDER BY DateColumn` 讀資料，僅挑超過保留月數的資料，避免一次鎖大量資料。
  3. `MoveBatch`：
     - **避免 MSDTC**：不使用跨 DB TransactionScope；改為「目標庫 Insert 交易」+「來源庫 Delete 交易」，並以主鍵集合控制。
     - 插入語法含 `WHERE NOT EXISTS` 去重，確保重試 idempotent。
     - 刪除動作用同一批主鍵 `IN (@Ids)`；若刪除失敗，會於下次重試（插入已具冪等特性）。
  4. CSV 匯出：分檔案（例如 50k 筆一檔）寫入，採 UTF-8、含表頭。寫檔成功後再刪除來源歷史庫資料。
- **重試**：使用 `RetryPolicyExecutor` 套用 Job 或全域設定（延遲、次數）。
- **併發**：可選擇 Job 串行或限制並行度（SemaphoreSlim），避免同一張表同時被搬移。

### Worker 程式碼片段（核心流程）
```csharp
/// <summary>
/// 背景排程，定期讀取 DB 設定並執行歸檔。
/// </summary>
public class ArchiveWorker : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromHours(24);
    private readonly IArchiveSettingsProvider _settingsProvider;
    private readonly IArchiveJobExecutor _executor;
    private readonly ILogger<ArchiveWorker> _logger;

    public ArchiveWorker(
        IArchiveSettingsProvider settingsProvider,
        IArchiveJobExecutor executor,
        ILogger<ArchiveWorker> logger)
    {
        _settingsProvider = settingsProvider;
        _executor = executor;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Archive Worker started");
        await RunOnceAsync(stoppingToken);

        using var timer = new PeriodicTimer(Interval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await RunOnceAsync(stoppingToken);
        }
    }

    private async Task RunOnceAsync(CancellationToken ct)
    {
        var jobs = await _settingsProvider.GetEnabledJobsAsync(ct);
        foreach (var job in jobs)
        {
            await _executor.ExecuteAsync(job, ct);
        }
    }
}
```

### Move/Fetch 實作要點
```csharp
/// <summary>
/// 單一表的搬移與 CSV 歷史匯出執行器。
/// </summary>
public class ArchiveJobExecutor : IArchiveJobExecutor
{
    private readonly SqlConnectionFactory _factory;
    private readonly RetryPolicyExecutor _retry;
    private readonly CsvExporter _csv;
    private readonly ILogger<ArchiveJobExecutor> _logger;

    public ArchiveJobExecutor(SqlConnectionFactory factory, RetryPolicyExecutor retry, CsvExporter csv, ILogger<ArchiveJobExecutor> logger)
    {
        _factory = factory;
        _retry = retry;
        _csv = csv;
        _logger = logger;
    }

    public async Task ExecuteAsync(ArchiveJobDefinition job, CancellationToken ct)
    {
        var cutoffOnline = DateTime.UtcNow.AddMonths(-job.OnlineRetentionMonths);
        var cutoffHistory = DateTime.UtcNow.AddMonths(-job.HistoryRetentionMonths);

        await MoveOnlineToHistoryAsync(job, cutoffOnline, ct);
        if (job.CsvEnabled)
        {
            await ExportHistoryToCsvAsync(job, cutoffHistory, ct);
        }
    }

    private async Task MoveOnlineToHistoryAsync(ArchiveJobDefinition job, DateTime cutoff, CancellationToken ct)
    {
        while (true)
        {
            var batch = await FetchBatchAsync(job.SourceConnection, job.TableName, job.DateColumn, cutoff, job.BatchSize, ct);
            if (batch.Count == 0) break;

            await _retry.ExecuteAsync(job.RetryPolicy, async () =>
            {
                await InsertIntoTargetAsync(job.TargetConnection, job.TableName, batch, job.PrimaryKeyColumn, ct);
                await DeleteFromSourceAsync(job.SourceConnection, job.TableName, job.PrimaryKeyColumn, batch, ct);
            }, ct);
        }
    }

    private async Task<IReadOnlyList<IDictionary<string, object>>> FetchBatchAsync(
        string connectionName,
        string table,
        string dateColumn,
        DateTime cutoff,
        int batchSize,
        CancellationToken ct)
    {
        var sql = $@"SELECT TOP (@BatchSize) *
                     FROM {table}
                     WHERE {dateColumn} < @Cutoff
                     ORDER BY {dateColumn}";

        await using var conn = _factory.Create(connectionName);
        var rows = await conn.QueryAsync(sql, new { BatchSize = batchSize, Cutoff = cutoff });
        return rows.Cast<IDictionary<string, object>>().ToList();
    }

    private async Task InsertIntoTargetAsync(
        string connectionName,
        string table,
        IReadOnlyList<IDictionary<string, object>> rows,
        string pkColumn,
        CancellationToken ct)
    {
        if (rows.Count == 0) return;
        var columns = rows[0].Keys.ToArray();
        var columnList = string.Join(",", columns);
        var parameters = string.Join(",", columns.Select(c => "@" + c));

        var sql = $@"INSERT INTO {table} ({columnList})
                     SELECT {parameters}
                     WHERE NOT EXISTS (
                         SELECT 1 FROM {table} WHERE {pkColumn} = @{pkColumn}
                     );";

        await using var conn = _factory.Create(connectionName);
        using var tx = conn.BeginTransaction();
        foreach (var row in rows)
        {
            await conn.ExecuteAsync(sql, row, tx);
        }
        await tx.CommitAsync(ct);
    }

    private async Task DeleteFromSourceAsync(
        string connectionName,
        string table,
        string pkColumn,
        IReadOnlyList<IDictionary<string, object>> rows,
        CancellationToken ct)
    {
        if (rows.Count == 0) return;
        var ids = rows.Select(r => r[pkColumn]).ToArray();
        var sql = $"DELETE FROM {table} WHERE {pkColumn} IN @Ids";

        await using var conn = _factory.Create(connectionName);
        using var tx = conn.BeginTransaction();
        await conn.ExecuteAsync(sql, new { Ids = ids }, tx);
        await tx.CommitAsync(ct);
    }
}
```

> **MSDTC 避免說明**：插入與刪除各自使用不同連線 + 各自交易，並依主鍵集合判斷。若插入成功但刪除失敗，因插入語法包含 NOT EXISTS，重試時不會重複寫入，確保最終一致性。

## 5. 架構圖與流程圖（文字化）

### 5.1 架構圖
```
[Web Admin MVC] --CRUD--> [DB: ArchiveJob + RetryPolicy]
        |                                    ^
        v                                    |
 [API Controller] ----DTO----> [Application: ArchiveJobService] --Dapper--> [ArchiveJob tables]

[Worker Service] --DI--> [IArchiveSettingsProvider] --Dapper--> [ArchiveJob tables]
        |
        v
[ArchiveJobExecutor] --> [SqlConnectionFactory: DB1/DB2] --> [CsvExporter]
        |
        v
   [Logging/Serilog] + [ArchiveJobAudit]
```

### 5.2 執行流程圖
```
後台使用者提交設定
    ↓
WebAdmin Controller -> Service -> Repository
    ↓
資料寫入 ArchiveJob/RetryPolicy 表
    ↓
Worker 定時喚起 -> SettingsProvider 讀取啟用任務
    ↓
對每個 Job：
    FetchBatch (DB1 or History DB)
    ↓
    MoveBatch (Insert DB2 -> Delete DB1)
    ↓
    (若啟用) 匯出 CSV -> 刪除歷史資料
    ↓
    寫入 Audit & Serilog Log
```

### 5.3 類別關聯示意（Class Diagram）
```
ArchiveWorker
    └─ uses IArchiveSettingsProvider
    └─ uses IArchiveJobExecutor

ArchiveJobExecutor
    ├─ uses SqlConnectionFactory
    ├─ uses RetryPolicyExecutor
    └─ uses CsvExporter

DbArchiveSettingsProvider : IArchiveSettingsProvider
    └─ uses SqlConnectionFactory (Admin/Metadata)

ArchiveJobsController
    └─ uses IArchiveJobService

ArchiveJobService
    └─ uses IArchiveJobRepository (Dapper)
    └─ uses IArchiveJobValidator (可選的 metadata 驗證)

CsvExporter
    └─ uses IFileSystem / ICsvWriter abstraction
```

## 6. 演算法與效能分析
- **批次處理**：每批次 O(BatchSize) 讀寫，時間複雜度隨批次線性；避免單次過大交易造成鎖表。空間複雜度 O(BatchSize * 欄位數)。
- **重試策略**：指數回退可替代固定延遲，能快速避開暫時性錯誤；若對效能敏感，可限制最大延遲避免長時間佔用 Worker。
- **SQL 優化**：DateColumn 加索引、PrimaryKeyClustered；批次刪除使用 PK IN 避免全表掃描；可選用分頁式批次 (`OFFSET/FETCH`) 以避免重複掃描。
- **替代方案比較**：
  - `TransactionScope` MSDTC vs. 單庫交易 + 冪等邏輯：後者避免跨機 MSDTC，容忍短暫不一致並可重試。
  - BulkCopy：若批次很大，可用 `SqlBulkCopy` 取代逐筆 Insert，需搭配 staging table 與 PK 去重。
  - Message Queue：若需要更高彈性，可將搬移工作寫入 Queue，由多個 Worker 分流處理。

## 7. 預期風險與防呆
- **設定錯誤（欄位名/表名）**：後台儲存前驗證欄位存在（可在 Service 中對 DB 做 metadata 驗證）。
- **連線權限不足**：連線字串應分權，Worker 需有來源讀 + 目標寫權限；後台需限制存取僅能選用白名單連線名稱。
- **大批次導致鎖表**：控制 BatchSize、加適當索引，必要時採行分區表或夜間時段執行。
- **CSV 路徑安全**：限制可寫入目錄白名單、檢查目錄存在且存取權限正確。
- **時區問題**：統一使用 UTC 儲存日期，顯示時再轉 LocalTime。

## 8. 測試與驗證建議
- **單元測試**：SettingsProvider 使用 InMemory DB/Mock ；ArchiveJobExecutor 以 Fake 連線驗證批次控制、冪等刪除邏輯。
- **整合測試**：利用 `Testcontainers` 啟動兩個 SQL Server 容器，覆蓋「插入成功刪除失敗」的重試場景。
- **負載測試**：以 JMeter/Locust 模擬大量歷史資料，調整 BatchSize/並行度觀察鎖定情況。

## 9. 可重構方向
- 將 CSV 匯出抽象成 `IArchiveSink`，日後可新增 S3/Blob Sink；ArchiveJobDefinition 攜帶 Sink 設定。
- 設定快取 + 事件通知（例如資料表 UpdatedAt timestamp）以降低 Worker 讀 DB 次數。
- 將 Audit 改為事件流（例如 Kafka/Service Bus）以利集中化監控。

## 10. 變更對照
- **從 IOptions → IArchiveSettingsProvider**：設定來源改為 DB，可熱更新。
- **新增 WebAdmin**：使用者可自行 CRUD；設定寫回 DB。
- **Worker 流程冪等化**：分離兩庫交易，避免 MSDTC，同時確保重試不重複寫入。

